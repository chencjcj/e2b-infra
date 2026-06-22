package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

// PostAdminSandboxesMigrate orchestrates Prepare → Receive → Commit (or
// Abort on any failure). Long-running per migration (~seconds for GBs).
func (a *APIStore) PostAdminSandboxesMigrate(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "admin-migrate-sandbox")
	defer span.End()

	body, err := ginutils.ParseBody[api.PostAdminSandboxesMigrateJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("parse body: %s", err))
		return
	}

	sbx, err := a.orchestrator.GetSandbox(ctx, body.TeamID, body.SandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("sandbox not found: %s", err))
		return
	}

	clusterID := clusters.WithClusterFallback(&sbx.ClusterID)
	srcNode := a.orchestrator.GetNode(clusterID, sbx.NodeID)
	if srcNode == nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "source node not registered")
		return
	}

	var destNode *nodemanager.Node
	if body.DestNodeID != nil && *body.DestNodeID != "" {
		destNode = a.orchestrator.GetNode(clusterID, *body.DestNodeID)
		if destNode == nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("destNodeID %q not found", *body.DestNodeID))
			return
		}
		if destNode.ID == srcNode.ID {
			a.sendAPIStoreError(c, http.StatusBadRequest, "destNodeID equals source")
			return
		}
	} else {
		// Empty MachineInfo → all nodes considered CPU-compatible. Fetching
		// the build to populate it is a follow-up.
		picked, err := a.orchestrator.PickMigrationDestination(ctx, sbx, srcNode.ID, machineinfo.MachineInfo{}, nil, false)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusServiceUnavailable, fmt.Sprintf("no eligible destination: %s", err))
			return
		}
		destNode = picked
	}

	logger.L().Info(ctx, "starting RDMA migration",
		logger.WithSandboxID(body.SandboxID),
		zap.String("source_node", srcNode.ID),
		zap.String("dest_node", destNode.ID),
	)

	t0 := time.Now()
	srcClient, srcCtx := srcNode.GetClient(ctx)

	prep, err := srcClient.Sandbox.PrepareMigrationSource(srcCtx, &orchestrator.MigrationPrepareSourceRequest{
		SandboxId: body.SandboxID,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("prepare source: %s", err))
		return
	}
	if prep.RdmaAddr == "" {
		prep.RdmaAddr = srcNode.IPAddress
	}

	destClient, destCtx := destNode.GetClient(ctx)
	recvResp, recvErr := destClient.Sandbox.ReceiveMigration(destCtx, &orchestrator.MigrationReceiveRequest{
		Source:    prep,
		StartTime: timestamppb.New(time.Now()),
		EndTime:   timestamppb.New(time.Now().Add(24 * time.Hour)),
	})

	if recvErr != nil {
		abortCtx, abortCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer abortCancel()
		_, abortErr := srcClient.Sandbox.AbortMigration(abortCtx, &orchestrator.MigrationAbortRequest{
			SandboxId: body.SandboxID,
			Reason:    fmt.Sprintf("dest recv failed: %s", recvErr),
		})
		if abortErr != nil {
			logger.L().Error(ctx, "abort source after recv failure failed",
				logger.WithSandboxID(body.SandboxID), zap.Error(abortErr))
		}
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("dest receive: %s", recvErr))
		return
	}

	// Update sandboxStore routing FIRST — before src kills FC. This way
	// API's reconciliation loop, when it sees src's sandbox disappear from
	// ServiceInfo (after Commit's SIGKILL), already knows to query dest.
	if err := a.orchestrator.MoveSandboxToNode(ctx, body.TeamID, body.SandboxID, destNode.ID, recvResp.GetClientId()); err != nil {
		logger.L().Error(ctx, "update sandbox node mapping failed; aborting before commit",
			logger.WithSandboxID(body.SandboxID), zap.Error(err))
		// Don't commit — sandbox would be killed on src but routing still points there.
		abortCtx, abortCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer abortCancel()
		_, _ = srcClient.Sandbox.AbortMigration(abortCtx, &orchestrator.MigrationAbortRequest{
			SandboxId: body.SandboxID,
			Reason:    fmt.Sprintf("MoveSandboxToNode failed: %s", err),
		})
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("update routing: %s", err))
		return
	}

	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer commitCancel()
	_, commitErr := srcClient.Sandbox.CommitMigration(commitCtx, &orchestrator.MigrationCommitRequest{
		SandboxId: body.SandboxID,
	})
	if commitErr != nil {
		// Dest is already serving + routing updated; source FC may be orphaned.
		logger.L().Error(ctx, "commit source failed AFTER dest receive + routing succeeded",
			logger.WithSandboxID(body.SandboxID), zap.Error(commitErr))
	}

	durationMs := int(time.Since(t0).Milliseconds())
	pagesDone := int(recvResp.GetPrefetchPagesDone())
	faultsHandled := int(recvResp.GetFaultsHandled())

	logger.L().Info(ctx, "RDMA migration complete",
		logger.WithSandboxID(body.SandboxID),
		zap.String("source_node", srcNode.ID),
		zap.String("dest_node", destNode.ID),
		zap.Int("duration_ms", durationMs),
		zap.Int("pages_done", pagesDone),
		zap.Int("faults_handled", faultsHandled),
	)

	c.JSON(http.StatusOK, api.AdminMigrateSandboxResult{
		SourceNodeID:      srcNode.ID,
		DestNodeID:        destNode.ID,
		DurationMs:        durationMs,
		PrefetchPagesDone: int64Ptr(int64(pagesDone)),
		FaultsHandled:     int64Ptr(int64(faultsHandled)),
	})
}

func int64Ptr(v int64) *int64 { return &v }
