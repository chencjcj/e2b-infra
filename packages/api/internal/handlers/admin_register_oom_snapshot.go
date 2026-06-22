package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// PostAdminSnapshotTemplatesFromOom writes the DB rows that make a
// GCS-resident OOM rescue build resolvable by Sandbox.create("<id>:<tag>").
func (a *APIStore) PostAdminSnapshotTemplatesFromOom(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.PostAdminSnapshotTemplatesFromOomJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("parse body: %s", err))
		return
	}
	if body.SandboxID == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "sandboxID is required")
		return
	}

	templateID := id.Generate()
	tag := sharedUtils.DerefOrDefault(body.Tag, "default")

	originNodeID := body.OriginNodeID
	buildID := body.BuildID

	_, err = a.sqlcDB.RegisterOOMSnapshotTemplate(ctx, queries.RegisterOOMSnapshotTemplateParams{
		TemplateID:         templateID,
		TeamID:             body.TeamID,
		SandboxID:          body.SandboxID,
		OriginNodeID:       &originNodeID,
		BuildID:            &buildID,
		Tag:                tag,
		Vcpu:               body.Vcpu,
		RamMb:              body.RamMB,
		FreeDiskSizeMb:     0,
		TotalDiskSizeMb:    &body.TotalDiskSizeMB,
		KernelVersion:      body.KernelVersion,
		FirecrackerVersion: body.FirecrackerVersion,
		EnvdVersion:        &body.EnvdVersion,
	})
	if err != nil {
		logger.L().Error(ctx, "register OOM snapshot template failed",
			logger.WithSandboxID(body.SandboxID), zap.String("build_id", body.BuildID.String()), zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("register: %s", err))
		return
	}

	snapshotID := fmt.Sprintf("%s:%s", templateID, tag)
	logger.L().Info(ctx, "registered OOM snapshot template",
		logger.WithSandboxID(body.SandboxID),
		logger.WithBuildID(body.BuildID.String()),
		logger.WithTemplateID(templateID),
		zap.String("snapshot_id", snapshotID),
	)

	c.JSON(http.StatusOK, api.OOMSnapshotRegistrationResult{
		SnapshotID: snapshotID,
		TemplateID: templateID,
		BuildID:    body.BuildID,
	})
}
