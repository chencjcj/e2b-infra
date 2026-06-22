package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

// PickMigrationDestination runs Sandbox.Create's placement algorithm with
// the source excluded. Empty buildMachineInfo skips CPU-compat filtering.
func (o *Orchestrator) PickMigrationDestination(
	ctx context.Context,
	sbx sandbox.Sandbox,
	srcNodeID string,
	buildMachineInfo machineinfo.MachineInfo,
	requiredLabels []string,
	filterByLabels bool,
) (*nodemanager.Node, error) {
	clusterID := clusters.WithClusterFallback(&sbx.ClusterID)
	clusterNodes := o.GetClusterNodes(clusterID)
	if len(clusterNodes) == 0 {
		return nil, errors.New("no cluster nodes available")
	}
	excluded := map[string]struct{}{srcNodeID: {}}

	node, err := placement.PickNode(
		ctx,
		o.placementAlgorithm,
		clusterNodes,
		excluded,
		nodemanager.SandboxResources{CPUs: sbx.VCpu, MiBMemory: sbx.RamMB},
		buildMachineInfo,
		filterByLabels,
		requiredLabels,
	)
	if err != nil {
		return nil, fmt.Errorf("pick migration destination: %w", err)
	}
	return node, nil
}

// MoveSandboxToNode rewrites the sandboxStore routing for sandboxID after
// migration so subsequent RPCs reach the new node. It also re-publishes the
// sandbox to the routing catalog so client-proxy resolves to the new node IP
// (without this update the catalog stays pinned to the source node and SDK
// connects fail with SandboxNotFound after migration completes).
func (o *Orchestrator) MoveSandboxToNode(
	ctx context.Context,
	teamID uuid.UUID,
	sandboxID string,
	newNodeID string,
	newClientID string,
) error {
	updated, err := o.sandboxStore.Update(ctx, teamID, sandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
		s.NodeID = newNodeID
		if newClientID != "" {
			s.ClientID = newClientID
		}
		return s, nil
	})
	if err != nil {
		return fmt.Errorf("update sandbox node mapping: %w", err)
	}
	logger.L().Info(ctx, "MoveSandboxToNode: sandboxStore updated",
		zap.String("sandbox_id", sandboxID),
		zap.String("new_node_id", newNodeID),
		zap.String("updated_node_id", updated.NodeID))

	// Re-write the routing catalog so client-proxy / edge layers route to the
	// new orchestrator. addSandboxToRoutingTable looks up the node via
	// GetNode(NodeID) and writes the catalog entry with that node's IP.
	o.addSandboxToRoutingTable(ctx, updated)
	logger.L().Info(ctx, "MoveSandboxToNode: routing catalog refreshed",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", updated.NodeID))

	return nil
}
