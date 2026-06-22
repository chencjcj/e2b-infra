package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (s *Server) waitForAcquire(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, acquireTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "wait-for-acquire")
	defer span.End()

	err := s.startingSandboxes.Acquire(ctx, 1)
	if err != nil {
		telemetry.ReportEvent(ctx, "too many resuming sandboxes on node")

		return status.Errorf(codes.ResourceExhausted, "too many sandboxes resuming on this node, please retry")
	}

	return nil
}

func (s *Server) waitForPauseAcquire(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pauseAcquireTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "wait-for-pause-acquire")
	defer span.End()

	_ = s.pausingSandboxes.SetLimit(int64(s.featureFlags.IntFlag(ctx, featureflags.OrchestratorMaxConcurrentPauses)))

	err := s.pausingSandboxes.Acquire(ctx, 1)
	if err != nil {
		telemetry.ReportEvent(ctx, "too many pausing sandboxes on node")

		return status.Errorf(codes.ResourceExhausted, "too many sandboxes pausing on this node, please retry")
	}

	return nil
}
