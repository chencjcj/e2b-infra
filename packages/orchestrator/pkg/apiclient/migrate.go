// Package apiclient is a tiny HTTP client orchestrator uses to call back
// into the API for the OOM rescue / live-migration paths.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Client struct {
	baseURL    string
	adminToken string
	httpClient *http.Client
}

// New returns nil when either parameter is empty (migration disabled).
func New(baseURL, adminToken string) *Client {
	if baseURL == "" || adminToken == "" {
		return nil
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		adminToken: adminToken,
		httpClient: &http.Client{},
	}
}

type MigrateSandboxRequest struct {
	TeamID     uuid.UUID `json:"teamID"`
	SandboxID  string    `json:"sandboxID"`
	DestNodeID *string   `json:"destNodeID,omitempty"`
}

type MigrateSandboxResponse struct {
	SourceNodeID      string `json:"sourceNodeID"`
	DestNodeID        string `json:"destNodeID"`
	DurationMs        int    `json:"durationMs"`
	PrefetchPagesDone *int64 `json:"prefetchPagesDone,omitempty"`
	FaultsHandled     *int64 `json:"faultsHandled,omitempty"`
}

type apiError struct {
	Code    int
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("api %d: %s", e.Code, e.Message)
}

func (c *Client) MigrateSandbox(ctx context.Context, req MigrateSandboxRequest) (*MigrateSandboxResponse, error) {
	if c == nil {
		return nil, errors.New("apiclient.Client is nil (migration disabled)")
	}
	var out MigrateSandboxResponse
	if err := c.postJSON(ctx, "/admin/sandboxes/migrate", req, &out, 60*time.Second); err != nil {
		return nil, err
	}
	return &out, nil
}

type RegisterOOMSnapshotRequest struct {
	BuildID            uuid.UUID `json:"buildID"`
	SandboxID          string    `json:"sandboxID"`
	TeamID             uuid.UUID `json:"teamID"`
	OriginNodeID       string    `json:"originNodeID"`
	Vcpu               int64     `json:"vcpu"`
	RamMB              int64     `json:"ramMB"`
	TotalDiskSizeMB    int64     `json:"totalDiskSizeMB"`
	KernelVersion      string    `json:"kernelVersion"`
	FirecrackerVersion string    `json:"firecrackerVersion"`
	EnvdVersion        string    `json:"envdVersion"`
	Tag                *string   `json:"tag,omitempty"`
}

type RegisterOOMSnapshotResponse struct {
	SnapshotID string    `json:"snapshotID"`
	TemplateID string    `json:"templateID"`
	BuildID    uuid.UUID `json:"buildID"`
}

func (c *Client) RegisterOOMSnapshot(ctx context.Context, req RegisterOOMSnapshotRequest) (*RegisterOOMSnapshotResponse, error) {
	if c == nil {
		return nil, errors.New("apiclient.Client is nil (registration disabled)")
	}
	var out RegisterOOMSnapshotResponse
	if err := c.postJSON(ctx, "/admin/snapshot-templates/from-oom", req, &out, 10*time.Second); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) postJSON(ctx context.Context, path string, reqBody, respBody any, defaultTimeout time.Duration) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Admin-Token", c.adminToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &apiError{Code: resp.StatusCode, Message: string(raw)}
	}
	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}
