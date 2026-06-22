package rdma

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type QPInfo struct {
	TCPPort int
	Hex     string
}

type Source struct {
	cmd *exec.Cmd

	mu        sync.Mutex
	tcpPort   int
	qpHex     string
	peerSeen  bool
	rtsSeen   bool
	doneSeen  bool
	stderrBuf strings.Builder

	readyCh chan struct{}
	doneCh  chan struct{}
	exitCh  chan error
}

// StartSource spawns rdma-source with memfd as fd 3. Caller must keep memfd
// open until Source has exited.
func StartSource(ctx context.Context, cfg Config, sandboxID string, memfd *os.File, sizeBytes uint64) (*Source, error) {
	if memfd == nil {
		return nil, errors.New("memfd is nil")
	}
	if cfg.SourceBinary == "" {
		return nil, errors.New("rdma source binary not configured")
	}

	args := []string{
		"--memfd-fd", "3",
		"--size", strconv.FormatUint(sizeBytes, 10),
		"--gid-idx", strconv.Itoa(int(cfg.GIDIndex)),
		"--port-num", strconv.Itoa(int(cfg.HCAPort)),
	}
	if cfg.Device != "" {
		args = append(args, "--dev", cfg.Device)
	}

	cmd := exec.CommandContext(ctx, cfg.SourceBinary, args...)
	cmd.ExtraFiles = []*os.File{memfd}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start rdma-source for %s: %w", sandboxID, err)
	}

	s := &Source{
		cmd:     cmd,
		readyCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
		exitCh:  make(chan error, 1),
	}

	go s.parseStdout(stdout)
	go s.drainStderr(stderr)
	go func() { s.exitCh <- cmd.Wait() }()

	return s, nil
}

func (s *Source) parseStdout(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "TCP_PORT: "):
			port, err := strconv.Atoi(strings.TrimPrefix(line, "TCP_PORT: "))
			if err == nil {
				s.mu.Lock()
				s.tcpPort = port
				s.maybeSignalReady()
				s.mu.Unlock()
			}
		case strings.HasPrefix(line, "QP_INFO: "):
			s.mu.Lock()
			s.qpHex = strings.TrimPrefix(line, "QP_INFO: ")
			s.maybeSignalReady()
			s.mu.Unlock()
		case line == "PEER_CONNECTED":
			s.mu.Lock()
			s.peerSeen = true
			s.mu.Unlock()
		case line == "QP_RTS":
			s.mu.Lock()
			s.rtsSeen = true
			s.mu.Unlock()
		case line == "DONE":
			s.mu.Lock()
			s.doneSeen = true
			s.mu.Unlock()
			close(s.doneCh)
		}
	}
}

func (s *Source) maybeSignalReady() {
	if s.tcpPort != 0 && s.qpHex != "" {
		select {
		case <-s.readyCh:
		default:
			close(s.readyCh)
		}
	}
}

func (s *Source) drainStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintf(os.Stderr, "rdma-source: %s\n", line)
		s.mu.Lock()
		s.stderrBuf.WriteString(line)
		s.stderrBuf.WriteByte('\n')
		s.mu.Unlock()
	}
}

func (s *Source) WaitReady(ctx context.Context) (QPInfo, error) {
	select {
	case <-s.readyCh:
		s.mu.Lock()
		defer s.mu.Unlock()
		return QPInfo{TCPPort: s.tcpPort, Hex: s.qpHex}, nil
	case err := <-s.exitCh:
		return QPInfo{}, fmt.Errorf("rdma-source exited before ready: %w (stderr: %s)", err, s.stderr())
	case <-ctx.Done():
		return QPInfo{}, fmt.Errorf("rdma-source not ready before ctx done: %w (stderr so far: %s)", ctx.Err(), s.stderr())
	}
}

func (s *Source) WaitDone(ctx context.Context) error {
	select {
	case err := <-s.exitCh:
		s.mu.Lock()
		done := s.doneSeen
		stderr := s.stderr()
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("rdma-source exited with %w (stderr: %s)", err, stderr)
		}
		if !done {
			return fmt.Errorf("rdma-source exited 0 but did not print DONE (stderr: %s)", stderr)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) Stop(ctx context.Context) error {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-s.exitCh:
		return nil
	case <-ctx.Done():
		_ = s.cmd.Process.Kill()
		<-s.exitCh
		return ctx.Err()
	}
}

func (s *Source) stderr() string {
	const max = 4096
	out := s.stderrBuf.String()
	if len(out) > max {
		return "..." + out[len(out)-max:]
	}
	return out
}
