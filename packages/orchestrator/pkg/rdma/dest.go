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
	"sync/atomic"
	"syscall"
)

type Dest struct {
	cmd *exec.Cmd

	mu             sync.Mutex
	rtsSeen        bool
	prefetchDone   bool
	doneSeen       bool
	faultsHandled  uint64
	progressTotal  uint64
	progressLatest uint64
	stderrBuf      strings.Builder

	rtsCh    chan struct{}
	doneCh   chan struct{}
	exitCh   chan error
	progress atomic.Uint64
}

type SourceEndpoint struct {
	Addr     string
	TCPPort  int
	QPInfoHx string
}

// StartDest spawns rdma-dest with uffd as fd 3 and dest's page-pool memfd as
// fd 4. fcBaseVA is FC's mmap base (UFFD VAs lie in [base, base+sizeBytes)),
// needed to convert fault VAs to MR offsets.
func StartDest(
	ctx context.Context, cfg Config,
	sandboxID string,
	uffd *os.File,
	memfd *os.File,
	sizeBytes uint64,
	fcBaseVA uint64,
	src SourceEndpoint,
) (*Dest, error) {
	if uffd == nil {
		return nil, errors.New("uffd is nil")
	}
	if memfd == nil {
		return nil, errors.New("memfd is nil")
	}
	if cfg.DestBinary == "" {
		return nil, errors.New("rdma dest binary not configured")
	}
	if src.Addr == "" || src.TCPPort == 0 || src.QPInfoHx == "" {
		return nil, errors.New("incomplete source endpoint")
	}

	args := []string{
		"--uffd-fd", "3",
		"--memfd-fd", "4",
		"--size", strconv.FormatUint(sizeBytes, 10),
		"--fc-base-va", fmt.Sprintf("0x%x", fcBaseVA),
		"--src-addr", src.Addr,
		"--src-port", strconv.Itoa(src.TCPPort),
		"--src-qp", src.QPInfoHx,
		"--gid-idx", strconv.Itoa(int(cfg.GIDIndex)),
		"--port-num", strconv.Itoa(int(cfg.HCAPort)),
	}
	if cfg.Device != "" {
		args = append(args, "--dev", cfg.Device)
	}

	cmd := exec.CommandContext(ctx, cfg.DestBinary, args...)
	cmd.ExtraFiles = []*os.File{uffd, memfd}
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
		return nil, fmt.Errorf("start rdma-dest for %s: %w", sandboxID, err)
	}

	d := &Dest{
		cmd:    cmd,
		rtsCh:  make(chan struct{}),
		doneCh: make(chan struct{}),
		exitCh: make(chan error, 1),
	}
	go d.parseStdout(stdout)
	go d.drainStderr(stderr)
	go func() { d.exitCh <- cmd.Wait() }()

	return d, nil
}

func (d *Dest) parseStdout(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "QP_RTS":
			d.mu.Lock()
			d.rtsSeen = true
			d.mu.Unlock()
			close(d.rtsCh)
		case strings.HasPrefix(line, "PREFETCH_PROGRESS: "):
			rest := strings.TrimPrefix(line, "PREFETCH_PROGRESS: ")
			parts := strings.SplitN(rest, "/", 2)
			if len(parts) == 2 {
				cur, _ := strconv.ParseUint(parts[0], 10, 64)
				tot, _ := strconv.ParseUint(parts[1], 10, 64)
				d.mu.Lock()
				d.progressLatest = cur
				d.progressTotal = tot
				d.mu.Unlock()
			}
		case line == "PREFETCH_DONE":
			d.mu.Lock()
			d.prefetchDone = true
			d.mu.Unlock()
		case strings.HasPrefix(line, "FAULTS_HANDLED: "):
			n, _ := strconv.ParseUint(strings.TrimPrefix(line, "FAULTS_HANDLED: "), 10, 64)
			d.mu.Lock()
			d.faultsHandled = n
			d.mu.Unlock()
		case line == "DONE":
			d.mu.Lock()
			d.doneSeen = true
			d.mu.Unlock()
			close(d.doneCh)
		}
	}
}

func (d *Dest) drainStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintf(os.Stderr, "rdma-dest: %s\n", line)
		d.mu.Lock()
		d.stderrBuf.WriteString(line)
		d.stderrBuf.WriteByte('\n')
		d.mu.Unlock()
	}
}

func (d *Dest) WaitRTS(ctx context.Context) error {
	select {
	case <-d.rtsCh:
		return nil
	case err := <-d.exitCh:
		return fmt.Errorf("rdma-dest exited before RTS: %w (stderr: %s)", err, d.stderr())
	case <-ctx.Done():
		return fmt.Errorf("rdma-dest not RTS before ctx done: %w (stderr so far: %s)", ctx.Err(), d.stderr())
	}
}

func (d *Dest) Progress() (done, total uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.progressLatest, d.progressTotal
}

func (d *Dest) WaitDone(ctx context.Context) (faultsHandled uint64, err error) {
	select {
	case err := <-d.exitCh:
		d.mu.Lock()
		ok := d.prefetchDone && d.doneSeen
		fh := d.faultsHandled
		stderr := d.stderr()
		d.mu.Unlock()
		if err != nil {
			return fh, fmt.Errorf("rdma-dest exited with %w (stderr: %s)", err, stderr)
		}
		if !ok {
			return fh, fmt.Errorf("rdma-dest exited 0 but did not finish prefetch (stderr: %s)", stderr)
		}
		return fh, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (d *Dest) Stop(ctx context.Context) error {
	if d.cmd.Process != nil {
		_ = d.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-d.exitCh:
		return nil
	case <-ctx.Done():
		_ = d.cmd.Process.Kill()
		<-d.exitCh
		return ctx.Err()
	}
}

func (d *Dest) stderr() string {
	const max = 4096
	out := d.stderrBuf.String()
	if len(out) > max {
		return "..." + out[len(out)-max:]
	}
	return out
}
