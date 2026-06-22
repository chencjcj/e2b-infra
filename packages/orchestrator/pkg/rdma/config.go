// Package rdma drives the per-migration rdma-source / rdma-dest child
// processes built from scripts/rdma-spike/.
package rdma

import (
	"errors"
	"fmt"
	"os"
)

type Config struct {
	SourceBinary string `env:"RDMA_SOURCE_BIN" envDefault:"/opt/e2b/rdma-source"`
	DestBinary   string `env:"RDMA_DEST_BIN"   envDefault:"/opt/e2b/rdma-dest"`
	Device       string `env:"RDMA_DEVICE"`
	GIDIndex     uint8  `env:"RDMA_GID_INDEX"  envDefault:"3"`
	HCAPort      uint8  `env:"RDMA_HCA_PORT"   envDefault:"1"`
}

func (c Config) Validate() error {
	for _, p := range []struct{ label, path string }{
		{"RDMA_SOURCE_BIN", c.SourceBinary},
		{"RDMA_DEST_BIN", c.DestBinary},
	} {
		st, err := os.Stat(p.path)
		if err != nil {
			return fmt.Errorf("%s=%q: %w", p.label, p.path, err)
		}
		if st.Mode()&0o100 == 0 {
			return fmt.Errorf("%s=%q is not executable", p.label, p.path)
		}
	}
	if c.GIDIndex == 0 {
		// Index 0 is link-local v1; RoCE v2 inter-node always wants a higher index.
		return errors.New("RDMA_GID_INDEX=0 is unusual; set explicitly")
	}
	return nil
}
