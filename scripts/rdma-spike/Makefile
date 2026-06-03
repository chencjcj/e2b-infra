# Phase 0 spike — validate libibverbs + hugepage memfd MR registration
# + RDMA Read latency/throughput on this hardware.
#
# Build prerequisites (Debian/Ubuntu):
#   apt install libibverbs-dev rdma-core ibverbs-utils
# Build prerequisites (RHEL/CentOS):
#   yum install libibverbs-devel rdma-core-devel
#
# Hugepage prerequisite (BOTH nodes):
#   echo 4096 | sudo tee /proc/sys/vm/nr_hugepages   # 8 GB of 2 MB pages
#   grep HugePages /proc/meminfo
#
# Build:
#   make
#
# Run (use the same -size on both sides):
#   On source node (e.g. 10.253.238.8):
#     ./rdma-server -size 4G -port 12345 -dev mlx5_bond_0 -gid-idx 3
#
#   On dest node:
#     ./rdma-client -addr 10.253.238.8 -port 12345 -size 4G \\
#                   -dev mlx5_bond_0 -gid-idx 3
#
# Pick the right -gid-idx with `show_gids` (from ibverbs-utils) — for RoCE v2
# it's the entry whose RoCE version is "v2". Common values: 1 or 3.

CFLAGS  = -O2 -Wall -Wextra -g -D_GNU_SOURCE
LDLIBS  = -libverbs -lpthread

all: rdma-server rdma-client migration-dest

rdma-server: server.c exchange.h
	$(CC) $(CFLAGS) -o $@ server.c $(LDLIBS)

rdma-client: client.c exchange.h
	$(CC) $(CFLAGS) -o $@ client.c $(LDLIBS)

# Phase 1 post-copy demo: connects to a running rdma-server and uses UFFD
# to bring pages on demand via RDMA Read.
migration-dest: migration-dest.c exchange.h
	$(CC) $(CFLAGS) -o $@ migration-dest.c $(LDLIBS)

clean:
	rm -f rdma-server rdma-client migration-dest

.PHONY: all clean
