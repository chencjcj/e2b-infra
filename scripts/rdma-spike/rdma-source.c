// rdma-source — production migration source agent.
//
// Spawned by the orchestrator when sandbox X is being migrated OUT.
// Receives the sandbox's memfd FD via ExtraFiles (becomes fd 3 in child),
// registers it as an RDMA Memory Region, listens on a TCP port for the
// destination orchestrator to connect, and serves one-sided RDMA Reads
// passively until the destination signals migration complete.
//
// The orchestrator parses stdout for these markers:
//   TCP_PORT: <n>          ← random port chosen, send this to dest
//   QP_INFO: <hex blob>    ← qp_info struct, hex-encoded; send to dest
//   PEER_CONNECTED         ← dest connected
//   QP_RTS                 ← QP fully established
//   DONE                   ← dest signaled completion; about to exit cleanly
//
// All errors go to stderr. Process exit code: 0 on clean DONE, nonzero on
// any failure — orchestrator should treat nonzero as migration failed.
//
// Args:
//   --memfd-fd N    : preopened FD to the sandbox memfd (typically 3)
//   --size N        : buffer size, must match dest
//   --dev DEV       : ibv device name
//   --gid-idx N     : RoCE v2 GID index
//   --port-num N    : HCA port (default 1)
//   --tcp-port N    : TCP port to listen on (default 0 = random)
//   --tcp-bind ADDR : interface to bind (default 0.0.0.0)

#include <getopt.h>
#include <signal.h>
#include "exchange.h"

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --memfd-fd N --size N[K|M|G] [--dev DEV] [--gid-idx N] "
        "[--port-num N] [--tcp-port N] [--tcp-bind ADDR]\n", prog);
}

static void print_qp_info_hex(const struct qp_info *info) {
    printf("QP_INFO: ");
    const uint8_t *p = (const uint8_t *)info;
    for (size_t i = 0; i < sizeof(*info); i++) printf("%02x", p[i]);
    printf("\n");
    fflush(stdout);
}

int main(int argc, char **argv) {
    int      memfd_fd = -1;
    uint64_t size = 0;
    const char *dev_name = NULL;
    uint8_t  gid_index = 3;
    uint8_t  port_num = 1;
    int      tcp_port = 0;
    const char *bind_addr = NULL;

    static struct option opts[] = {
        {"memfd-fd",  required_argument, 0, 'f'},
        {"size",      required_argument, 0, 's'},
        {"dev",       required_argument, 0, 'd'},
        {"gid-idx",   required_argument, 0, 'g'},
        {"port-num",  required_argument, 0, 'n'},
        {"tcp-port",  required_argument, 0, 'p'},
        {"tcp-bind",  required_argument, 0, 'b'},
        {0, 0, 0, 0},
    };
    int c;
    while ((c = getopt_long_only(argc, argv, "f:s:d:g:n:p:b:", opts, NULL)) != -1) {
        switch (c) {
        case 'f': memfd_fd = atoi(optarg); break;
        case 's': size = parse_size(optarg); break;
        case 'd': dev_name = optarg; break;
        case 'g': gid_index = (uint8_t)atoi(optarg); break;
        case 'n': port_num = (uint8_t)atoi(optarg); break;
        case 'p': tcp_port = atoi(optarg); break;
        case 'b': bind_addr = optarg; break;
        default: usage(argv[0]); return 1;
        }
    }
    if (memfd_fd < 0 || size == 0) { usage(argv[0]); return 1; }
    (void)bind_addr;  // future: bind specific iface; for now INADDR_ANY

    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));

    // 1. mmap caller's memfd
    // MAP_NORESERVE: page pool itself maps with NORESERVE so hugepages are
    // reserved lazily on actual fault. We must mirror that — without it the
    // kernel tries to reserve `size`/2MB hugepages here and ENOMEMs when the
    // pool is partially populated.
    void *buf = mmap(NULL, size, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_NORESERVE | MAP_HUGETLB | (21 << MAP_HUGE_SHIFT),
                     memfd_fd, 0);
    if (buf == MAP_FAILED) {
        // Caller's memfd may not have been created with MFD_HUGETLB; retry plain.
        buf = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_NORESERVE, memfd_fd, 0);
        if (buf == MAP_FAILED) { perror("mmap memfd"); return 1; }
    }
    fprintf(stderr, "[+] mapped memfd fd=%d at %p (%.2f GB)\n",
            memfd_fd, buf, (double)size / (1024.0*1024.0*1024.0));

    // 2. RDMA setup
    struct ibv_context *ibv = open_device(dev_name);
    if (!ibv) return 1;
    struct ibv_pd *pd = ibv_alloc_pd(ibv); if (!pd) { perror("alloc_pd"); return 1; }
    struct ibv_cq *cq = ibv_create_cq(ibv, 16, NULL, NULL, 0);
    if (!cq) { perror("create_cq"); return 1; }

    struct ibv_qp_init_attr qpa = {
        .send_cq = cq, .recv_cq = cq,
        .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC,
    };
    struct ibv_qp *qp = ibv_create_qp(pd, &qpa);
    if (!qp) { perror("create_qp"); return 1; }

    struct ibv_mr *mr = ibv_reg_mr(pd, buf, size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) { perror("reg_mr"); return 1; }
    fprintf(stderr, "[+] MR registered: rkey=0x%x lkey=0x%x\n", mr->rkey, mr->lkey);

    // 3. TCP listen on requested or random port
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons((uint16_t)tcp_port),
    };
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(listen_fd, 1) < 0) { perror("listen"); return 1; }
    socklen_t slen = sizeof(addr);
    getsockname(listen_fd, (struct sockaddr*)&addr, &slen);
    int chosen_port = ntohs(addr.sin_port);

    // 4. Build local qp_info, announce to caller via stdout
    struct ibv_port_attr pattr; ibv_query_port(ibv, port_num, &pattr);
    union ibv_gid gid; ibv_query_gid(ibv, port_num, gid_index, &gid);
    struct qp_info local = {
        .lid  = pattr.lid,
        .qpn  = qp->qp_num,
        .psn  = (uint32_t)rand() & 0xffffff,
        .addr = (uint64_t)(uintptr_t)mr->addr,
        .rkey = mr->rkey,
        .size = size,
    };
    memcpy(local.gid, &gid, 16);

    printf("TCP_PORT: %d\n", chosen_port);
    print_qp_info_hex(&local);

    // 5. Wait for dest to connect, exchange QP
    fprintf(stderr, "[+] waiting for peer on TCP :%d\n", chosen_port);
    int conn_fd = accept(listen_fd, NULL, NULL);
    if (conn_fd < 0) { perror("accept"); return 1; }
    printf("PEER_CONNECTED\n"); fflush(stdout);
    close(listen_fd);

    if (write_full(conn_fd, &local, sizeof(local)) < 0) { perror("send qp"); return 1; }
    struct qp_info remote;
    if (read_full(conn_fd, &remote, sizeof(remote)) < 0) { perror("recv qp"); return 1; }

    // 6. Transition QP to RTS
    if (qp_to_rts(qp, &remote, port_num, gid_index, local.psn) < 0) return 1;
    printf("QP_RTS\n"); fflush(stdout);

    // 7. Block until dest sends "done" — single byte, anything will do.
    char done;
    int rc = (int)read_full(conn_fd, &done, 1);
    if (rc < 0) {
        fprintf(stderr, "[!] peer disconnected without DONE\n");
        return 2;
    }

    printf("DONE\n"); fflush(stdout);

    // 8. cleanup
    ibv_destroy_qp(qp);
    ibv_destroy_cq(cq);
    ibv_dereg_mr(mr);
    ibv_dealloc_pd(pd);
    ibv_close_device(ibv);
    munmap(buf, size);
    close(conn_fd);
    close(memfd_fd);

    return 0;
}
