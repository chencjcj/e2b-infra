// rdma-server — source side of the spike. Allocates a hugepage memfd,
// fills it with a verifiable pattern, registers it as an RDMA Memory
// Region, and waits while the client pulls data via one-sided RDMA Read.
// No Send/Recv path: server is purely passive after the QP handshake.
//
// Verifying pattern: each 8-byte word at offset O contains the value O,
// so the client can validate every page after the read completes.

#include <getopt.h>
#include <signal.h>
#include <time.h>

#include "exchange.h"

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s -size N[K|M|G] -port P [-dev DEV] [-gid-idx N] [-port-num N] [-no-hugepage]\n"
        "  -size       buffer size, e.g. 4G\n"
        "  -port       TCP port for QP-info exchange\n"
        "  -dev        ibv device name (default: first found)\n"
        "  -gid-idx    RoCE v2 GID index (default 3, find via show_gids)\n"
        "  -port-num   HCA port number (default 1)\n"
        "  -no-hugepage  use regular pages (default: hugepage memfd)\n",
        prog);
}

int main(int argc, char **argv) {
    uint64_t size = 0;
    int      tcp_port = 0;
    const char *dev_name = NULL;
    uint8_t  gid_index = 3;
    uint8_t  port_num = 1;
    int      hugepage = 1;

    static struct option long_opts[] = {
        {"size",        required_argument, 0, 's'},
        {"port",        required_argument, 0, 'p'},
        {"dev",         required_argument, 0, 'd'},
        {"gid-idx",     required_argument, 0, 'g'},
        {"port-num",    required_argument, 0, 'n'},
        {"no-hugepage", no_argument,       0, 'H'},
        {0, 0, 0, 0},
    };
    int c;
    while ((c = getopt_long_only(argc, argv, "s:p:d:g:n:H", long_opts, NULL)) != -1) {
        switch (c) {
        case 's': size = parse_size(optarg); break;
        case 'p': tcp_port = atoi(optarg); break;
        case 'd': dev_name = optarg; break;
        case 'g': gid_index = (uint8_t)atoi(optarg); break;
        case 'n': port_num = (uint8_t)atoi(optarg); break;
        case 'H': hugepage = 0; break;
        default: usage(argv[0]); return 1;
        }
    }
    if (!size || !tcp_port) { usage(argv[0]); return 1; }

    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));

    // 1. allocate hugepage memfd buffer + fill pattern
    int memfd_fd = -1;
    void *buf = alloc_buffer(size, hugepage, &memfd_fd);
    if (!buf) return 1;
    fprintf(stderr, "[+] allocated %.2f GB %s buffer (memfd fd=%d) at %p\n",
            (double)size / (1024 * 1024 * 1024),
            hugepage ? "hugepage" : "regular", memfd_fd, buf);

    uint64_t *words = (uint64_t *)buf;
    uint64_t  nwords = size / 8;
    uint64_t t0 = now_ns();
    for (uint64_t i = 0; i < nwords; i++) words[i] = i;
    fprintf(stderr, "[+] filled %.2f GB pattern in %.2f s (%.2f GB/s)\n",
            (double)size / (1024.0 * 1024.0 * 1024.0),
            (now_ns() - t0) / 1e9,
            (double)size / 1024.0 / 1024.0 / 1024.0 / ((now_ns() - t0) / 1e9));

    // 2. open device, alloc PD, CQ, QP
    struct ibv_context *ctx = open_device(dev_name);
    if (!ctx) return 1;

    struct ibv_pd *pd = ibv_alloc_pd(ctx);
    if (!pd) { perror("ibv_alloc_pd"); return 1; }

    struct ibv_cq *cq = ibv_create_cq(ctx, 16, NULL, NULL, 0);
    if (!cq) { perror("ibv_create_cq"); return 1; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq,
        .recv_cq = cq,
        .cap = {
            .max_send_wr  = 16,
            .max_recv_wr  = 16,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };
    struct ibv_qp *qp = ibv_create_qp(pd, &qp_attr);
    if (!qp) { perror("ibv_create_qp"); return 1; }

    // 3. register MR — THE critical step we're validating
    t0 = now_ns();
    struct ibv_mr *mr = ibv_reg_mr(pd, buf, size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        perror("ibv_reg_mr");
        fprintf(stderr, "    ^ THIS is the spike's critical failure point.\n"
                        "    If this fails on hugepage memfd, the architecture\n"
                        "    needs to be reconsidered. Try -no-hugepage to compare.\n");
        return 1;
    }
    fprintf(stderr, "[+] ibv_reg_mr OK in %.2f ms — addr=%p len=%zu rkey=0x%x lkey=0x%x\n",
            (now_ns() - t0) / 1e6, mr->addr, mr->length, mr->rkey, mr->lkey);

    // 4. query port + GID
    struct ibv_port_attr port_attr;
    if (ibv_query_port(ctx, port_num, &port_attr)) { perror("ibv_query_port"); return 1; }
    union ibv_gid gid;
    if (ibv_query_gid(ctx, port_num, gid_index, &gid)) { perror("ibv_query_gid"); return 1; }

    struct qp_info local = {
        .lid  = port_attr.lid,
        .qpn  = qp->qp_num,
        .psn  = (uint32_t)rand() & 0xffffff,
        .addr = (uint64_t)(uintptr_t)mr->addr,
        .rkey = mr->rkey,
        .size = size,
    };
    memcpy(local.gid, &gid, 16);

    fprintf(stderr, "[+] local: lid=%u qpn=%u psn=%u addr=0x%" PRIx64 " rkey=0x%x gid=",
            local.lid, local.qpn, local.psn, local.addr, local.rkey);
    print_gid(local.gid);
    fprintf(stderr, "\n");

    // 5. TCP listen, accept, exchange qp_info
    int listen_fd = tcp_listen(tcp_port);
    if (listen_fd < 0) return 1;
    fprintf(stderr, "[+] listening on TCP :%d for QP-info exchange...\n", tcp_port);

    struct sockaddr_in peer;
    socklen_t plen = sizeof(peer);
    int conn_fd = accept(listen_fd, (struct sockaddr *)&peer, &plen);
    if (conn_fd < 0) { perror("accept"); return 1; }
    fprintf(stderr, "[+] accepted connection from %s:%d\n",
            inet_ntoa(peer.sin_addr), ntohs(peer.sin_port));
    close(listen_fd);

    if (write_full(conn_fd, &local, sizeof(local)) < 0) { perror("send qp_info"); return 1; }

    struct qp_info remote;
    if (read_full(conn_fd, &remote, sizeof(remote)) < 0) { perror("recv qp_info"); return 1; }
    fprintf(stderr, "[+] remote: lid=%u qpn=%u psn=%u addr=0x%" PRIx64 " rkey=0x%x size=%" PRIu64 " gid=",
            remote.lid, remote.qpn, remote.psn, remote.addr, remote.rkey, remote.size);
    print_gid(remote.gid);
    fprintf(stderr, "\n");

    // 6. transition QP to RTS
    if (qp_to_rts(qp, &remote, port_num, gid_index, local.psn) < 0) return 1;
    fprintf(stderr, "[+] QP transitioned to RTS — server is now ready for RDMA Reads\n");

    // 7. wait for client's "done" signal
    char done;
    if (read_full(conn_fd, &done, 1) < 0) {
        fprintf(stderr, "[!] client disconnected without 'done' signal\n");
    } else {
        fprintf(stderr, "[+] client signaled done — total wall time %.2f s\n",
                (now_ns() - t0) / 1e9);
    }

    // 8. cleanup
    ibv_destroy_qp(qp);
    ibv_destroy_cq(cq);
    ibv_dereg_mr(mr);
    ibv_dealloc_pd(pd);
    ibv_close_device(ctx);
    munmap(buf, size);
    close(memfd_fd);
    close(conn_fd);

    fprintf(stderr, "[+] clean exit\n");
    return 0;
}
