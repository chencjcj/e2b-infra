// rdma-client — destination side of the spike. Connects to server, exchanges
// QP info, then runs two benchmarks against the remote MR:
//
//   1. Latency: post one 2 MB RDMA Read at a time, measure round-trip ns.
//      This is what UFFD-driven page fetches would look like in production.
//
//   2. Throughput: pipeline N reads of the full buffer, measure aggregate
//      Gbps. This validates the HCA can saturate the wire.
//
// After completion the local buffer is verified against the expected pattern
// (each 8-byte word = its offset).

#include <getopt.h>
#include <signal.h>
#include <time.h>
#include <stdbool.h>

#include "exchange.h"

#define LATENCY_ITERS    1000
#define LATENCY_PAGE     HUGE_PAGE_SIZE
#define BULK_SQ_DEPTH    64
#define BULK_CHUNK       (4 * 1024 * 1024)  // 4 MB per WR for throughput test

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s -addr HOST -port P -size N[K|M|G] [-dev DEV] [-gid-idx N] [-port-num N] [-no-hugepage] [-skip-verify]\n"
        "  -addr        server IP\n"
        "  -port        server TCP port\n"
        "  -size        buffer size, MUST match server\n"
        "  -dev         ibv device name (default: first found)\n"
        "  -gid-idx     RoCE v2 GID index (default 3)\n"
        "  -port-num    HCA port number (default 1)\n"
        "  -no-hugepage use regular pages (default: hugepage memfd)\n"
        "  -skip-verify skip post-transfer pattern check (saves a few seconds)\n",
        prog);
}

static int post_read(struct ibv_qp *qp, uint64_t laddr, uint32_t lkey,
                     uint64_t raddr, uint32_t rkey, uint32_t length,
                     uint64_t wr_id) {
    struct ibv_sge sge = { .addr = laddr, .length = length, .lkey = lkey };
    struct ibv_send_wr wr = {
        .wr_id     = wr_id,
        .sg_list   = &sge,
        .num_sge   = 1,
        .opcode    = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
    };
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey        = rkey;
    struct ibv_send_wr *bad = NULL;
    int ret = ibv_post_send(qp, &wr, &bad);
    if (ret) { fprintf(stderr, "ibv_post_send: %s\n", strerror(ret)); return -1; }
    return 0;
}

static int poll_one(struct ibv_cq *cq) {
    struct ibv_wc wc;
    while (1) {
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n < 0) { fprintf(stderr, "ibv_poll_cq error\n"); return -1; }
        if (n == 0) continue;
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC error: status=%d (%s) wr_id=%" PRIu64 "\n",
                    wc.status, ibv_wc_status_str(wc.status), wc.wr_id);
            return -1;
        }
        return 0;
    }
}

int main(int argc, char **argv) {
    const char *server_ip = NULL;
    int      tcp_port = 0;
    uint64_t size = 0;
    const char *dev_name = NULL;
    uint8_t  gid_index = 3;
    uint8_t  port_num = 1;
    int      hugepage = 1;
    int      skip_verify = 0;

    static struct option long_opts[] = {
        {"addr",        required_argument, 0, 'a'},
        {"port",        required_argument, 0, 'p'},
        {"size",        required_argument, 0, 's'},
        {"dev",         required_argument, 0, 'd'},
        {"gid-idx",     required_argument, 0, 'g'},
        {"port-num",    required_argument, 0, 'n'},
        {"no-hugepage", no_argument,       0, 'H'},
        {"skip-verify", no_argument,       0, 'V'},
        {0, 0, 0, 0},
    };
    int c;
    while ((c = getopt_long_only(argc, argv, "a:p:s:d:g:n:HV", long_opts, NULL)) != -1) {
        switch (c) {
        case 'a': server_ip = optarg; break;
        case 'p': tcp_port = atoi(optarg); break;
        case 's': size = parse_size(optarg); break;
        case 'd': dev_name = optarg; break;
        case 'g': gid_index = (uint8_t)atoi(optarg); break;
        case 'n': port_num = (uint8_t)atoi(optarg); break;
        case 'H': hugepage = 0; break;
        case 'V': skip_verify = 1; break;
        default: usage(argv[0]); return 1;
        }
    }
    if (!server_ip || !tcp_port || !size) { usage(argv[0]); return 1; }

    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));

    // 1. allocate matching local buffer
    int memfd_fd = -1;
    void *buf = alloc_buffer(size, hugepage, &memfd_fd);
    if (!buf) return 1;
    memset(buf, 0, 4096);  // touch first page only — rest fills via RDMA Read
    fprintf(stderr, "[+] allocated %.2f GB %s buffer (memfd fd=%d) at %p\n",
            (double)size / (1024 * 1024 * 1024),
            hugepage ? "hugepage" : "regular", memfd_fd, buf);

    // 2. open device + setup
    struct ibv_context *ctx = open_device(dev_name);
    if (!ctx) return 1;

    struct ibv_pd *pd = ibv_alloc_pd(ctx);
    if (!pd) { perror("ibv_alloc_pd"); return 1; }

    struct ibv_cq *cq = ibv_create_cq(ctx, BULK_SQ_DEPTH * 2, NULL, NULL, 0);
    if (!cq) { perror("ibv_create_cq"); return 1; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq,
        .recv_cq = cq,
        .cap = {
            .max_send_wr  = BULK_SQ_DEPTH,
            .max_recv_wr  = 16,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };
    struct ibv_qp *qp = ibv_create_qp(pd, &qp_attr);
    if (!qp) { perror("ibv_create_qp"); return 1; }

    uint64_t t0 = now_ns();
    struct ibv_mr *mr = ibv_reg_mr(pd, buf, size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        perror("ibv_reg_mr");
        fprintf(stderr, "    ^ MR registration on local buffer failed\n");
        return 1;
    }
    fprintf(stderr, "[+] ibv_reg_mr OK in %.2f ms — lkey=0x%x rkey=0x%x\n",
            (now_ns() - t0) / 1e6, mr->lkey, mr->rkey);

    struct ibv_port_attr port_attr;
    if (ibv_query_port(ctx, port_num, &port_attr)) { perror("ibv_query_port"); return 1; }
    union ibv_gid gid;
    if (ibv_query_gid(ctx, port_num, gid_index, &gid)) { perror("ibv_query_gid"); return 1; }

    struct qp_info local = {
        .lid  = port_attr.lid,
        .qpn  = qp->qp_num,
        .psn  = (uint32_t)rand() & 0xffffff,
        .addr = (uint64_t)(uintptr_t)buf,
        .rkey = mr->rkey,
        .size = size,
    };
    memcpy(local.gid, &gid, 16);

    fprintf(stderr, "[+] local: lid=%u qpn=%u psn=%u gid=", local.lid, local.qpn, local.psn);
    print_gid(local.gid);
    fprintf(stderr, "\n");

    // 3. TCP connect, exchange qp_info
    int conn_fd = tcp_connect(server_ip, tcp_port);
    if (conn_fd < 0) return 1;
    fprintf(stderr, "[+] TCP connected to %s:%d\n", server_ip, tcp_port);

    struct qp_info remote;
    if (read_full(conn_fd, &remote, sizeof(remote)) < 0) { perror("recv qp_info"); return 1; }
    if (write_full(conn_fd, &local, sizeof(local)) < 0)  { perror("send qp_info"); return 1; }

    if (remote.size != size) {
        fprintf(stderr, "[!] size mismatch: local=%" PRIu64 " remote=%" PRIu64 "\n", size, remote.size);
        return 1;
    }
    fprintf(stderr, "[+] remote: qpn=%u psn=%u addr=0x%" PRIx64 " rkey=0x%x gid=",
            remote.qpn, remote.psn, remote.addr, remote.rkey);
    print_gid(remote.gid);
    fprintf(stderr, "\n");

    // 4. transition QP
    if (qp_to_rts(qp, &remote, port_num, gid_index, local.psn) < 0) return 1;
    fprintf(stderr, "[+] QP transitioned to RTS\n\n");

    // ===========================================================
    // Test 1: single-page latency (this models UFFD on-demand fetch)
    // ===========================================================
    fprintf(stderr, "=== Test 1: %d × 2 MB RDMA Read latency ===\n", LATENCY_ITERS);
    uint64_t total_lat_ns = 0;
    uint64_t min_ns = UINT64_MAX, max_ns = 0;
    uint64_t *lats = malloc(sizeof(uint64_t) * LATENCY_ITERS);
    for (int i = 0; i < LATENCY_ITERS; i++) {
        // pull a different page each iteration to defeat any per-MR caching
        uint64_t off = ((uint64_t)i * LATENCY_PAGE) % (size - LATENCY_PAGE);
        uint64_t s = now_ns();
        if (post_read(qp,
                      (uint64_t)(uintptr_t)buf + off, mr->lkey,
                      remote.addr + off, remote.rkey,
                      LATENCY_PAGE, (uint64_t)i) < 0) return 1;
        if (poll_one(cq) < 0) return 1;
        uint64_t elapsed = now_ns() - s;
        lats[i] = elapsed;
        total_lat_ns += elapsed;
        if (elapsed < min_ns) min_ns = elapsed;
        if (elapsed > max_ns) max_ns = elapsed;
    }
    // simple p50 / p99 (sort)
    for (int i = 1; i < LATENCY_ITERS; i++) {
        uint64_t v = lats[i];
        int j = i - 1;
        while (j >= 0 && lats[j] > v) { lats[j + 1] = lats[j]; j--; }
        lats[j + 1] = v;
    }
    fprintf(stderr, "    avg %.1f us   min %.1f us   p50 %.1f us   p99 %.1f us   max %.1f us\n",
            (total_lat_ns / (double)LATENCY_ITERS) / 1000.0,
            min_ns / 1000.0,
            lats[LATENCY_ITERS / 2] / 1000.0,
            lats[(LATENCY_ITERS * 99) / 100] / 1000.0,
            max_ns / 1000.0);
    free(lats);

    // ===========================================================
    // Test 2: bulk throughput — pipelined RDMA Read of whole buffer
    // ===========================================================
    fprintf(stderr, "\n=== Test 2: pipelined throughput, %.2f GB total, %d MB chunks ===\n",
            (double)size / (1024.0 * 1024.0 * 1024.0), BULK_CHUNK / (1024 * 1024));
    uint64_t off = 0;
    uint64_t inflight = 0;
    uint64_t completed_bytes = 0;
    t0 = now_ns();
    while (completed_bytes < size) {
        // top up SQ
        while (inflight < BULK_SQ_DEPTH && off < size) {
            uint32_t this_len = (size - off > BULK_CHUNK) ? BULK_CHUNK : (uint32_t)(size - off);
            if (post_read(qp,
                          (uint64_t)(uintptr_t)buf + off, mr->lkey,
                          remote.addr + off, remote.rkey,
                          this_len, off) < 0) return 1;
            off += this_len;
            inflight++;
        }
        // reap one
        if (poll_one(cq) < 0) return 1;
        completed_bytes += BULK_CHUNK; // (final WR may be smaller, but only off-by-once at the tail; harmless for the rate calc)
        if (completed_bytes > size) completed_bytes = size;
        inflight--;
    }
    uint64_t elapsed_ns = now_ns() - t0;
    double sec = elapsed_ns / 1e9;
    double gbps = (double)size * 8.0 / 1e9 / sec;     // gigabits/s, decimal
    double gibs = (double)size / (1024.0 * 1024.0 * 1024.0) / sec;
    fprintf(stderr, "    %.2f s   %.2f Gbps   %.2f GiB/s\n", sec, gbps, gibs);

    // ===========================================================
    // Test 3 (optional): verify pattern
    // ===========================================================
    if (!skip_verify) {
        fprintf(stderr, "\n=== Test 3: verifying %.2f GB pattern ===\n",
                (double)size / (1024.0 * 1024.0 * 1024.0));
        uint64_t *words = (uint64_t *)buf;
        uint64_t nwords = size / 8;
        bool ok = true;
        uint64_t mismatch_at = 0;
        for (uint64_t i = 0; i < nwords; i++) {
            if (words[i] != i) { ok = false; mismatch_at = i; break; }
        }
        if (ok) fprintf(stderr, "    OK — every 8-byte word matches expected pattern\n");
        else    fprintf(stderr, "    FAIL — first mismatch at word index %" PRIu64
                                " (offset %" PRIu64 ", got 0x%016" PRIx64 ")\n",
                                mismatch_at, mismatch_at * 8, words[mismatch_at]);
    } else {
        fprintf(stderr, "\n[+] skipping pattern verification\n");
    }

    // 5. signal server we're done
    char done = '!';
    write_full(conn_fd, &done, 1);

    // 6. cleanup
    ibv_destroy_qp(qp);
    ibv_destroy_cq(cq);
    ibv_dereg_mr(mr);
    ibv_dealloc_pd(pd);
    ibv_close_device(ctx);
    munmap(buf, size);
    close(memfd_fd);
    close(conn_fd);

    fprintf(stderr, "\n[+] clean exit\n");
    return 0;
}
