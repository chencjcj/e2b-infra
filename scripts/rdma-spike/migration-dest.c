// migration-dest — Phase 1 post-copy demo (destination side).
//
// Connects to rdma-server, sets up an EMPTY hugepage memfd buffer, registers
// it with userfaultfd, then spawns:
//
//   * a fault-handler thread: reads UFFD events, issues one-sided RDMA Read
//     from the source MR into a staging buffer, then UFFDIO_COPY into the
//     missing page, waking the fault.
//
//   * a "FC simulator" thread: walks the buffer page by page (random order
//     to defeat readahead), measures first-touch latency per page.
//
// This is the mechanism Firecracker would see in production: each cold page
// access takes a UFFD round-trip + RDMA Read. The numbers measured here are
// the BEST-case latency for each page brought in via post-copy.
//
// Connect to a running rdma-server:
//   ./rdma-server -size 4G -port 12345 -dev mlx5_10 -gid-idx 3
//   ./migration-dest -addr <src-ip> -port 12345 -size 4G -dev mlx5_10 -gid-idx 3

#include <pthread.h>
#include <poll.h>
#include <signal.h>
#include <getopt.h>
#include <stdatomic.h>
#include <linux/userfaultfd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>

#include "exchange.h"

#ifndef UFFD_USER_MODE_ONLY
#define UFFD_USER_MODE_ONLY 1
#endif

struct ctx {
    void   *fc_view;        // VA under UFFD; FC simulator reads here
    size_t  size;
    int     memfd_fd;

    void   *staging;        // hugepage-aligned 2 MB landing pad for RDMA Read
    int     uffd;

    struct ibv_context *ibv;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *staging_mr;
    struct qp_info remote;

    pthread_mutex_t qp_mu;

    atomic_uint_fast64_t faults_handled;
    atomic_uint_fast64_t total_fault_ns;

    int stop;
};

static struct ctx C;

static void* alloc_staging(void) {
    void *p = mmap(NULL, HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_ANONYMOUS | MAP_HUGETLB | (21 << MAP_HUGE_SHIFT),
                   -1, 0);
    if (p == MAP_FAILED) {
        perror("mmap staging (anon hugepage)");
        return NULL;
    }
    // touch to allocate the page now (avoid first-touch cost during measurement)
    *(volatile char *)p = 0;
    return p;
}

static int setup_uffd(void *base, size_t size) {
    int u = (int)syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK | UFFD_USER_MODE_ONLY);
    if (u < 0) {
        // older kernels reject UFFD_USER_MODE_ONLY; retry without it
        u = (int)syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK);
        if (u < 0) { perror("userfaultfd"); return -1; }
    }

    struct uffdio_api api = { .api = UFFD_API, .features = 0 };
    if (ioctl(u, UFFDIO_API, &api) < 0) { perror("UFFDIO_API"); close(u); return -1; }

    struct uffdio_register reg = {
        .range = { .start = (uint64_t)(uintptr_t)base, .len = size },
        .mode  = UFFDIO_REGISTER_MODE_MISSING,
    };
    if (ioctl(u, UFFDIO_REGISTER, &reg) < 0) { perror("UFFDIO_REGISTER"); close(u); return -1; }
    if (!(reg.ioctls & (1ULL << _UFFDIO_COPY))) {
        fprintf(stderr, "UFFDIO_COPY not supported on this range — kernel too old?\n");
        close(u); return -1;
    }
    return u;
}

// One blocking RDMA Read of HUGE_PAGE_SIZE from remote offset → staging.
// Caller holds qp_mu.
static int rdma_read_into_staging(uint64_t remote_off) {
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)C.staging,
        .length = HUGE_PAGE_SIZE,
        .lkey   = C.staging_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id      = remote_off,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
    };
    wr.wr.rdma.remote_addr = C.remote.addr + remote_off;
    wr.wr.rdma.rkey        = C.remote.rkey;

    struct ibv_send_wr *bad = NULL;
    int ret = ibv_post_send(C.qp, &wr, &bad);
    if (ret) { fprintf(stderr, "ibv_post_send: %s\n", strerror(ret)); return -1; }

    struct ibv_wc wc;
    while (1) {
        int n = ibv_poll_cq(C.cq, 1, &wc);
        if (n < 0) { fprintf(stderr, "ibv_poll_cq error\n"); return -1; }
        if (n == 0) continue;
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC failed: status=%d (%s)\n", wc.status, ibv_wc_status_str(wc.status));
            return -1;
        }
        return 0;
    }
}

static void* fault_handler_thread(void *arg) {
    (void)arg;
    struct pollfd pfd = { .fd = C.uffd, .events = POLLIN };
    while (!C.stop) {
        int pr = poll(&pfd, 1, 200);
        if (pr <= 0) continue;

        struct uffd_msg msg;
        ssize_t n = read(C.uffd, &msg, sizeof(msg));
        if (n < 0) {
            if (errno == EAGAIN) continue;
            perror("uffd read"); break;
        }
        if (n != sizeof(msg)) continue;
        if (msg.event != UFFD_EVENT_PAGEFAULT) continue;

        uint64_t fault_addr = msg.arg.pagefault.address;
        uint64_t aligned    = fault_addr & ~(HUGE_PAGE_SIZE - 1);
        uint64_t off        = aligned - (uint64_t)(uintptr_t)C.fc_view;

        uint64_t t0 = now_ns();

        pthread_mutex_lock(&C.qp_mu);
        int rc = rdma_read_into_staging(off);
        pthread_mutex_unlock(&C.qp_mu);
        if (rc) continue;

        struct uffdio_copy cp = {
            .dst  = aligned,
            .src  = (uint64_t)(uintptr_t)C.staging,
            .len  = HUGE_PAGE_SIZE,
            .mode = 0,
        };
        if (ioctl(C.uffd, UFFDIO_COPY, &cp) < 0) {
            // EEXIST = race: another faulter already filled this page. Harmless.
            if (errno != EEXIST) { perror("UFFDIO_COPY"); continue; }
        }

        atomic_fetch_add(&C.faults_handled, 1);
        atomic_fetch_add(&C.total_fault_ns, now_ns() - t0);
    }
    return NULL;
}

// FC simulator: read each page once in random order, measure first-touch latency.
static void run_fc_sim(int random_order) {
    uint64_t npages = C.size / HUGE_PAGE_SIZE;
    uint64_t *order = malloc(sizeof(uint64_t) * npages);
    for (uint64_t i = 0; i < npages; i++) order[i] = i;
    if (random_order) {
        for (uint64_t i = npages - 1; i > 0; i--) {
            uint64_t j = (((uint64_t)rand() << 32) ^ (uint64_t)rand()) % (i + 1);
            uint64_t t = order[i]; order[i] = order[j]; order[j] = t;
        }
    }

    uint64_t *lats = malloc(sizeof(uint64_t) * npages);
    uint64_t total_ns = 0, mismatches = 0;

    fprintf(stderr, "[fc-sim] %s walk of %" PRIu64 " pages (%.2f GB)\n",
            random_order ? "random" : "sequential",
            npages, (double)C.size / (1024.0 * 1024.0 * 1024.0));

    uint64_t walk_start = now_ns();
    for (uint64_t i = 0; i < npages; i++) {
        uint64_t off = order[i] * HUGE_PAGE_SIZE;
        volatile uint64_t *p = (uint64_t *)((char *)C.fc_view + off);

        uint64_t t0 = now_ns();
        uint64_t v = *p;             // ← triggers UFFD MISSING if not yet populated
        uint64_t elapsed = now_ns() - t0;

        lats[i] = elapsed;
        total_ns += elapsed;

        // server.c fills word-at-offset = offset/8 (each 8-byte word = its index)
        uint64_t expected = off / 8;
        if (v != expected) {
            if (mismatches < 4) {
                fprintf(stderr, "[fc-sim] PATTERN MISMATCH at off=%" PRIu64
                                ": got 0x%" PRIx64 " expected 0x%" PRIx64 "\n",
                        off, v, expected);
            }
            mismatches++;
        }
    }
    uint64_t walk_total = now_ns() - walk_start;

    // sort latencies
    for (uint64_t i = 1; i < npages; i++) {
        uint64_t v = lats[i]; int64_t j = (int64_t)i - 1;
        while (j >= 0 && lats[j] > v) { lats[j+1] = lats[j]; j--; }
        lats[j+1] = v;
    }

    fprintf(stderr, "\n=== FC simulator results (%s) ===\n",
            random_order ? "random" : "sequential");
    fprintf(stderr, "    pages walked       : %" PRIu64 "\n", npages);
    fprintf(stderr, "    pattern mismatches : %" PRIu64 "\n", mismatches);
    fprintf(stderr, "    total wall time    : %.3f s\n", walk_total / 1e9);
    fprintf(stderr, "    effective bandwidth: %.2f Gbps  (%.2f GiB/s)\n",
            (double)C.size * 8.0 / 1e9 / (walk_total / 1e9),
            (double)C.size / (1024.0 * 1024.0 * 1024.0) / (walk_total / 1e9));
    fprintf(stderr, "    per-page first-touch latency:\n");
    fprintf(stderr, "      avg %.1f us   min %.1f us   p50 %.1f us   p99 %.1f us   max %.1f us\n",
            (total_ns / (double)npages) / 1000.0,
            lats[0] / 1000.0,
            lats[npages/2] / 1000.0,
            lats[(npages*99)/100] / 1000.0,
            lats[npages-1] / 1000.0);

    free(order);
    free(lats);
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s -addr HOST -port P -size N[K|M|G] [-dev DEV] [-gid-idx N] [-port-num N] [-pattern seq|rand]\n"
        "  -addr        rdma-server IP\n"
        "  -port        rdma-server TCP port\n"
        "  -size        buffer size, must match server\n"
        "  -dev         ibv device name (default: first found)\n"
        "  -gid-idx     RoCE v2 GID index (default 3)\n"
        "  -port-num    HCA port number (default 1)\n"
        "  -pattern     access pattern: seq|rand (default rand — defeats hugepage readahead)\n",
        prog);
}

int main(int argc, char **argv) {
    const char *server_ip = NULL;
    int      tcp_port = 0;
    uint64_t size = 0;
    const char *dev_name = NULL;
    uint8_t  gid_index = 3;
    uint8_t  port_num = 1;
    int      random_order = 1;

    static struct option long_opts[] = {
        {"addr",     required_argument, 0, 'a'},
        {"port",     required_argument, 0, 'p'},
        {"size",     required_argument, 0, 's'},
        {"dev",      required_argument, 0, 'd'},
        {"gid-idx",  required_argument, 0, 'g'},
        {"port-num", required_argument, 0, 'n'},
        {"pattern",  required_argument, 0, 'P'},
        {0, 0, 0, 0},
    };
    int c;
    while ((c = getopt_long_only(argc, argv, "a:p:s:d:g:n:P:", long_opts, NULL)) != -1) {
        switch (c) {
        case 'a': server_ip = optarg; break;
        case 'p': tcp_port = atoi(optarg); break;
        case 's': size = parse_size(optarg); break;
        case 'd': dev_name = optarg; break;
        case 'g': gid_index = (uint8_t)atoi(optarg); break;
        case 'n': port_num = (uint8_t)atoi(optarg); break;
        case 'P': random_order = (strcmp(optarg, "seq") != 0); break;
        default: usage(argv[0]); return 1;
        }
    }
    if (!server_ip || !tcp_port || !size) { usage(argv[0]); return 1; }

    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));
    pthread_mutex_init(&C.qp_mu, NULL);

    // 1. allocate fc_view (hugepage memfd, sparse)
    C.size = size;
    C.fc_view = alloc_buffer(size, 1, &C.memfd_fd);
    if (!C.fc_view) return 1;
    fprintf(stderr, "[+] fc_view at %p (memfd fd=%d, %.2f GB)\n",
            C.fc_view, C.memfd_fd, (double)size / (1024.0*1024.0*1024.0));

    // 2. allocate staging (anon hugepage)
    C.staging = alloc_staging();
    if (!C.staging) return 1;
    fprintf(stderr, "[+] staging at %p (anon hugepage, 2 MB)\n", C.staging);

    // 3. RDMA setup
    C.ibv = open_device(dev_name);
    if (!C.ibv) return 1;
    C.pd = ibv_alloc_pd(C.ibv); if (!C.pd) { perror("ibv_alloc_pd"); return 1; }
    C.cq = ibv_create_cq(C.ibv, 16, NULL, NULL, 0); if (!C.cq) { perror("ibv_create_cq"); return 1; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = C.cq, .recv_cq = C.cq,
        .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC,
    };
    C.qp = ibv_create_qp(C.pd, &qp_attr);
    if (!C.qp) { perror("ibv_create_qp"); return 1; }

    // MR covers staging only — that's what RDMA writes into
    C.staging_mr = ibv_reg_mr(C.pd, C.staging, HUGE_PAGE_SIZE,
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!C.staging_mr) { perror("ibv_reg_mr(staging)"); return 1; }
    fprintf(stderr, "[+] staging MR registered: lkey=0x%x rkey=0x%x\n",
            C.staging_mr->lkey, C.staging_mr->rkey);

    // 4. UFFD on fc_view
    C.uffd = setup_uffd(C.fc_view, size);
    if (C.uffd < 0) return 1;
    fprintf(stderr, "[+] UFFD registered (MISSING mode) on fc_view\n");

    // 5. exchange QP info
    struct ibv_port_attr pattr; ibv_query_port(C.ibv, port_num, &pattr);
    union ibv_gid gid; ibv_query_gid(C.ibv, port_num, gid_index, &gid);

    struct qp_info local = {
        .lid  = pattr.lid,
        .qpn  = C.qp->qp_num,
        .psn  = (uint32_t)rand() & 0xffffff,
        .addr = (uint64_t)(uintptr_t)C.staging,
        .rkey = C.staging_mr->rkey,
        .size = size,
    };
    memcpy(local.gid, &gid, 16);

    int conn_fd = tcp_connect(server_ip, tcp_port);
    if (conn_fd < 0) return 1;
    fprintf(stderr, "[+] TCP connected to %s:%d\n", server_ip, tcp_port);

    if (read_full(conn_fd, &C.remote, sizeof(C.remote)) < 0) { perror("recv qp_info"); return 1; }
    if (write_full(conn_fd, &local, sizeof(local)) < 0)      { perror("send qp_info"); return 1; }
    if (C.remote.size != size) {
        fprintf(stderr, "size mismatch: local=%" PRIu64 " remote=%" PRIu64 "\n", size, C.remote.size);
        return 1;
    }
    fprintf(stderr, "[+] remote: addr=0x%" PRIx64 " rkey=0x%x\n", C.remote.addr, C.remote.rkey);

    if (qp_to_rts(C.qp, &C.remote, port_num, gid_index, local.psn) < 0) return 1;
    fprintf(stderr, "[+] QP transitioned to RTS\n");

    // 6. spawn fault handler
    pthread_t fh;
    if (pthread_create(&fh, NULL, fault_handler_thread, NULL) != 0) {
        perror("pthread_create"); return 1;
    }
    fprintf(stderr, "[+] fault handler thread running\n\n");

    // 7. run FC simulator in main thread
    run_fc_sim(random_order);

    // 8. summary from fault handler's perspective
    uint64_t fh_count = atomic_load(&C.faults_handled);
    uint64_t fh_total = atomic_load(&C.total_fault_ns);
    fprintf(stderr, "\n=== Fault handler stats ===\n");
    fprintf(stderr, "    faults handled: %" PRIu64 "\n", fh_count);
    if (fh_count > 0) {
        fprintf(stderr, "    avg handler time (RDMA Read + UFFDIO_COPY): %.1f us\n",
                (fh_total / (double)fh_count) / 1000.0);
    }

    // 9. signal source done, cleanup
    char done = '!';
    write_full(conn_fd, &done, 1);

    C.stop = 1;
    pthread_join(fh, NULL);

    ibv_destroy_qp(C.qp);
    ibv_destroy_cq(C.cq);
    ibv_dereg_mr(C.staging_mr);
    ibv_dealloc_pd(C.pd);
    ibv_close_device(C.ibv);
    munmap(C.staging, HUGE_PAGE_SIZE);
    munmap(C.fc_view, size);
    close(C.memfd_fd);
    close(C.uffd);
    close(conn_fd);

    fprintf(stderr, "\n[+] clean exit\n");
    return 0;
}
