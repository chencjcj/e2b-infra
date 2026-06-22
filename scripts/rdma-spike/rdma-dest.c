// rdma-dest — production migration destination agent.
//
// Spawned by the orchestrator when sandbox X is being migrated IN.
// Receives:
//   - FC's userfaultfd FD (intercepts page faults on FC's memfd mmap)  as fd 3
//   - dest's page-pool memfd FD (MAP_SHARED-mmapped here)              as fd 4
//   - the source's qp_info + tcp address via CLI args
//   - FC's memfd mmap base VA via --fc-base-va (so we can compute
//     memfd offset from fault VAs)
//
// Architecture:
//   - The dest page-pool memfd is mmap'd MAP_SHARED here and registered as the
//     local MR. RDMA Reads write directly into the memfd's pagecache.
//   - FC mmaps the same memfd MAP_PRIVATE and registers UFFD for MISSING+MINOR
//     faults. When FC accesses a page that exists in pagecache (we filled it
//     via RDMA), kernel raises MINOR fault — we resolve via UFFDIO_CONTINUE,
//     which maps the existing pagecache page into FC's page table (zero-copy).
//   - For pages not yet RDMA-fetched, FC sees a MISSING fault. We RDMA Read
//     the page first, then UFFDIO_CONTINUE (now MINOR-resolvable since the
//     page is in pagecache). We never use UFFDIO_COPY because that would
//     create a private CoW page divorced from the memfd pagecache — FC's
//     subsequent reads on that VA via shared_memfd_path would still see
//     zeros from the still-empty pagecache.
//
// Connects to source over TCP, exchanges QP info, transitions to RTS.
// Spawns:
//   - fault-handler thread: reads UFFD events, RDMA Reads the missing page
//     into the dest memfd, then UFFDIO_CONTINUE on FC's address space
//   - prefetch thread: walks pages in order, RDMA Reads each into memfd
//     ahead of demand to populate pagecache
//
// When all pages are populated, sends a single byte over the source TCP
// link to signal completion, then exits 0.
//
// Stdout markers (orchestrator parses):
//   QP_INFO: <hex>          ← our qp_info (sent over TCP to source)
//   QP_RTS                  ← QP fully established
//   PREFETCH_PROGRESS: x/y  ← periodic update during transfer
//   PREFETCH_DONE
//   FAULTS_HANDLED: n
//   DONE                    ← about to exit cleanly
//
// Nonzero exit = migration failed; orchestrator should fall back to GCS path.
//
// Args:
//   --uffd-fd N      : preopened userfaultfd FD (typically 3)
//   --memfd-fd N     : preopened dest page-pool memfd FD (typically 4)
//   --size N         : buffer size, must match source
//   --fc-base-va HEX : FC's mmap base VA (where UFFD-registered range starts)
//                       hex with optional 0x prefix
//   --src-addr IP    : source orchestrator's RDMA TCP address
//   --src-port N     : source's TCP port
//   --src-qp HEX     : source's qp_info struct, hex-encoded
//   --dev DEV        : ibv device name
//   --gid-idx N      : RoCE v2 GID index
//   --port-num N     : HCA port (default 1)
//
// Multi-region sandboxes (rare): for MVP we assume single contiguous region
// of size --size starting at --fc-base-va. To extend, accept --region tuples.

#include <getopt.h>
#include <pthread.h>
#include <poll.h>
#include <signal.h>
#include <stdatomic.h>
#include <linux/userfaultfd.h>
#include <sys/ioctl.h>

#include "exchange.h"

#ifndef UFFD_USER_MODE_ONLY
#define UFFD_USER_MODE_ONLY 1
#endif

struct ctx {
    uint64_t fc_base_va;    // FC's mmap base; UFFD VAs span [fc_base_va, fc_base_va+size)
    size_t   size;

    int      uffd;
    int      memfd_fd;
    void    *memfd_buf;     // MAP_SHARED mmap of memfd_fd, full size

    struct ibv_context *ibv;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *memfd_mr;   // registered on memfd_buf for RDMA Read landing
    struct qp_info remote;

    pthread_mutex_t qp_mu;     // serialize ibv_post_send + poll_one
    pthread_mutex_t bitmap_mu; // protect populated[]
    uint64_t *populated;       // 1 bit per hugepage; set when page is RDMA-Read'd
    uint64_t  npages;

    atomic_uint_fast64_t faults_handled;

    int conn_fd;               // TCP to source, kept open until DONE
    volatile int stop;
};

static struct ctx C;

static int parse_hex_to(uint8_t *out, size_t out_len, const char *hex) {
    if (strlen(hex) != out_len * 2) return -1;
    for (size_t i = 0; i < out_len; i++) {
        unsigned int v;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

static void print_qp_info_hex(const struct qp_info *info) {
    printf("QP_INFO: ");
    const uint8_t *p = (const uint8_t *)info;
    for (size_t i = 0; i < sizeof(*info); i++) printf("%02x", p[i]);
    printf("\n");
    fflush(stdout);
}

static int bit_get(const uint64_t *bm, uint64_t i) { return (bm[i / 64] >> (i & 63)) & 1; }
static void bit_set(uint64_t *bm, uint64_t i)      { bm[i / 64] |= (1ULL << (i & 63)); }

// Caller must hold qp_mu.
// RDMA Read directly into memfd_buf at offset off, populating pagecache.
static int rdma_read_into_memfd(uint64_t off) {
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)C.memfd_buf + off,
        .length = HUGE_PAGE_SIZE,
        .lkey   = C.memfd_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = off, .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
    };
    wr.wr.rdma.remote_addr = C.remote.addr + off;
    wr.wr.rdma.rkey        = C.remote.rkey;
    struct ibv_send_wr *bad = NULL;
    int ret = ibv_post_send(C.qp, &wr, &bad);
    if (ret) { fprintf(stderr, "ibv_post_send: %s\n", strerror(ret)); return -1; }
    struct ibv_wc wc;
    while (1) {
        int n = ibv_poll_cq(C.cq, 1, &wc);
        if (n < 0) { fprintf(stderr, "poll_cq error\n"); return -1; }
        if (n == 0) continue;
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WC failed: %s\n", ibv_wc_status_str(wc.status));
            return -1;
        }
        return 0;
    }
}

// Ensure the page at offset `off` is populated in memfd pagecache (RDMA Read
// it if not yet). Sets populated bit on success.
static int ensure_page_populated(uint64_t off) {
    pthread_mutex_lock(&C.bitmap_mu);
    int already = bit_get(C.populated, off / HUGE_PAGE_SIZE);
    pthread_mutex_unlock(&C.bitmap_mu);
    if (already) return 0;

    pthread_mutex_lock(&C.qp_mu);
    int rc = rdma_read_into_memfd(off);
    pthread_mutex_unlock(&C.qp_mu);
    if (rc) return -1;

    pthread_mutex_lock(&C.bitmap_mu);
    bit_set(C.populated, off / HUGE_PAGE_SIZE);
    pthread_mutex_unlock(&C.bitmap_mu);
    return 0;
}

// Resolve a fault on FC's address space using UFFDIO_CONTINUE — maps the
// existing memfd pagecache page (which we populate via RDMA Read) into FC's
// page table. Falls back to RDMA Read first if the page hasn't been
// prefetched yet.
static int resolve_fault(uint64_t fault_va, uint64_t off) {
    if (ensure_page_populated(off) < 0) return -1;

    struct uffdio_continue cont = {
        .range = { .start = fault_va, .len = HUGE_PAGE_SIZE },
        .mode = 0,
    };
    if (ioctl(C.uffd, UFFDIO_CONTINUE, &cont) < 0) {
        if (errno != EEXIST) {  // EEXIST = race; the page is already mapped
            perror("UFFDIO_CONTINUE");
            return -1;
        }
    }
    return 0;
}

static void* fault_thread(void *arg) {
    (void)arg;
    struct pollfd pfd = { .fd = C.uffd, .events = POLLIN };
    while (!C.stop) {
        int pr = poll(&pfd, 1, 200);
        if (pr <= 0) continue;
        struct uffd_msg msg;
        ssize_t n = read(C.uffd, &msg, sizeof(msg));
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) continue;
            perror("uffd read"); break;
        }
        if (n != sizeof(msg)) continue;
        if (msg.event != UFFD_EVENT_PAGEFAULT) continue;
        uint64_t fault_va = msg.arg.pagefault.address & ~(HUGE_PAGE_SIZE - 1);
        if (fault_va < C.fc_base_va || fault_va >= C.fc_base_va + C.size) {
            fprintf(stderr, "fault VA 0x%" PRIx64 " outside [0x%" PRIx64 ", 0x%" PRIx64 ")\n",
                    fault_va, C.fc_base_va, C.fc_base_va + C.size);
            continue;
        }
        uint64_t off = fault_va - C.fc_base_va;
        if (resolve_fault(fault_va, off) == 0) {
            atomic_fetch_add(&C.faults_handled, 1);
        }
    }
    return NULL;
}

// Combine RDMA Read + UFFDIO_CONTINUE into one loop. Saves the second pass
// over all 1024 pages (~150ms). Each page: RDMA fetch into pagecache, then
// install in FC's PT immediately.
static void* prefetch_thread(void *arg) {
    (void)arg;
    uint64_t done = 0;
    uint64_t last_report = 0;
    uint64_t report_every = C.npages / 32;
    if (report_every < 1) report_every = 1;

    for (uint64_t i = 0; i < C.npages && !C.stop; i++) {
        uint64_t off = i * HUGE_PAGE_SIZE;
        uint64_t fault_va = C.fc_base_va + off;

        if (ensure_page_populated(off) != 0) {
            fprintf(stderr, "prefetch failed at offset %" PRIu64 "\n", off);
            C.stop = 1;
            return (void*)1;
        }

        // Install PT entry now so FC can read directly without faulting later.
        struct uffdio_continue cont = {
            .range = { .start = fault_va, .len = HUGE_PAGE_SIZE },
            .mode = 0,
        };
        if (ioctl(C.uffd, UFFDIO_CONTINUE, &cont) < 0) {
            if (errno != EEXIST) {
                // FC may have faulted in this page concurrently; the fault
                // handler will UFFDIO_CONTINUE it. Skip on benign errors.
                fprintf(stderr, "UFFDIO_CONTINUE va 0x%" PRIx64 ": %s (continuing)\n",
                        fault_va, strerror(errno));
            }
        }

        done++;
        if (done - last_report >= report_every || done == C.npages) {
            printf("PREFETCH_PROGRESS: %" PRIu64 "/%" PRIu64 "\n", done, C.npages);
            fflush(stdout);
            last_report = done;
        }
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --uffd-fd N --memfd-fd N --size N[K|M|G] --fc-base-va HEX "
        "--src-addr IP --src-port N --src-qp HEX "
        "[--dev DEV] [--gid-idx N] [--port-num N]\n", prog);
}

static int parse_hex_u64(const char *s, uint64_t *out) {
    if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) s += 2;
    char *end;
    *out = strtoull(s, &end, 16);
    return *end == '\0' ? 0 : -1;
}

int main(int argc, char **argv) {
    int uffd_fd = -1;
    int memfd_fd = -1;
    uint64_t size = 0;
    uint64_t fc_base_va = 0;
    int fc_base_va_set = 0;
    const char *src_addr = NULL;
    int src_port = 0;
    const char *src_qp_hex = NULL;
    const char *dev_name = NULL;
    uint8_t gid_index = 3;
    uint8_t port_num = 1;

    static struct option opts[] = {
        {"uffd-fd",     required_argument, 0, 'u'},
        {"memfd-fd",    required_argument, 0, 'm'},
        {"size",        required_argument, 0, 's'},
        {"fc-base-va",  required_argument, 0, 'B'},
        {"src-addr",    required_argument, 0, 'A'},
        {"src-port",    required_argument, 0, 'P'},
        {"src-qp",      required_argument, 0, 'Q'},
        {"dev",         required_argument, 0, 'd'},
        {"gid-idx",     required_argument, 0, 'g'},
        {"port-num",    required_argument, 0, 'n'},
        {0, 0, 0, 0},
    };
    int c;
    while ((c = getopt_long_only(argc, argv, "u:m:s:B:A:P:Q:d:g:n:", opts, NULL)) != -1) {
        switch (c) {
        case 'u': uffd_fd = atoi(optarg); break;
        case 'm': memfd_fd = atoi(optarg); break;
        case 's': size = parse_size(optarg); break;
        case 'B':
            if (parse_hex_u64(optarg, &fc_base_va) < 0) {
                fprintf(stderr, "bad --fc-base-va: %s\n", optarg); return 1;
            }
            fc_base_va_set = 1;
            break;
        case 'A': src_addr = optarg; break;
        case 'P': src_port = atoi(optarg); break;
        case 'Q': src_qp_hex = optarg; break;
        case 'd': dev_name = optarg; break;
        case 'g': gid_index = (uint8_t)atoi(optarg); break;
        case 'n': port_num = (uint8_t)atoi(optarg); break;
        default: usage(argv[0]); return 1;
        }
    }
    if (uffd_fd < 0 || memfd_fd < 0 || size == 0 || !fc_base_va_set ||
        !src_addr || !src_port || !src_qp_hex) {
        usage(argv[0]); return 1;
    }
    if (fc_base_va & (HUGE_PAGE_SIZE - 1)) {
        fprintf(stderr, "--fc-base-va 0x%" PRIx64 " not aligned to 2 MB\n", fc_base_va);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));
    pthread_mutex_init(&C.qp_mu, NULL);
    pthread_mutex_init(&C.bitmap_mu, NULL);

    C.size       = size;
    C.fc_base_va = fc_base_va;
    C.uffd       = uffd_fd;
    C.memfd_fd   = memfd_fd;
    C.npages     = size / HUGE_PAGE_SIZE;
    C.populated  = calloc((C.npages + 63) / 64, sizeof(uint64_t));
    if (!C.populated) { perror("calloc"); return 1; }

    fprintf(stderr, "[+] dest agent: %.2f GB across [0x%" PRIx64 ", 0x%" PRIx64 ") (%" PRIu64 " hugepages)\n",
            (double)size / (1024.0*1024.0*1024.0),
            fc_base_va, fc_base_va + size, C.npages);

    // mmap the dest page-pool memfd MAP_SHARED — RDMA Reads land here and
    // populate pagecache so FC's MAP_PRIVATE mapping sees data via MINOR
    // fault + UFFDIO_CONTINUE.
    C.memfd_buf = mmap(NULL, size, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_HUGETLB | (21 << MAP_HUGE_SHIFT),
                       memfd_fd, 0);
    if (C.memfd_buf == MAP_FAILED) {
        // Caller's memfd may not be MFD_HUGETLB; retry plain.
        C.memfd_buf = mmap(NULL, size, PROT_READ | PROT_WRITE,
                           MAP_SHARED, memfd_fd, 0);
        if (C.memfd_buf == MAP_FAILED) { perror("mmap memfd"); return 1; }
    }
    fprintf(stderr, "[+] mapped dest memfd fd=%d at %p (%.2f GB)\n",
            memfd_fd, C.memfd_buf, (double)size / (1024.0*1024.0*1024.0));

    // 3. RDMA setup
    C.ibv = open_device(dev_name);
    if (!C.ibv) return 1;
    C.pd = ibv_alloc_pd(C.ibv); if (!C.pd) { perror("alloc_pd"); return 1; }
    C.cq = ibv_create_cq(C.ibv, 16, NULL, NULL, 0); if (!C.cq) { perror("create_cq"); return 1; }
    struct ibv_qp_init_attr qpa = {
        .send_cq = C.cq, .recv_cq = C.cq,
        .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC,
    };
    C.qp = ibv_create_qp(C.pd, &qpa); if (!C.qp) { perror("create_qp"); return 1; }

    // Register MR on memfd_buf (full size) — RDMA Reads write here.
    C.memfd_mr = ibv_reg_mr(C.pd, C.memfd_buf, size,
        IBV_ACCESS_LOCAL_WRITE);
    if (!C.memfd_mr) { perror("reg_mr memfd"); return 1; }
    fprintf(stderr, "[+] MR registered: lkey=0x%x rkey=0x%x\n",
            C.memfd_mr->lkey, C.memfd_mr->rkey);

    // 4. Parse source's qp_info from --src-qp hex blob
    if (parse_hex_to((uint8_t*)&C.remote, sizeof(C.remote), src_qp_hex) < 0) {
        fprintf(stderr, "bad --src-qp hex\n"); return 1;
    }
    if (C.remote.size != size) {
        fprintf(stderr, "size mismatch: local=%" PRIu64 " src=%" PRIu64 "\n", size, C.remote.size);
        return 1;
    }

    // 5. TCP connect, exchange qp_info
    C.conn_fd = tcp_connect(src_addr, src_port);
    if (C.conn_fd < 0) return 1;

    struct ibv_port_attr pattr; ibv_query_port(C.ibv, port_num, &pattr);
    union ibv_gid gid; ibv_query_gid(C.ibv, port_num, gid_index, &gid);
    struct qp_info local = {
        .lid  = pattr.lid,
        .qpn  = C.qp->qp_num,
        .psn  = (uint32_t)rand() & 0xffffff,
        .addr = (uint64_t)(uintptr_t)C.memfd_buf,
        .rkey = C.memfd_mr->rkey,
        .size = size,
    };
    memcpy(local.gid, &gid, 16);
    print_qp_info_hex(&local);

    // Source's accept loop expects: send qp_info first (from source), then
    // recv from peer. So we recv first, send second.
    struct qp_info src_local;
    if (read_full(C.conn_fd, &src_local, sizeof(src_local)) < 0) {
        perror("recv src qp"); return 1;
    }
    C.remote = src_local;
    if (write_full(C.conn_fd, &local, sizeof(local)) < 0) { perror("send qp"); return 1; }

    if (qp_to_rts(C.qp, &C.remote, port_num, gid_index, local.psn) < 0) return 1;
    printf("QP_RTS\n"); fflush(stdout);

    // 6. Spawn fault handler + prefetch in parallel.
    pthread_t fh, pf;
    if (pthread_create(&fh, NULL, fault_thread, NULL) != 0) { perror("pthread fault"); return 1; }
    if (pthread_create(&pf, NULL, prefetch_thread, NULL) != 0) { perror("pthread prefetch"); return 1; }

    void *prefetch_ret = NULL;
    pthread_join(pf, &prefetch_ret);
    if (prefetch_ret != NULL) {
        fprintf(stderr, "prefetch failed\n");
        C.stop = 1;
        pthread_join(fh, NULL);
        return 2;
    }
    printf("PREFETCH_DONE\n"); fflush(stdout);

    // PT entries already installed inline by prefetch_thread. Stop fault
    // handler now — any remaining FC accesses will hit existing PT entries
    // (no fault), or trigger one final fault that resolve_fault catches via
    // the still-running handler (we'll join below).
    C.stop = 1;
    pthread_join(fh, NULL);

    printf("FAULTS_HANDLED: %" PRIu64 "\n", atomic_load(&C.faults_handled));

    // 7. Signal source we're done
    char done = '!';
    if (write_full(C.conn_fd, &done, 1) < 0) {
        fprintf(stderr, "[!] failed to signal source\n");
        return 3;
    }

    printf("DONE\n"); fflush(stdout);

    // 8. cleanup
    ibv_destroy_qp(C.qp);
    ibv_destroy_cq(C.cq);
    ibv_dereg_mr(C.memfd_mr);
    ibv_dealloc_pd(C.pd);
    ibv_close_device(C.ibv);
    munmap(C.memfd_buf, size);
    close(C.uffd);
    close(C.memfd_fd);
    close(C.conn_fd);
    free(C.populated);

    return 0;
}
