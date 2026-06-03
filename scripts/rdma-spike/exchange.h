// exchange.h — shared helpers: hugepage memfd, TCP QP-info exchange,
// device opening, QP state-machine transitions for RC over RoCE v2.
#ifndef EXCHANGE_H
#define EXCHANGE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <inttypes.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <linux/memfd.h>

#include <infiniband/verbs.h>

#ifndef MFD_HUGETLB
#define MFD_HUGETLB 0x0004U
#endif
#ifndef MFD_HUGE_2MB
#define MFD_HUGE_2MB (21U << 26)
#endif

#define HUGE_PAGE_SIZE (2UL * 1024 * 1024)

struct qp_info {
    uint16_t lid;     // IB only; 0 for pure RoCE
    uint32_t qpn;
    uint32_t psn;
    uint64_t addr;    // remote buffer VA
    uint32_t rkey;
    uint64_t size;    // buffer size in bytes
    uint8_t  gid[16]; // RoCE: must be set
} __attribute__((packed));

static inline uint64_t parse_size(const char *s) {
    char *end;
    uint64_t v = strtoull(s, &end, 10);
    if (*end == 'k' || *end == 'K') v *= 1024UL;
    else if (*end == 'm' || *end == 'M') v *= 1024UL * 1024UL;
    else if (*end == 'g' || *end == 'G') v *= 1024UL * 1024UL * 1024UL;
    return v;
}

// Create an anon hugepage-backed memfd. Returns mapping or NULL.
static inline void* alloc_buffer(uint64_t size, int hugepage, int *out_fd) {
    int fd;
    unsigned int flags = 0;
    if (hugepage) flags |= MFD_HUGETLB | MFD_HUGE_2MB;
    fd = (int)syscall(SYS_memfd_create, "rdma-spike", flags);
    if (fd < 0) {
        perror("memfd_create");
        return NULL;
    }
    if (ftruncate(fd, size) < 0) {
        perror("ftruncate");
        close(fd);
        return NULL;
    }
    int mflags = MAP_SHARED;
    if (hugepage) mflags |= MAP_HUGETLB | (21 << MAP_HUGE_SHIFT);
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, mflags, fd, 0);
    if (p == MAP_FAILED) {
        // hugepage-backed memfd already implies hugepage; some kernels reject
        // MAP_HUGETLB on top. Retry without it.
        p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED) {
            perror("mmap");
            close(fd);
            return NULL;
        }
    }
    *out_fd = fd;
    return p;
}

static inline int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(port),
    };
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); close(fd); return -1; }
    if (listen(fd, 1) < 0) { perror("listen"); close(fd); return -1; }
    return fd;
}

static inline int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        fprintf(stderr, "bad address: %s\n", host); close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); close(fd); return -1;
    }
    return fd;
}

static inline int read_full(int fd, void *buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (char *)buf + total, n - total);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) return -1;
        total += r;
    }
    return 0;
}

static inline int write_full(int fd, const void *buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = write(fd, (const char *)buf + total, n - total);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        total += r;
    }
    return 0;
}

static inline struct ibv_context* open_device(const char *want) {
    int n = 0;
    struct ibv_device **list = ibv_get_device_list(&n);
    if (!list || n == 0) {
        fprintf(stderr, "no RDMA devices found (is rdma-core installed? is the driver loaded?)\n");
        return NULL;
    }
    struct ibv_device *dev = NULL;
    if (want) {
        for (int i = 0; i < n; i++) {
            if (strcmp(ibv_get_device_name(list[i]), want) == 0) {
                dev = list[i];
                break;
            }
        }
        if (!dev) {
            fprintf(stderr, "device %s not found. Available:\n", want);
            for (int i = 0; i < n; i++) fprintf(stderr, "  %s\n", ibv_get_device_name(list[i]));
            ibv_free_device_list(list);
            return NULL;
        }
    } else {
        dev = list[0];
    }
    fprintf(stderr, "[+] using device %s\n", ibv_get_device_name(dev));
    struct ibv_context *ctx = ibv_open_device(dev);
    ibv_free_device_list(list);
    if (!ctx) perror("ibv_open_device");
    return ctx;
}

// Transition our QP through INIT → RTR → RTS using the remote's qp_info.
// gid_index: pick the RoCE v2 entry (find with `show_gids`).
static inline int qp_to_rts(struct ibv_qp *qp, const struct qp_info *remote,
                            uint8_t port_num, uint8_t gid_index, uint32_t local_psn) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = 0;
    attr.port_num        = port_num;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        perror("modify QP to INIT"); return -1;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state              = IBV_QPS_RTR;
    attr.path_mtu              = IBV_MTU_4096;
    attr.dest_qp_num           = remote->qpn;
    attr.rq_psn                = remote->psn;
    attr.max_dest_rd_atomic    = 16;
    attr.min_rnr_timer         = 12;
    attr.ah_attr.is_global     = 1; // RoCE: GRH required
    attr.ah_attr.port_num      = port_num;
    attr.ah_attr.dlid          = remote->lid;
    attr.ah_attr.sl            = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.grh.hop_limit  = 1;
    attr.ah_attr.grh.sgid_index = gid_index;
    memcpy(&attr.ah_attr.grh.dgid, remote->gid, 16);
    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
        perror("modify QP to RTR"); return -1;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.timeout       = 14;
    attr.retry_cnt     = 7;
    attr.rnr_retry     = 7;
    attr.sq_psn        = local_psn;
    attr.max_rd_atomic = 16;
    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC)) {
        perror("modify QP to RTS"); return -1;
    }
    return 0;
}

static inline void print_gid(const uint8_t gid[16]) {
    for (int i = 0; i < 16; i++) {
        fprintf(stderr, "%02x", gid[i]);
        if (i % 2 == 1 && i != 15) fprintf(stderr, ":");
    }
}

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

#endif // EXCHANGE_H
