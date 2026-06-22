// protocol.h — wire format between Go orchestrator and rdma-agent C daemon.
//
// Each request/response is:
//   [4-byte little-endian payload length][payload bytes]
//
// Payload is JSON. FDs travel as ancillary data alongside the request msg
// (SCM_RIGHTS), exactly as Linux Unix-domain sockets handle them.
//
// Commands (all keyed by "cmd"):
//
//   setup-source  { sandbox_id, size_bytes, gid_index, dev_name }  + memfd FD
//     → { ok: true, qp_info: { lid, qpn, psn, addr, rkey, gid_b64, tcp_port } }
//
//   setup-dest    { sandbox_id, size_bytes, gid_index, dev_name,
//                   remote: { lid, qpn, psn, addr, rkey, gid_b64, tcp_addr, tcp_port } }
//                 + memfd FD + uffd FD
//     → { ok: true }    (returns immediately; UFFD/RDMA threads run in agent)
//
//   status        { sandbox_id }
//     → { ok: true, role: "source"|"dest", state: "running"|"complete"|"error",
//          pages_total, pages_pulled, prefetch_done, last_error }
//
//   cleanup       { sandbox_id }
//     → { ok: true }    (joins threads, deregisters MR, closes QP/MR/UFFD)
//
//   shutdown      {}
//     → { ok: true }    (process exits after replying)
//
// Errors come back as { "ok": false, "error": "<message>" }.

#ifndef RDMA_AGENT_PROTOCOL_H
#define RDMA_AGENT_PROTOCOL_H

#include <stdint.h>

#define AGENT_DEFAULT_SOCKET "/var/run/e2b/rdma-agent.sock"

// Maximum JSON payload size. We never send raw page data over this socket
// (data flows over RDMA), so a few KB is plenty.
#define AGENT_MAX_PAYLOAD (16 * 1024)

#endif
