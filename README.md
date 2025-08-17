# Mini‑etcd

A **Go**‑based, Raft‑backed key‑value store that re‑implements the **core of etcd**, focus on Raft consensus, WAL +
snapshot persistence, MVCC state machine, linearizable KV & Watch API.

---

## 🚦Project Phases

| Phase                         | Target                                     | Deliverables                                         |
|-------------------------------|--------------------------------------------|------------------------------------------------------|
| **0. Local KV (DONE)**        | Fast in‑memory B‑Tree with MVCC            | `PUT / GET / DELETE / RANGE / WATCH` over gRPC       |
| **1. Durability (DONE)**      | **WAL** + **snapshots** for crash recovery | Automatic replay at startup                          |
| **2. High Availability(WIP)** | **Single Raft Group** (3–5 nodes)          | Leader election, log replication, strong consistency |
| **3. Production Polish**      | CLI, Web UI                                | Ops‑friendly experience                              |

> **Why this order?**Build a correct single‑node database → make it durable → replicate via Raft → refine UX & security.

---

## 🏗️ Architecture

```
           ┌──────────────┐  RPC (HTTP/2)
 client ──▶│  gRPC Client │  PUT / GET / DELETE / RANGE / WATCH
           └──────┬───────┘
                  │ linearizable‑read hits **Leader**
                  ▼
        ┌──────────────────────────────┐
        │         mini‑etcd Node       │   (all nodes run identical stack)
        │                              │
        │  ┌──────────────┐            │   Watch fan‑out
        │  │   KvServer   │◄───┐       │
        │  └─────┬────────┘    │       │
        │        ▼  apply      │       │  revision → event
        │  ┌──────────────┐    │       │
        │  │   B‑Tree     │    │       │  MVCC state‑machine
        │  └──────────────┘    │       │
        │        ▲ snapshot    │       │
        │  ┌──────────────┐    │       │
        │  │     WAL      │────┘       │   append + fsync
        │  └──────────────┘            │
        │        ▲ Raft log            │
        │  ┌──────────────┐            │
        │  │  Raft Node   │────────────┼─── replicate entries to followers
        │  └──────────────┘            │
        └──────────────────────────────┘
```

* **Full replication, single Raft group.**  Each node maintains the **entire** key‑space; quorum replication guarantees
  strong consistency & fault‑tolerance (2F+1 → tolerate F failures).
* **Write path**: client → Leader → Raft **append** → WAL **fsync** → **commit** → apply to B‑Tree → notify watchers.
* **Read path**:
    * **Linearizable** (default): go to Leader (or use ReadIndex) so that `rev ≤ commitIndex`.
    * **Serializable** (opt‑in): issue to any follower—may lag by a few entries.
* **Watch**: each event carries `modRevision == raftIndex`; consumers can resume seamlessly after reconnect.

---

## ✨ Current Feature Set (Phase 0 & Phase 1)

* **Google B‑Tree** (degree 64) → *O(logn)* read/write
* **MVCC** with global `revision` & per‑key versioning
* **gRPC v3‑subset** (`Put / Range / DeleteRange / Watch`)
* **Concurrency** via `sync.RWMutex` + goroutines; non‑blocking watch fan‑out
* **Deterministic state‑machine** interface ready for Raft integration
* **Write‑Ahead Log (WAL)** – protobuf‑framed, `fsync` on commit
* **Snapshots & Compaction** – periodic full dump, WAL truncation, defrag API
* **Crash Recovery** – snapshot load → WAL replay → serve

---

## 🔜 Near‑Term TODO (Phase 2 & 3)

* **Single‑Group Raft** – integrate `raft`; election, log replication, ReadIndex path
* **Health & Metrics** – `/health`, Prometheus counters
* **Chaos Tests** – kill‑9, network partition; target failover <2 s

---

## 🚀 Quick Start (TODO)

```bash
# Run single‑node dev server
mini-etcd --data=data/default --listen=:2379

# Put / Get via CLI (sample)
mini-etcdctl --endpoint localhost:2379 put foo bar
mini-etcdctl --endpoint localhost:2379 get foo
```

**3‑node cluster** once Raft is ready:

```bash
mini-etcd --name n1 --listen=:2380 --peers=n1:2380,n2:2381,n3:2382 &
mini-etcd --name n2 --listen=:2381 --peers=n1:2380,n2:2381,n3:2382 &
mini-etcd --name n3 --listen=:2382 --peers=n1:2380,n2:2381,n3:2382 &
```

---

## 🛠️ Core Tech Stack

* **Go 1.22+** – goroutines, generics, low‑level `sync` primitives
* **gRPC & Protocol Buffers** – compact binary RPC over HTTP/2
* **Google B‑Tree** – in‑memory ordered storage engine
* **raft** – consensus & replication layer
* **WAL + Snapshots** – durability, crash recovery
* **Prometheus & OpenTelemetry** – metrics and tracing
* **Docker** – reproducible dev & CI environment

---

## ❤️ Contributing

PRs & issues welcome—especially around Raft integration, WAL format, and snapshot tuning.

---

## License

MIT
