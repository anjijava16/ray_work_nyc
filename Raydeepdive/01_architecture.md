# 01 — Ray Architecture

A Ray cluster is two planes layered over standard Linux processes:

- **Control plane** — metadata, scheduling decisions, autoscaling, dashboard. Lives mostly on the **head node**.
- **Data plane** — Python workers, object store, network transfer. Lives on **every node** (head + workers).

This file builds the picture top-down: cluster → node → process.

---

## 1. Cluster topology

```text
                       ┌─────────────────────────────────────┐
                       │            RAY CLUSTER              │
                       └─────────────────────────────────────┘

   ┌───────────────────────────────┐      ┌───────────────────────────────┐
   │          HEAD NODE            │      │         WORKER NODE           │
   │  (exactly 1, always running)  │      │  (0..N, can autoscale)        │
   │                               │      │                               │
   │  ┌─────────────────────────┐  │      │                               │
   │  │  GCS (Global Control    │  │      │                               │
   │  │  Store) — cluster brain │  │      │                               │
   │  └─────────────────────────┘  │      │                               │
   │  ┌─────────────────────────┐  │      │  ┌─────────────────────────┐  │
   │  │  Raylet                 │◄─┼──────┼─►│  Raylet                 │  │
   │  │  (scheduler + node mgr) │  │      │  │  (scheduler + node mgr) │  │
   │  └─────────────────────────┘  │      │  └─────────────────────────┘  │
   │  ┌─────────────────────────┐  │      │  ┌─────────────────────────┐  │
   │  │  Plasma object store    │  │      │  │  Plasma object store    │  │
   │  │  (shared memory)        │  │      │  │  (shared memory)        │  │
   │  └─────────────────────────┘  │      │  └─────────────────────────┘  │
   │  ┌─────────────────────────┐  │      │  ┌─────────────────────────┐  │
   │  │  Worker processes (N)   │  │      │  │  Worker processes (N)   │  │
   │  │  — your Python code     │  │      │  │  — your Python code     │  │
   │  └─────────────────────────┘  │      │  └─────────────────────────┘  │
   │  ┌─────────────────────────┐  │      │                               │
   │  │  Driver (your script)   │  │      │                               │
   │  └─────────────────────────┘  │      │                               │
   │  ┌─────────────────────────┐  │      │                               │
   │  │  Dashboard (port 8265)  │  │      │                               │
   │  │  Autoscaler             │  │      │                               │
   │  └─────────────────────────┘  │      │                               │
   └───────────────────────────────┘      └───────────────────────────────┘
```

### Head node — singleton

The head node hosts everything that **must exist exactly once** for the cluster to function:

- **GCS** — cluster metadata, actor table, node table
- **Autoscaler** — adds/removes worker nodes based on resource demand
- **Dashboard** — port 8265 UI + REST API
- **(Optional) Driver** — if you run `python my_script.py` on the head

> If the head dies, the cluster is dead. There is no native HA in OSS Ray, though GCS can be backed by external Redis to survive restarts.

### Worker nodes — horizontally scalable

Worker nodes have **no head-only services**. They are pure execution units. They can be added or removed at will — the autoscaler does this automatically based on pending work.

Every node — head and worker — runs a **Raylet**, a **Plasma object store**, and a pool of **worker processes**.

---

## 2. Process-level view of a single node

```text
┌──────────────────────── ONE NODE (head or worker) ────────────────────────┐
│                                                                            │
│   ┌─────────────────┐                                                      │
│   │ Driver (option) │   ◄── your script with ray.init()                    │
│   └────────┬────────┘                                                      │
│            │ submits tasks via gRPC                                        │
│            ▼                                                               │
│   ┌─────────────────┐    gRPC     ┌─────────────────────────────────────┐  │
│   │     Raylet      │◄───────────►│    GCS (on head node only)          │  │
│   │  - local scheduler            │  - actor table                      │  │
│   │  - object mgr                 │  - node table                       │  │
│   │  - resource accounting        │  - resource map                     │  │
│   └────────┬────────┘             └─────────────────────────────────────┘  │
│            │ spawns / kills                                                │
│            ▼                                                               │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│   │ Worker proc 1   │  │ Worker proc 2   │  │ Worker proc N   │            │
│   │ (Python)        │  │ (Python)        │  │ (Python)        │            │
│   │ runs tasks      │  │ runs tasks      │  │ runs actors     │            │
│   └────────┬────────┘  └────────┬────────┘  └────────┬────────┘            │
│            │ shared-memory I/O via /dev/shm                                │
│            ▼                                                               │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │             Plasma Object Store  (shared memory)                  │   │
│   │   - holds ray.put() values, task return values                    │   │
│   │   - zero-copy reads for numpy/Arrow buffers                       │   │
│   │   - spills to disk under pressure                                 │   │
│   └──────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```

### Why this layering exists

| Layer | Job | Why it's separate |
|---|---|---|
| GCS | Cluster-wide metadata | Single source of truth; survives worker churn |
| Raylet | Per-node decisions | Local scheduling has low latency; offloads work from GCS |
| Worker | Run Python | Isolation — a crash doesn't bring down the node |
| Plasma | Hold objects | Shared memory means N workers read the same bytes once |

This is **hierarchical scheduling**: the Raylet handles most placements locally; only when it can't satisfy a task does it ask the GCS to find another node.

---

## 3. Control plane vs data plane

```text
            CONTROL PLANE                          DATA PLANE
        (small, frequent messages)         (large, throughput-sensitive)
       ─────────────────────────           ─────────────────────────
   ┌──────────────────────────┐         ┌──────────────────────────┐
   │  task submission         │         │  task arguments          │
   │  task completion         │         │  task return values      │
   │  actor lifecycle         │         │  ray.put() values        │
   │  resource updates        │         │  ray.get() reads         │
   │  heartbeats              │         │  cross-node object xfer  │
   └──────────────────────────┘         └──────────────────────────┘
            gRPC                              Plasma + gRPC bulk
```

> **Rule of thumb:** if it's a decision, it goes through Raylet/GCS. If it's bytes, it goes through Plasma.

---

## 4. Single-node vs multi-node cluster

When you run `ray.init()` with no arguments on your laptop, you get a **single-node cluster** where the head node and the only worker node are the same machine. Everything described above is still true — there's just one node hosting all of it.

When you run `ray start --head` and then `ray start --address=<head-ip>:6379` on other machines, those new machines become worker nodes that **register with the head's GCS** and start their own Raylet + Plasma + worker pool.

In **KubeRay**, the head node and each worker node are separate **Kubernetes pods**. The pod boundary is the node boundary.

---

## 5. The driver — special but not unique

The **driver** is the Python process running `ray.init()` and submitting work. Three important facts:

1. The driver is a **worker** from Ray's perspective — it has a worker ID, can hold ObjectRefs, and lives in the worker table.
2. The driver does **not** execute `@ray.remote` functions itself. It only submits them.
3. The driver **can run anywhere** — on the head node, on a worker node, or off-cluster (using `ray.init(address="ray://head:10001")`).

`ray job submit` is the production pattern: it ships your script to the head node and runs it as a job, isolating its lifecycle from your laptop.

---

## 6. Networking summary

| Port | Role | Where |
|---|---|---|
| 6379 | GCS Redis-compatible endpoint | Head |
| 10001 | Ray Client server | Head |
| 8265 | Dashboard UI + Jobs API | Head |
| 8000 | Ray Serve HTTP (default) | Head (proxy) |
| Random | Raylet → Raylet gRPC | All nodes |
| Random | Plasma store sockets | All nodes (Unix sockets) |

---

## Next

Read [02_components.md](./02_components.md) for a deep dive into each component listed above.
