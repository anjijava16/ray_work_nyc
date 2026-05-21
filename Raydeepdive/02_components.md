# 02 — Ray Components in Detail

This file zooms into each component of the cluster. The components are not "modules in a Python package" — they are **separate OS processes** that communicate over gRPC and shared memory.

---

## 1. GCS — Global Control Store

**One per cluster. Lives on the head node.**

The GCS is Ray's metadata service. Think of it as the "control plane database":

```text
┌─────────────────────────────────────┐
│              GCS Tables             │
├─────────────────────────────────────┤
│ node_table       — all nodes        │
│ actor_table      — every named/anon │
│                    actor + owner    │
│ worker_table     — every worker     │
│ job_table        — driver jobs      │
│ resource_table   — CPU/GPU/custom   │
│ placement_group  — co-location reqs │
│ kv_store         — generic K/V      │
└─────────────────────────────────────┘
```

### What goes through GCS

- New node registration ("hello, I have 8 CPUs and 1 GPU")
- Actor creation/death events
- Driver job lifecycle
- Heartbeats from every node (default 1 Hz)
- Cluster-wide resource view used by the autoscaler

### What does NOT go through GCS

- Task scheduling decisions (Raylet does these)
- Object data (Plasma does this)
- Routine task submission (driver → Raylet directly)

### Persistence

By default GCS is in-memory and dies with the head pod. In production you mount an **external Redis** (`RAY_REDIS_ADDRESS`) so the GCS can be restored if the head pod restarts. This is what enables KubeRay "head pod recovery."

---

## 2. Raylet — the per-node manager

**One per node.** Written in C++. It's the most important data-plane process you'll never see directly.

The Raylet is three things in one process:

### 2.1 Local scheduler

When the driver calls `f.remote(1)`, the task is sent to the **driver's Raylet**. That Raylet:

1. Looks at the task's resource requirements (`num_cpus=1`).
2. Checks local resources. If they fit, it picks a local idle worker and dispatches.
3. If not, it asks GCS for a node that has capacity and forwards the task to **that node's Raylet**.

```text
   Driver          Raylet (local)        Raylet (remote)       Worker
     │   f.remote   │                       │                    │
     ├─────────────►│                       │                    │
     │              │ local resources?      │                    │
     │              │ no — forward          │                    │
     │              ├──────────────────────►│                    │
     │              │                       │ assign worker      │
     │              │                       ├───────────────────►│
     │              │                       │                    │ exec
     │              │                       │                    │
     │              │                       │◄───────────────────│
     │              │ obj ref returned      │                    │
     │◄─────────────┤                       │                    │
```

### 2.2 Object manager

The Raylet tracks which objects live in **its own Plasma store** and arranges transfers when a remote worker needs them. Cross-node object transfer is Raylet → Raylet gRPC pulling bytes out of Plasma.

### 2.3 Worker lifecycle manager

Workers don't auto-start. The Raylet:

- Pre-spawns a pool (default = number of CPUs)
- Spawns new workers on demand for actors
- Kills idle workers when memory is tight
- Restarts crashed workers (and reports lineage for retries)

---

## 3. Plasma — the shared-memory object store

**One per node.** Originally a separate project (Apache Arrow's Plasma), now built into Ray.

```text
Worker A                Plasma (shared memory mmap'd /dev/shm)               Worker B
   │   ray.put(arr)          │                                                  │
   ├────────────────────────►│ allocate buffer, copy bytes once                 │
   │  ◄── ObjectRef ─────────┤                                                  │
   │                         │                                                  │
   │                         │   ◄────── ray.get(ref) (zero-copy mmap) ────────┤
   │                         │                                                  │
```

### Why shared memory matters

| Without Plasma | With Plasma |
|---|---|
| pickle → socket → unpickle | mmap pointer |
| O(N) copies per consumer | 1 copy total |
| 100 MB tensor = 100 MB per reader | 100 MB total |

For numpy arrays, Arrow buffers, and most ML tensors, `ray.get()` returns a **read-only view into Plasma memory** — no deserialization happens.

### Object spilling

When Plasma fills up, Ray spills cold objects to local disk (`/tmp/ray/session_*/spilled_objects/`). The Raylet keeps a directory of what's in memory vs on disk and pulls back on demand.

### Object lifetime

An object lives as long as some ObjectRef references it. When the last ref is dropped (garbage-collected in Python), the Raylet eventually evicts the object from Plasma.

---

## 4. Worker processes

**Many per node.** These are the Python processes that actually run your code.

Two flavors:

| Flavor | Lifetime | Spawned by |
|---|---|---|
| Task worker | Per task or recycled | Raylet, on demand |
| Actor worker | Lifetime of the actor | Raylet, when actor is created |

Each worker:

- Talks to its local Raylet over gRPC for task fetching and result reporting
- Talks to its local Plasma over Unix socket for object I/O
- Imports your code (the **runtime env** ships any dependencies)

### Driver = special worker

The driver process is just a worker with two privileges:

1. It can submit tasks (regular workers can too — task chaining)
2. It owns the job; when it exits, all detached-but-job-scoped actors die

---

## 5. Autoscaler

**Runs as a thread inside the head pod.**

Loop:
1. Read pending tasks + idle resources from GCS.
2. Compute the resource demand.
3. Compare against the running node count.
4. Call the cloud provider (or Kubernetes via KubeRay) to add/remove nodes.

Configured via `cluster.yaml` (VM mode) or `RayCluster` CRD (KubeRay mode).

---

## 6. Dashboard

**Process on head node, port 8265.** A FastAPI app + React UI.

It exposes:

- `/api/jobs` — Ray Jobs CLI hits this
- `/api/v0/...` — internal cluster state
- Real-time task timeline, actor list, worker memory, log viewer

> Critical insight: the dashboard reads from GCS — it's **read-only** for cluster state. Killing the dashboard does not affect running jobs.

---

## 7. Putting it all together — a "where does each thing live" cheat sheet

| Thing | Process | Host |
|---|---|---|
| Your `@ray.remote def f` code | Worker | Any node |
| `ray.put(x)` value | Plasma | Node that called put |
| Actor `self.state` | Worker (Python heap) | Node where actor was placed |
| Pending task queue | Raylet | Owning node |
| Cluster's list of nodes | GCS | Head |
| Cluster's list of actors | GCS | Head |
| Resource accounting | Raylet (local) + GCS (cluster) | Both |
| Task DAG / lineage | Owner worker | Wherever the caller lives |

---

## 8. Lineage and ownership

Each ObjectRef has an **owner** — usually the worker that submitted the task. The owner tracks:

- The function and arguments (so it can replay)
- All references to the object across the cluster
- Where the object currently lives

If a node holding the object dies, the owner re-submits the task to a different node. This is **lineage-based reconstruction** and is the basis of Ray's fault tolerance.

If the **owner** dies (e.g., the driver crashed), the object is gone forever — there's no one to rebuild it.

---

## Next

Read [03_flow.md](./03_flow.md) to walk through a task end-to-end across these components.
