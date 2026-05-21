# 05 — Ray Internals

What's under the hood: scheduling, object store, serialization, fault tolerance.

---

## 1. Hierarchical scheduling

Ray uses a two-level scheduler:

```text
         GCS (cluster view)               ◄── consulted only when local can't satisfy
          ▲
          │ slow path
          │
   ┌──────┴──────┐
   │  Raylet     │  ◄── fast path (local placement)
   │  (per node) │
   └─────────────┘
```

### Local fast path

Each Raylet maintains:
- Local resource counters (free CPUs/GPUs/custom)
- A queue of pending tasks
- A pool of idle workers

If a task's resource requirements fit the local node and a worker is available, the Raylet **dispatches immediately** without consulting any other process. This is sub-millisecond.

### Distributed slow path

When local resources are insufficient:
1. Raylet queries GCS for nodes with capacity.
2. Picks one based on locality (closer to task's input objects) and load.
3. Forwards the task spec.
4. Receiving Raylet enqueues the task locally.

### Scheduling policies

Configurable via `@ray.remote(scheduling_strategy=...)`:

| Strategy | Behavior |
|---|---|
| `DEFAULT` | Spread load, prefer locality |
| `SPREAD` | Force tasks onto different nodes |
| `NodeAffinitySchedulingStrategy` | Pin to a specific node |
| `PlacementGroupSchedulingStrategy` | Use reserved resource bundle |

---

## 2. Placement Groups

A **placement group** reserves a set of resource "bundles" before scheduling. Used when you need multiple tasks/actors to be **co-located** or **gang-scheduled**.

```python
from ray.util.placement_group import placement_group

pg = placement_group(
    bundles=[{"CPU": 1, "GPU": 1}] * 4,
    strategy="STRICT_PACK",   # all bundles on same node
)
ray.get(pg.ready())

@ray.remote(num_cpus=1, num_gpus=1)
def worker(): ...

for i in range(4):
    worker.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(pg, i)
    ).remote()
```

### Strategies

| Strategy | Meaning |
|---|---|
| `PACK` | Same node if possible, else spread |
| `STRICT_PACK` | Must be same node, else fail |
| `SPREAD` | Different nodes if possible |
| `STRICT_SPREAD` | Must be different nodes |

This is how Ray Train sets up DDP — gang-schedules N workers, one per GPU, ensuring NCCL connectivity.

---

## 3. The object store deep dive

### Object size handling

| Size | Path |
|---|---|
| ≤ 100KB (inline threshold) | Inlined into task spec/reply — no Plasma |
| > 100KB | Stored in Plasma, ref returned |
| > 80% of object_store_memory | Spilled to disk |

You can configure the inline threshold with `RAY_max_direct_call_object_size` and store size with `--object-store-memory` at `ray start`.

### Zero-copy reads

For Arrow-compatible buffers (numpy, pandas via Arrow, raw bytes), `ray.get()` returns a **read-only mmap** into Plasma's shared memory. No deserialization, no copy. This is how Ray moves multi-GB tensors essentially for free between processes on one node.

```python
arr = np.zeros((10000, 10000), dtype=np.float32)  # ~400 MB
ref = ray.put(arr)

@ray.remote
def consume(x):
    # x is a view into Plasma — read-only
    return x.shape

ray.get(consume.remote(ref))  # no copy made
```

If you need to **write** to the array inside the task, copy it first (`x = x.copy()`) or you'll get a read-only-buffer error.

### Object spilling

When Plasma is full:
1. Ray picks the least-recently-used objects.
2. Writes them to local disk (configurable: `--temp-dir`).
3. Updates the directory so the next `ray.get()` will pull from disk → mem.

Spilling kills throughput. The fix is either:
- Larger `object_store_memory`
- Smaller batches
- Free refs sooner (`del ref`)

---

## 4. Serialization

Ray uses three serializers in order of preference:

1. **Arrow** — for numpy, pandas, pyarrow tables → zero-copy
2. **Pickle 5 out-of-band** — for numpy that's not Arrow-friendly
3. **cloudpickle** — for everything else (functions, classes, closures)

### What serializes well

- Pure Python objects (dicts, lists, ints)
- Numpy arrays, pandas frames, Arrow tables
- PyTorch tensors (on CPU)
- Most ML model weights

### What serializes badly

| Object | Why |
|---|---|
| Open file handles | OS resource, can't move |
| Database connections | Same |
| Thread locks | OS primitive |
| Generators | State not picklable |
| Lambdas with closures over big globals | Closes over too much |
| CUDA tensors | Need explicit `.cpu()` before transfer |

When in doubt, do `ray.cloudpickle.dumps(obj)` in a Python shell — if it works there, Ray will handle it.

---

## 5. Lineage and fault tolerance

Each ObjectRef has an **owner** — the worker that submitted the task that created it.

The owner remembers:

```text
ref_X
 ├── creating_task   : (function_id, args, resource_req)
 ├── locations       : {nodeA, nodeC}
 ├── refcount        : 4
 └── deps            : [ref_Y, ref_Z]
```

### Recovery

If `nodeA` dies and `ref_X` was only there:
1. Owner detects via GCS (heartbeat timeout, default ~30s).
2. Owner re-submits `creating_task` to another node.
3. The task re-runs and produces a new copy of the object.

This is **lineage-based reconstruction**. It's what allows Ray clusters to lose worker nodes mid-job and keep going.

### Limits

- If the **owner dies**, the object is permanently lost (no one to replay).
- Tasks must be **deterministic** for replay to be valid. Non-deterministic tasks (e.g., random sampling without seeding) can produce different results on retry.
- Side effects (writing to a database) **get repeated** on retry. Make them idempotent.

### Actor fault tolerance

```python
@ray.remote(max_restarts=3, max_task_retries=2)
class Server:
    ...
```

- `max_restarts` — how many times Ray restarts the actor if its process dies
- `max_task_retries` — how many times Ray retries a method call if the actor restarts mid-call

State is **not** automatically preserved across restarts. You need to checkpoint manually (often by saving to a `ray.put()`-backed object or external storage).

---

## 6. Resource accounting

Resources in Ray are **logical**, not enforced. Declaring `num_cpus=2` tells the scheduler "this task should be placed where 2 CPUs are free" — it does **not** confine your code to 2 CPU cores at the OS level.

### Built-in resources

- `CPU` — defaults to physical CPU count of the node
- `GPU` — defaults to detected CUDA devices
- `memory` — heap memory in bytes
- `object_store_memory` — Plasma allocation

### Custom resources

```bash
ray start --head --resources='{"TPU": 4, "fast_disk": 1}'
```

```python
@ray.remote(resources={"TPU": 1})
def f(): ...
```

Useful for modeling heterogeneous clusters (e.g., "this node has a fast NVMe", "this node has Inferentia accelerators").

### Fractional GPUs

```python
@ray.remote(num_gpus=0.5)
class Replica: ...
```

Two replicas share one physical GPU. Ray won't isolate them — it just lets two actors land on the same GPU. Useful for small models.

---

## 7. The runtime environment

`runtime_env` lets you ship different Python dependencies per task/actor:

```python
@ray.remote(runtime_env={
    "pip": ["torch==2.1.0", "transformers"],
    "env_vars": {"HF_HOME": "/data/hf"},
})
def f(): ...
```

How it works:
- The Raylet builds a Conda env or venv per unique `runtime_env` hash.
- Workers that need it bind-mount or activate the env before starting Python.
- Subsequent tasks with the same env reuse it.

This is critical for **multi-tenant clusters** where different jobs need different versions of libraries.

---

## 8. Object lifetime and refcounting

Ray uses **distributed reference counting** to know when to evict an object:

```text
when ray.put returns ref:    refcount = 1
ref copied to another node:  refcount = 2
ref goes out of scope locally: refcount = 1
last ref dropped:            refcount = 0  → eligible for eviction
```

Workers periodically report ref counts to the owner. When the count hits zero, the owner tells all Raylets to drop the object from Plasma.

### Pinning

`ray.put(value, _pin=True)` (internal) — prevents eviction. Used by Ray Data to keep block boundaries alive.

Detached actors:
```python
@ray.remote
class S: ...
S.options(name="my_actor", lifetime="detached").remote()
```
A detached actor outlives its creating job. Manual cleanup required.

---

## 9. Anti-patterns to avoid

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| Tiny tasks (`@ray.remote def add1(x): return x+1`) | Scheduling overhead > work | Batch into chunks |
| `for x in xs: ray.get(f.remote(x))` | Sequentializes work | `ray.get([f.remote(x) for x in xs])` |
| Submitting millions of refs upfront | Object store / scheduler OOM | Use `ray.wait()` + backpressure |
| Passing huge objects by value | Repeated serialization | `ray.put()` once, pass ref |
| Reloading models per task | Slow + GPU memory churn | Use an actor to hold the model |
| `ray.get` inside a remote task | Blocks worker, deadlock risk | Pass refs through; let DAG resolve |

---

## Next

[06_production.md](./06_production.md) — KubeRay, autoscaling, observability, real failures.
