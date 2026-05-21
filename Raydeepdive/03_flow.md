# 03 — End-to-End Flow

Tracing what happens when you call `f.remote(x)` and `ray.get(ref)`, across processes.

---

## 1. The 30-second version

```text
1. Driver calls f.remote(x)
       │
2. Local Raylet receives task spec
       │
3. Raylet finds a node with capacity (local or remote)
       │
4. Target Worker pulls task, runs f(x)
       │
5. Worker stores return value in Plasma
       │
6. Driver calls ray.get(ref)
       │
7. Plasma serves the bytes (zero-copy if possible)
```

The rest of this file expands each step.

---

## 2. Step-by-step trace of a simple task

```python
import ray
ray.init()

@ray.remote
def square(x):
    return x * x

ref = square.remote(7)
print(ray.get(ref))   # 49
```

### Step 1 — `ray.init()`

- Starts a head node locally (if not already running)
- Boots: GCS, Raylet, Plasma, dashboard, autoscaler thread
- Connects the driver as a worker, gets a worker ID
- Registers the driver in GCS `worker_table`

### Step 2 — `@ray.remote` (decoration time)

- `square` becomes a `RemoteFunction` object
- Its source code is **cloudpickled** lazily on first `.remote(...)` call
- The pickled function is **exported to GCS** so any node can fetch it

### Step 3 — `square.remote(7)`

The driver's worker:

1. Builds a **task spec** (function ID, args, resource req, owner = driver)
2. Picks an ID for the future return value (`ObjectRef`)
3. Hands the task spec to its **local Raylet** over gRPC
4. Returns the `ObjectRef` to your Python code **immediately** (non-blocking)

### Step 4 — Raylet places the task

The local Raylet:

1. Looks at the task's resources (`num_cpus=1` default).
2. Checks if a local **idle worker** can take it.
   - If yes → assign immediately.
   - If no → check resources from GCS, pick a remote node, forward the task to that node's Raylet.

### Step 5 — Worker executes

The selected worker:

1. Receives the task spec.
2. If it hasn't seen `square` before, downloads the pickled function from GCS.
3. Fetches arguments from Plasma (here `7` is so small it's inlined into the task spec — no Plasma round-trip).
4. Runs `square(7)`.

### Step 6 — Worker stores return value

- The return value `49` is **inlined** in the task reply because it's a tiny int.
- For larger values, the worker would `plasma.put(value)` and return only the ObjectRef.

### Step 7 — `ray.get(ref)`

The driver:

1. Blocks until the object is available.
2. Reads from Plasma (or accepts the inlined return value).
3. Deserializes and returns the Python value.

For numpy arrays, step 7 is a **zero-copy mmap** — no deserialization, just a pointer.

---

## 3. Sequence diagram (with one remote node)

```text
 Driver        Local-Raylet      GCS         Remote-Raylet     Remote-Worker     Plasma(remote)
   │                │             │                │                │                  │
   │ f.remote(x)    │             │                │                │                  │
   ├───────────────►│             │                │                │                  │
   │                │ local full? │                │                │                  │
   │                ├────query───►│                │                │                  │
   │                │◄ where? ────┤                │                │                  │
   │                ├─forward task──────────────► │                │                  │
   │                │             │                │ assign worker  │                  │
   │                │             │                ├───────────────►│                  │
   │                │             │                │                │ run f(x)         │
   │                │             │                │                │ put result       │
   │                │             │                │                ├─────────────────►│
   │                │             │                │ ack done       │                  │
   │                │◄────────────┤                │◄───────────────│                  │
   │                │                                                                  │
   │ ray.get(ref)   │                                                                  │
   ├───────────────►│ resolve location                                                 │
   │                │ pull object across nodes ───────────────────────────────────────►│
   │                │◄────────────────────────── bytes ─────────────────────────────── │
   │◄── 49 ─────────│                                                                  │
```

---

## 4. What changes for an Actor

```python
@ray.remote
class Counter:
    def __init__(self):
        self.n = 0
    def inc(self):
        self.n += 1
        return self.n

c = Counter.remote()
ray.get(c.inc.remote())
```

Differences from a task:

1. `Counter.remote()` creates an **actor handle** and reserves a worker process for the actor's lifetime.
2. The actor is registered in the GCS `actor_table` (so other workers can look it up by handle).
3. `c.inc.remote()` is **routed only to that one worker** — no scheduling choice.
4. The actor's worker process **never recycles between calls** — `self.n` persists.
5. If the actor's node dies, by default the actor is gone; with `max_restarts=N` Ray will reincarnate it.

---

## 5. What changes for a chained task (DAG)

```python
a = step1.remote(10)
b = step2.remote(a)   # passing an ObjectRef as argument
```

When the driver submits `step2`:

1. `a` is passed as an `ObjectRef`, not a value.
2. The Raylet sees `step2` depends on `a` — it **waits** to dispatch `step2` until `a` is available.
3. When `step1` finishes, the Raylet that receives the result notifies dependents.
4. `step2` is then placed, ideally on the same node where `a` lives (data locality).

This is the **task DAG** — built implicitly from ObjectRef arguments.

---

## 6. Memory flow for big objects

```python
import numpy as np
arr = np.zeros((1000, 1000))   # 8 MB
ref = ray.put(arr)             # stored in driver's local Plasma
# Plasma has 1 copy of the 8MB buffer.

@ray.remote
def consume(x):
    return x.sum()

ray.get(consume.remote(ref))
```

What happens:

1. `ray.put(arr)` → driver Plasma store now holds the buffer; returns `ref`.
2. `consume.remote(ref)` → task spec says "input = obj X".
3. Raylet places task on a worker. If the worker is on the **same node**, it mmaps the buffer (zero-copy).
4. If the worker is on a **different node**, the remote Raylet pulls the object across the network into its local Plasma, then the worker mmaps it.
5. The task runs; result is small int, returned inline.

> Key insight: large objects move at most **once per node**, not once per consumer.

---

## 7. Failure flow — node dies mid-task

```python
ref = long_task.remote()   # running on worker node W
# ... node W dies ...
ray.get(ref)               # what happens?
```

1. Owner (driver) detects node W is dead via GCS heartbeat loss.
2. Owner re-submits the task to a healthy node, **as long as the original task spec is still in the owner's lineage map**.
3. New worker runs `long_task`, stores result.
4. `ray.get(ref)` returns the new result.

If `long_task` had `max_retries=0` or the **owner** died, you get `RayActorError` / `ObjectLostError`.

---

## 8. ray.wait — non-blocking results

```python
refs = [task.remote(i) for i in range(100)]
done, pending = ray.wait(refs, num_returns=10, timeout=2.0)
```

`ray.wait`:

- Returns as soon as `num_returns` refs are ready, or `timeout` elapses.
- Lets you **stream** results instead of blocking on all of them.

This is the right pattern for high-throughput pipelines — pull finished work, submit more.

---

## 9. The full lifecycle of an ObjectRef

```text
created ────► pending ────► local ────► [maybe remote] ────► consumed ────► evicted
   │             │             │              │                   │             │
   │             │             │              │                   │             └─ refcount hit zero
   │             │             │              │                   └─ ray.get() reads it
   │             │             │              └─ pulled into another node's Plasma
   │             │             └─ value materialized in some Plasma
   │             └─ task in scheduler queue
   └─ .remote() returned the ref
```

---

## Next

[04_services.md](./04_services.md) — what each Ray library (Core, Data, Train, Tune, Serve) gives you on top of the flow above.
