# 06 — Ray in Production

What changes when you move from `ray.init()` on your laptop to running Ray as cluster infrastructure.

---

## 1. KubeRay — the standard production deployment

KubeRay is an operator that runs Ray clusters as Kubernetes CRDs.

```text
                   ┌──────────────────────────────────────────────┐
                   │            Kubernetes Cluster                │
                   │                                              │
                   │  ┌──────────────────────────────────────┐    │
                   │  │      KubeRay Operator (pod)          │    │
                   │  │  watches RayCluster / RayJob /       │    │
                   │  │  RayService CRDs                     │    │
                   │  └──────────────────────────────────────┘    │
                   │                                              │
                   │  ┌──────────────────────────────────────┐    │
                   │  │      RayCluster CR (your spec)       │    │
                   │  │      ──────────────────────────      │    │
                   │  │      Head Pod (Deployment, 1)        │    │
                   │  │      Worker Pods (Deployment, N)     │    │
                   │  └──────────────────────────────────────┘    │
                   └──────────────────────────────────────────────┘
```

### Three CRDs

| CRD | Purpose |
|---|---|
| `RayCluster` | Long-running cluster you connect to |
| `RayJob` | One-shot: launch cluster → run script → tear down |
| `RayService` | Production serving with zero-downtime upgrades |

### Minimal RayCluster YAML

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: my-cluster
spec:
  rayVersion: 2.40.0
  headGroupSpec:
    rayStartParams:
      dashboard-host: 0.0.0.0
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.40.0
          resources:
            limits:
              cpu: 4
              memory: 8Gi
  workerGroupSpecs:
  - groupName: gpu-workers
    replicas: 0
    minReplicas: 0
    maxReplicas: 10
    rayStartParams:
      num-gpus: "1"
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.40.0-gpu
          resources:
            limits:
              nvidia.com/gpu: 1
              cpu: 8
              memory: 32Gi
```

### Submitting jobs

```bash
# Forward dashboard port
kubectl port-forward svc/my-cluster-head-svc 8265:8265

# Submit
ray job submit --address http://localhost:8265 \
  --runtime-env-json='{"pip": ["transformers"]}' \
  --working-dir . \
  -- python my_script.py
```

---

## 2. Autoscaling

Two layers stack up:

```text
   Ray Autoscaler          ◄── decides "I need 3 more worker pods with GPU=1"
        │
        ▼
   KubeRay Operator        ◄── translates to pod replica count change
        │
        ▼
   Kubernetes              ◄── schedules pods to nodes
        │
        ▼
   Cluster Autoscaler      ◄── adds new EC2 / GKE / AKS nodes if needed
```

### Tuning knobs

| Knob | What it controls |
|---|---|
| `maxReplicas` | Cap on worker pods |
| `idleTimeoutSeconds` | When idle worker scales down |
| `upscalingMode` (Default/Aggressive/Conservative) | How fast to add pods |
| Cluster autoscaler's `scale-down-delay-after-add` | Avoid flapping |

### Common pitfall

Autoscaler reacts to **pending tasks with resource constraints it can't satisfy**. If your tasks say `num_cpus=1` and the cluster has free CPU but no idle worker, the autoscaler won't act. Make sure your resource requests are realistic.

---

## 3. Observability

Three layers:

### 3.1 Ray Dashboard (port 8265)

- Task timeline, actor list, worker memory
- Logs viewer (streams from each worker)
- Serve dashboard tab — replicas, latency, QPS

### 3.2 Metrics (Prometheus)

Ray exposes Prometheus metrics on port 8080 by default:
- `ray_tasks_total`
- `ray_actors_alive`
- `ray_object_store_memory_used_bytes`
- `ray_node_cpu_utilization`

Standard pattern: scrape these into Prometheus → Grafana dashboards.

### 3.3 Distributed tracing

Set `RAY_PROFILING=1` to enable per-task spans exported in Chrome tracing JSON. Open in `chrome://tracing` or Perfetto.

For production, integrate OpenTelemetry inside your application code — Ray doesn't auto-instrument user code with OTel.

---

## 4. Logging

Each worker writes to `/tmp/ray/session_latest/logs/`:
- `worker-{id}.out`
- `worker-{id}.err`
- `raylet.out`
- `gcs_server.out`

In KubeRay, sidecar log shippers (Fluent Bit, Vector) tail these files and ship to Loki/CloudWatch/Datadog.

> Always include `worker_id` / `actor_id` in your structured logs — without them you can't trace a request across replicas.

---

## 5. Common production failures

### 5.1 Object store OOM

```
The actor died because of memory pressure (OOMKilled)
The object's owner has exited.
```

Causes:
- Plasma full → spilling → disk full → cascade
- Driver builds huge result lists and never frees

Fix:
- `--object-store-memory=<bytes>` larger
- `del refs` aggressively
- Stream with `ray.wait()` instead of accumulating

### 5.2 Task explosion

Submitting millions of tasks at once → scheduler queue grows unbounded → GCS memory spikes.

Fix: batch + backpressure.

```python
pending = []
for x in big_iter:
    pending.append(f.remote(x))
    if len(pending) >= 1000:
        done, pending = ray.wait(pending, num_returns=500)
        ray.get(done)
```

### 5.3 Head pod OOM

GCS in-memory state can grow with millions of objects / actors. Symptoms: dashboard slow, autoscaler stops, eventually head pod restarts and the whole cluster dies.

Fix:
- Use external Redis for GCS so head can restart without data loss
- Smaller object counts (batch ETL into bigger blocks)
- Disable verbose actor tracking with `RAY_record_ref_creation_sites=0`

### 5.4 GPU fragmentation

Many small inference replicas across many nodes → most GPUs partially used → can't schedule a job that needs a whole node.

Fix:
- Placement groups with `STRICT_PACK`
- Dedicated node groups for training vs serving

### 5.5 Slow serialization

Symptom: tasks spend 80% of time in pickle, not in your code. Cause: passing pandas DataFrames with object dtype, deep Python class hierarchies, or NumPy `object` arrays.

Fix:
- Convert to Arrow before passing
- Use `ray.put()` once for static data
- Profile with `RAY_PROFILING=1`

---

## 6. Security

OSS Ray's default mode is **no auth, no encryption**. This is fine inside a Kubernetes network policy but disastrous if exposed to the public internet.

| Concern | Mitigation |
|---|---|
| Network exposure | Use ClusterIP services + ingress with auth |
| Code execution from job submission | Restrict Jobs API with auth proxy |
| Multi-tenant isolation | One Ray cluster per tenant; runtime envs not a security boundary |
| Secrets in env vars | Use Kubernetes Secret refs in pod spec, not `runtime_env.env_vars` |

Ray on Anyscale / Anyscale Hosted adds proper auth, network isolation, RBAC.

---

## 7. Rolling updates with RayService

`RayService` does blue/green:

1. Submit a new RayService spec with `serveConfigV2` changes.
2. Operator boots a **new cluster** with the new code.
3. Once healthy, switches the K8s Service selector to point at it.
4. Drains traffic from the old cluster, deletes it.

This is the production-correct way to ship a new model — never `kubectl apply` an edit to a running RayCluster.

---

## 8. Cost optimization

- **Spot / preemptible workers**: Ray's lineage recovery handles node loss gracefully — use spot for batch jobs.
- **Heterogeneous worker groups**: cheap CPU group for preprocessing, GPU group for inference.
- **Scale-to-zero workers**: set `minReplicas: 0`; head pod alone is cheap.
- **Right-size object store**: 30% of pod memory is the default; tune based on workload.

---

## 9. The promotion path

Typical journey from notebook to production:

```text
1. ray.init() in a notebook
       ↓
2. Local script with ray start --head
       ↓
3. ray job submit to a long-running RayCluster
       ↓
4. RayJob CRD for batch (auto teardown)
       ↓
5. RayService CRD for online inference (blue/green)
```

Each step adds isolation and reduces "works on my machine" risk.

---

## Next

[07_end_to_end_example.md](./07_end_to_end_example.md) — a worked example combining the pieces.
