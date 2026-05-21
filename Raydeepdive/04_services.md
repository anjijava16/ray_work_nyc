# 04 — Ray Services (Core, Data, Train, Tune, Serve)

Ray Core is the foundation: tasks, actors, object store. The "AI libraries" — Data, Train, Tune, Serve — are higher-level abstractions implemented on top of Core. You can mix and match them in one app.

```text
              ┌────────────────────────────────────────────────────┐
              │              Your Application                      │
              └────────────────────────────────────────────────────┘
              ┌──────────┬──────────┬──────────┬──────────────────┐
              │ Ray Data │ Ray Train│ Ray Tune │  Ray Serve       │
              │  ETL +   │ Distrib. │ HPO      │  Online serving  │
              │  batch   │ training │ search   │  + autoscaling   │
              └──────────┴──────────┴──────────┴──────────────────┘
              ┌────────────────────────────────────────────────────┐
              │   Ray Core  — tasks, actors, object store, sched.  │
              └────────────────────────────────────────────────────┘
              ┌────────────────────────────────────────────────────┐
              │   Cluster runtime  — GCS, Raylet, Plasma, workers  │
              └────────────────────────────────────────────────────┘
```

---

## 1. Ray Core

The primitives. Everything else builds on these.

```python
import ray
ray.init()

@ray.remote
def f(x): return x * 2

@ray.remote
class Counter:
    def __init__(self): self.n = 0
    def inc(self): self.n += 1; return self.n

# tasks
refs = [f.remote(i) for i in range(10)]
print(ray.get(refs))

# actors
c = Counter.remote()
print(ray.get(c.inc.remote()))

# explicit object store
big = ray.put([0]*1_000_000)
print(ray.get(big)[:5])
```

**Use Core directly when:** you want full control over placement, custom DAGs, or building your own higher-level system.

---

## 2. Ray Data

A distributed dataset abstraction for batch ETL and offline inference. Thinks in terms of **blocks** (Arrow tables) that get processed in parallel across workers.

```python
import ray

ds = ray.data.read_parquet("s3://bucket/events/")
ds = ds.filter(lambda r: r["amount"] > 0)
ds = ds.map_batches(lambda batch: enrich(batch), batch_format="pandas")
ds.write_parquet("s3://bucket/out/")
```

### Key methods

| Method | What it does |
|---|---|
| `read_parquet / read_csv / read_text` | Lazy ingestion |
| `map / map_batches` | Per-row or per-batch transform |
| `filter` | Drop rows |
| `groupby` | Aggregation |
| `random_shuffle` | Shuffle across workers |
| `iter_batches` | Streaming consumption |
| `write_parquet / write_csv` | Sink |

### How it works underneath

- Each block is an Arrow table (default ~512MB).
- `map_batches` spawns one task per block, scheduled by Ray Core.
- For inference workloads, you pass an **actor class** to `map_batches(compute=ray.data.ActorPoolStrategy(...))` so a model loads once and processes many batches.

### When to use

- Multi-TB ETL that doesn't fit in pandas
- Batch inference (LLM, embedding, scoring)
- Feeding Ray Train with a distributed dataset

---

## 3. Ray Train

Distributed training abstraction. Wraps PyTorch, TensorFlow, XGBoost, LightGBM so you don't write the distributed boilerplate.

```python
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_loop(config):
    import torch, ray.train as rtrain
    model = build_model()
    model = rtrain.torch.prepare_model(model)   # wraps in DDP
    for epoch in range(config["epochs"]):
        for batch in rtrain.get_dataset_shard("train").iter_torch_batches():
            ...
        rtrain.report({"loss": loss}, checkpoint=ckpt)

trainer = TorchTrainer(
    train_loop_per_worker=train_loop,
    train_loop_config={"epochs": 10},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    datasets={"train": train_ds},
)
result = trainer.fit()
```

### What Ray Train handles

- Spawning N actors (one per training worker)
- Wiring PyTorch DDP / TF MultiWorker process groups
- Sharding the dataset across workers via Ray Data
- Coordinating checkpoints
- Restarting workers on failure

### Backends

- `TorchTrainer` — DDP / FSDP
- `TensorflowTrainer`
- `XGBoostTrainer` / `LightGBMTrainer`
- `HorovodTrainer` (legacy)

---

## 4. Ray Tune

Hyperparameter search. Built on Ray Core actors, so trials run in parallel across the cluster.

```python
from ray import tune

def objective(config):
    score = train_model(lr=config["lr"], hidden=config["hidden"])
    tune.report({"score": score})

tuner = tune.Tuner(
    objective,
    param_space={
        "lr": tune.loguniform(1e-5, 1e-1),
        "hidden": tune.choice([64, 128, 256]),
    },
    tune_config=tune.TuneConfig(num_samples=50, metric="score", mode="max"),
)
results = tuner.fit()
print(results.get_best_result().config)
```

### Search algorithms

- Random / Grid
- Bayesian (HyperOpt, Optuna, BoTorch wrappers)
- ASHA / PBT for early stopping & population-based search

Tune + Train compose: `tune.with_parameters(TorchTrainer)` lets you sweep training jobs at scale.

---

## 5. Ray Serve

Production model serving. Each "deployment" is a class wrapped in an actor; Serve manages replicas, routes HTTP/gRPC, batches, and autoscales.

```python
from ray import serve
from starlette.requests import Request

@serve.deployment(
    num_replicas=3,
    ray_actor_options={"num_cpus": 2, "num_gpus": 0},
)
class Sentiment:
    def __init__(self):
        from transformers import pipeline
        self.model = pipeline("sentiment-analysis")

    async def __call__(self, request: Request):
        text = (await request.json())["text"]
        return self.model(text)[0]

serve.run(Sentiment.bind(), route_prefix="/sentiment")
# curl -X POST localhost:8000/sentiment -d '{"text":"I love Ray"}'
```

### Concepts

| Concept | Meaning |
|---|---|
| Deployment | A class scheduled as N replicas |
| Replica | One actor instance handling requests |
| Application | A bound graph of deployments |
| HTTP proxy | Front-door actor on each node, routes to replicas |
| Router | Decides which replica gets the next request |

### Advanced features

- **Autoscaling** — `autoscaling_config=` scales replicas based on QPS / queue depth
- **Batching** — `@serve.batch` aggregates concurrent calls into a single inference
- **Model composition** — DAG of deployments using `.bind()`
- **Fractional GPUs** — `num_gpus=0.5` to share one GPU across two replicas

### Mental model

> Ray Serve is "Kubernetes for model replicas, but inside one Ray cluster."

---

## 6. How the libraries compose

A real production stack often looks like:

```text
   user request
        │
        ▼
   Ray Serve  (FastAPI-like, sync API)
        │
        ▼   ◄── actor pool of N replicas
   Model actor  (loads weights once)
        │
        ├──► writes async to event log (Ray task)
        │
        ▼
   Ray Data  (offline)
        │   aggregates events, generates training set
        ▼
   Ray Train (distributed retrain)
        │
        ▼
   New checkpoint  ──► Ray Serve picks it up via blue/green deploy
```

The whole loop — serving, logging, retraining, redeploy — can run inside **one Ray cluster** because everything is just tasks and actors.

---

## 7. When to pick which

| Goal | Library |
|---|---|
| Run arbitrary Python code in parallel | Core (tasks) |
| Long-lived stateful workers / model loaded once | Core (actors) |
| ETL or batch inference on big data | Data |
| Train a model across many GPUs/machines | Train |
| Sweep hyperparameters | Tune |
| Serve a model over HTTP at scale | Serve |
| All of the above in one cluster | Just install `ray[default,data,train,tune,serve]` |

---

## Next

[05_internals.md](./05_internals.md) — scheduling, object store, lineage internals.
