# 07 — End-to-End Example

A worked example that combines **Ray Core + Ray Data + Ray Serve** in one application: a sentiment-analysis pipeline that ingests reviews, runs batch inference, then serves the same model online over HTTP.

The runnable code lives at [examples/10_e2e_ml_pipeline.py](./examples/10_e2e_ml_pipeline.py).

---

## 1. The use case

Imagine you have:
- A folder of product reviews (CSV files)
- A sentiment model (we'll fake one with a tiny rule-based scorer)
- A need to: (a) score the historical reviews in batch, (b) serve new reviews over HTTP

You want **one Ray cluster** doing both — same code, same model, no operational separation.

---

## 2. Architecture

```text
   ┌─────────────────────────────────────────────────────────────────┐
   │                      RAY CLUSTER                                 │
   │                                                                  │
   │   ┌─────────────────┐                                            │
   │   │  Driver script  │  python 10_e2e_ml_pipeline.py              │
   │   └────────┬────────┘                                            │
   │            │                                                     │
   │   ┌────────▼─────────┐    ┌─────────────────────┐               │
   │   │   Ray Data       │───►│  Sentiment actor    │               │
   │   │ read_csv(...)    │    │  pool (4 replicas)  │ ◄── batch     │
   │   │ map_batches      │    │  loads model once   │     scoring   │
   │   │ write_parquet    │    └─────────────────────┘               │
   │   └──────────────────┘                                          │
   │                                                                  │
   │   ┌──────────────────────────────────────────────┐              │
   │   │             Ray Serve                          │             │
   │   │   @serve.deployment Sentiment                  │             │
   │   │   - 3 replicas, autoscaling 1..10              │             │
   │   │   - HTTP on :8000/score                        │ ◄── online  │
   │   └──────────────────────────────────────────────┘              │
   │                                                                  │
   └─────────────────────────────────────────────────────────────────┘
                          │
                ┌─────────┴──────────┐
                │                    │
            S3/disk:              clients
            scored.parquet        curl :8000/score
```

---

## 3. Walkthrough

### 3.1 Bootstrap

```python
import ray
from ray import serve

ray.init()      # one cluster, used for both batch + online
```

### 3.2 The model (one class, two uses)

Define the model logic once. The **same class** powers both the batch pool and the online deployment.

```python
class SentimentModel:
    def __init__(self):
        # In a real system: load_pretrained_model(...)
        self.positive_words = {"good", "great", "love", "amazing", "best"}
        self.negative_words = {"bad", "terrible", "hate", "worst", "awful"}

    def score(self, text: str) -> float:
        words = text.lower().split()
        pos = sum(1 for w in words if w in self.positive_words)
        neg = sum(1 for w in words if w in self.negative_words)
        if pos + neg == 0:
            return 0.0
        return (pos - neg) / (pos + neg)
```

### 3.3 Batch inference with Ray Data

Use an **actor pool** so the model loads once per replica, not once per row.

```python
import ray
import pandas as pd

class BatchScorer:
    def __init__(self):
        self.model = SentimentModel()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        batch["score"] = batch["review"].apply(self.model.score)
        return batch

ds = ray.data.read_csv("reviews/")
ds = ds.map_batches(
    BatchScorer,
    batch_format="pandas",
    batch_size=128,
    concurrency=4,         # 4 actors, model loaded 4 times total
)
ds.write_parquet("scored/")
```

### 3.4 Online serving with Ray Serve

Same model class, wrapped as a deployment.

```python
from ray import serve
from starlette.requests import Request

@serve.deployment(
    num_replicas=3,
    autoscaling_config={"min_replicas": 1, "max_replicas": 10},
)
class SentimentDeployment:
    def __init__(self):
        self.model = SentimentModel()

    async def __call__(self, request: Request) -> dict:
        body = await request.json()
        text = body["text"]
        return {"score": self.model.score(text)}

serve.run(SentimentDeployment.bind(), route_prefix="/score")
```

Now `curl -X POST localhost:8000/score -d '{"text":"I love this"}'` returns `{"score": 1.0}`.

### 3.5 Run both in one process

```python
if __name__ == "__main__":
    # 1. Batch scoring
    run_batch_inference()

    # 2. Stand up the online service
    serve.run(SentimentDeployment.bind(), route_prefix="/score")

    # 3. Keep the script alive so the service stays up
    import time
    while True:
        time.sleep(60)
```

---

## 4. Why this matters

Doing this **without Ray** typically means:

| Concern | Without Ray | With Ray |
|---|---|---|
| Batch infra | Spark / Beam cluster | `map_batches` |
| Online infra | Triton / TF Serving / KServe | `@serve.deployment` |
| Model packaging | Separate Docker images | One Python class |
| Failure recovery | Per-system retries | Lineage + actor restarts |
| Autoscaling | Per-system controllers | Ray autoscaler + KubeRay |
| Skew between batch and online | Different libraries, drift | Same class everywhere |

This is the killer feature of Ray for ML platforms: **one runtime, many workloads.**

---

## 5. What to try next

Pick one and modify the example:

1. **Swap the model** for a real Hugging Face pipeline (`transformers.pipeline("sentiment-analysis")`).
2. **Add Ray Train** — train a logistic regression on the scored reviews, then hot-swap the model into the Serve deployment.
3. **Add Ray Tune** — sweep the threshold that converts the float score into a positive/negative label.
4. **Deploy to KubeRay** — wrap the script in a `RayJob` for batch and a `RayService` for online.

---

## End of deep dive

Reading order if you want to revisit:

1. [README](./README.md) — overview
2. [01_architecture](./01_architecture.md) — cluster shape
3. [02_components](./02_components.md) — process-level pieces
4. [03_flow](./03_flow.md) — task lifecycle
5. [04_services](./04_services.md) — libraries on top of Core
6. [05_internals](./05_internals.md) — scheduling, store, lineage
7. [06_production](./06_production.md) — KubeRay, scaling, ops
8. [07_end_to_end_example](./07_end_to_end_example.md) — the worked example (this file)

Code: [examples/](./examples/)
