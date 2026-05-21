"""
End-to-end: Ray Core + Ray Data + Ray Serve in one script.

Phase 1: Score historical reviews in batch using Ray Data + actor pool.
Phase 2: Stand up the same model behind an HTTP endpoint with Ray Serve.

Run:
  python 10_e2e_ml_pipeline.py

Test the online endpoint in another shell:
  curl -X POST http://localhost:8000/score \
       -H 'Content-Type: application/json' \
       -d '{"text":"I love this product, it is amazing"}'
"""

import time
import ray
import pandas as pd
from ray import serve
from starlette.requests import Request


# --------------------------------------------------------------------------- #
# Model (used by both batch and online paths — same class, no skew)           #
# --------------------------------------------------------------------------- #
class SentimentModel:
    POS = {"good", "great", "love", "amazing", "best", "excellent", "awesome"}
    NEG = {"bad", "terrible", "hate", "worst", "awful", "horrible", "poor"}

    def score(self, text: str) -> float:
        words = text.lower().split()
        pos = sum(1 for w in words if w in self.POS)
        neg = sum(1 for w in words if w in self.NEG)
        if pos + neg == 0:
            return 0.0
        return (pos - neg) / (pos + neg)


# --------------------------------------------------------------------------- #
# Phase 1 — batch scoring with Ray Data                                       #
# --------------------------------------------------------------------------- #
class BatchScorer:
    def __init__(self) -> None:
        # In real life this is where you load model weights
        self.model = SentimentModel()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        batch["score"] = batch["review"].apply(self.model.score)
        return batch


def run_batch_inference() -> None:
    rows = [
        {"id": i, "review": text}
        for i, text in enumerate([
            "I love this, it is amazing",
            "Terrible product, hate it",
            "Just okay, nothing special",
            "Best purchase ever, awesome quality",
            "Worst experience, awful service",
            "Pretty good overall, would buy again",
        ] * 5)
    ]
    ds = ray.data.from_items(rows)
    scored = ds.map_batches(
        BatchScorer,
        batch_format="pandas",
        batch_size=8,
        concurrency=2,
    )
    print("\n=== Phase 1: batch results (first 5) ===")
    for row in scored.take(5):
        print(row)


# --------------------------------------------------------------------------- #
# Phase 2 — online inference with Ray Serve                                   #
# --------------------------------------------------------------------------- #
@serve.deployment(
    num_replicas=2,
    ray_actor_options={"num_cpus": 0.5},
)
class SentimentDeployment:
    def __init__(self) -> None:
        self.model = SentimentModel()

    async def __call__(self, request: Request) -> dict:
        body = await request.json()
        text = body.get("text", "")
        return {"text": text, "score": self.model.score(text)}


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #
def main() -> None:
    ray.init()

    run_batch_inference()

    print("\n=== Phase 2: serving on http://localhost:8000/score ===")
    serve.start(http_options={"host": "0.0.0.0", "port": 8000})
    serve.run(SentimentDeployment.bind(), route_prefix="/score")

    print("Try: curl -X POST http://localhost:8000/score "
          "-H 'Content-Type: application/json' "
          "-d '{\"text\":\"I love ray\"}'")
    print("Press Ctrl-C to stop.\n")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        serve.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    main()
