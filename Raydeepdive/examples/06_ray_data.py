"""
Ray Data — distributed dataset, map_batches, and an actor pool for inference.

This example fakes a small in-memory dataset and runs:
  - a stateless row-level transform with map_batches
  - an actor-pool transform that 'loads a model' once per replica

Run:
  python 06_ray_data.py
"""

import ray
import pandas as pd


class FakeModel:
    """Pretend this is a HuggingFace pipeline that's expensive to load."""

    def __init__(self) -> None:
        import time
        time.sleep(0.5)
        self.weights = 0.42

    def predict(self, text: str) -> float:
        return self.weights * len(text)


class Scorer:
    """Callable class — Ray Data creates an actor per replica."""

    def __init__(self) -> None:
        self.model = FakeModel()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        batch["score"] = batch["text"].apply(self.model.predict)
        return batch


def main() -> None:
    ray.init()

    rows = [{"id": i, "text": f"review number {i} is excellent"} for i in range(40)]
    ds = ray.data.from_items(rows)
    print("input dataset:", ds.schema())

    # Stateless map_batches — function gets a pandas DataFrame
    ds_upper = ds.map_batches(
        lambda batch: batch.assign(text=batch["text"].str.upper()),
        batch_format="pandas",
    )

    # Stateful map_batches — Ray Data instantiates Scorer as an actor pool
    ds_scored = ds_upper.map_batches(
        Scorer,
        batch_format="pandas",
        batch_size=8,
        concurrency=2,         # 2 actors, each loads FakeModel once
    )

    out = ds_scored.take_all()
    for row in out[:5]:
        print(row)

    ray.shutdown()


if __name__ == "__main__":
    main()
