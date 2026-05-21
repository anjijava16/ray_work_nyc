"""
Ray Actors — stateful distributed objects.

Concept covered:
  - @ray.remote on a class
  - .remote() on the class creates a long-lived worker process
  - Method calls return ObjectRefs
  - State persists across calls

Run:
  python 02_actors.py
"""

import ray


@ray.remote
class Counter:
    def __init__(self, start: int = 0) -> None:
        self.n = start

    def inc(self, by: int = 1) -> int:
        self.n += by
        return self.n

    def value(self) -> int:
        return self.n


@ray.remote
class ModelServer:
    """Demonstrates the 'load model once, reuse forever' pattern."""

    def __init__(self) -> None:
        print("[ModelServer] loading model (would take seconds in real life)")
        self.weights = {"intercept": 0.5, "coef": 1.2}

    def predict(self, x: float) -> float:
        return self.weights["intercept"] + self.weights["coef"] * x


def main() -> None:
    ray.init()

    counter = Counter.remote()
    refs = [counter.inc.remote() for _ in range(5)]
    print("counter:", ray.get(refs))   # [1, 2, 3, 4, 5]

    server = ModelServer.remote()
    preds = ray.get([server.predict.remote(x) for x in range(5)])
    print("preds:", preds)

    ray.shutdown()


if __name__ == "__main__":
    main()
