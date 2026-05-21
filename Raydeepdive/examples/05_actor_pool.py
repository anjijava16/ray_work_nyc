"""
Actor pool pattern — round-robin work over N stateful workers.

Useful when:
  - each worker holds expensive state (e.g., a loaded model)
  - you have many small jobs to feed them

Run:
  python 05_actor_pool.py
"""

import time
import ray
from ray.util.actor_pool import ActorPool


@ray.remote
class Tokenizer:
    def __init__(self, worker_id: int) -> None:
        self.worker_id = worker_id
        # imagine loading vocab/model here
        time.sleep(0.5)

    def tokenize(self, text: str) -> dict:
        time.sleep(0.1)
        return {"worker": self.worker_id, "tokens": text.split()}


def main() -> None:
    ray.init()

    workers = [Tokenizer.remote(i) for i in range(4)]
    pool = ActorPool(workers)

    sentences = [f"sentence number {i}" for i in range(12)]

    # ActorPool.map yields results in submission order, while distributing
    # work across all actors.
    for result in pool.map(lambda actor, s: actor.tokenize.remote(s), sentences):
        print(result)

    ray.shutdown()


if __name__ == "__main__":
    main()
