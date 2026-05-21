"""
ObjectRefs and the object store.

Concepts covered:
  - ray.put() stores a value once, returns an ObjectRef
  - Passing refs to tasks avoids re-serializing big data
  - ray.wait() returns refs as they complete (backpressure pattern)

Run:
  python 03_object_refs.py
"""

import time
import numpy as np
import ray


@ray.remote
def summarize(arr: np.ndarray) -> dict:
    return {"shape": arr.shape, "sum": float(arr.sum()), "mean": float(arr.mean())}


@ray.remote
def slow_step(i: int) -> int:
    time.sleep(0.5 + (i % 3) * 0.5)
    return i * i


def big_object_demo() -> None:
    arr = np.random.randn(1000, 1000).astype(np.float32)
    print(f"created array of {arr.nbytes/1e6:.1f} MB")

    # Bad: pass by value, serialized every call
    refs_bad = [summarize.remote(arr) for _ in range(4)]

    # Good: put once, share the ref
    ref = ray.put(arr)
    refs_good = [summarize.remote(ref) for _ in range(4)]

    print("bad path  results:", ray.get(refs_bad)[0])
    print("good path results:", ray.get(refs_good)[0])


def ray_wait_demo() -> None:
    refs = [slow_step.remote(i) for i in range(12)]
    remaining = list(refs)
    while remaining:
        done, remaining = ray.wait(remaining, num_returns=1, timeout=10.0)
        for r in done:
            print("finished:", ray.get(r))


def main() -> None:
    ray.init()
    big_object_demo()
    print("---")
    ray_wait_demo()
    ray.shutdown()


if __name__ == "__main__":
    main()
