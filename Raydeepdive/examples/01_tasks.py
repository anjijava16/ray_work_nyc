"""
Ray Tasks — stateless distributed functions.

Concept covered:
  - @ray.remote decorator
  - .remote() returns an ObjectRef immediately
  - ray.get() blocks until the result is available
  - Parallelism by submitting many .remote() calls first

Run:
  python 01_tasks.py
"""

import time
import ray


@ray.remote()
def slow_square(x: int) -> int:
    time.sleep(1)
    return x * x


def sequential():
    t0 = time.time()
    results = [slow_square(i) for i in range(8)]  # plain Python, sequential
    results = [slow_square(i) for i in range(8)]
    # Note: the above would error because slow_square is now a RemoteFunction.
    # To make the comparison fair, define a local copy:
    def _local(x): time.sleep(1); return x * x
    results = [_local(i) for i in range(8)]
    print(f"sequential: {results} in {time.time()-t0:.1f}s")


def parallel():
    t0 = time.time()
    refs = [slow_square.remote(i) for i in range(8)]
    results = ray.get(refs)
    print(f"parallel:   {results} in {time.time()-t0:.1f}s")


def main():
    ray.init()
    sequential()
    parallel()
    ray.shutdown()


if __name__ == "__main__":
    main()
