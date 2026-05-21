"""
Task DAGs — Ray builds the dependency graph for you when you pass ObjectRefs.

Pipeline:
  load -> clean -> embed -> reduce

Each stage is a separate @ray.remote function. Passing one stage's ObjectRef
as the input to the next stage makes Ray:
  - schedule downstream tasks only when upstream finishes
  - prefer the same node for data locality

Run:
  python 04_dag_pipeline.py
"""

import time
import ray


@ray.remote
def load(src: str) -> list[str]:
    time.sleep(0.3)
    return [f"{src}-row-{i}" for i in range(5)]


@ray.remote
def clean(rows: list[str]) -> list[str]:
    time.sleep(0.3)
    return [r.upper() for r in rows]


@ray.remote
def embed(rows: list[str]) -> list[tuple[str, int]]:
    time.sleep(0.3)
    return [(r, len(r)) for r in rows]


@ray.remote
def reduce_all(parts: list[list[tuple[str, int]]]) -> dict:
    total = sum(length for chunk in parts for _, length in chunk)
    count = sum(len(chunk) for chunk in parts)
    return {"rows": count, "total_chars": total}


def main() -> None:
    ray.init()

    sources = ["A", "B", "C", "D"]
    parts = []
    for s in sources:
        loaded = load.remote(s)
        cleaned = clean.remote(loaded)
        embedded = embed.remote(cleaned)
        parts.append(embedded)

    # reduce_all depends on every embedded ref; Ray waits for all of them.
    result = reduce_all.remote(parts)
    print("pipeline result:", ray.get(result))

    ray.shutdown()


if __name__ == "__main__":
    main()
