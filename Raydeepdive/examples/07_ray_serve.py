"""
Ray Serve — HTTP deployment with multiple replicas.

Run:
  python 07_ray_serve.py

Then in another shell:
  curl -X POST http://localhost:8000/echo -H 'Content-Type: application/json' \
       -d '{"text": "hello ray"}'

Press Ctrl-C to stop.
"""

import ray
from ray import serve
from starlette.requests import Request


@serve.deployment(
    num_replicas=3,
    ray_actor_options={"num_cpus": 0.5},
)
class Echo:
    def __init__(self) -> None:
        import os
        self.pid = os.getpid()

    async def __call__(self, request: Request) -> dict:
        body = await request.json()
        return {
            "replica_pid": self.pid,
            "echo": body.get("text", ""),
            "length": len(body.get("text", "")),
        }


def main() -> None:
    ray.init()
    serve.start(http_options={"host": "0.0.0.0", "port": 8000})
    serve.run(Echo.bind(), route_prefix="/echo")
    print("Serve is up on http://localhost:8000/echo  (Ctrl-C to quit)")

    import time
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        serve.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    main()
