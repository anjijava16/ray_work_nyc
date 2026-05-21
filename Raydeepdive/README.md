# Ray Deep Dive

A complete, opinionated walkthrough of Ray — the distributed computing framework that powers ChatGPT-scale ML systems at OpenAI, Uber, Shopify, Pinterest, and many others.

This deep dive is organized so you can read it linearly (beginner → expert) or jump to the section you need. Each markdown file is paired with runnable Python examples in `examples/`.

---

## Table of contents

| # | File | What it covers |
|---|---|---|
| 01 | [01_architecture.md](./01_architecture.md) | Cluster topology, head vs worker nodes, control plane vs data plane |
| 02 | [02_components.md](./02_components.md) | GCS, Raylet, Plasma, Worker, Driver, Dashboard — each component in detail |
| 03 | [03_flow.md](./03_flow.md) | End-to-end execution flow of a task, with sequence diagrams |
| 04 | [04_services.md](./04_services.md) | Ray Core, Ray Data, Ray Train, Ray Tune, Ray Serve — what each library does |
| 05 | [05_internals.md](./05_internals.md) | Scheduling internals, object store, serialization, lineage recovery |
| 06 | [06_production.md](./06_production.md) | KubeRay, autoscaling, observability, common production failures |
| 07 | [07_end_to_end_example.md](./07_end_to_end_example.md) | A worked LLM-inference pipeline using Core + Data + Serve |

---

## Examples folder

Every example is self-contained and runnable with `python <file>` after `pip install "ray[default,data,train,tune,serve]"`.

| File | Concept |
|---|---|
| [examples/01_tasks.py](./examples/01_tasks.py) | `@ray.remote` functions, `ObjectRef`, parallelism |
| [examples/02_actors.py](./examples/02_actors.py) | Stateful actors, method calls, lifecycle |
| [examples/03_object_refs.py](./examples/03_object_refs.py) | Passing refs between tasks, `ray.put`, `ray.wait` |
| [examples/04_dag_pipeline.py](./examples/04_dag_pipeline.py) | Task DAG with dependencies (lineage) |
| [examples/05_actor_pool.py](./examples/05_actor_pool.py) | Actor pool pattern, load balancing |
| [examples/06_ray_data.py](./examples/06_ray_data.py) | Distributed dataset, `map_batches` |
| [examples/07_ray_serve.py](./examples/07_ray_serve.py) | HTTP deployment + replicas |
| [examples/08_ray_train.py](./examples/08_ray_train.py) | Distributed PyTorch training skeleton |
| [examples/09_ray_tune.py](./examples/09_ray_tune.py) | Hyperparameter search |
| [examples/10_e2e_ml_pipeline.py](./examples/10_e2e_ml_pipeline.py) | End-to-end: ingest → preprocess → infer → serve |
| [examples/run_all.sh](./examples/run_all.sh) | Convenience script to run the Core examples |

---

## The one-paragraph mental model

> Ray is a **distributed operating system for Python**. You annotate functions with `@ray.remote` and classes with `@ray.remote`, call them with `.remote(...)`, and get back `ObjectRef`s (distributed futures). Ray's **GCS** stores cluster state, **Raylet** processes on every node schedule work and manage local resources, **Plasma** is the shared-memory object store on each node, and **Workers** are the Python processes that actually run your code. Tasks and actors are placed by a hierarchical scheduler, results travel through the object store with zero-copy reads, and if a node dies Ray rebuilds lost objects from their lineage. On top of Core, four libraries — **Data, Train, Tune, Serve** — give you batch pipelines, distributed training, hyperparameter search, and model serving.

Read [01_architecture.md](./01_architecture.md) to start.
