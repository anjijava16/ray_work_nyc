# CLAUDE.md — Ray Experiments

This file provides guidance for Claude (and other AI agents) working in this repository.

## Project Overview

A hands-on Ray distributed computing experiments project, progressing from beginner to expert level. Covers Ray Tasks, Actors, remote job submission, actor-based worker pools, and parallel web scraping.

## Repository Structure

```
ray_experiments/
├── pyproject.toml              # Project metadata & dependencies
├── README.md                   # Deep-dive Ray reference guide
├── CLAUDE.md                   # AI agent guidance (this file)
├── AGENTS.md                   # Agent roles & responsibilities
└── src/
    ├── ray_exp.py              # Minimal Ray init smoke test
    ├── ray_tasks.py            # Remote task basics (@ray.remote functions)
    ├── ray_actors.py           # Stateful actor example (Counter)
    ├── ray_jobs_auto.py        # Job submission to an existing cluster (address="auto")
    ├── ray_workers_actor_based.py  # Actor-based worker pool (4 workers, 20 tasks)
    ├── ray_webscrapping.py     # Parallel HTTP fetching with Ray tasks
    └── ray_start.sh            # Reference output from `ray start --head`
```

## Environment Setup

- **Python**: ≥ 3.10
- **Virtual env**: `.venv/` at project root
- **Activate**: `source .venv/bin/activate`
- **Install**: `pip install -e ".[dev]"`
- **Core dependency**: `ray[data,train,tune,serve]`

## Running Scripts

### Local cluster (single machine)
```bash
# Scripts using ray.init() start a local cluster automatically
python src/ray_tasks.py
python src/ray_actors.py
python src/ray_exp.py
python src/ray_webscrapping.py
```

### Remote / existing cluster
```bash
# Start a head node first
ray start --head --dashboard-host=127.0.0.1

# Then run scripts that use address="auto"
python src/ray_jobs_auto.py
python src/ray_workers_actor_based.py

# Stop the cluster when done
ray stop
```

### Ray Dashboard
Available at `http://127.0.0.1:8265` after starting the head node.

## Key Patterns in This Codebase

| Pattern | File | Description |
|---|---|---|
| Remote task | `ray_tasks.py` | `@ray.remote` function, `square.remote(5)`, `ray.get()` |
| Stateful actor | `ray_actors.py` | `@ray.remote` class, `.remote()` instantiation, method calls |
| Heavy batch jobs | `ray_jobs_auto.py` | 20 tasks with `time.sleep`, cluster submission |
| Worker pool | `ray_workers_actor_based.py` | Round-robin dispatch across 4 actor workers |
| Parallel I/O | `ray_webscrapping.py` | Concurrent HTTP with `requests` via Ray tasks |

## Code Conventions

- `ray.init()` — local mode (auto-creates cluster)
- `ray.init(address="auto")` — connect to running cluster
- Always call `ray.get(refs)` to materialize results from `ObjectRef`s
- Actor methods called as `actor.method.remote(args)` return `ObjectRef`s

## Testing

```bash
pytest  # runs any tests under the project root
```

No test files exist yet — when adding tests place them in `tests/`.

## Common Issues

- **`ConnectionError` with `address="auto"`** — start the Ray cluster first: `ray start --head`
- **Port 8265 already in use** — run `ray stop` then restart
- **`requests` not installed** — `pip install requests` (not in pyproject.toml yet)
