# AGENTS.md — Ray Experiments

This file describes the roles, responsibilities, and rules for AI agents (GitHub Copilot, Claude, etc.) working in this repository.

## Agent Roles

### 1. Code Author
Writes new Ray experiments under `src/`.

**Responsibilities:**
- Follow the existing file naming convention: `ray_<topic>.py`
- Keep each script self-contained and runnable from the project root
- Use `ray.init()` for local experiments; use `ray.init(address="auto")` only when an external cluster is expected
- Always call `ray.get()` on returned `ObjectRef`s before printing results
- Add a brief module-level comment describing what the script demonstrates

**Must not:**
- Introduce new dependencies without adding them to `pyproject.toml`
- Leave `ray.init()` calls without matching `ray.shutdown()` in long-running scripts

---

### 2. Refactoring Agent
Improves existing scripts for clarity and correctness.

**Responsibilities:**
- Preserve observable behaviour (same stdout output)
- Extract repeated patterns (e.g. round-robin dispatch) into helper functions
- Prefer `ray.get([ref1, ref2, ...])` batch form over individual calls in loops

**Must not:**
- Change `address="auto"` to `ray.init()` or vice versa without explicit user request
- Remove intentional `time.sleep()` calls used to simulate work

---

### 3. Documentation Agent
Keeps `README.md`, `CLAUDE.md`, and `AGENTS.md` accurate and up-to-date.

**Responsibilities:**
- Update the file table in `CLAUDE.md` when new `src/` files are added
- Keep the Key Patterns table current
- Reflect any new dependencies or Python version requirements

**Must not:**
- Rewrite the README deep-dive content — that is a human-authored reference guide
- Delete existing sections without user approval

---

### 4. Test Author
Writes pytest tests for Ray scripts.

**Responsibilities:**
- Place all tests under `tests/` (create the directory if absent)
- Name test files `test_<module>.py` matching the `src/` filename
- Use `ray.init(num_cpus=2)` with a small resource limit to keep CI fast
- Call `ray.shutdown()` in test teardown (`yield`-based fixtures)
- Mock external HTTP calls in `ray_webscrapping.py` tests (do not make live requests)

**Must not:**
- Import from `src/` using relative paths — use `sys.path` insertion or install the package in editable mode (`pip install -e .`)

---

## Shared Rules for All Agents

1. **Read before editing** — always read the target file before making changes.
2. **One concern per script** — each `src/ray_*.py` file demonstrates exactly one concept.
3. **No hardcoded secrets** — no API keys, tokens, or credentials in source files.
4. **Cluster hygiene** — scripts using `address="auto"` must document that a head node must be running (comment at top of file or in docstring).
5. **Dependency tracking** — any new `import` that is not in the stdlib or `ray` must be added to `pyproject.toml` before committing.
6. **Do not auto-push** — never run `git push` without explicit user confirmation.

---

## Project Context Quick Reference

| Item | Value |
|---|---|
| Language | Python ≥ 3.10 |
| Core framework | Ray (`ray[data,train,tune,serve]`) |
| Virtual env | `.venv/` — activate with `source .venv/bin/activate` |
| Dashboard | `http://127.0.0.1:8265` (when head node is running) |
| Start cluster | `ray start --head --dashboard-host=127.0.0.1` |
| Stop cluster | `ray stop` |
| Run tests | `pytest` |

---

## Out of Scope

The following are **not** part of this repository and agents should not attempt them without explicit user instruction:

- Ray Tune hyperparameter search
- Ray Train distributed model training
- Ray Serve model serving endpoints
- Kubernetes / cloud cluster deployment
- CI/CD pipeline configuration
