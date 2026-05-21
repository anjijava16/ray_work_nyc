#!/usr/bin/env bash
# Run the Core examples sequentially. Skips the long-running Serve examples.
#
# Usage:
#   chmod +x run_all.sh
#   ./run_all.sh

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

CORE_EXAMPLES=(
  "01_tasks.py"
  "02_actors.py"
  "03_object_refs.py"
  "04_dag_pipeline.py"
  "05_actor_pool.py"
  "06_ray_data.py"
  "09_ray_tune.py"
)

for f in "${CORE_EXAMPLES[@]}"; do
  echo "──────────────────────────────────────────"
  echo "  Running: $f"
  echo "──────────────────────────────────────────"
  python "$HERE/$f"
done

echo
echo "Done. Skipped long-running examples:"
echo "  - 07_ray_serve.py   (needs Ctrl-C to stop)"
echo "  - 08_ray_train.py   (needs torch installed)"
echo "  - 10_e2e_ml_pipeline.py (needs Ctrl-C to stop)"
