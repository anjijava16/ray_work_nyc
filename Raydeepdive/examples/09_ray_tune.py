"""
Ray Tune — hyperparameter search.

Searches for the best (lr, hidden) on a tiny problem.

Run:
  pip install "ray[tune]"
  python 09_ray_tune.py
"""

import math
import ray
from ray import tune


def objective(config: dict) -> None:
    # Pretend training: a smooth function with a known optimum
    lr = config["lr"]
    hidden = config["hidden"]
    # best around lr=1e-3, hidden=128
    score = -((math.log10(lr) + 3) ** 2 + ((hidden - 128) / 64) ** 2)
    tune.report({"score": score})


def main() -> None:
    ray.init()
    tuner = tune.Tuner(
        objective,
        param_space={
            "lr": tune.loguniform(1e-5, 1e-1),
            "hidden": tune.choice([32, 64, 128, 256, 512]),
        },
        tune_config=tune.TuneConfig(num_samples=12, metric="score", mode="max"),
    )
    results = tuner.fit()
    best = results.get_best_result()
    print("best config:", best.config)
    print("best score :", best.metrics["score"])
    ray.shutdown()


if __name__ == "__main__":
    main()
