"""
Ray Train — distributed PyTorch training skeleton.

This is a minimal end-to-end script that shows the API surface. It uses a tiny
synthetic regression problem so it runs without a GPU and finishes in seconds.

Run:
  pip install "ray[train]" torch
  python 08_ray_train.py
"""

import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_loop_per_worker(config: dict) -> None:
    import torch
    import torch.nn as nn
    import ray.train as rtrain

    torch.manual_seed(0)
    X = torch.randn(512, 4)
    y = (X.sum(dim=1, keepdim=True) > 0).float()

    model = nn.Sequential(nn.Linear(4, 8), nn.ReLU(), nn.Linear(8, 1), nn.Sigmoid())
    model = rtrain.torch.prepare_model(model)
    opt = torch.optim.Adam(model.parameters(), lr=config["lr"])
    loss_fn = nn.BCELoss()

    for epoch in range(config["epochs"]):
        opt.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        opt.step()
        rtrain.report({"epoch": epoch, "loss": float(loss.item())})


def main() -> None:
    ray.init()
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"lr": 1e-2, "epochs": 5},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    )
    result = trainer.fit()
    print("final metrics:", result.metrics)
    ray.shutdown()


if __name__ == "__main__":
    main()
