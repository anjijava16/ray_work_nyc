import ray
import time

ray.init(address="auto")

@ray.remote
def heavy_task(x):
    time.sleep(10)
    return x * x

tasks = [heavy_task.remote(i) for i in range(20)]

print(ray.get(tasks))