import ray
import time

ray.init(address="auto")

@ray.remote
class Worker:
    def process(self, x):
        time.sleep(5)
        return x + 1

workers = [Worker.remote() for _ in range(4)]

results = []
for i in range(20):
    results.append(workers[i % 4].process.remote(i))

print(ray.get(results))