
import ray

ray.init()


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1
        return self.count

counter = Counter.remote()

print(ray.get(counter.increment.remote()))
print(ray.get(counter.increment.remote()))
