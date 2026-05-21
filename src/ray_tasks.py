import ray

ray.init()

@ray.remote
def square(x):
    return x * x

future = square.remote(5)

print(ray.get(future))
