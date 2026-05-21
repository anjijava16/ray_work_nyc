import ray
import requests

ray.init()

@ray.remote
def fetch(url):
    return requests.get(url).status_code

urls = [
    "https://example.com",
    "https://python.org",
]

refs = [fetch.remote(u) for u in urls]

print(ray.get(refs))
