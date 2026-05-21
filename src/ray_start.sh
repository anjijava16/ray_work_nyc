(.venv) welcome@jaisairams-Laptop ray_experiments % ray start --head --dashboard-host=127.0.0.1
Usage stats collection is enabled. To disable this, add `--disable-usage-stats` to the command that starts the cluster, or run the following command: `ray disable-usage-stats` before starting the cluster. See https://docs.ray.io/en/master/cluster/usage-stats.html for more details.

Local node IP: 127.0.0.1

--------------------
Ray runtime started.
--------------------

Next steps

  To connect to this Ray cluster:
    import ray
    ray.init()

  To submit a Ray job using the Ray Jobs CLI:
    RAY_API_SERVER_ADDRESS='http://127.0.0.1:8265' ray job submit --working-dir . -- python my_script.py

  See https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html
  for more information on submitting Ray jobs to the Ray cluster.

  To terminate the Ray runtime, run
    ray stop

  To view the status of the cluster, use
    ray status

  To monitor and debug Ray, view the dashboard at
    127.0.0.1:8265

  If connection to the dashboard fails, check your firewall settings and network configuration.
(.venv) welcome@jaisairams-Laptop ray_experiments %