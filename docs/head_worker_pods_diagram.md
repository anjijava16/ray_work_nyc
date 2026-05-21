# Ray on EKS — Driver → Head Pod → Worker Pods

Mermaid diagram of the execution flow described in [head_worker_pods.md](./head_worker_pods.md).

```mermaid
flowchart TD
    Driver["Driver Code<br/>(User Python App)<br/>tasks = [heavy_task.remote(i) for i in range(20)]"]

    subgraph HeadPod["Head Pod (Control Plane)"]
        GCS["GCS<br/>(Global Control Store)"]
        Scheduler["Distributed Scheduler"]
        Autoscaler["Ray Autoscaler<br/>(decides worker count)"]
        Raylet_H["Raylet (head)"]
    end

    subgraph EKS["EKS Cluster"]
        subgraph W1["Worker Pod 1"]
            Raylet1["Raylet"]
            Exec1["Task Executor"]
            Plasma1["Plasma Object Store"]
        end
        subgraph W2["Worker Pod 2"]
            Raylet2["Raylet"]
            Exec2["Task Executor"]
            Plasma2["Plasma Object Store"]
        end
        subgraph W3["Worker Pod N"]
            Raylet3["Raylet"]
            Exec3["Task Executor"]
            Plasma3["Plasma Object Store"]
        end
    end

    ObjectStore[("Distributed Object Store<br/>(shared via Plasma)")]

    Driver -->|"1 - submit task metadata"| GCS
    GCS --> Scheduler
    Scheduler -->|"2 - queue tasks"| Autoscaler
    Autoscaler -.->|"3 - scale up pods<br/>(if demand high)"| EKS

    W1 -.->|"register once<br/>+ heartbeat"| GCS
    W2 -.->|"register once<br/>+ heartbeat"| GCS
    W3 -.->|"register once<br/>+ heartbeat"| GCS

    Raylet1 -->|"4 - pull task"| Scheduler
    Raylet2 -->|"4 - pull task"| Scheduler
    Raylet3 -->|"4 - pull task"| Scheduler

    Exec1 -->|"5 - execute"| Plasma1
    Exec2 -->|"5 - execute"| Plasma2
    Exec3 -->|"5 - execute"| Plasma3

    Plasma1 -->|"6 - store result"| ObjectStore
    Plasma2 -->|"6 - store result"| ObjectStore
    Plasma3 -->|"6 - store result"| ObjectStore

    ObjectStore -->|"7 - ray.get(result)"| Driver

    classDef driver fill:#fef3c7,stroke:#b45309,color:#000
    classDef head fill:#dbeafe,stroke:#1d4ed8,color:#000
    classDef worker fill:#dcfce7,stroke:#15803d,color:#000
    classDef store fill:#fce7f3,stroke:#be185d,color:#000

    class Driver driver
    class GCS,Scheduler,Autoscaler,Raylet_H head
    class Raylet1,Raylet2,Raylet3,Exec1,Exec2,Exec3,Plasma1,Plasma2,Plasma3 worker
    class ObjectStore store
```

## Key points

- **Driver → Head Pod**: sends task *metadata* only, not execution
- **Workers register once** with GCS (dashed lines) — no continuous polling
- **Workers PULL** tasks from the scheduler (not push from head)
- **Autoscaler** (inside head pod) decides when EKS spins up more worker pods
- **Object Store** is how results flow back to the driver, not through the head

## One-liner mental model

> Head = brain (schedule + state)
> Workers = muscles (execute tasks)
> Scheduler = nervous system (dispatch logic)
> Object store = memory (data exchange)
