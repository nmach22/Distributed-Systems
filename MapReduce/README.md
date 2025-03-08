# 6.5840

## [MapReduce](http://nil.csail.mit.edu/6.5840/2024/labs/lab-mr.html)

### Introduction

This project is an implementation of a distributed MapReduce framework inspired by the original [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). The implementation consists of a **coordinator** process that manages task distribution and fault tolerance, and multiple **worker** processes that execute map and reduce tasks in parallel. Workers communicate with the coordinator using RPC.

### Features

- Distributed execution of Map and Reduce tasks.
- Fault tolerance: Tasks are reassigned if a worker fails.
- Parallel execution with multiple workers.
- Automatic aggregation of intermediate key/value pairs.

---

## Getting Started

### Prerequisites

Ensure you have **Go installed** on your system. You can check your installation with:

```sh
$ go version
```

### Setup

Clone the repository and navigate to the MapReduce directory:

```sh
$ git clone https://github.com/nmach22/Distributed-Systems.git Distributed-Systems
$ cd Distributed-Systems/MapReduce
```

### Initial Setup
The provided starter code includes:
- A simple sequential MapReduce implementation: `src/main/mrsequential.go`
- Sample MapReduce applications:
    - **Word Count** (`mrapps/wc.go`)
    - **Text Indexer** (`mrapps/indexer.go`)

To run a sequential word count:
```
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```

The output will be in `mr-out-0` and should contain word counts from the input files.

---

## Implementation Details
Task was to implement a **distributed MapReduce system** consisting of three main components:

1. **Coordinator (mrcoordinator.go)** (`src/mr/coordinator.go`)
    - Assigns Map and Reduce tasks to workers
    - Detects worker failures and reassigns tasks
    - Terminates when all tasks are complete
2. **Worker (mrworker.go)** (`src/mr/worker.go`)
    - Requests tasks from the coordinator
    - Executes Map or Reduce functions
    - Reads and writes intermediate files
    - Reports completion to the coordinator
3. **RPC Definitions** (`src/mr/rpc.go`)

Workers communicate with the coordinator using **RPC**. Tasks must be reassigned if a worker fails to complete them within **10 seconds**.

---

### Execution Flow

1. **Start the Coordinator** with input files:
   ```sh
   $ rm mr-out*
   $ go run src/main/mrcoordinator.go pg-*.txt
   ```
2. **Start Worker Processes**:
   ```sh
   $ go run src/main/mrworker.go mrapps/wc.so
   ```
3. The workers will request tasks from the coordinator, process them, and write output files.
4. When complete, verify the output:
   ```sh
   $ cat mr-out-* | sort | more
   ```

### Task Scheduling

- The coordinator assigns **Map tasks** first. Each worker processes a split of the input and produces intermediate files.
- Once all Map tasks are completed, the coordinator assigns **Reduce tasks**.
- The coordinator detects failed workers by checking task completion within a 10-second timeout.

---

## Testing

A test script is provided to validate the correctness and robustness of the implementation:

```sh
$ cd src/main
$ bash test-mr.sh
```

Expected output upon successful completion:

```
*** PASSED ALL TESTS
```

---

## Design Considerations

### Intermediate File Storage

- **Intermediate File Naming:** Use the format `mr-X-Y`, where **X** is the Map task number and **Y** is the Reduce task number.
- **Temporary Files:** Write output to a temporary file and rename it atomically using `os.Rename`.
- JSON encoding is recommended for storing key/value pairs.

### Worker Termination

- Workers should exit when the coordinator terminates.
- If `call()` to the coordinator fails, the worker should assume the job is done and exit.

### Concurrency & Synchronization

- The coordinator manages concurrent RPC requests.
- Use locks to protect shared data.
---


### References

- [Original MapReduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
- [Go RPC Documentation](https://pkg.go.dev/net/rpc)
- [Go Concurrency Patterns](https://go.dev/doc/effective_go#concurrency)
- [Lab guidance](https://pdos.csail.mit.edu/6.824/labs/guidance.html)


## Code Structure
```
MapReduce/
├── src/
│   ├── main/
│   │   ├── mrcoordinator.go  # Coordinator logic
│   │   ├── mrworker.go       # Worker logic
│   │   ├── mrsequential.go   # Sequential implementation
│   │   ├── test-mr.sh        # Test script
│   ├── mr/
│   │   ├── coordinator.go    # Implement your coordinator here
│   │   ├── worker.go         # Implement your worker here
│   │   ├── rpc.go            # RPC definitions
│   ├── mrapps/
│   │   ├── wc.go             # Word count application
│   │   ├── indexer.go        # Text indexer application
```



### License
This project is part of MIT's 6.5840 Distributed Systems course and is for educational purposes only.
