# 6.5840 - Spring 2025

## Lab 1: MapReduce

### Introduction

This project is an implementation of a distributed MapReduce framework inspired by the original [MapReduce paper](https://research.google/pubs/pub62/). The implementation consists of a **coordinator** process that manages task distribution and fault tolerance, and multiple **worker** processes that execute map and reduce tasks in parallel. Workers communicate with the coordinator using RPC.

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

Clone the repository and navigate to the project directory:

```sh
$ git clone git://g.csail.mit.edu/6.5840-golabs-2025 6.5840
$ cd 6.5840
```

### Running the Sequential Implementation

To get familiar with the MapReduce framework, start by running the provided sequential version:

```sh
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
```

The output will be in `mr-out-0` and should contain word counts from the input files.

---

## Implementing Distributed MapReduce

Your task is to implement the distributed version using:

- **Coordinator** (`mr/coordinator.go`)
- **Worker** (`mr/worker.go`)
- **RPC Definitions** (`mr/rpc.go`)

### Execution Flow

1. **Start the Coordinator** with input files:
   ```sh
   $ rm mr-out*
   $ go run mrcoordinator.go pg-*.txt
   ```
2. **Start Worker Processes**:
   ```sh
   $ go run mrworker.go wc.so
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
$ cd ~/6.5840/src/main
$ bash test-mr.sh
```

Expected output upon successful completion:

```
*** PASSED ALL TESTS
```

If a test fails, ensure your output files are formatted correctly and that tasks are executed in parallel.

---

## Design Considerations

### Intermediate File Storage

- Use the format `mr-X-Y`, where **X** is the Map task number and **Y** is the Reduce task number.
- JSON encoding is recommended for storing key/value pairs.

### Worker Termination

- Workers should exit when the coordinator terminates.
- If `call()` to the coordinator fails, the worker should assume the job is done and exit.

### Concurrency & Synchronization

- The coordinator manages concurrent RPC requests.
- Use locks to protect shared data.
- Test for race conditions with:
  ```sh
  $ go run -race mrworker.go wc.so
  ```

---

## Hints & Debugging

- Start by implementing **basic task request handling** in `mr/coordinator.go` and `mr/worker.go`.
- Use `log.Printf()` for debugging RPC interactions.
- Test with `mrapps/crash.go` to ensure fault tolerance.
- Use **temporary files** and rename them atomically to prevent incomplete writes.

---

## Conclusion

This lab provides hands-on experience with distributed systems and fault-tolerant task execution. By completing it, you will gain insights into scheduling, concurrency, and distributed coordination.

---

### References

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/)
- [Go RPC Documentation](https://pkg.go.dev/net/rpc)
- [Go Concurrency Patterns](https://go.dev/doc/effective_go#concurrency)
