# 6.5840

## [Fault-tolerant Key/Value Service](http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvraft.html)

### Introduction
This project implements a fault-tolerant key/value service using the Raft consensus algorithm. It is designed to handle client requests reliably, even in the presence of server failures and network partitions. The implementation is based on **MIT's 6.824 Distributed Systems course**.

## Features
- **Fault tolerance:** Uses the Raft protocol to maintain consensus across replicas.
- **Linearizability:** Ensures strong consistency for read and write operations.
- **Leader election:** Automatically handles leader failures and re-elects a new leader.
- **Persistent state:** Maintains logs to recover from crashes.
- **Concurrency:** Efficient request handling with coordination through Raft.

## Implementation Details
- **Clerk**: Handles client interactions and manages RPC communication with key/value servers.
- **Service**: Implements the key/value store and coordinates with Raft.
- **Raft**: Ensures replication and consensus.

### Functionality
Clients interact with the key/value service using the following operations:
- `Put(key, value)`: Replaces the value for the given key.
- `Append(key, arg)`: Appends `arg` to the existing value.
- `Get(key)`: Retrieves the value of the key.

Operations should be **linearizable**, ensuring that they behave as if executed sequentially in a single state machine.


## Project Structure
```
Fault-Tolerant-KV-Server
  ├── src
  │   ├── kvraft
  │   │   ├── client.go
  │   │   ├── common.go
  │   │   ├── config.go
  │   │   ├── server.go
  │   │   ├── test_test.go
```

### Lab Structure
This lab is divided into two parts:
#### **Part A: Key/Value Service Without Snapshots**
1. Implement a key/value server that integrates with Raft.
2. Ensure proper client-server interaction, retrying requests in case of failures.
3. Maintain correctness under leader election and network partitions.
4. Implement duplicate request detection to ensure **idempotency**.
5. Pass all **Lab 4A tests** to verify correctness under different failure scenarios.

#### **Part B: Key/Value Service With Snapshots**
1. Modify the key/value service to use **Raft snapshots** when the log grows too large.
2. Implement state persistence and recovery using snapshots.
3. Ensure efficient memory usage by discarding old log entries.
4. Pass all **Lab 4B tests** to confirm correct behavior with snapshotting and recovery.

#### Tests:
```sh
go test -run 4A  # Test Part A
```
```sh
go test -run 4B  # Test Part B
```

### References
- [Raft Paper](https://raft.github.io/raft.pdf) (Sections 7 & 8)
- [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/)


### License
This project is part of MIT's 6.5840 Distributed Systems course and is for educational purposes only.
