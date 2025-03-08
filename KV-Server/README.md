# 6.5840

## [Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)

### Introduction

This lab involves building a single-machine key/value server that ensures **at-most-once execution** for `Put` operations despite network failures and guarantees **linearizability**. The KV server will later be extended in future labs to handle server crashes.

### Features
- **Key/Value Storage**: Stores key-value pairs with version numbers to enforce conditional updates.
- **Linearizability**: Ensures operations appear as if executed sequentially.
- **At-Most-Once Semantics**: Prevents duplicate `Put` operations due to network retransmissions.
- **Client-Server Communication**: Clients interact with the server via RPCs (`Put` and `Get`).
- **Lock Implementation**: Implements a distributed lock using `Put` and `Get`.
- **Fault Tolerance**: Handles dropped and reordered RPC messages.

---

## Implementation Details
### 1. **Key/Value Server**
Each client uses a `Clerk` to interact with the key/value server via RPC. The server maintains an in-memory map that records, for each key, a `(value, version)` tuple.

- **`Put(key, value, version)`**: Updates the key's value if the version matches; otherwise, returns `rpc.ErrVersion`.
    - New keys are created with version `1`.
    - If `Put` refers to a non-existent key (and version > 0), returns `rpc.ErrNoKey`.
- **`Get(key)`**: Retrieves the current value and version of a key.
    - Returns `rpc.ErrNoKey` if the key doesn't exist.

### 2. **Ensuring At-Most-Once Execution**
Since RPCs can be lost or duplicated due to unreliable networks, the `Clerk` retries failed requests. However, re-executing `Put` requests blindly could lead to duplicate updates. To avoid this:
- The **server enforces version control**—a `Put` executes only if the given version matches the stored version.
- If a retransmitted `Put` receives `rpc.ErrVersion`, the `Clerk` returns `rpc.ErrMaybe`, leaving the application to decide how to proceed.

### 3. **Lock Implementation**
To coordinate distributed clients, a lock is implemented using the KV store:
- **`Acquire(lockKey)`**: Attempts to set the lock key using a unique identifier.
- **`Release(lockKey)`**: Removes the lock by deleting the key.

This ensures that only one client holds the lock at a time. Clients must handle cases where a crashed client never releases its lock.

### 4. **Handling Dropped RPC Messages**
With an unreliable network:
- The `Clerk` retries RPCs until a response is received.
- If a `Put` RPC is retried and the server has already processed it, the version check prevents a duplicate execution.
- If the server has responded with `rpc.ErrVersion`, the `Clerk` returns `rpc.ErrMaybe`.

---

## Testing
Ensure your implementation passes all provided tests:
```
# Run all tests
$ cd ~/KV-Server/src/kvsrv
$ go test -v

# Run unreliable network tests
$ go test -v -run Unreliable
```
Check for race conditions using:
```
$ go test -race
```

---

## Key Learnings
- Implementing **linearizable** state machines for distributed systems.
- Using **idempotent RPCs** to achieve at-most-once semantics.
- Handling **retransmissions and dropped messages** in an unreliable network.
- Designing **synchronization mechanisms** (locks) using a simple key/value store.


## Code Structure
```
KV-Server/
├── src/
│   ├── kvsrv/
│   │   ├── client.go
│   │   ├── common.go
│   │   ├── config.go
│   │   ├── server.go
│   │   ├── test_test.go
```

### License
This project is part of MIT's 6.5840 Distributed Systems course and is for educational purposes only.
