# 6.5840

## [Raft](http://nil.csail.mit.edu/6.5840/2024/labs/lab-raft.html)

### Introduction

This repository contains an implementation of the Raft consensus algorithm as part of Lab 3 in the MIT 6.5840 course. The goal of this lab is to build a fault-tolerant, replicated state machine using Raft, which serves as the foundation for a distributed key/value store.

## Overview

Raft is a consensus algorithm designed to manage a replicated log across multiple servers, ensuring consistency despite failures. It achieves fault tolerance by replicating logs across a cluster and electing a leader to coordinate operations. If the leader fails, a new leader is elected, ensuring continued operation as long as a majority of servers remain reachable.

This implementation follows the Raft paper, focusing on:
- Leader election
- Log replication
- Persistence and recovery
- Safety guarantees

## Project Structure

```
📁 src/
   📁 raft/              # Core Raft implementation
      ├── raft.go        # Main Raft logic
      ├── persister.go   # Persistent state handling
   📁 labrpc/            # RPC library for simulating network conditions
   📁 kvraft/            # Key/value store (used in later labs)
   📄 Makefile           # Build automation
   📄 README.md          # Project documentation
```

## Raft API

The Raft module exposes the following API:

```go
rf := Make(peers, me, persister, applyCh) // Create a new Raft instance

index, term, isLeader := rf.Start(command) // Propose a new log entry

term, isLeader := rf.GetState() // Get the current term and leadership status
```

Each Raft peer communicates with others via Remote Procedure Calls (RPCs) to maintain log consistency and handle elections.

## Implementation Details

### Part 3A: Leader Election

- Each server starts as a **follower**.
- If no leader is detected, a server transitions to **candidate** state and requests votes.
- The candidate receiving a majority of votes becomes the **leader**.
- The leader sends periodic **heartbeat (AppendEntries RPCs)** to maintain authority.

#### Tests:
```sh
go test -run 3A
```

### Part 3B: Log Replication

- The leader appends commands to its log and propagates them via **AppendEntries RPCs**.
- Followers acknowledge receipt; once a majority confirms, entries are **committed**.
- Committed entries are applied to the state machine via `applyCh`.

#### Tests:
```sh
go test -run 3B
```

### Part 3C & 3D: Persistence & Safety

- Persistent state (log, term, vote) is stored and reloaded after failures.
- **Log consistency guarantees** prevent conflicting entries from being committed.

#### Tests:
```sh
go test -run 3C
```

```sh
go test -run 3D
```

## References
- [Raft Paper](https://raft.github.io/raft.pdf)
- [MIT 6.5840 Course](http://nil.csail.mit.edu/6.5840/2024/)

## License
This project is for educational purposes as part of MIT's 6.5840 course.

