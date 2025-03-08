# Distributed Systems Projects

This repository contains the implementations of the assignments from the [MIT 6.824: Distributed Systems](http://nil.csail.mit.edu/6.5840/2024/) course. I have successfully completed each assignment and achieved a 100% grade in this labs.

## Course Overview

MIT's 6.824 is a graduate-level course focusing on the principles and techniques behind the design of distributed systems. Key topics include fault tolerance, replication, and consistency. The course extensively uses case studies to explore real-world distributed systems.

## Assignments

The course comprises several hands-on programming assignments designed to reinforce the concepts discussed in lectures. The assignments I completed are as follows:

1. **MapReduce**
    - **Objective:** Implement a basic MapReduce framework to process large datasets across multiple machines.
    - **Details:** Developed both the Map and Reduce functions, managed task distribution, and handled fault tolerance mechanisms.

2. **Fault-Tolerant Key/Value Service**
    - **Objective:** Build a single-node key/value store that guarantees at-most-once execution of operations despite network failures.
    - **Details:** Used versioning to prevent stale writes and ensure linearizability, providing strong consistency guarantees.

3. **Raft Consensus Algorithm**
    - **Objective:** Implement the Raft consensus algorithm to achieve distributed consensus in a network of unreliable processors.
    - **Details:** Covered leader election, log replication, and state persistence to ensure consistency across nodes.

4. **Sharded Key/Value Service**
    - **Objective:** Implement a sharded key/value service to distribute data across multiple servers.
    - **Details:** Designed dynamic reconfiguration mechanisms for balancing shards across multiple replicated Raft groups.

## Running the Projects

Each assignment is contained within its respective directory. To run a specific project:

1. Navigate to the project's directory:
   ```
   cd <lab-name>


## Code Structure
```
Distributed-Systems
├── MapReduce/
│   ├── src/
│   ├── README.md
├── KV-Server/
│   ├── src/
│   ├── README.md
├── Raft/
│   ├── src/
│   ├── README.md
├── Fault-Tolerant-KV-Server/
│   ├── src/
│   ├── README.md
├── .gitignore
├── go.mod
├── README.md

```


### References
- [MIT 6.824: Distributed Systems](http://nil.csail.mit.edu/6.5840/2024/)


### License
This project is part of MIT's 6.5840 Distributed Systems course and is for educational purposes only.

