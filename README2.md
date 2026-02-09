# Final Project: Distributed KV-Cache with Sharding & Replication

- **Release Date:** 25th January, 2026
- **Due Date:** `11:59 PM`, ~31st January~ 1st February, 2026
- **Total Points:** 100

***Note:** The final project is a strict extension to the [2nd assignment: **kv-cache**](https://github.com/AgarwalPragy/kv-cache). You MUST complete the 2nd assignment before you approach the final project.*

## Overview

Extend your Assignment 2 **KV-Cache** to support **sharding** (distributing keys across multiple nodes) and **replication** (copying data for fault tolerance).

You will transform your single-node cache into a **3-node cluster** where:
- Keys are **partitioned** across nodes using consistent hashing
- Each key is **replicated** to one other node for redundancy


## What You're Building

```
        Client
           │
           ▼
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │    Node 1    │◄────►│    Node 2    │◄────►│    Node 3    │
    │  Shard 0,1   │      │  Shard 1,2   │      │  Shard 2,0   │
    └──────────────┘      └──────────────┘      └──────────────┘
    
    Each node:
    - Is PRIMARY for some shards (handles writes)
    - Is REPLICA for other shards (receives copies)
```

**Example with 3 shards:**
| Shard | Primary | Replica |
|-------|---------|---------|
| 0 | Node 1 | Node 3 |
| 1 | Node 2 | Node 1 |
| 2 | Node 3 | Node 2 |


## Requirements

### Sharding (Key Routing)

- Use consistent hashing to map keys to shards: `shard = hash(key) % num_shards`
- Client connects to **any node**
- If a node receives a request for a key it doesn't own, it **forwards** the request to the correct node and returns the response

### Replication

- Every write (PUT/DELETE) must be sent to **both primary and replica**
- Use **synchronous replication**: return success only after both nodes acknowledge
- Reads (GET/EXISTS) can be served by primary only (simpler)

### Simplifying Assumptions (You May Assume)

- All 3 nodes are always running (no failure handling required)
- Cluster configuration is static (no nodes joining/leaving)
- Network is reliable (no timeouts needed, but don't block forever)
- No need for cluster management commands


## Tasks & Scoring

### Task 1: Consistent Hashing & Request Forwarding (30 points)

**Goal:** Route requests to the correct node based on key hash.

**Implement:**
1. Hash function to determine shard: `shard_id = hash(key) % NUM_SHARDS`
2. Configuration to know which node owns which shard
3. Request forwarding: if key belongs to another node, forward and relay response

**Example:**
```
# Connect to Node 1
PUT user:alice hello    # If hash("user:alice") maps to Node 2, 
                        # Node 1 forwards to Node 2, returns "OK stored"

GET user:alice          # Node 1 forwards to Node 2, returns "OK hello"
```

---

### Task 2: Replication (30 points)

**Goal:** Copy writes to a replica node for redundancy.

**Implement:**
1. Configuration mapping each shard to its replica node
2. On PUT/DELETE: write to primary, then replicate to replica
3. Return success only after both writes complete

**Example:**
```
# user:bob hashes to Shard 0 (Primary: Node 1, Replica: Node 3)
PUT user:bob world      # Node 1 stores locally AND sends to Node 3
                        # Returns "OK stored" after both succeed

# Verify replication by reading from replica directly
# (for testing - normally clients don't do this)
```


### Task 3: Integration & Documentation (10 points)

**Goal:** Working 3-node cluster with documentation.

**Deliverables:**
1. `docker-compose.yml` that starts 3 nodes
2. `PROJECT.md` documenting:
   - Your architecture
   - How sharding works in your implementation
   - How replication works in your implementation
   - Any challenges you faced


### Task 4: Research Write-up (30 points)


Your implementation makes simplifying assumptions (all nodes always up, no 
failures, static config). In this section, explain how you would handle 
the harder problems, or how production systems like Redis Cluster solve them.

Pick **ANY THREE** of the following topics and write 2-3 paragraphs each:

#### 1. Failure Detection
How would you detect that a node has crashed or become unreachable? 
What's the difference between a crashed node and a slow node? 
How does Redis Cluster handle this?

#### 2. Automatic Failover
If a primary node dies, how would you promote a replica to become the 
new primary? How do other nodes learn about this change? 
What problems can occur during failover?

#### 3. Handling Network Partitions
What happens if Node 1 can talk to Node 2, but neither can reach Node 3? 
What is split-brain and how do you prevent it?
How does the CAP theorem apply here?

#### 4. Dynamic Cluster Membership
How would you add a new node to a running cluster? 
How would you rebalance data across nodes? 
What is slot migration in Redis Cluster?

#### 5. Consistency vs Availability Trade-offs
Your implementation uses synchronous replication. What are the trade-offs 
vs asynchronous replication? When might you lose data with async replication?
What consistency guarantees does Redis Cluster provide?

#### 6. Read Scaling
How would you distribute read load across replicas? 
What is replica lag and why does it matter?
How would a client know if a replica has stale data?


### `PROJECT.md` Template

```markdown
Final Project: Distributed KV-Cache
===================================

Part A: Your Implementation
---------------------------

### Architecture
[Simple explanation of how the KV-cache is architectured, and how keys/replicas are distributed]

### Sharding
[2-3 sentences on how you route keys to nodes]

### Replication  
[2-3 sentences on how you replicate writes]

### Challenges
[What was hardest? How did you solve it?]


Part B: Beyond the Basics (Research Write-up)
---------------------------------------------

```

**Note:** You may use LLMs, documentation, blog posts, or papers to research these topics. However, you must be prepared to explain and discuss your answers during vivas.

## What's NOT Required

To keep scope manageable, you do **NOT** need to implement:

- Failure detection or handling (assume all nodes are up)
- Automatic failover
- Dynamic cluster membership
- CLUSTER INFO/NODES commands
- Read from replicas
- Conflict resolution
- AWS deployment (Docker Compose is sufficient)

## Scoring Summary

| Task | Points |
|------|--------|
| Task 1: Sharding & Forwarding | 30 |
| Task 2: Replication | 30 |
| Task 3: Integration & Docker | 10 |
| Task 4: Research Write-up (Part B) | 30 |
| **Total** | **100** |

---

## Submission

### Submission Process
1. Push `final-project-submission` branch in the **same repository as assignment-2**
2. Submit via form: https://forms.gle/kCNWSnXtYCx7MrcJ7
3. **Due:** `11:59 PM`, ~31st January~ 1st February, 2026

### Required Files
- All source code in `final-project-submission` branch
- `docker-compose.yml`
- `PROJECT.md` (must include both Part A and Part B)

### Repository Access
Private repo with read access to:
- [AgarwalPragy](https://github.com/AgarwalPragy)
- [anshumansingh](https://github.com/anshumansingh)

---

## FAQ

**Q: What if a node is down?**
A: For this project, assume all nodes are always running. Don't worry about failures.

**Q: Should reads go to replicas too?**
A: No. Keep it simple -  reads go to primary only. Replicas just store backup copies.

**Q: Can I use external libraries?**
A: Standard library only. No Redis, etcd, or distributed systems libraries.

**Q: Can I use LLMs to help?**
A: Yes! But you must understand and explain every line during vivas. Inability to explain your code = **zero** score.

**Q: My Assignment 2 has bugs. Should I fix them first?**
A: Yes. This project builds on Assignment 2.

# "hello" hashes to Shard 0 → Node 1 is primary
# But let's send it to Node 2 (port 7172)
echo "PUT hello world" | nc -w 2 localhost 7172
# Output: OK stored  ← Forwarded to Node 1!

# Verify it went to Node 1:
echo "GET hello" | nc -w 2 localhost 7171
# Output: OK world