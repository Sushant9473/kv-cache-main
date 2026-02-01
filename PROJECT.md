Final Project: Distributed KV-Cache
===================================

Part A: Your Implementation
---------------------------

### Architecture

The distributed KV-Cache is a 3-node cluster that provides a fault-tolerant, 
scalable key-value store. Each node acts as both a primary for some shards 
and a replica for others, ensuring data redundancy across the cluster.

```
        Client (connects to any node)
           │
           ▼
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │    Node 1    │◄────►│    Node 2    │◄────►│    Node 3    │
    │  Primary: 0  │      │  Primary: 1  │      │  Primary: 2  │
    │  Replica: 1  │      │  Replica: 2  │      │  Replica: 0  │
    │  Port: 7171  │      │  Port: 7172  │      │  Port: 7173  │
    └──────────────┘      └──────────────┘      └──────────────┘
```

**Shard Distribution:**

| Shard | Primary | Replica |
|-------|---------|---------|
| 0     | Node 1  | Node 3  |
| 1     | Node 2  | Node 1  |
| 2     | Node 3  | Node 2  |

**Key Components:**
- **ClusterConfig** (`src/cluster/config.py`): Defines node topology and shard mappings
- **Consistent Hashing** (`src/cluster/hashing.py`): MD5-based key-to-shard mapping
- **NodeClient** (`src/cluster/client.py`): Async TCP client for inter-node communication
- **KVServer** (`src/network/tcp_server.py`): Extended server with routing and replication

### Sharding

Keys are distributed across shards using consistent hashing with the formula 
`shard_id = hash(key) % 3`. We use MD5 hashing to ensure deterministic results 
across all nodes—the same key always maps to the same shard regardless of which 
node receives the request. When a node receives a request for a key it doesn't 
own as primary, it forwards the request to the correct primary node using the 
`FORWARD` protocol prefix.

### Replication

Every write operation (PUT/DELETE) uses synchronous replication. When the 
primary node receives a write request, it first stores the data locally, then 
sends a `REPLICATE` command to the designated replica node. The primary waits 
for the replica's acknowledgment before returning success to the client. This 
ensures that data is stored on at least two nodes before confirming the write, 
providing fault tolerance.

### Challenges

The main challenge was designing the internal protocol for distinguishing 
between different types of requests. Client requests need routing logic, 
forwarded requests should execute locally but still replicate, and replicate 
requests should store locally without further forwarding. I solved this by 
using protocol prefixes (`FORWARD` and `REPLICATE`) that the server strips 
before processing the underlying command.

Another challenge was ensuring proper async handling of inter-node 
communication. The NodeClient uses asyncio for non-blocking connections, 
but I had to carefully handle timeouts and connection errors to prevent 
the server from hanging when other nodes are temporarily unreachable.

---

Part B: Beyond the Basics (Research Write-up)
---------------------------------------------

### 1. Failure Detection

Detecting node failures in a distributed system is fundamentally challenging 
because there's no way to distinguish between a crashed node, a slow node, 
and a network partition. A common approach is using heartbeat mechanisms where 
nodes periodically exchange "I'm alive" messages. If a node misses several 
consecutive heartbeats (typically 3-5), it's marked as potentially failed. 
However, setting the right timeout is tricky: too short causes false positives 
during garbage collection pauses or network congestion; too long delays 
failure detection.

Redis Cluster uses a gossip protocol called PING-PONG where each node sends 
random PING messages to other nodes. When a node doesn't respond within 
`cluster-node-timeout` milliseconds, it's marked as PFAIL (possible failure). 
If the majority of master nodes also mark that node as PFAIL, it becomes 
FAIL status. This consensus approach helps distinguish between actual failures 
and temporary network issues between specific node pairs.

The key insight is that failure detection must be probabilistic rather than 
deterministic. Modern systems like Redis also use techniques like calculating 
packet loss percentages over time windows rather than relying on single missed 
heartbeats. This provides more reliable detection at the cost of slightly 
slower failure identification.

### 2. Automatic Failover

When a primary node fails, the system must promote a replica to become the 
new primary. This involves several steps: first, replicas must detect that 
the primary is truly down (using the failure detection mechanisms above). 
Then, if multiple replicas exist, they must elect which one becomes the new 
primary—typically the replica with the most up-to-date data. Finally, all 
nodes must be notified of the topology change so they can route requests 
to the new primary.

Redis Cluster handles this through a leader election process. When a master 
fails, its replicas wait a randomized delay (to prevent split-brain), then 
request votes from all other masters. A replica needs votes from a majority 
of masters to become the new primary. Once elected, the new primary announces 
its promotion through the gossip protocol, and other nodes update their 
routing tables. The randomized delay and vote requirement ensure that only 
one replica wins, preventing scenarios where multiple replicas think they're 
the primary.

Problems during failover include: data loss if the replica was behind the 
failed primary (asynchronous replication), brief unavailability during the 
election process, and potential split-brain if network partitions occur 
during failover. Redis accepts some data loss for availability, making it 
an AP system in CAP terms during network partitions.

### 3. Consistency vs Availability Trade-offs

My implementation uses synchronous replication: writes return success only 
after both primary and replica acknowledge. This provides strong durability 
guarantees—data exists on two nodes before confirmation—but at the cost of 
latency (waiting for replica) and availability (if replica is down, writes 
fail). Synchronous replication is essentially choosing consistency over 
availability in the CAP theorem.

Asynchronous replication, used by default in Redis Cluster, provides better 
performance and availability. The primary returns success immediately after 
local storage, then replicates in the background. This means writes are fast 
and still succeed even if replicas are unreachable. However, if the primary 
crashes before replication completes, those writes are lost. Redis 
documentation explicitly states that acknowledged writes can be lost during 
failover.

Redis Cluster provides eventual consistency: given enough time without 
failures, all replicas will converge to the same state. It also offers 
optional WAIT command where clients can request synchronous replication 
for specific critical writes. This gives applications flexibility to choose 
per-operation consistency levels. The trade-off is clear: async is faster 
and more available, sync is more durable but slower and less available.
