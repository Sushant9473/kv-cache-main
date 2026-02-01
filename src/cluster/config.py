"""
Cluster Configuration Module

Defines the cluster topology including:
- Node definitions (id, host, port)
- Shard-to-primary mapping
- Shard-to-replica mapping

Configuration is read from environment variables or uses defaults for local development.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional


# Number of shards in the cluster
NUM_SHARDS = 3

# Number of nodes in the cluster
NUM_NODES = 3


@dataclass
class NodeInfo:
    """Information about a single node in the cluster."""
    node_id: int
    host: str
    port: int
    
    def address(self) -> str:
        """Return the node address as host:port."""
        return f"{self.host}:{self.port}"


@dataclass
class ClusterConfig:
    """
    Cluster configuration defining the topology.
    
    Attributes:
        nodes: Dictionary of node_id -> NodeInfo
        shard_to_primary: Dictionary of shard_id -> node_id (primary owner)
        shard_to_replica: Dictionary of shard_id -> node_id (replica owner)
        current_node_id: The ID of this node
    """
    nodes: Dict[int, NodeInfo]
    shard_to_primary: Dict[int, int]
    shard_to_replica: Dict[int, int]
    current_node_id: int
    
    def get_primary_node_for_shard(self, shard_id: int) -> NodeInfo:
        """Get the primary node for a given shard."""
        node_id = self.shard_to_primary[shard_id]
        return self.nodes[node_id]
    
    def get_replica_node_for_shard(self, shard_id: int) -> NodeInfo:
        """Get the replica node for a given shard."""
        node_id = self.shard_to_replica[shard_id]
        return self.nodes[node_id]
    
    def is_primary_for_shard(self, shard_id: int) -> bool:
        """Check if the current node is the primary for the given shard."""
        return self.shard_to_primary[shard_id] == self.current_node_id
    
    def is_replica_for_shard(self, shard_id: int) -> bool:
        """Check if the current node is the replica for the given shard."""
        return self.shard_to_replica[shard_id] == self.current_node_id
    
    def get_shards_as_primary(self) -> List[int]:
        """Get list of shard IDs where this node is primary."""
        return [shard_id for shard_id, node_id in self.shard_to_primary.items()
                if node_id == self.current_node_id]
    
    def get_shards_as_replica(self) -> List[int]:
        """Get list of shard IDs where this node is replica."""
        return [shard_id for shard_id, node_id in self.shard_to_replica.items()
                if node_id == self.current_node_id]


def get_cluster_config(node_id: Optional[int] = None) -> ClusterConfig:
    """
    Get the cluster configuration.
    
    The configuration can be customized via environment variables:
    - KV_NODE_ID: The ID of this node (1, 2, or 3)
    - KV_NODE1_HOST, KV_NODE1_PORT: Node 1 address
    - KV_NODE2_HOST, KV_NODE2_PORT: Node 2 address
    - KV_NODE3_HOST, KV_NODE3_PORT: Node 3 address
    
    Default configuration (for docker-compose):
        Node 1: node1:7171, Primary for shard 0, Replica for shard 1
        Node 2: node2:7172, Primary for shard 1, Replica for shard 2
        Node 3: node3:7173, Primary for shard 2, Replica for shard 0
    
    Args:
        node_id: Override for the current node ID (defaults to KV_NODE_ID env var)
    
    Returns:
        ClusterConfig with the cluster topology
    """
    # Determine current node ID
    if node_id is None:
        node_id = int(os.environ.get("KV_NODE_ID", "1"))
    
    # Build node definitions from environment or defaults
    nodes = {
        1: NodeInfo(
            node_id=1,
            host=os.environ.get("KV_NODE1_HOST", "node1"),
            port=int(os.environ.get("KV_NODE1_PORT", "7171"))
        ),
        2: NodeInfo(
            node_id=2,
            host=os.environ.get("KV_NODE2_HOST", "node2"),
            port=int(os.environ.get("KV_NODE2_PORT", "7172"))
        ),
        3: NodeInfo(
            node_id=3,
            host=os.environ.get("KV_NODE3_HOST", "node3"),
            port=int(os.environ.get("KV_NODE3_PORT", "7173"))
        ),
    }
    
    # Shard to primary mapping
    # Shard 0 -> Node 1, Shard 1 -> Node 2, Shard 2 -> Node 3
    shard_to_primary = {
        0: 1,
        1: 2,
        2: 3,
    }
    
    # Shard to replica mapping (each replica is on the next node)
    # Shard 0 -> Node 3, Shard 1 -> Node 1, Shard 2 -> Node 2
    shard_to_replica = {
        0: 3,
        1: 1,
        2: 2,
    }
    
    return ClusterConfig(
        nodes=nodes,
        shard_to_primary=shard_to_primary,
        shard_to_replica=shard_to_replica,
        current_node_id=node_id,
    )
