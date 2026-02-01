"""
Cluster Package

This package provides distributed cluster functionality including:
- Cluster configuration and node definitions
- Consistent hashing for key-to-shard mapping
- Inter-node communication client
"""

from .config import ClusterConfig, NodeInfo, get_cluster_config
from .hashing import get_shard_for_key
from .client import NodeClient

__all__ = [
    'ClusterConfig',
    'NodeInfo', 
    'get_cluster_config',
    'get_shard_for_key',
    'NodeClient',
]
