"""
Consistent Hashing Module

Provides deterministic key-to-shard mapping using consistent hashing.
Uses MD5 hash for reproducible results across all nodes.
"""

import hashlib
from .config import NUM_SHARDS


def get_shard_for_key(key: str, num_shards: int = NUM_SHARDS) -> int:
    """
    Determine the shard ID for a given key using consistent hashing.
    
    Uses MD5 hash to ensure deterministic results across all nodes.
    The same key will always map to the same shard.
    
    Args:
        key: The key to hash
        num_shards: Number of shards in the cluster (default: NUM_SHARDS)
    
    Returns:
        Shard ID (0 to num_shards-1)
    
    Examples:
        >>> get_shard_for_key("user:alice")
        1
        >>> get_shard_for_key("user:bob")
        0
    """
    # Use MD5 for deterministic hashing
    hash_bytes = hashlib.md5(key.encode('utf-8')).digest()
    
    # Convert first 4 bytes to integer
    hash_int = int.from_bytes(hash_bytes[:4], byteorder='big')
    
    # Return shard ID using modulo
    return hash_int % num_shards
