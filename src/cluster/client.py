"""
Node Client Module

Provides async TCP client for inter-node communication.
Used for request forwarding and synchronous replication.
"""

import asyncio
import logging
from typing import Optional

from .config import NodeInfo

logger = logging.getLogger(__name__)


class NodeClient:
    """
    Async TCP client for communicating with other nodes in the cluster.
    
    Used for:
    - Forwarding requests to the primary node when this node receives
      a request for a key it doesn't own
    - Replicating writes to the replica node
    
    Usage:
        client = NodeClient()
        response = await client.send_command(node_info, "PUT key value")
    """
    
    def __init__(self, timeout: float = 5.0):
        """
        Initialize the node client.
        
        Args:
            timeout: Connection and read timeout in seconds
        """
        self.timeout = timeout
    
    async def send_command(
        self, 
        node: NodeInfo, 
        command: str,
        is_internal: bool = False
    ) -> Optional[str]:
        """
        Send a command to another node and return the response.
        
        Args:
            node: The target node information
            command: The raw command string to send (without trailing newline)
            is_internal: If True, this is an internal request (forwarded or replicate)
        
        Returns:
            The response string from the node (without trailing newline),
            or None if the request failed.
        
        Raises:
            asyncio.TimeoutError: If the connection or read times out
            ConnectionError: If unable to connect to the node
        """
        try:
            # Connect to the target node
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.host, node.port),
                timeout=self.timeout
            )
            
            try:
                # Ensure command ends with newline
                if not command.endswith('\n'):
                    command = command + '\n'
                
                # Send the command
                writer.write(command.encode('utf-8'))
                await writer.drain()
                
                # Read the response
                response_bytes = await asyncio.wait_for(
                    reader.readline(),
                    timeout=self.timeout
                )
                
                response = response_bytes.decode('utf-8').strip()
                return response
                
            finally:
                # Close the connection
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to node {node.node_id} at {node.address()}")
            raise
        except ConnectionError as e:
            logger.error(f"Connection error to node {node.node_id} at {node.address()}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error sending command to node {node.node_id}: {e}")
            raise
    
    async def forward_request(self, node: NodeInfo, command: str) -> Optional[str]:
        """
        Forward a client request to another node.
        
        The command is prefixed with FORWARD to indicate it's a forwarded request.
        The receiving node should execute the command and return the response.
        
        Args:
            node: The target node to forward to
            command: The original command from the client
        
        Returns:
            The response from the forwarded request
        """
        # Strip any existing prefix and add FORWARD prefix
        command = command.strip()
        forward_command = f"FORWARD {command}"
        
        return await self.send_command(node, forward_command, is_internal=True)
    
    async def replicate_command(self, node: NodeInfo, command: str) -> Optional[str]:
        """
        Replicate a write command to a replica node.
        
        The command is prefixed with REPLICATE to indicate it's a replication request.
        The receiving node should store the data locally without further forwarding.
        
        Args:
            node: The replica node to replicate to
            command: The PUT or DELETE command to replicate
        
        Returns:
            The acknowledgment from the replica node
        """
        # Strip any existing prefix and add REPLICATE prefix
        command = command.strip()
        replicate_command = f"REPLICATE {command}"
        
        return await self.send_command(node, replicate_command, is_internal=True)
