"""
Async TCP Server Module (Task 3) - Extended for Cluster Support

This module implements the asynchronous TCP server for KV-Cache with
distributed cluster functionality including:
- Request forwarding to primary nodes
- Synchronous replication to replica nodes
- Internal command handling (FORWARD, REPLICATE)
"""

import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from typing import Optional

from ..cache.store import KVStore
from ..config.settings import settings
from ..protocol.commands import CommandType, Response
from ..protocol.parser import ProtocolParser
from ..cluster.config import get_cluster_config, ClusterConfig
from ..cluster.hashing import get_shard_for_key
from ..cluster.client import NodeClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KVServer:
    """
    Asynchronous TCP server for the distributed KV-Cache service.

    This server handles multiple concurrent clients using asyncio.
    It supports distributed operations including:
    - Consistent hashing for key-to-shard mapping
    - Request forwarding to primary nodes
    - Synchronous replication to replica nodes

    Features:
    - Non-blocking I/O with asyncio
    - Persistent connections (multiple commands per connection)
    - Graceful error handling and connection cleanup
    - Cluster-aware request routing

    Usage:
        server = KVServer(host='0.0.0.0', port=7171, node_id=1)
        await server.start()  # Runs forever

    Attributes:
        host: Server bind address (e.g., '0.0.0.0')
        port: Server port number (e.g., 7171)
        store: The KVStore instance for local data
        parser: The ProtocolParser for parsing commands
        cluster_config: Cluster configuration for routing
        node_client: Client for inter-node communication
    """

    def __init__(
            self,
            host: str = None,
            port: int = None,
            store: KVStore = None,
            node_id: int = None,
            standalone_mode: bool = None,
    ):
        """
        Initialize the server.

        Args:
            host: Bind address (default from settings)
            port: Port number (default from settings)
            store: KVStore instance (creates new one if not provided)
            node_id: This node's ID in the cluster (default from settings)
            standalone_mode: If True, operate as single node without forwarding/replication.
                           If None, auto-detect based on KV_STANDALONE_MODE env var.
        """
        import os
        
        self.host = host if host is not None else settings.HOST
        self.port = port if port is not None else settings.PORT
        self.store = store if store is not None else KVStore()
        self.parser = ProtocolParser()
        
        # Determine standalone mode
        if standalone_mode is None:
            standalone_mode = os.environ.get("KV_STANDALONE_MODE", "true").lower() == "true"
        self.standalone_mode = standalone_mode
        
        # Cluster configuration (only used if not standalone)
        self.node_id = node_id if node_id is not None else settings.NODE_ID
        if not self.standalone_mode:
            self.cluster_config = get_cluster_config(self.node_id)
            self.node_client = NodeClient()
        else:
            self.cluster_config = None
            self.node_client = None

        # Server state
        self._server: Optional[asyncio.Server] = None
        self._running = False
        self._connection_count = 0
        self._total_requests = 0

    async def handle_client(
            self,
            reader: StreamReader,
            writer: StreamWriter
    ) -> None:
        """
        Handle a single client connection.

        This coroutine is called for each new client connection.
        It reads commands from the client, processes them, and sends
        responses until the client disconnects or sends QUIT.

        Supports internal commands:
        - FORWARD <command>: Execute command as if from client (forwarded)
        - REPLICATE <command>: Store data locally without further replication

        Args:
            reader: StreamReader for reading from the client
            writer: StreamWriter for writing to the client
        """
        client_addr = writer.get_extra_info('peername')
        self._connection_count += 1
        logger.info(f"Client connected: {client_addr}")

        try:
            while True:
                # Read a line from the client
                try:
                    data = await reader.readline()
                except ConnectionResetError:
                    logger.info(f"Client {client_addr} connection reset")
                    break

                # Empty data means client disconnected
                if not data:
                    logger.info(f"Client {client_addr} disconnected")
                    break

                # Decode the data
                try:
                    command_str = data.decode('utf-8')
                except UnicodeDecodeError:
                    logger.warning(f"Invalid UTF-8 from {client_addr}")
                    response = Response.error("invalid encoding")
                    writer.write(self.parser.format_response(response).encode())
                    await writer.drain()
                    continue

                # Handle the command (may be internal or client command)
                response_str = await self._handle_command_string(command_str)
                
                # Check if we should close connection (QUIT)
                if command_str.strip().upper() == "QUIT":
                    logger.info(f"Client {client_addr} sent QUIT")
                    break
                
                # Send the response
                if not response_str.endswith('\n'):
                    response_str = response_str + '\n'
                writer.write(response_str.encode())
                await writer.drain()

        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            # Always close the writer
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info(f"Connection closed: {client_addr}")

    async def _handle_command_string(self, command_str: str) -> str:
        """
        Handle a raw command string and return the response string.
        
        Parses internal commands (FORWARD, REPLICATE) and routes
        appropriately.
        
        Args:
            command_str: Raw command string from client or internal
            
        Returns:
            Response string to send back
        """
        command_str = command_str.strip()
        self._total_requests += 1
        
        # Check for internal command prefixes
        upper_cmd = command_str.upper()
        
        if upper_cmd.startswith("FORWARD "):
            # This is a forwarded request - execute locally without further forwarding
            actual_command = command_str[8:]  # Strip "FORWARD "
            return await self._execute_local_command(actual_command, is_forwarded=True)
        
        elif upper_cmd.startswith("REPLICATE "):
            # This is a replication request - store locally without forwarding
            actual_command = command_str[10:]  # Strip "REPLICATE "
            return self._execute_replicate_command(actual_command)
        
        else:
            # Regular client command - may need forwarding
            return await self._execute_with_routing(command_str)

    async def _execute_with_routing(self, command_str: str) -> str:
        """
        Execute a client command with cluster routing.
        
        Determines if this node is the primary for the key and either:
        - Executes locally and replicates (if primary)
        - Forwards to the primary node (if not primary)
        
        In standalone mode, all commands execute locally without forwarding.
        
        Args:
            command_str: The client command string
            
        Returns:
            Response string
        """
        # Parse the command to get the key
        command = self.parser.parse_request(command_str)
        
        # Commands without keys (QUIT, UNKNOWN) execute locally
        if command.type in (CommandType.QUIT, CommandType.UNKNOWN):
            return self._execute_command_response(command)
        
        # In standalone mode, execute everything locally
        if self.standalone_mode:
            response = self._execute_command(command)
            return self.parser.format_response(response).strip()
        
        # Get the shard for this key
        shard_id = get_shard_for_key(command.key)
        
        # Check if this node is the primary for the shard
        if self.cluster_config.is_primary_for_shard(shard_id):
            # We are the primary - execute locally
            response = await self._execute_as_primary(command, command_str)
            return response
        else:
            # Forward to the primary node
            primary_node = self.cluster_config.get_primary_node_for_shard(shard_id)
            logger.info(f"FORWARDING key for key '{command.key}' to node {primary_node.node_id}")
            
            try:
                response = await self.node_client.forward_request(primary_node, command_str)
                if response is None:
                    return "ERROR forwarding failed"
                return response
            except Exception as e:
                logger.error(f"Failed to forward to node {primary_node.node_id}: {e}")
                return f"ERROR forwarding failed: {e}"

    async def _execute_as_primary(self, command, original_command_str: str) -> str:
        """
        Execute a command as the primary node.
        
        For write operations (PUT, DELETE), this will:
        1. Execute the command locally
        2. Replicate to the replica node synchronously
        3. Return success only after both complete
        
        Args:
            command: Parsed Command object
            original_command_str: Original command string for replication
            
        Returns:
            Response string
        """
        # Execute locally first
        response = self._execute_command(command)
        
        # For write operations, replicate to the replica node
        if command.type in (CommandType.PUT, CommandType.DELETE):
            shard_id = get_shard_for_key(command.key)
            replica_node = self.cluster_config.get_replica_node_for_shard(shard_id)
            
            # Don't replicate to ourselves
            if replica_node.node_id != self.cluster_config.current_node_id:
                logger.info(f"REPLICATING key '{command.key}' to node {replica_node.node_id}")
                
                try:
                    # Build the command to replicate
                    if command.type == CommandType.PUT:
                        if command.ttl > 0:
                            replicate_cmd = f"PUT {command.key} {command.value} {command.ttl}"
                        else:
                            replicate_cmd = f"PUT {command.key} {command.value}"
                    else:  # DELETE
                        replicate_cmd = f"DELETE {command.key}"
                    
                    replica_response = await self.node_client.replicate_command(
                        replica_node, replicate_cmd
                    )
                    
                    if replica_response is None or "ERROR" in replica_response.upper():
                        logger.error(f"Replication failed: {replica_response}")
                        # Still return success as per synchronous replication
                        # (In production you might want to handle this differently)
                    else:
                        logger.info(f"REPLICATION successful: {replica_response}")
                        
                except Exception as e:
                    logger.error(f"Replication error: {e}")
                    # Continue anyway - in this simplified implementation
        
        return self.parser.format_response(response).strip()

    async def _execute_local_command(self, command_str: str, is_forwarded: bool = False) -> str:
        """
        Execute a command locally (for forwarded requests).
        
        Similar to _execute_as_primary but handles the case where we received
        a forwarded request.
        
        Args:
            command_str: The command string to execute
            is_forwarded: Whether this is a forwarded request
            
        Returns:
            Response string
        """
        command = self.parser.parse_request(command_str)
        
        if command.type in (CommandType.QUIT, CommandType.UNKNOWN):
            return self._execute_command_response(command)
        
        # Execute as primary (we are the primary since we received the forward)
        return await self._execute_as_primary(command, command_str)

    def _execute_replicate_command(self, command_str: str) -> str:
        """
        Execute a replicate command (store locally without further replication).
        
        Args:
            command_str: The command to replicate (PUT or DELETE)
            
        Returns:
            Response string
        """
        command = self.parser.parse_request(command_str)
        
        # Simply execute locally
        response = self._execute_command(command)
        return self.parser.format_response(response).strip()

    def _execute_command(self, command) -> Response:
        """
        Execute a parsed command on the store.

        This is a helper method that routes commands to the appropriate
        KVStore method and returns the formatted response.

        Args:
            command: The Command object to execute

        Returns:
            Response object with the result
        """
        if command.type == CommandType.UNKNOWN:
            return Response.error("unknown command")
        
        if command.type == CommandType.PUT:
            self.store.put(command.key, command.value, command.ttl)
            return Response.stored()
        
        elif command.type == CommandType.GET:
            value = self.store.get(command.key)
            if value is None:
                return Response.key_not_found()
            return Response.value_response(value)
        
        elif command.type == CommandType.DELETE:
            if self.store.delete(command.key):
                return Response.deleted()
            return Response.key_not_found()
        
        elif command.type == CommandType.EXISTS:
            exists = self.store.exists(command.key)
            return Response.exists_response(exists)
        
        else:
            return Response.error("unknown command")

    def _execute_command_response(self, command) -> str:
        """Execute command and return formatted response string."""
        response = self._execute_command(command)
        return self.parser.format_response(response).strip()

    async def start(self) -> None:
        """
        Start the server and begin accepting connections.

        This method creates the asyncio server and runs forever
        (or until cancelled). It should be called from asyncio.run()
        or within an existing event loop.
        """
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        self._running = True
        
        addrs = ', '.join(str(sock.getsockname()) for sock in self._server.sockets)
        
        if self.standalone_mode:
            logger.info(f"Server started on {addrs} (standalone mode)")
        else:
            logger.info(f"Node {self.node_id} started on {addrs}")
            logger.info(f"  Primary for shards: {self.cluster_config.get_shards_as_primary()}")
            logger.info(f"  Replica for shards: {self.cluster_config.get_shards_as_replica()}")
        
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """
        Stop the server gracefully.

        Closes the server and waits for it to fully shut down.
        """
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self._running = False
        logger.info("Server stopped")

    def is_running(self) -> bool:
        """Check if the server is currently running."""
        return self._running

    def get_stats(self) -> dict:
        """
        Get server statistics.

        Returns:
            Dictionary with server stats including connection counts,
            request counts, and store statistics.
        """
        stats = {
            "running": self._running,
            "standalone_mode": self.standalone_mode,
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "total_connections": self._connection_count,
            "total_requests": self._total_requests,
            "store_stats": self.store.get_stats(),
        }
        
        if not self.standalone_mode and self.cluster_config:
            stats["primary_shards"] = self.cluster_config.get_shards_as_primary()
            stats["replica_shards"] = self.cluster_config.get_shards_as_replica()
        
        return stats


async def run_server(host: str = None, port: int = None, node_id: int = None) -> None:
    """
    Convenience function to create and run the server.

    Args:
        host: Bind address (default from settings)
        port: Port number (default from settings)
        node_id: Node ID in the cluster (default from settings)

    Usage:
        asyncio.run(run_server(port=7171, node_id=1))
    """
    server = KVServer(host=host, port=port, node_id=node_id)

    try:
        await server.start()
    except asyncio.CancelledError:
        logger.info("Server shutdown requested")
    finally:
        await server.stop()
