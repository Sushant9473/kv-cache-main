"""
Async TCP Server Module (Task 3)

This module implements the asynchronous TCP server for KV-Cache.

Students must implement:
- handle_client(): Handle a single client connection
- start(): Start the server and accept connections

Key asyncio concepts needed:
- asyncio.start_server(): Create a TCP server
- StreamReader.readline(): Read a line from client
- StreamWriter.write() / drain(): Send data to client
- Proper connection cleanup with writer.close() / wait_closed()
"""

import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from typing import Optional

from ..cache.store import KVStore
from ..config.settings import settings
from ..protocol.commands import CommandType, Response
from ..protocol.parser import ProtocolParser

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KVServer:
    """
    Asynchronous TCP server for the KV-Cache service.

    This server handles multiple concurrent clients using asyncio.
    Each client connection is handled in a separate coroutine,
    allowing for high concurrency without threading.

    Features:
    - Non-blocking I/O with asyncio
    - Persistent connections (multiple commands per connection)
    - Graceful error handling and connection cleanup
    - Shared KVStore across all connections

    Usage:
        server = KVServer(host='0.0.0.0', port=7171)
        await server.start()  # Runs forever

    Attributes:
        host: Server bind address (e.g., '0.0.0.0')
        port: Server port number (e.g., 7171)
        store: The KVStore instance shared by all connections
        parser: The ProtocolParser for parsing commands
    """

    def __init__(
            self,
            host: str = None,
            port: int = None,
            store: KVStore = None,
    ):
        """
        Initialize the server.

        Args:
            host: Bind address (default from settings)
            port: Port number (default from settings)
            store: KVStore instance (creates new one if not provided)
        """
        self.host = host if host is not None else settings.HOST
        self.port = port if port is not None else settings.PORT
        self.store = store if store is not None else KVStore()
        self.parser = ProtocolParser()

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

        Args:
            reader: StreamReader for reading from the client
            writer: StreamWriter for writing to the client

        Protocol flow:
            1. Read a line (command) from the client
            2. Parse the command using ProtocolParser
            3. Execute the command on the KVStore
            4. Format and send the response
            5. Repeat until QUIT or client disconnects

        Implementation requirements:
        - Get client address for logging (writer.get_extra_info('peername'))
        - Loop reading lines until empty data (disconnect) or QUIT
        - Handle decode errors gracefully
        - Always close the writer in a finally block
        - Handle ConnectionResetError and other exceptions
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

                # Parse the command
                command = self.parser.parse_request(command_str)
                self._total_requests += 1

                # Check for QUIT command
                if command.type == CommandType.QUIT:
                    logger.info(f"Client {client_addr} sent QUIT")
                    break

                # Execute the command
                response = self._execute_command(command)

                # Send the response
                response_str = self.parser.format_response(response)
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

    async def start(self) -> None:
        """
        Start the server and begin accepting connections.

        This method creates the asyncio server and runs forever
        (or until cancelled). It should be called from asyncio.run()
        or within an existing event loop.

        Implementation:
        - Use asyncio.start_server() with self.handle_client as callback
        - Log the server address when started
        - Use server.serve_forever() to run indefinitely

        Example:
            server = KVServer(port=7171)
            asyncio.run(server.start())
        """
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        self._running = True
        
        addrs = ', '.join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"Server started on {addrs}")
        
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
        return {
            "running": self._running,
            "host": self.host,
            "port": self.port,
            "total_connections": self._connection_count,
            "total_requests": self._total_requests,
            "store_stats": self.store.get_stats(),
        }


async def run_server(host: str = None, port: int = None) -> None:
    """
    Convenience function to create and run the server.

    Args:
        host: Bind address (default from settings)
        port: Port number (default from settings)

    Usage:
        asyncio.run(run_server(port=7171))
    """
    server = KVServer(host=host, port=port)

    try:
        await server.start()
    except asyncio.CancelledError:
        logger.info("Server shutdown requested")
    finally:
        await server.stop()
