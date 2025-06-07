"""Main application entry point for a single Raft node."""

import asyncio
import logging
import sys
import signal
from typing import Optional

from config import NODES, LOG_LEVEL, LOG_FORMAT
from raft_node import RaftNode
from cache import DistributedCache
from http_server import CacheHTTPServer


class DistributedCacheNode:
    """Main application class for a distributed cache node."""
    
    def __init__(self, node_id: str):
        if node_id not in NODES:
            raise ValueError(f"Unknown node_id: {node_id}")
        
        self.node_id = node_id
        node_config = NODES[node_id]
        
        # Initialize components
        self.raft_node = RaftNode(
            node_id, 
            node_config['host'], 
            node_config['port'], 
            NODES
        )
        self.cache = DistributedCache(node_id)
        self.http_server = CacheHTTPServer(self.raft_node, self.cache)
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Shutdown flag
        self.shutdown_event = asyncio.Event()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format=LOG_FORMAT,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'node_{self.node_id}.log')
            ]
        )
        
        logger = logging.getLogger(f'Node-{self.node_id}')
        return logger
    
    async def initialize(self):
        """Initialize all components."""
        self.logger.info(f"Initializing node {self.node_id}")
        
        # Initialize cache
        await self.cache.initialize()
        
        # Set callback for Raft commits
        self.raft_node.on_commit_callback = self.cache.apply_command
        
        # Initialize Raft node
        await self.raft_node.initialize()
        
        # Start HTTP server
        await self.http_server.start()
        
        self.logger.info(f"Node {self.node_id} initialized successfully")
    
    async def run(self):
        """Run the node until shutdown."""
        try:
            await self.initialize()
            
            # Setup signal handlers
            self._setup_signal_handlers()
            
            self.logger.info(f"Node {self.node_id} is running")
            
            # Give servers time to fully start and verify binding
            await asyncio.sleep(2)
            
            # Verify HTTP server is accessible
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(f'http://127.0.0.1:{self.raft_node.port}/health', 
                                         timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        if resp.status == 200:
                            self.logger.info(f"HTTP server verified accessible on port {self.raft_node.port}")
                        else:
                            self.logger.warning(f"HTTP server responding with status {resp.status}")
            except Exception as e:
                self.logger.error(f"HTTP server accessibility check failed: {e}")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
        except Exception as e:
            self.logger.error(f"Error running node: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown all components gracefully."""
        self.logger.info(f"Shutting down node {self.node_id}")
        
        try:
            # Shutdown Raft node
            await self.raft_node.shutdown()
            
            # Shutdown cache
            await self.cache.shutdown()
            
            self.logger.info(f"Node {self.node_id} shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown")
            asyncio.create_task(self._trigger_shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _trigger_shutdown(self):
        """Trigger shutdown event."""
        self.shutdown_event.set()


async def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python main.py <node_id>")
        print(f"Available nodes: {list(NODES.keys())}")
        sys.exit(1)
    
    node_id = sys.argv[1]
    
    try:
        app = DistributedCacheNode(node_id)
        await app.run()
    except KeyboardInterrupt:
        print("\nShutdown initiated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
