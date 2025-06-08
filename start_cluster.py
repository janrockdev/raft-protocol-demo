"""Script to start all nodes of the distributed cache cluster."""

import asyncio
import subprocess
import sys
import time
import signal
import os
from typing import List, Optional

from config import NODES


class ClusterManager:
    """Manages the distributed cache cluster."""
    
    def __init__(self):
        self.processes: List[subprocess.Popen] = []
        self.shutdown_requested = False
        
    def start_cluster(self):
        """Start all nodes in the cluster."""
        print("Starting distributed cache cluster...")
        
        for node_id in NODES.keys():
            try:
                cmd = [sys.executable, "main.py", node_id]
                
                # Start the process
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1
                )
                
                self.processes.append(process)
                print(f"Started node {node_id} (PID: {process.pid})")
                
                # Give each node a moment to start
                time.sleep(1)
                
            except Exception as e:
                print(f"Failed to start node {node_id}: {e}")
                self.cleanup()
                return False
        
        print(f"All {len(self.processes)} nodes started successfully!")
        self.print_cluster_info()
        return True
    
    def print_cluster_info(self):
        """Print information about the cluster."""
        print("\n" + "="*60)
        print("DISTRIBUTED CACHE CLUSTER INFORMATION")
        print("="*60)
        
        for node_id, config in NODES.items():
            host = config['host']
            port = config['port']
            print(f"Node {node_id}: http://{host}:{port}")
        
        print("\nAPI Endpoints:")
        print("  GET    /cache/{key}     - Get value by key")
        print("  POST   /cache/{key}     - Set value for key")
        print("  DELETE /cache/{key}     - Delete key")
        print("  GET    /cache           - List all keys")
        print("  DELETE /cache           - Clear all keys")
        print("  GET    /status          - Get node status")
        print("  GET    /stats           - Get cache statistics")
        print("  GET    /health          - Health check")
        
        print("\nExample usage:")
        print("  curl -X POST http://127.0.0.1:3001/cache/mykey -H 'Content-Type: application/json' -d '{\"value\": \"hello world\"}'")
        print("  curl http://127.0.0.1:3001/cache/mykey")
        print("  curl http://127.0.0.1:3001/status")
        
        print("\nPress Ctrl+C to stop the cluster")
        print("="*60)
    
    def monitor_processes(self):
        """Monitor running processes and handle output."""
        try:
            while not self.shutdown_requested:
                # Check if any process has terminated unexpectedly
                for i, process in enumerate(self.processes):
                    if process.poll() is not None:
                        node_id = list(NODES.keys())[i]
                        print(f"\nWARNING: Node {node_id} has terminated (exit code: {process.returncode})")
                        
                        # Read any remaining output
                        if process.stdout:
                            output = process.stdout.read()
                            if output:
                                print(f"Final output from {node_id}: {output}")
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nShutdown signal received...")
            self.shutdown_requested = True
    
    def cleanup(self):
        """Stop all running processes."""
        print("\nShutting down cluster...")
        
        for i, process in enumerate(self.processes):
            if process.poll() is None:  # Process is still running
                node_id = list(NODES.keys())[i]
                print(f"Stopping node {node_id} (PID: {process.pid})")
                
                try:
                    # Send SIGTERM first
                    process.terminate()
                    
                    # Wait for graceful shutdown
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        # Force kill if not responding
                        print(f"Force killing node {node_id}")
                        process.kill()
                        process.wait()
                        
                except Exception as e:
                    print(f"Error stopping node {node_id}: {e}")
        
        self.processes.clear()
        print("Cluster shutdown complete.")
    
    def run(self):
        """Run the cluster manager."""
        # Setup signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            if self.start_cluster():
                self.monitor_processes()
        finally:
            self.cleanup()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print(f"\nReceived signal {signum}")
        self.shutdown_requested = True


def check_dependencies():
    """Check if required dependencies are available."""
    try:
        import aiohttp
        import aiohttp_cors
        print("Dependencies check passed")
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Please install required packages:")
        print("  pip install aiohttp aiohttp-cors")
        return False


def main():
    """Main entry point."""
    print("Distributed Cache Cluster Manager")
    print("=================================")
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check if main.py exists
    if not os.path.exists("main.py"):
        print("Error: main.py not found in current directory")
        sys.exit(1)
    
    manager = ClusterManager()
    
    try:
        manager.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        manager.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()
