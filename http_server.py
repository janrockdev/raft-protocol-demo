"""HTTP server for the distributed cache API."""

import asyncio
import json
import logging
from typing import Dict, Any, Optional
from aiohttp import web, web_request
import aiohttp_cors

from raft_node import RaftNode
from cache import DistributedCache


class CacheHTTPServer:
    """HTTP server providing REST API for the distributed cache."""
    
    def __init__(self, raft_node: RaftNode, cache: DistributedCache):
        self.raft_node = raft_node
        self.cache = cache
        self.app = web.Application()
        self.logger = logging.getLogger(f'HTTPServer-{raft_node.node_id}')
        
        self._setup_routes()
        self._setup_cors()
    
    def _setup_routes(self):
        """Setup HTTP routes."""
        # Cache API routes
        self.app.router.add_get('/cache/{key}', self.get_key)
        self.app.router.add_post('/cache/{key}', self.set_key)
        self.app.router.add_delete('/cache/{key}', self.delete_key)
        self.app.router.add_get('/cache', self.list_keys)
        self.app.router.add_delete('/cache', self.clear_cache)
        
        # Status and monitoring routes
        self.app.router.add_get('/status', self.get_status)
        self.app.router.add_get('/stats', self.get_stats)
        self.app.router.add_get('/health', self.health_check)
        
        # Raft protocol routes (internal)
        self.app.router.add_post('/raft/request_vote', self.handle_request_vote)
        self.app.router.add_post('/raft/append_entries', self.handle_append_entries)
        
        # Root route
        self.app.router.add_get('/', self.root)
    
    def _setup_cors(self):
        """Setup CORS for the application."""
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        # Add CORS to all routes
        for route in list(self.app.router.routes()):
            cors.add(route)
    
    async def root(self, request: web_request.Request) -> web.Response:
        """Root endpoint with API information."""
        info = {
            'service': 'Distributed KV Cache',
            'node_id': self.raft_node.node_id,
            'version': '1.0.0',
            'endpoints': {
                'cache': {
                    'GET /cache/{key}': 'Get value by key',
                    'POST /cache/{key}': 'Set value for key',
                    'DELETE /cache/{key}': 'Delete key',
                    'GET /cache': 'List all keys',
                    'DELETE /cache': 'Clear all keys'
                },
                'monitoring': {
                    'GET /status': 'Get node status',
                    'GET /stats': 'Get cache statistics',
                    'GET /health': 'Health check'
                }
            }
        }
        return web.json_response(info)
    
    async def get_key(self, request: web_request.Request) -> web.Response:
        """Get a value from the cache."""
        key = request.match_info['key']
        
        try:
            value = await self.cache.get(key)
            if value is None:
                return web.json_response(
                    {'error': 'Key not found'}, 
                    status=404
                )
            
            return web.json_response({
                'key': key,
                'value': value,
                'retrieved_at': asyncio.get_event_loop().time()
            })
            
        except Exception as e:
            self.logger.error(f"Error getting key '{key}': {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def set_key(self, request: web_request.Request) -> web.Response:
        """Set a value in the cache."""
        key = request.match_info['key']
        
        try:
            data = await request.json()
            value = data.get('value')
            ttl = data.get('ttl')
            
            if value is None:
                return web.json_response(
                    {'error': 'Value is required'}, 
                    status=400
                )
            
            # If this node is the leader, replicate through Raft
            if self.raft_node.state.value == 'leader':
                command = {
                    'operation': 'set',
                    'key': key,
                    'value': value,
                    'ttl': ttl
                }
                
                success = await self.raft_node.propose_command(command)
                if success:
                    return web.json_response({
                        'message': 'Key set successfully',
                        'key': key,
                        'replicated': True
                    })
                else:
                    return web.json_response(
                        {'error': 'Failed to replicate command'}, 
                        status=500
                    )
            else:
                # Forward to leader if known
                leader_info = self._get_leader_info()
                if leader_info:
                    return web.json_response({
                        'error': 'Not the leader',
                        'leader': leader_info
                    }, status=307)  # Temporary Redirect
                else:
                    return web.json_response({
                        'error': 'No leader available'
                    }, status=503)  # Service Unavailable
                    
        except json.JSONDecodeError:
            return web.json_response(
                {'error': 'Invalid JSON'}, 
                status=400
            )
        except Exception as e:
            self.logger.error(f"Error setting key '{key}': {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def delete_key(self, request: web_request.Request) -> web.Response:
        """Delete a key from the cache."""
        key = request.match_info['key']
        
        try:
            # If this node is the leader, replicate through Raft
            if self.raft_node.state.value == 'leader':
                command = {
                    'operation': 'delete',
                    'key': key
                }
                
                success = await self.raft_node.propose_command(command)
                if success:
                    return web.json_response({
                        'message': 'Key deleted successfully',
                        'key': key,
                        'replicated': True
                    })
                else:
                    return web.json_response(
                        {'error': 'Failed to replicate command'}, 
                        status=500
                    )
            else:
                # Forward to leader if known
                leader_info = self._get_leader_info()
                if leader_info:
                    return web.json_response({
                        'error': 'Not the leader',
                        'leader': leader_info
                    }, status=307)  # Temporary Redirect
                else:
                    return web.json_response({
                        'error': 'No leader available'
                    }, status=503)  # Service Unavailable
                    
        except Exception as e:
            self.logger.error(f"Error deleting key '{key}': {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def list_keys(self, request: web_request.Request) -> web.Response:
        """List all keys in the cache."""
        try:
            keys = self.cache.get_all_keys()
            return web.json_response({
                'keys': keys,
                'count': len(keys)
            })
            
        except Exception as e:
            self.logger.error(f"Error listing keys: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def clear_cache(self, request: web_request.Request) -> web.Response:
        """Clear all keys from the cache."""
        try:
            # If this node is the leader, replicate through Raft
            if self.raft_node.state.value == 'leader':
                command = {
                    'operation': 'clear'
                }
                
                success = await self.raft_node.propose_command(command)
                if success:
                    return web.json_response({
                        'message': 'Cache cleared successfully',
                        'replicated': True
                    })
                else:
                    return web.json_response(
                        {'error': 'Failed to replicate command'}, 
                        status=500
                    )
            else:
                # Forward to leader if known
                leader_info = self._get_leader_info()
                if leader_info:
                    return web.json_response({
                        'error': 'Not the leader',
                        'leader': leader_info
                    }, status=307)  # Temporary Redirect
                else:
                    return web.json_response({
                        'error': 'No leader available'
                    }, status=503)  # Service Unavailable
                    
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def get_status(self, request: web_request.Request) -> web.Response:
        """Get the status of the node."""
        try:
            raft_status = self.raft_node.get_status()
            cache_stats = self.cache.get_stats()
            
            status = {
                'node_id': self.raft_node.node_id,
                'timestamp': asyncio.get_event_loop().time(),
                'raft': raft_status,
                'cache': cache_stats,
                'cluster': {
                    'nodes': list(self.raft_node.cluster_nodes.keys()),
                    'leader': self._get_leader_info()
                }
            }
            
            return web.json_response(status)
            
        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def get_stats(self, request: web_request.Request) -> web.Response:
        """Get cache statistics."""
        try:
            stats = self.cache.get_stats()
            return web.json_response(stats)
            
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def health_check(self, request: web_request.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({
            'status': 'healthy',
            'node_id': self.raft_node.node_id,
            'timestamp': asyncio.get_event_loop().time()
        })
    
    async def handle_request_vote(self, request: web_request.Request) -> web.Response:
        """Handle Raft request vote RPC."""
        try:
            data = await request.json()
            result = await self.raft_node.handle_request_vote(data)
            return web.json_response(result)
            
        except Exception as e:
            self.logger.error(f"Error handling request vote: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    async def handle_append_entries(self, request: web_request.Request) -> web.Response:
        """Handle Raft append entries RPC."""
        try:
            data = await request.json()
            result = await self.raft_node.handle_append_entries(data)
            return web.json_response(result)
            
        except Exception as e:
            self.logger.error(f"Error handling append entries: {e}")
            return web.json_response(
                {'error': 'Internal server error'}, 
                status=500
            )
    
    def _get_leader_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current leader."""
        # This is a simplified implementation
        # In a real system, we'd track the leader more accurately
        if self.raft_node.state.value == 'leader':
            return {
                'node_id': self.raft_node.node_id,
                'host': self.raft_node.host,
                'port': self.raft_node.port
            }
        return None
    
    async def start(self):
        """Start the HTTP server."""
        try:
            runner = web.AppRunner(self.app)
            await runner.setup()
            
            # Bind to localhost with proper error handling
            site = web.TCPSite(runner, "127.0.0.1", self.raft_node.port)
            await site.start()
            
            # Verify the server is actually listening
            await asyncio.sleep(0.1)
            
            self.logger.info(f"HTTP server started on 127.0.0.1:{self.raft_node.port}")
            
            # Store runner for cleanup
            self.runner = runner
            
        except Exception as e:
            self.logger.error(f"Failed to start HTTP server: {e}")
            raise
