"""
Web dashboard for monitoring and testing the distributed cache cluster.
Provides real-time monitoring, interactive testing, and failure simulation.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional
from aiohttp import web, ClientSession, ClientTimeout
import aiohttp_cors
import os
import signal
import subprocess

logger = logging.getLogger('Dashboard')

class CacheDashboard:
    """Web dashboard for the distributed cache cluster."""
    
    def __init__(self):
        self.app = web.Application()
        self.session: Optional[ClientSession] = None
        self.nodes = {
            'node1': {'host': '127.0.0.1', 'port': 3001, 'status': 'unknown'},
            'node2': {'host': '127.0.0.1', 'port': 3002, 'status': 'unknown'},
            'node3': {'host': '127.0.0.1', 'port': 3003, 'status': 'unknown'}
        }
        self.cluster_stats = {}
        self.test_results = []
        
        self._setup_routes()
        self._setup_cors()
        
        # Initialize session immediately
        asyncio.create_task(self._init_session())
    
    async def _init_session(self):
        """Initialize HTTP session."""
        if not self.session:
            self.session = ClientSession(timeout=ClientTimeout(total=5.0))
        
    def _setup_routes(self):
        """Setup HTTP routes for the dashboard."""
        # Dashboard UI
        self.app.router.add_get('/', self.dashboard_home)
        self.app.router.add_get('/dashboard', self.dashboard_home)
        
        # API endpoints
        self.app.router.add_get('/api/cluster/status', self.get_cluster_status)
        self.app.router.add_get('/api/cluster/stats', self.get_cluster_stats)
        self.app.router.add_post('/api/cache/set', self.set_cache_value)
        self.app.router.add_get('/api/cache/get/{key}', self.get_cache_value)
        self.app.router.add_delete('/api/cache/delete/{key}', self.delete_cache_value)
        self.app.router.add_get('/api/cache/list', self.list_cache_keys)
        self.app.router.add_delete('/api/cache/clear', self.clear_cache)
        
        # Testing endpoints
        self.app.router.add_post('/api/test/raft', self.test_raft_consensus)
        self.app.router.add_post('/api/test/performance', self.test_performance)
        self.app.router.add_post('/api/test/failover', self.test_failover)
        self.app.router.add_get('/api/test/results', self.get_test_results)
        
        # Simulation endpoints
        self.app.router.add_post('/api/simulate/node-failure', self.simulate_node_failure)
        self.app.router.add_post('/api/simulate/node-recovery', self.simulate_node_recovery)
        self.app.router.add_post('/api/simulate/network-partition', self.simulate_network_partition)
        
        # No static files needed - everything is embedded

    def _setup_cors(self):
        """Setup CORS for the dashboard."""
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        for route in list(self.app.router.routes()):
            cors.add(route)

    async def dashboard_home(self, request):
        """Serve the main dashboard HTML."""
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Distributed Cache Cluster Dashboard</title>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f7fa; }
                .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
                .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }
                .header h1 { font-size: 2.5em; margin-bottom: 10px; }
                .header p { font-size: 1.2em; opacity: 0.9; }
                .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin-bottom: 30px; }
                .card { background: white; border-radius: 10px; padding: 25px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #667eea; }
                .card h3 { color: #333; margin-bottom: 20px; font-size: 1.3em; }
                .node-status { display: flex; align-items: center; margin-bottom: 15px; padding: 10px; border-radius: 5px; background: #f8f9fa; }
                .status-indicator { width: 12px; height: 12px; border-radius: 50%; margin-right: 10px; }
                .status-healthy { background: #28a745; }
                .status-unhealthy { background: #dc3545; }
                .status-unknown { background: #6c757d; }
                .btn { padding: 12px 20px; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; margin: 5px; transition: all 0.3s; }
                .btn-primary { background: #667eea; color: white; }
                .btn-success { background: #28a745; color: white; }
                .btn-warning { background: #ffc107; color: #212529; }
                .btn-danger { background: #dc3545; color: white; }
                .btn:hover { transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
                .input-group { margin-bottom: 15px; }
                .input-group label { display: block; margin-bottom: 5px; font-weight: bold; color: #555; }
                .input-group input, .input-group textarea { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; font-size: 14px; }
                .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; }
                .stat-item { text-align: center; padding: 15px; background: #f8f9fa; border-radius: 5px; }
                .stat-value { font-size: 2em; font-weight: bold; color: #667eea; }
                .stat-label { color: #6c757d; font-size: 0.9em; margin-top: 5px; }
                .log-container { background: #1e1e1e; color: #00ff00; padding: 20px; border-radius: 5px; font-family: 'Courier New', monospace; height: 300px; overflow-y: auto; font-size: 12px; }
                .test-results { margin-top: 20px; }
                .test-result { padding: 10px; margin-bottom: 10px; border-radius: 5px; border-left: 4px solid #28a745; background: #f8f9fa; }
                .test-result.error { border-left-color: #dc3545; }
                .raft-info { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
                .raft-metric { background: #e9ecef; padding: 15px; border-radius: 5px; text-align: center; }
                .leader-badge { background: #28a745; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px; font-weight: bold; }
                .follower-badge { background: #6c757d; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px; font-weight: bold; }
                .candidate-badge { background: #ffc107; color: #212529; padding: 5px 10px; border-radius: 15px; font-size: 12px; font-weight: bold; }
                .auto-refresh { margin-bottom: 20px; }
                .auto-refresh input[type="checkbox"] { margin-right: 8px; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 Distributed Cache Cluster Dashboard</h1>
                    <p>Monitor, test, and simulate your Raft consensus protocol implementation</p>
                </div>

                <div class="auto-refresh">
                    <label>
                        <input type="checkbox" id="autoRefresh" checked> Auto-refresh every 2 seconds
                    </label>
                </div>

                <div class="grid">
                    <!-- Cluster Status -->
                    <div class="card">
                        <h3>🎯 Cluster Status</h3>
                        <div id="clusterStatus">
                            <div class="node-status">
                                <div class="status-indicator status-unknown"></div>
                                <span>Loading cluster status...</span>
                            </div>
                        </div>
                        <button class="btn btn-primary" onclick="refreshClusterStatus()">Refresh Status</button>
                    </div>

                    <!-- Raft Consensus Info -->
                    <div class="card">
                        <h3>⚡ Raft Consensus</h3>
                        <div id="raftInfo" class="raft-info">
                            <div class="raft-metric">
                                <div class="stat-value" id="currentTerm">-</div>
                                <div class="stat-label">Current Term</div>
                            </div>
                            <div class="raft-metric">
                                <div class="stat-value" id="leaderNode">-</div>
                                <div class="stat-label">Leader Node</div>
                            </div>
                            <div class="raft-metric">
                                <div class="stat-value" id="logLength">-</div>
                                <div class="stat-label">Log Entries</div>
                            </div>
                            <div class="raft-metric">
                                <div class="stat-value" id="commitIndex">-</div>
                                <div class="stat-label">Commit Index</div>
                            </div>
                        </div>
                    </div>

                    <!-- Cache Operations -->
                    <div class="card">
                        <h3>💾 Cache Operations</h3>
                        <div class="input-group">
                            <label>Key:</label>
                            <input type="text" id="cacheKey" placeholder="Enter cache key">
                        </div>
                        <div class="input-group">
                            <label>Value:</label>
                            <textarea id="cacheValue" placeholder="Enter cache value" rows="3"></textarea>
                        </div>
                        <div class="input-group">
                            <label>TTL (seconds, optional):</label>
                            <input type="number" id="cacheTTL" placeholder="300">
                        </div>
                        <button class="btn btn-success" onclick="setCacheValue()">Set Value</button>
                        <button class="btn btn-primary" onclick="getCacheValue()">Get Value</button>
                        <button class="btn btn-warning" onclick="deleteCacheValue()">Delete Value</button>
                        <button class="btn btn-danger" onclick="clearCache()">Clear All</button>
                        <button class="btn btn-primary" onclick="listCacheKeys()">List Keys</button>
                        <div id="cacheResult" class="test-results"></div>
                    </div>

                    <!-- Performance Testing -->
                    <div class="card">
                        <h3>📊 Performance Testing</h3>
                        <div class="input-group">
                            <label>Number of Operations:</label>
                            <input type="number" id="perfOps" value="100" min="1" max="1000">
                        </div>
                        <div class="input-group">
                            <label>Operation Type:</label>
                            <select id="perfType">
                                <option value="set">Set Operations</option>
                                <option value="get">Get Operations</option>
                                <option value="mixed">Mixed Operations</option>
                            </select>
                        </div>
                        <button class="btn btn-primary" onclick="runPerformanceTest()">Run Performance Test</button>
                        <div id="perfResults" class="test-results"></div>
                    </div>

                    <!-- Failure Simulation -->
                    <div class="card">
                        <h3>🔧 Failure Simulation</h3>
                        <div class="input-group">
                            <label>Target Node:</label>
                            <select id="targetNode">
                                <option value="node1">Node 1 (Port 3001)</option>
                                <option value="node2">Node 2 (Port 3002)</option>
                                <option value="node3">Node 3 (Port 3003)</option>
                            </select>
                        </div>
                        <button class="btn btn-danger" onclick="simulateNodeFailure()">Simulate Node Failure</button>
                        <button class="btn btn-success" onclick="simulateNodeRecovery()">Simulate Node Recovery</button>
                        <button class="btn btn-warning" onclick="simulateNetworkPartition()">Simulate Network Partition</button>
                        <div id="simulationResults" class="test-results"></div>
                    </div>

                    <!-- Cache Statistics -->
                    <div class="card">
                        <h3>📈 Cache Statistics</h3>
                        <div id="cacheStats" class="stats-grid">
                            <div class="stat-item">
                                <div class="stat-value" id="cacheSize">-</div>
                                <div class="stat-label">Cache Size</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-value" id="hitRate">-</div>
                                <div class="stat-label">Hit Rate (%)</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-value" id="totalHits">-</div>
                                <div class="stat-label">Total Hits</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-value" id="totalMisses">-</div>
                                <div class="stat-label">Total Misses</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Test Results Log -->
                <div class="card">
                    <h3>📝 Activity Log</h3>
                    <div id="activityLog" class="log-container">
                        Starting dashboard...
                    </div>
                    <button class="btn btn-warning" onclick="clearLog()">Clear Log</button>
                </div>
            </div>

            <script>
                let autoRefreshEnabled = true;
                let refreshInterval;

                // Auto-refresh toggle
                document.getElementById('autoRefresh').addEventListener('change', function(e) {
                    autoRefreshEnabled = e.target.checked;
                    if (autoRefreshEnabled) {
                        startAutoRefresh();
                    } else {
                        stopAutoRefresh();
                    }
                });

                function startAutoRefresh() {
                    refreshInterval = setInterval(() => {
                        refreshClusterStatus();
                        refreshCacheStats();
                    }, 2000);
                }

                function stopAutoRefresh() {
                    if (refreshInterval) {
                        clearInterval(refreshInterval);
                    }
                }

                function logActivity(message, type = 'info') {
                    const log = document.getElementById('activityLog');
                    const timestamp = new Date().toLocaleTimeString();
                    const entry = `[${timestamp}] ${message}`;
                    const newline = `ln \n`;
                    log.textContent += newline + entry;
                    log.scrollTop = log.scrollHeight;
                }

                function clearLog() {
                    document.getElementById('activityLog').textContent = '';
                    logActivity('Log cleared');
                }

                async function refreshClusterStatus() {
                    try {
                        const response = await fetch('/api/cluster/status');
                        const data = await response.json();
                        
                        let statusHtml = '';
                        let leaderNode = '-';
                        let currentTerm = 0;
                        let logLength = 0;
                        let commitIndex = 0;

                        for (const [nodeId, nodeData] of Object.entries(data.nodes)) {
                            const statusClass = nodeData.status === 'healthy' ? 'status-healthy' : 
                                              nodeData.status === 'unhealthy' ? 'status-unhealthy' : 'status-unknown';
                            
                            let badge = '';
                            if (nodeData.raft && nodeData.raft.state === 'leader') {
                                badge = '<span class="leader-badge">LEADER</span>';
                                leaderNode = nodeId;
                                currentTerm = nodeData.raft.term;
                                logLength = nodeData.raft.log_length;
                                commitIndex = nodeData.raft.commit_index;
                            } else if (nodeData.raft && nodeData.raft.state === 'follower') {
                                badge = '<span class="follower-badge">FOLLOWER</span>';
                            } else if (nodeData.raft && nodeData.raft.state === 'candidate') {
                                badge = '<span class="candidate-badge">CANDIDATE</span>';
                            }

                            statusHtml += `
                                <div class="node-status">
                                    <div class="status-indicator ${statusClass}"></div>
                                    <span><strong>${nodeId}</strong> (${nodeData.host}:${nodeData.port}) ${badge}</span>
                                </div>
                            `;
                        }

                        document.getElementById('clusterStatus').innerHTML = statusHtml;
                        document.getElementById('currentTerm').textContent = currentTerm;
                        document.getElementById('leaderNode').textContent = leaderNode;
                        document.getElementById('logLength').textContent = logLength;
                        document.getElementById('commitIndex').textContent = commitIndex;

                    } catch (error) {
                        logActivity(`Error refreshing cluster status: ${error.message}`, 'error');
                        document.getElementById('clusterStatus').innerHTML = 
                            '<div class="node-status"><div class="status-indicator status-unhealthy"></div><span>Failed to connect to cluster</span></div>';
                    }
                }

                async function refreshCacheStats() {
                    try {
                        const response = await fetch('/api/cluster/stats');
                        const data = await response.json();
                        
                        if (data.cache) {
                            document.getElementById('cacheSize').textContent = data.cache.size || 0;
                            document.getElementById('hitRate').textContent = (data.cache.hit_rate || 0).toFixed(1);
                            document.getElementById('totalHits').textContent = data.cache.hits || 0;
                            document.getElementById('totalMisses').textContent = data.cache.misses || 0;
                        }
                    } catch (error) {
                        logActivity(`Error refreshing cache stats: ${error.message}`, 'error');
                    }
                }

                async function setCacheValue() {
                    const key = document.getElementById('cacheKey').value;
                    const value = document.getElementById('cacheValue').value;
                    const ttl = document.getElementById('cacheTTL').value;

                    if (!key || !value) {
                        alert('Please provide both key and value');
                        return;
                    }

                    try {
                        const payload = { key, value };
                        if (ttl) payload.ttl = parseInt(ttl);

                        const response = await fetch('/api/cache/set', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(payload)
                        });

                        const result = await response.json();
                        displayCacheResult(result, response.ok);
                        logActivity(`Set cache key "${key}" with value "${value}"`);
                        
                        if (response.ok) {
                            refreshCacheStats();
                        }
                    } catch (error) {
                        logActivity(`Error setting cache value: ${error.message}`, 'error');
                    }
                }

                async function getCacheValue() {
                    const key = document.getElementById('cacheKey').value;
                    if (!key) {
                        alert('Please provide a key');
                        return;
                    }

                    try {
                        const response = await fetch(`/api/cache/get/${encodeURIComponent(key)}`);
                        const result = await response.json();
                        displayCacheResult(result, response.ok);
                        logActivity(`Retrieved cache key "${key}"`);
                        refreshCacheStats();
                    } catch (error) {
                        logActivity(`Error getting cache value: ${error.message}`, 'error');
                    }
                }

                async function deleteCacheValue() {
                    const key = document.getElementById('cacheKey').value;
                    if (!key) {
                        alert('Please provide a key');
                        return;
                    }

                    try {
                        const response = await fetch(`/api/cache/delete/${encodeURIComponent(key)}`, {
                            method: 'DELETE'
                        });
                        const result = await response.json();
                        displayCacheResult(result, response.ok);
                        logActivity(`Deleted cache key "${key}"`);
                        refreshCacheStats();
                    } catch (error) {
                        logActivity(`Error deleting cache value: ${error.message}`, 'error');
                    }
                }

                async function clearCache() {
                    if (!confirm('Are you sure you want to clear all cache data?')) {
                        return;
                    }

                    try {
                        const response = await fetch('/api/cache/clear', { method: 'DELETE' });
                        const result = await response.json();
                        displayCacheResult(result, response.ok);
                        logActivity('Cleared all cache data');
                        refreshCacheStats();
                    } catch (error) {
                        logActivity(`Error clearing cache: ${error.message}`, 'error');
                    }
                }

                async function listCacheKeys() {
                    try {
                        const response = await fetch('/api/cache/list');
                        const result = await response.json();
                        displayCacheResult(result, response.ok);
                        logActivity('Retrieved cache key list');
                    } catch (error) {
                        logActivity(`Error listing cache keys: ${error.message}`, 'error');
                    }
                }

                function displayCacheResult(result, success) {
                    const container = document.getElementById('cacheResult');
                    const resultClass = success ? '' : 'error';
                    container.innerHTML = `<div class="test-result ${resultClass}"><pre>${JSON.stringify(result, null, 2)}</pre></div>`;
                }

                async function runPerformanceTest() {
                    const ops = parseInt(document.getElementById('perfOps').value);
                    const type = document.getElementById('perfType').value;

                    logActivity(`Starting performance test: ${ops} ${type} operations`);

                    try {
                        const response = await fetch('/api/test/performance', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ operations: ops, type: type })
                        });

                        const result = await response.json();
                        document.getElementById('perfResults').innerHTML = 
                            `<div class="test-result"><pre>${JSON.stringify(result, null, 2)}</pre></div>`;
                        logActivity(`Performance test completed: ${result.summary}`);
                    } catch (error) {
                        logActivity(`Performance test failed: ${error.message}`, 'error');
                    }
                }

                async function simulateNodeFailure() {
                    const node = document.getElementById('targetNode').value;
                    
                    if (!confirm(`Are you sure you want to simulate failure of ${node}?`)) {
                        return;
                    }

                    logActivity(`Simulating failure of ${node}`);

                    try {
                        const response = await fetch('/api/simulate/node-failure', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ node: node })
                        });

                        const result = await response.json();
                        document.getElementById('simulationResults').innerHTML = 
                            `<div class="test-result"><pre>${JSON.stringify(result, null, 2)}</pre></div>`;
                        logActivity(`Node failure simulation result: ${result.message}`);
                        
                        setTimeout(refreshClusterStatus, 2000);
                    } catch (error) {
                        logActivity(`Node failure simulation failed: ${error.message}`, 'error');
                    }
                }

                async function simulateNodeRecovery() {
                    const node = document.getElementById('targetNode').value;
                    
                    logActivity(`Simulating recovery of ${node}`);

                    try {
                        const response = await fetch('/api/simulate/node-recovery', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ node: node })
                        });

                        const result = await response.json();
                        document.getElementById('simulationResults').innerHTML = 
                            `<div class="test-result"><pre>${JSON.stringify(result, null, 2)}</pre></div>`;
                        logActivity(`Node recovery simulation result: ${result.message}`);
                        
                        setTimeout(refreshClusterStatus, 2000);
                    } catch (error) {
                        logActivity(`Node recovery simulation failed: ${error.message}`, 'error');
                    }
                }

                async function simulateNetworkPartition() {
                    if (!confirm('Are you sure you want to simulate a network partition? This may affect cluster availability.')) {
                        return;
                    }

                    logActivity('Simulating network partition');

                    try {
                        const response = await fetch('/api/simulate/network-partition', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({})
                        });

                        const result = await response.json();
                        document.getElementById('simulationResults').innerHTML = 
                            `<div class="test-result"><pre>${JSON.stringify(result, null, 2)}</pre></div>`;
                        logActivity(`Network partition simulation result: ${result.message}`);
                        
                        setTimeout(refreshClusterStatus, 2000);
                    } catch (error) {
                        logActivity(`Network partition simulation failed: ${error.message}`, 'error');
                    }
                }

                // Initialize dashboard
                logActivity('Dashboard initialized');
                refreshClusterStatus();
                refreshCacheStats();
                startAutoRefresh();
            </script>
        </body>
        </html>
        """
        return web.Response(text=html_content, content_type='text/html')

    async def initialize(self):
        """Initialize the dashboard."""
        if not self.session:
            self.session = ClientSession(timeout=ClientTimeout(total=5.0))
        logger.info("Dashboard initialized")

    async def shutdown(self):
        """Shutdown the dashboard."""
        if self.session:
            await self.session.close()

    async def _get_node_status(self, node_id: str, node_info: Dict) -> Dict:
        """Get status of a single node."""
        if not self.session:
            await self._init_session()
        
        try:
            url = f"http://{node_info['host']}:{node_info['port']}/status"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'status': 'healthy',
                        'host': node_info['host'],
                        'port': node_info['port'],
                        'raft': data.get('raft', {}),
                        'cache': data.get('cache', {})
                    }
        except Exception as e:
            logger.warning(f"Failed to get status for {node_id}: {e}")
        
        return {
            'status': 'unhealthy',
            'host': node_info['host'],
            'port': node_info['port'],
            'error': 'Connection failed'
        }

    async def get_cluster_status(self, request):
        """Get status of all nodes in the cluster."""
        try:
            tasks = []
            for node_id, node_info in self.nodes.items():
                task = self._get_node_status(node_id, node_info)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            cluster_status = {}
            for i, (node_id, node_info) in enumerate(self.nodes.items()):
                if isinstance(results[i], Exception):
                    cluster_status[node_id] = {
                        'status': 'unhealthy',
                        'host': node_info['host'],
                        'port': node_info['port'],
                        'error': str(results[i])
                    }
                else:
                    cluster_status[node_id] = results[i]
            
            return web.json_response({'nodes': cluster_status})
            
        except Exception as e:
            logger.error(f"Error getting cluster status: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_cluster_stats(self, request):
        """Get aggregated cluster statistics."""
        try:
            # Get stats from the leader node (typically node1)
            url = "http://127.0.0.1:3001/stats"
            async with self.session.get(url) as response:
                if response.status == 200:
                    stats = await response.json()
                    return web.json_response({'cache': stats})
                    
        except Exception as e:
            logger.warning(f"Failed to get cluster stats: {e}")
            
        return web.json_response({'cache': {}})

    async def set_cache_value(self, request):
        """Set a value in the cache."""
        try:
            data = await request.json()
            key = data.get('key')
            value = data.get('value')
            ttl = data.get('ttl')
            
            if not key or value is None:
                return web.json_response({'error': 'Key and value are required'}, status=400)
            
            payload = {'value': value}
            if ttl:
                payload['ttl'] = ttl
            
            # Send to leader node
            url = f"http://127.0.0.1:3001/cache/{key}"
            async with self.session.post(url, json=payload) as response:
                result = await response.json()
                return web.json_response(result, status=response.status)
                
        except Exception as e:
            logger.error(f"Error setting cache value: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_cache_value(self, request):
        """Get a value from the cache."""
        try:
            key = request.match_info['key']
            
            # Try to read from any healthy node
            for node_id, node_info in self.nodes.items():
                try:
                    url = f"http://{node_info['host']}:{node_info['port']}/cache/{key}"
                    async with self.session.get(url) as response:
                        if response.status in [200, 404]:
                            result = await response.json()
                            return web.json_response(result, status=response.status)
                except:
                    continue
                    
            return web.json_response({'error': 'All nodes unavailable'}, status=503)
            
        except Exception as e:
            logger.error(f"Error getting cache value: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def delete_cache_value(self, request):
        """Delete a value from the cache."""
        try:
            key = request.match_info['key']
            
            url = f"http://127.0.0.1:3001/cache/{key}"
            async with self.session.delete(url) as response:
                result = await response.json()
                return web.json_response(result, status=response.status)
                
        except Exception as e:
            logger.error(f"Error deleting cache value: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def list_cache_keys(self, request):
        """List all cache keys."""
        try:
            url = "http://127.0.0.1:3001/cache"
            async with self.session.get(url) as response:
                result = await response.json()
                return web.json_response(result, status=response.status)
                
        except Exception as e:
            logger.error(f"Error listing cache keys: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def clear_cache(self, request):
        """Clear all cache data."""
        try:
            url = "http://127.0.0.1:3001/cache"
            async with self.session.delete(url) as response:
                result = await response.json()
                return web.json_response(result, status=response.status)
                
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def test_raft_consensus(self, request):
        """Test Raft consensus by performing multiple operations."""
        try:
            operations = []
            start_time = time.time()
            
            # Test multiple set operations
            for i in range(10):
                try:
                    url = f"http://127.0.0.1:3001/cache/raft_test_{i}"
                    payload = {'value': f'consensus_test_value_{i}'}
                    async with self.session.post(url, json=payload) as response:
                        result = await response.json()
                        operations.append({
                            'operation': f'set_raft_test_{i}',
                            'success': response.status == 200,
                            'result': result
                        })
                except Exception as e:
                    operations.append({
                        'operation': f'set_raft_test_{i}',
                        'success': False,
                        'error': str(e)
                    })
            
            end_time = time.time()
            
            return web.json_response({
                'test_type': 'raft_consensus',
                'total_operations': len(operations),
                'successful_operations': sum(1 for op in operations if op['success']),
                'duration_seconds': round(end_time - start_time, 3),
                'operations': operations,
                'summary': f"Completed {len(operations)} operations in {round(end_time - start_time, 3)}s"
            })
            
        except Exception as e:
            logger.error(f"Error testing Raft consensus: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def test_performance(self, request):
        """Run performance tests on the cache."""
        try:
            data = await request.json()
            operations = data.get('operations', 100)
            test_type = data.get('type', 'mixed')
            
            results = []
            start_time = time.time()
            
            if test_type in ['set', 'mixed']:
                # Set operations
                for i in range(operations if test_type == 'set' else operations // 2):
                    op_start = time.time()
                    try:
                        url = f"http://127.0.0.1:3001/cache/perf_test_{i}"
                        payload = {'value': f'performance_test_value_{i}'}
                        async with self.session.post(url, json=payload) as response:
                            await response.json()
                            results.append({
                                'operation': 'set',
                                'duration_ms': round((time.time() - op_start) * 1000, 2),
                                'success': response.status == 200
                            })
                    except Exception as e:
                        results.append({
                            'operation': 'set',
                            'duration_ms': round((time.time() - op_start) * 1000, 2),
                            'success': False,
                            'error': str(e)
                        })
            
            if test_type in ['get', 'mixed']:
                # Get operations
                for i in range(operations if test_type == 'get' else operations // 2):
                    op_start = time.time()
                    try:
                        url = f"http://127.0.0.1:3001/cache/perf_test_{i}"
                        async with self.session.get(url) as response:
                            await response.json()
                            results.append({
                                'operation': 'get',
                                'duration_ms': round((time.time() - op_start) * 1000, 2),
                                'success': response.status in [200, 404]
                            })
                    except Exception as e:
                        results.append({
                            'operation': 'get',
                            'duration_ms': round((time.time() - op_start) * 1000, 2),
                            'success': False,
                            'error': str(e)
                        })
            
            end_time = time.time()
            total_duration = end_time - start_time
            successful_ops = sum(1 for r in results if r['success'])
            avg_latency = sum(r['duration_ms'] for r in results if r['success']) / max(successful_ops, 1)
            
            return web.json_response({
                'test_type': 'performance',
                'total_operations': len(results),
                'successful_operations': successful_ops,
                'failed_operations': len(results) - successful_ops,
                'total_duration_seconds': round(total_duration, 3),
                'operations_per_second': round(successful_ops / total_duration, 2),
                'average_latency_ms': round(avg_latency, 2),
                'summary': f"{successful_ops}/{len(results)} operations succeeded, {round(successful_ops / total_duration, 2)} ops/sec, {round(avg_latency, 2)}ms avg latency"
            })
            
        except Exception as e:
            logger.error(f"Error running performance test: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def test_failover(self, request):
        """Test cluster failover capabilities."""
        try:
            # This would involve more complex testing scenarios
            return web.json_response({
                'message': 'Failover testing not yet implemented',
                'suggested_tests': [
                    'Kill leader node and observe election',
                    'Kill majority of nodes and test unavailability',
                    'Recover nodes and test data consistency'
                ]
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def get_test_results(self, request):
        """Get recent test results."""
        return web.json_response({'results': self.test_results})

    async def simulate_node_failure(self, request):
        """Simulate failure of a specific node."""
        try:
            data = await request.json()
            node = data.get('node', 'node2')
            
            # Try to find and kill the process
            try:
                result = subprocess.run(['pgrep', '-f', f'python main.py {node}'], 
                                      capture_output=True, text=True)
                if result.stdout.strip():
                    pid = result.stdout.strip()
                    subprocess.run(['kill', '-TERM', pid])
                    return web.json_response({
                        'message': f'Simulated failure of {node} (PID: {pid})',
                        'node': node,
                        'action': 'terminated',
                        'pid': pid
                    })
                else:
                    return web.json_response({
                        'message': f'{node} is not currently running',
                        'node': node,
                        'action': 'already_down'
                    })
            except Exception as e:
                return web.json_response({
                    'message': f'Failed to simulate failure of {node}: {str(e)}',
                    'node': node,
                    'action': 'failed',
                    'error': str(e)
                })
                
        except Exception as e:
            logger.error(f"Error simulating node failure: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def simulate_node_recovery(self, request):
        """Simulate recovery of a specific node."""
        try:
            data = await request.json()
            node = data.get('node', 'node2')
            
            # Try to start the node
            try:
                # Check if already running
                result = subprocess.run(['pgrep', '-f', f'python main.py {node}'], 
                                      capture_output=True, text=True)
                if result.stdout.strip():
                    return web.json_response({
                        'message': f'{node} is already running',
                        'node': node,
                        'action': 'already_running',
                        'pid': result.stdout.strip()
                    })
                
                # Start the node
                process = subprocess.Popen(['python', 'main.py', node], 
                                         stdout=subprocess.PIPE, 
                                         stderr=subprocess.PIPE)
                
                return web.json_response({
                    'message': f'Simulated recovery of {node}',
                    'node': node,
                    'action': 'started',
                    'pid': process.pid
                })
                
            except Exception as e:
                return web.json_response({
                    'message': f'Failed to simulate recovery of {node}: {str(e)}',
                    'node': node,
                    'action': 'failed',
                    'error': str(e)
                })
                
        except Exception as e:
            logger.error(f"Error simulating node recovery: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def simulate_network_partition(self, request):
        """Simulate network partition."""
        try:
            return web.json_response({
                'message': 'Network partition simulation is complex and requires careful setup',
                'suggestion': 'Use manual node failure/recovery to simulate partition effects',
                'steps': [
                    '1. Kill 2 nodes to simulate partition',
                    '2. Observe remaining node cannot achieve majority',
                    '3. Restart nodes to simulate partition healing'
                ]
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def start(self, host='0.0.0.0', port=8080):
        """Start the dashboard server."""
        await self.initialize()
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"Dashboard started on http://{host}:{port}")
        return runner


async def main():
    """Main entry point for the dashboard."""
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    dashboard = CacheDashboard()
    
    try:
        runner = await dashboard.start()
        print("🚀 Distributed Cache Dashboard started!")
        print("📊 Access the dashboard at: http://127.0.0.1:8080")
        print("Press Ctrl+C to stop...")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down dashboard...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await dashboard.shutdown()
        if 'runner' in locals():
            await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())