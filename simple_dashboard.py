"""
Simple web dashboard for monitoring and testing the distributed cache cluster.
"""

from aiohttp import web, ClientSession, ClientTimeout
import asyncio
import json
import subprocess
import time

class SimpleDashboard:
    def __init__(self):
        self.app = web.Application()
        self.session = None
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get('/', self.dashboard)
        self.app.router.add_get('/api/status', self.get_status)
        self.app.router.add_post('/api/test', self.run_test)

    async def dashboard(self, request):
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Distributed Cache Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .btn { padding: 10px 20px; margin: 5px; border: none; border-radius: 4px; cursor: pointer; }
        .btn-primary { background: #3498db; color: white; }
        .btn-success { background: #27ae60; color: white; }
        .btn-danger { background: #e74c3c; color: white; }
        .status-healthy { color: #27ae60; font-weight: bold; }
        .status-unhealthy { color: #e74c3c; font-weight: bold; }
        .log { background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 4px; font-family: monospace; height: 300px; overflow-y: auto; }
        input, textarea { width: 100%; padding: 8px; margin: 5px 0; border: 1px solid #ddd; border-radius: 4px; }
        .metric { text-align: center; padding: 15px; background: #ecf0f1; border-radius: 4px; margin: 5px; }
        .metric-value { font-size: 2em; font-weight: bold; color: #2c3e50; }
        .metric-label { color: #7f8c8d; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Distributed Cache Cluster Dashboard</h1>
            <p>Monitor and test your Raft consensus protocol implementation</p>
        </div>

        <div class="grid">
            <div class="card">
                <h3>Cluster Status</h3>
                <div id="cluster-status">Loading...</div>
                <button class="btn btn-primary" onclick="refreshStatus()">Refresh Status</button>
            </div>

            <div class="card">
                <h3>Cache Operations</h3>
                <input type="text" id="cache-key" placeholder="Cache Key">
                <textarea id="cache-value" placeholder="Cache Value" rows="3"></textarea>
                <input type="number" id="cache-ttl" placeholder="TTL (seconds)">
                <div>
                    <button class="btn btn-success" onclick="setCache()">Set</button>
                    <button class="btn btn-primary" onclick="getCache()">Get</button>
                    <button class="btn btn-danger" onclick="deleteCache()">Delete</button>
                </div>
                <div id="cache-result"></div>
            </div>

            <div class="card">
                <h3>Performance Test</h3>
                <input type="number" id="test-ops" value="50" placeholder="Number of operations">
                <button class="btn btn-primary" onclick="runPerformanceTest()">Run Test</button>
                <div id="perf-result"></div>
            </div>

            <div class="card">
                <h3>Node Simulation</h3>
                <select id="target-node">
                    <option value="node2">Node 2</option>
                    <option value="node3">Node 3</option>
                </select>
                <div>
                    <button class="btn btn-danger" onclick="killNode()">Kill Node</button>
                    <button class="btn btn-success" onclick="restartNode()">Restart Node</button>
                </div>
                <div id="sim-result"></div>
            </div>
        </div>

        <div class="card">
            <h3>Activity Log</h3>
            <div id="activity-log" class="log">Dashboard started...\n</div>
            <button class="btn btn-primary" onclick="clearLog()">Clear Log</button>
        </div>
    </div>

    <script>
        function log(message) {
            const logEl = document.getElementById('activity-log');
            const timestamp = new Date().toLocaleTimeString();
            logEl.textContent += `[${timestamp}] ${message}\\n`;
            logEl.scrollTop = logEl.scrollHeight;
        }

        function clearLog() {
            document.getElementById('activity-log').textContent = 'Log cleared...\\n';
        }

        async function refreshStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                let html = '';
                for (const [node, info] of Object.entries(data.nodes || {})) {
                    const statusClass = info.healthy ? 'status-healthy' : 'status-unhealthy';
                    const state = info.raft ? info.raft.state : 'unknown';
                    const badge = state === 'leader' ? ' [LEADER]' : state === 'follower' ? ' [FOLLOWER]' : '';
                    html += `<div><span class="${statusClass}">${node}</span> (${info.host}:${info.port}) ${state.toUpperCase()}${badge}</div>`;
                }
                
                document.getElementById('cluster-status').innerHTML = html || 'No nodes available';
                log('Refreshed cluster status');
            } catch (error) {
                log(`Error refreshing status: ${error.message}`);
            }
        }

        async function setCache() {
            const key = document.getElementById('cache-key').value;
            const value = document.getElementById('cache-value').value;
            const ttl = document.getElementById('cache-ttl').value;

            if (!key || !value) {
                alert('Please provide both key and value');
                return;
            }

            try {
                const response = await fetch('http://127.0.0.1:3000/cache/' + encodeURIComponent(key), {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({value: value, ttl: ttl ? parseInt(ttl) : undefined})
                });
                const result = await response.json();
                document.getElementById('cache-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Set cache key: ${key}`);
            } catch (error) {
                log(`Error setting cache: ${error.message}`);
            }
        }

        async function getCache() {
            const key = document.getElementById('cache-key').value;
            if (!key) {
                alert('Please provide a key');
                return;
            }

            try {
                const response = await fetch('http://127.0.0.1:3000/cache/' + encodeURIComponent(key));
                const result = await response.json();
                document.getElementById('cache-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Retrieved cache key: ${key}`);
            } catch (error) {
                log(`Error getting cache: ${error.message}`);
            }
        }

        async function deleteCache() {
            const key = document.getElementById('cache-key').value;
            if (!key) {
                alert('Please provide a key');
                return;
            }

            try {
                const response = await fetch('http://127.0.0.1:3000/cache/' + encodeURIComponent(key), {
                    method: 'DELETE'
                });
                const result = await response.json();
                document.getElementById('cache-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Deleted cache key: ${key}`);
            } catch (error) {
                log(`Error deleting cache: ${error.message}`);
            }
        }

        async function runPerformanceTest() {
            const ops = parseInt(document.getElementById('test-ops').value) || 50;
            log(`Starting performance test with ${ops} operations`);

            try {
                const response = await fetch('/api/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({type: 'performance', operations: ops})
                });
                const result = await response.json();
                document.getElementById('perf-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Performance test completed: ${result.summary}`);
            } catch (error) {
                log(`Performance test failed: ${error.message}`);
            }
        }

        async function killNode() {
            const node = document.getElementById('target-node').value;
            if (!confirm(`Kill ${node}?`)) return;

            try {
                const response = await fetch('/api/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({type: 'kill_node', node: node})
                });
                const result = await response.json();
                document.getElementById('sim-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Killed node: ${node}`);
                setTimeout(refreshStatus, 2000);
            } catch (error) {
                log(`Error killing node: ${error.message}`);
            }
        }

        async function restartNode() {
            const node = document.getElementById('target-node').value;
            
            try {
                const response = await fetch('/api/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({type: 'restart_node', node: node})
                });
                const result = await response.json();
                document.getElementById('sim-result').innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
                log(`Restarted node: ${node}`);
                setTimeout(refreshStatus, 3000);
            } catch (error) {
                log(`Error restarting node: ${error.message}`);
            }
        }

        // Auto-refresh every 5 seconds
        setInterval(refreshStatus, 5000);
        refreshStatus();
    </script>
</body>
</html>
        """
        return web.Response(text=html, content_type='text/html')

    async def get_status(self, request):
        """Get cluster status."""
        if not self.session:
            self.session = ClientSession()
        
        nodes = {}
        ports = [3000, 4000, 5000]
        node_names = ['node1', 'node2', 'node3']
        
        for i, (name, port) in enumerate(zip(node_names, ports)):
            try:
                async with self.session.get(f'http://127.0.0.1:{port}/status', timeout=ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        nodes[name] = {
                            'host': '127.0.0.1',
                            'port': port,
                            'healthy': True,
                            'raft': data.get('raft', {}),
                            'cache': data.get('cache', {})
                        }
                    else:
                        nodes[name] = {'host': '127.0.0.1', 'port': port, 'healthy': False}
            except:
                nodes[name] = {'host': '127.0.0.1', 'port': port, 'healthy': False}
        
        return web.json_response({'nodes': nodes})

    async def run_test(self, request):
        """Run various tests."""
        data = await request.json()
        test_type = data.get('type')
        
        if test_type == 'performance':
            return await self._performance_test(data.get('operations', 50))
        elif test_type == 'kill_node':
            return await self._kill_node(data.get('node', 'node2'))
        elif test_type == 'restart_node':
            return await self._restart_node(data.get('node', 'node2'))
        
        return web.json_response({'error': 'Unknown test type'})

    async def _performance_test(self, ops):
        """Run performance test."""
        if not self.session:
            self.session = ClientSession()
            
        start_time = time.time()
        successful = 0
        failed = 0
        latencies = []
        
        # Test SET operations with better error handling
        for i in range(ops):
            op_start = time.time()
            try:
                async with self.session.post(
                    f'http://127.0.0.1:3000/cache/perf_test_{i}',
                    json={'value': f'test_value_{i}'},
                    timeout=ClientTimeout(total=2)
                ) as resp:
                    op_time = time.time() - op_start
                    if resp.status == 200:
                        successful += 1
                        latencies.append(op_time * 1000)  # Convert to ms
                    else:
                        failed += 1
            except asyncio.TimeoutError:
                failed += 1
            except Exception as e:
                failed += 1
        
        duration = time.time() - start_time
        ops_per_sec = successful / duration if duration > 0 else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        
        return web.json_response({
            'test_type': 'performance',
            'operations': ops,
            'successful': successful,
            'failed': failed,
            'duration_seconds': round(duration, 2),
            'ops_per_second': round(ops_per_sec, 2),
            'avg_latency_ms': round(avg_latency, 2),
            'summary': f'{successful}/{ops} ops succeeded, {round(ops_per_sec, 1)} ops/sec, {round(avg_latency, 1)}ms avg'
        })

    async def _kill_node(self, node):
        """Kill a specific node."""
        try:
            result = subprocess.run(['pgrep', '-f', f'python main.py {node}'], 
                                  capture_output=True, text=True)
            if result.stdout.strip():
                pid = result.stdout.strip()
                subprocess.run(['kill', '-TERM', pid])
                return web.json_response({
                    'action': 'kill_node',
                    'node': node,
                    'pid': pid,
                    'status': 'killed'
                })
            else:
                return web.json_response({
                    'action': 'kill_node',
                    'node': node,
                    'status': 'not_running'
                })
        except Exception as e:
            return web.json_response({
                'action': 'kill_node',
                'node': node,
                'error': str(e)
            })

    async def _restart_node(self, node):
        """Restart a specific node."""
        try:
            # Check if already running
            result = subprocess.run(['pgrep', '-f', f'python main.py {node}'], 
                                  capture_output=True, text=True)
            if result.stdout.strip():
                return web.json_response({
                    'action': 'restart_node',
                    'node': node,
                    'status': 'already_running',
                    'pid': result.stdout.strip()
                })
            
            # Start the node
            process = subprocess.Popen(['python', 'main.py', node], 
                                     stdout=subprocess.DEVNULL, 
                                     stderr=subprocess.DEVNULL)
            
            return web.json_response({
                'action': 'restart_node',
                'node': node,
                'status': 'started',
                'pid': process.pid
            })
        except Exception as e:
            return web.json_response({
                'action': 'restart_node',
                'node': node,
                'error': str(e)
            })

    async def start(self, port=8080):
        """Start the dashboard."""
        self.session = ClientSession()
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        print(f"Dashboard running at http://127.0.0.1:{port}")
        return runner

async def main():
    dashboard = SimpleDashboard()
    runner = await dashboard.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if dashboard.session:
            await dashboard.session.close()
        await runner.cleanup()

if __name__ == '__main__':
    asyncio.run(main())