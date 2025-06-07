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
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 24px;
            padding: 40px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            border: 1px solid rgba(255,255,255,0.2);
        }
        
        .header h1 {
            font-size: 3rem;
            font-weight: 700;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 10px;
        }
        
        .header p {
            font-size: 1.2rem;
            color: #64748b;
            font-weight: 400;
        }
        
        .metrics-overview {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 20px;
            padding: 30px;
            text-align: center;
            box-shadow: 0 15px 35px rgba(0,0,0,0.08);
            border: 1px solid rgba(255,255,255,0.2);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 25px 50px rgba(0,0,0,0.12);
        }
        
        .metric-icon {
            font-size: 2.5rem;
            margin-bottom: 15px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .metric-value {
            font-size: 2.5rem;
            font-weight: 700;
            color: #1e293b;
            margin-bottom: 5px;
            display: block;
        }
        
        .metric-label {
            color: #64748b;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-size: 0.9rem;
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }
        
        .card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 24px;
            padding: 30px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.08);
            border: 1px solid rgba(255,255,255,0.2);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-2px);
            box-shadow: 0 25px 50px rgba(0,0,0,0.12);
        }
        
        .card h3 {
            font-size: 1.5rem;
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .card h3 i {
            color: #667eea;
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.95rem;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.3);
            margin: 5px;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 30px rgba(102, 126, 234, 0.4);
        }
        
        .btn:active {
            transform: translateY(0);
        }
        
        .btn-primary {
            background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
            box-shadow: 0 8px 20px rgba(59, 130, 246, 0.3);
        }
        
        .btn-success {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            box-shadow: 0 8px 20px rgba(16, 185, 129, 0.3);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            box-shadow: 0 8px 20px rgba(239, 68, 68, 0.3);
        }
        
        .node-status {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 15px 20px;
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            border-radius: 16px;
            margin: 10px 0;
            border-left: 5px solid #667eea;
            transition: all 0.3s ease;
        }
        
        .node-status:hover {
            transform: translateX(5px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.08);
        }
        
        .node-status.healthy {
            border-left-color: #10b981;
            background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
        }
        
        .node-status.unhealthy {
            border-left-color: #ef4444;
            background: linear-gradient(135deg, #fef2f2 0%, #fecaca 100%);
        }
        
        .node-name {
            font-weight: 600;
            font-size: 1.1rem;
            color: #1e293b;
        }
        
        .node-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-healthy {
            background: #10b981;
            color: white;
        }
        
        .status-unhealthy {
            background: #ef4444;
            color: white;
        }
        
        .leader-badge {
            background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
            color: white;
            margin-left: 8px;
        }
        
        .follower-badge {
            background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
            color: white;
            margin-left: 8px;
        }
        
        input, textarea, select {
            width: 100%;
            padding: 12px 16px;
            margin: 8px 0;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            font-size: 0.95rem;
            font-family: inherit;
            transition: all 0.3s ease;
            background: rgba(255,255,255,0.9);
        }
        
        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
            background: white;
        }
        
        .log {
            background: #1e293b;
            color: #e2e8f0;
            padding: 20px;
            border-radius: 16px;
            font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
            font-size: 0.9rem;
            line-height: 1.6;
            height: 350px;
            overflow-y: auto;
            box-shadow: inset 0 4px 8px rgba(0,0,0,0.2);
            border: 1px solid #334155;
        }
        
        .log::-webkit-scrollbar {
            width: 8px;
        }
        
        .log::-webkit-scrollbar-track {
            background: #334155;
            border-radius: 4px;
        }
        
        .log::-webkit-scrollbar-thumb {
            background: #64748b;
            border-radius: 4px;
        }
        
        .form-group {
            margin-bottom: 15px;
        }
        
        .form-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: 500;
            color: #374151;
        }
        
        .button-group {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 15px;
        }
        
        .result-container {
            margin-top: 20px;
            padding: 20px;
            background: #f8fafc;
            border-radius: 12px;
            border: 1px solid #e2e8f0;
        }
        
        .result-container pre {
            background: #1e293b;
            color: #e2e8f0;
            padding: 15px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 0.85rem;
            line-height: 1.5;
        }
        
        .loading {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f4f6;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .pulse {
            animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
        }
        
        .status-indicator.online {
            background: #10b981;
            box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.2);
        }
        
        .status-indicator.offline {
            background: #ef4444;
            box-shadow: 0 0 0 3px rgba(239, 68, 68, 0.2);
        }
        
        @media (max-width: 768px) {
            .container {
                padding: 15px;
            }
            
            .header h1 {
                font-size: 2rem;
            }
            
            .grid {
                grid-template-columns: 1fr;
            }
            
            .metrics-overview {
                grid-template-columns: repeat(2, 1fr);
            }
            
            .button-group {
                flex-direction: column;
            }
            
            .btn {
                width: 100%;
                justify-content: center;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><i class="fas fa-network-wired"></i> Distributed Cache Cluster</h1>
            <p>Real-time monitoring and testing dashboard for Raft consensus protocol</p>
        </div>

        <div class="metrics-overview">
            <div class="metric-card">
                <div class="metric-icon">
                    <i class="fas fa-server"></i>
                </div>
                <span class="metric-value" id="healthy-nodes">-</span>
                <div class="metric-label">Healthy Nodes</div>
            </div>
            <div class="metric-card">
                <div class="metric-icon">
                    <i class="fas fa-crown"></i>
                </div>
                <span class="metric-value" id="leader-count">-</span>
                <div class="metric-label">Leaders</div>
            </div>
            <div class="metric-card">
                <div class="metric-icon">
                    <i class="fas fa-clock"></i>
                </div>
                <span class="metric-value" id="current-term">-</span>
                <div class="metric-label">Current Term</div>
            </div>
            <div class="metric-card">
                <div class="metric-icon">
                    <i class="fas fa-tachometer-alt"></i>
                </div>
                <span class="metric-value" id="ops-per-sec">-</span>
                <div class="metric-label">Ops/Sec</div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h3><i class="fas fa-heartbeat"></i> Cluster Status</h3>
                <div id="cluster-status">
                    <div class="loading"></div> Loading cluster information...
                </div>
                <div class="button-group">
                    <button class="btn btn-primary" onclick="refreshStatus()">
                        <i class="fas fa-sync-alt"></i> Refresh Status
                    </button>
                </div>
            </div>

            <div class="card">
                <h3><i class="fas fa-database"></i> Cache Operations</h3>
                <div class="form-group">
                    <label for="cache-key">Cache Key</label>
                    <input type="text" id="cache-key" placeholder="Enter cache key">
                </div>
                <div class="form-group">
                    <label for="cache-value">Cache Value</label>
                    <textarea id="cache-value" placeholder="Enter cache value" rows="3"></textarea>
                </div>
                <div class="form-group">
                    <label for="cache-ttl">TTL (seconds)</label>
                    <input type="number" id="cache-ttl" placeholder="Time to live in seconds">
                </div>
                <div class="button-group">
                    <button class="btn btn-success" onclick="setCache()">
                        <i class="fas fa-plus"></i> Set
                    </button>
                    <button class="btn btn-primary" onclick="getCache()">
                        <i class="fas fa-search"></i> Get
                    </button>
                    <button class="btn btn-danger" onclick="deleteCache()">
                        <i class="fas fa-trash"></i> Delete
                    </button>
                </div>
                <div id="cache-result" class="result-container" style="display: none;"></div>
            </div>

            <div class="card">
                <h3><i class="fas fa-rocket"></i> Performance Testing</h3>
                <div class="form-group">
                    <label for="test-ops">Number of Operations</label>
                    <input type="number" id="test-ops" value="50" placeholder="Number of operations">
                </div>
                <div class="button-group">
                    <button class="btn btn-primary" onclick="runPerformanceTest()">
                        <i class="fas fa-play"></i> Run Performance Test
                    </button>
                </div>
                <div id="perf-result" class="result-container" style="display: none;"></div>
            </div>

            <div class="card">
                <h3><i class="fas fa-cog"></i> Node Simulation</h3>
                <div class="form-group">
                    <label for="target-node">Target Node</label>
                    <select id="target-node">
                        <option value="node2">Node 2 (Port 4000)</option>
                        <option value="node3">Node 3 (Port 5000)</option>
                    </select>
                </div>
                <div class="button-group">
                    <button class="btn btn-danger" onclick="killNode()">
                        <i class="fas fa-skull"></i> Kill Node
                    </button>
                    <button class="btn btn-success" onclick="restartNode()">
                        <i class="fas fa-play"></i> Restart Node
                    </button>
                </div>
                <div id="sim-result" class="result-container" style="display: none;"></div>
            </div>
        </div>

        <div class="card">
            <h3><i class="fas fa-terminal"></i> Activity Log</h3>
            <div id="activity-log" class="log">Dashboard started...\n</div>
            <div class="button-group">
                <button class="btn btn-primary" onclick="clearLog()">
                    <i class="fas fa-broom"></i> Clear Log
                </button>
                <button class="btn" onclick="downloadLog()">
                    <i class="fas fa-download"></i> Download Log
                </button>
            </div>
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

        function downloadLog() {
            const logContent = document.getElementById('activity-log').textContent;
            const blob = new Blob([logContent], { type: 'text/plain' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `cache-dashboard-log-${new Date().toISOString().split('T')[0]}.txt`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            log('Log downloaded');
        }

        function showNotification(message, type = 'info') {
            // Remove any existing notifications
            const existingNotification = document.querySelector('.notification');
            if (existingNotification) {
                existingNotification.remove();
            }

            const notification = document.createElement('div');
            notification.className = `notification notification-${type}`;
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 15px 20px;
                border-radius: 12px;
                color: white;
                font-weight: 500;
                z-index: 1000;
                min-width: 300px;
                box-shadow: 0 10px 25px rgba(0,0,0,0.2);
                transform: translateX(100%);
                transition: transform 0.3s ease;
            `;

            const colors = {
                success: '#10b981',
                error: '#ef4444',
                info: '#3b82f6',
                warning: '#f59e0b'
            };

            notification.style.background = colors[type] || colors.info;
            notification.innerHTML = `
                <div style="display: flex; align-items: center; gap: 10px;">
                    <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
                    <span>${message}</span>
                </div>
            `;

            document.body.appendChild(notification);
            
            // Animate in
            setTimeout(() => {
                notification.style.transform = 'translateX(0)';
            }, 100);

            // Auto remove after 4 seconds
            setTimeout(() => {
                notification.style.transform = 'translateX(100%)';
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.remove();
                    }
                }, 300);
            }, 4000);
        }

        async function refreshStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                let healthyCount = 0;
                let leaderCount = 0;
                let maxTerm = 0;
                let html = '';
                
                for (const [node, info] of Object.entries(data.nodes || {})) {
                    const isHealthy = info.healthy;
                    const state = info.raft ? info.raft.state : 'unknown';
                    const term = info.raft ? info.raft.term : 0;
                    const isLeader = state === 'leader';
                    
                    if (isHealthy) healthyCount++;
                    if (isLeader) leaderCount++;
                    if (term > maxTerm) maxTerm = term;
                    
                    const statusClass = isHealthy ? 'healthy' : 'unhealthy';
                    const statusIndicator = isHealthy ? 'online' : 'offline';
                    const roleClass = isLeader ? 'leader-badge' : 'follower-badge';
                    
                    html += `
                        <div class="node-status ${statusClass}">
                            <div style="display: flex; align-items: center;">
                                <span class="status-indicator ${statusIndicator}"></span>
                                <div>
                                    <div class="node-name">${node.toUpperCase()}</div>
                                    <div style="font-size: 0.9rem; color: #64748b;">${info.host}:${info.port}</div>
                                </div>
                            </div>
                            <div style="display: flex; align-items: center; gap: 8px;">
                                <span class="node-badge ${isHealthy ? 'status-healthy' : 'status-unhealthy'}">
                                    ${isHealthy ? 'ONLINE' : 'OFFLINE'}
                                </span>
                                ${isHealthy ? `<span class="node-badge ${roleClass}">${state.toUpperCase()}</span>` : ''}
                            </div>
                        </div>
                    `;
                }
                
                // Update metrics
                document.getElementById('healthy-nodes').textContent = healthyCount;
                document.getElementById('leader-count').textContent = leaderCount;
                document.getElementById('current-term').textContent = maxTerm;
                
                document.getElementById('cluster-status').innerHTML = html || '<div class="node-status unhealthy"><div class="node-name">No nodes available</div></div>';
                log('Refreshed cluster status');
            } catch (error) {
                document.getElementById('cluster-status').innerHTML = `
                    <div class="node-status unhealthy">
                        <div class="node-name">Error loading cluster status</div>
                        <span class="node-badge status-unhealthy">ERROR</span>
                    </div>
                `;
                log(`Error refreshing status: ${error.message}`);
            }
        }

        async function setCache() {
            const key = document.getElementById('cache-key').value;
            const value = document.getElementById('cache-value').value;
            const ttl = document.getElementById('cache-ttl').value;

            if (!key || !value) {
                showNotification('Please provide both key and value', 'error');
                return;
            }

            const resultContainer = document.getElementById('cache-result');
            resultContainer.style.display = 'block';
            resultContainer.innerHTML = '<div class="loading"></div> Setting cache value...';

            try {
                const response = await fetch('http://127.0.0.1:3000/cache/' + encodeURIComponent(key), {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({value: value, ttl: ttl ? parseInt(ttl) : undefined})
                });
                const result = await response.json();
                
                if (response.ok) {
                    resultContainer.innerHTML = `
                        <div style="color: #10b981; margin-bottom: 10px;">
                            <i class="fas fa-check-circle"></i> Cache set successfully
                        </div>
                        <pre>${JSON.stringify(result, null, 2)}</pre>
                    `;
                    showNotification(`Cache key "${key}" set successfully`, 'success');
                } else {
                    resultContainer.innerHTML = `
                        <div style="color: #ef4444; margin-bottom: 10px;">
                            <i class="fas fa-exclamation-circle"></i> Failed to set cache
                        </div>
                        <pre>${JSON.stringify(result, null, 2)}</pre>
                    `;
                }
                
                log(`Set cache key: ${key}`);
            } catch (error) {
                resultContainer.innerHTML = `
                    <div style="color: #ef4444;">
                        <i class="fas fa-times-circle"></i> Error: ${error.message}
                    </div>
                `;
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