"""
Advanced Raft consensus analysis and testing suite.
Provides detailed insights into cluster behavior and consensus protocol.
"""

import asyncio
import aiohttp
import json
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
import statistics


@dataclass
class NodeState:
    node_id: str
    state: str  # leader, follower, candidate
    term: int
    is_leader: bool
    log_length: int
    commit_index: int
    healthy: bool
    response_time: float


@dataclass
class ClusterSnapshot:
    timestamp: float
    nodes: List[NodeState]
    leader_count: int
    term_range: tuple
    healthy_count: int


class RaftAnalyzer:
    def __init__(self):
        self.nodes = {
            'node1': 3000,
            'node2': 4000,
            'node3': 5000
        }
        self.session = None
        self.snapshots = []
        
    async def initialize(self):
        self.session = aiohttp.ClientSession()
        
    async def cleanup(self):
        if self.session:
            await self.session.close()
            
    async def get_node_state(self, node_id: str, port: int) -> NodeState:
        """Get detailed state of a single node."""
        start_time = time.time()
        try:
            async with self.session.get(
                f'http://127.0.0.1:{port}/status',
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                response_time = (time.time() - start_time) * 1000
                
                if resp.status == 200:
                    data = await resp.json()
                    raft = data.get('raft', {})
                    
                    return NodeState(
                        node_id=node_id,
                        state=raft.get('state', 'unknown'),
                        term=raft.get('term', 0),
                        is_leader=raft.get('is_leader', False),
                        log_length=raft.get('log_length', 0),
                        commit_index=raft.get('commit_index', 0),
                        healthy=True,
                        response_time=response_time
                    )
        except Exception:
            pass
            
        return NodeState(
            node_id=node_id,
            state='offline',
            term=0,
            is_leader=False,
            log_length=0,
            commit_index=0,
            healthy=False,
            response_time=999.0
        )
        
    async def capture_cluster_snapshot(self) -> ClusterSnapshot:
        """Capture a complete cluster state snapshot."""
        tasks = []
        for node_id, port in self.nodes.items():
            tasks.append(self.get_node_state(node_id, port))
            
        node_states = await asyncio.gather(*tasks)
        
        healthy_nodes = [n for n in node_states if n.healthy]
        leader_count = sum(1 for n in healthy_nodes if n.is_leader)
        
        terms = [n.term for n in healthy_nodes if n.term > 0]
        term_range = (min(terms), max(terms)) if terms else (0, 0)
        
        snapshot = ClusterSnapshot(
            timestamp=time.time(),
            nodes=node_states,
            leader_count=leader_count,
            term_range=term_range,
            healthy_count=len(healthy_nodes)
        )
        
        self.snapshots.append(snapshot)
        return snapshot
        
    def analyze_cluster_stability(self, snapshots: List[ClusterSnapshot]) -> Dict:
        """Analyze cluster stability over time."""
        if not snapshots:
            return {'status': 'no_data'}
            
        leader_counts = [s.leader_count for s in snapshots]
        healthy_counts = [s.healthy_count for s in snapshots]
        term_variations = [s.term_range[1] - s.term_range[0] for s in snapshots]
        
        stability_score = 0
        issues = []
        
        # Check leader consistency
        if all(count == 1 for count in leader_counts):
            stability_score += 40
        elif any(count > 1 for count in leader_counts):
            issues.append("Multiple leaders detected")
        elif any(count == 0 for count in leader_counts):
            issues.append("No leader periods detected")
            
        # Check node health
        if all(count == 3 for count in healthy_counts):
            stability_score += 30
        elif min(healthy_counts) >= 2:
            stability_score += 20
            issues.append("Some node failures")
        else:
            issues.append("Majority node failures")
            
        # Check term stability
        if max(term_variations) <= 1:
            stability_score += 30
        else:
            issues.append("High term variation")
            
        return {
            'stability_score': stability_score,
            'max_score': 100,
            'issues': issues,
            'leader_consistency': statistics.mode(leader_counts) if leader_counts else 0,
            'avg_healthy_nodes': statistics.mean(healthy_counts) if healthy_counts else 0,
            'term_stability': max(term_variations) if term_variations else 0
        }
        
    async def test_leader_election_behavior(self, duration: int = 10):
        """Monitor leader election behavior over time."""
        print(f"Monitoring leader election for {duration} seconds...")
        
        start_time = time.time()
        snapshots = []
        
        while time.time() - start_time < duration:
            snapshot = await self.capture_cluster_snapshot()
            snapshots.append(snapshot)
            
            # Print current state
            leaders = [n.node_id for n in snapshot.nodes if n.is_leader]
            candidates = [n.node_id for n in snapshot.nodes if n.state == 'candidate']
            followers = [n.node_id for n in snapshot.nodes if n.state == 'follower']
            
            print(f"T+{time.time()-start_time:.1f}s: Leaders={leaders}, Candidates={candidates}, Followers={followers}, Term={snapshot.term_range}")
            
            await asyncio.sleep(1)
            
        return snapshots
        
    async def test_network_partition_simulation(self):
        """Simulate network partition scenarios."""
        print("Testing network partition scenarios...")
        
        # Baseline measurement
        baseline = await self.capture_cluster_snapshot()
        print(f"Baseline: {baseline.healthy_count}/3 nodes healthy, {baseline.leader_count} leaders")
        
        # Simulate killing one node via dashboard
        try:
            async with self.session.post(
                'http://127.0.0.1:8080/api/test',
                json={'type': 'kill_node', 'node': 'node2'},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                print("Simulated node2 failure")
        except:
            print("Could not simulate node failure via dashboard")
            
        # Monitor recovery
        print("Monitoring cluster adaptation...")
        for i in range(8):
            await asyncio.sleep(1)
            snapshot = await self.capture_cluster_snapshot()
            
            healthy = [n.node_id for n in snapshot.nodes if n.healthy]
            leaders = [n.node_id for n in snapshot.nodes if n.is_leader]
            
            print(f"T+{i+1}s: Healthy={healthy}, Leaders={leaders}")
            
        # Attempt recovery
        try:
            async with self.session.post(
                'http://127.0.0.1:8080/api/test',
                json={'type': 'restart_node', 'node': 'node2'},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                print("Attempting node2 recovery")
        except:
            print("Could not restart node via dashboard")
            
        await asyncio.sleep(3)
        recovery = await self.capture_cluster_snapshot()
        print(f"Recovery: {recovery.healthy_count}/3 nodes healthy, {recovery.leader_count} leaders")
        
    async def test_data_consistency_verification(self):
        """Test data consistency across the cluster."""
        print("Testing data consistency...")
        
        # Find a healthy node to write to
        snapshot = await self.capture_cluster_snapshot()
        healthy_nodes = [(n.node_id, self.nodes[n.node_id]) for n in snapshot.nodes if n.healthy]
        
        if not healthy_nodes:
            print("No healthy nodes available for consistency test")
            return
            
        test_node_id, test_port = healthy_nodes[0]
        test_key = f"consistency_test_{int(time.time())}"
        test_value = f"test_data_{time.time()}"
        
        print(f"Writing test data to {test_node_id}...")
        
        try:
            async with self.session.post(
                f'http://127.0.0.1:{test_port}/cache/{test_key}',
                json={'value': test_value},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                write_status = resp.status
                if resp.status < 500:
                    write_data = await resp.json()
                    print(f"Write result: {write_status} - {write_data.get('message', 'No message')}")
                else:
                    print(f"Write failed with status: {write_status}")
        except Exception as e:
            print(f"Write operation failed: {e}")
            return
            
        # Wait for potential replication
        await asyncio.sleep(2)
        
        # Check consistency across all healthy nodes
        print("Checking data consistency across nodes...")
        consistency_results = {}
        
        for node_id, port in self.nodes.items():
            try:
                async with self.session.get(
                    f'http://127.0.0.1:{port}/cache/{test_key}',
                    timeout=aiohttp.ClientTimeout(total=2)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        consistency_results[node_id] = data.get('value') == test_value
                    else:
                        consistency_results[node_id] = False
            except:
                consistency_results[node_id] = False
                
        consistent_nodes = sum(consistency_results.values())
        print(f"Consistency results: {consistent_nodes}/{len(consistency_results)} nodes consistent")
        
        for node_id, is_consistent in consistency_results.items():
            status = "✓" if is_consistent else "✗"
            print(f"  {node_id}: {status}")
            
    async def generate_comprehensive_report(self):
        """Generate a comprehensive Raft analysis report."""
        print("\n" + "="*60)
        print("COMPREHENSIVE RAFT CONSENSUS ANALYSIS REPORT")
        print("="*60)
        
        # Current cluster state
        current_snapshot = await self.capture_cluster_snapshot()
        
        print("\n1. CURRENT CLUSTER STATE")
        print("-" * 30)
        print(f"Timestamp: {time.ctime(current_snapshot.timestamp)}")
        print(f"Healthy Nodes: {current_snapshot.healthy_count}/3")
        print(f"Leader Count: {current_snapshot.leader_count}")
        print(f"Term Range: {current_snapshot.term_range[0]} - {current_snapshot.term_range[1]}")
        
        print("\nNode Details:")
        for node in current_snapshot.nodes:
            status = "HEALTHY" if node.healthy else "OFFLINE"
            print(f"  {node.node_id}: {node.state.upper()} | Term {node.term} | {status} | {node.response_time:.1f}ms")
            
        # Run specific tests
        print("\n2. LEADER ELECTION MONITORING")
        print("-" * 35)
        election_snapshots = await self.test_leader_election_behavior(8)
        
        print("\n3. CLUSTER STABILITY ANALYSIS")
        print("-" * 35)
        stability = self.analyze_cluster_stability(election_snapshots)
        print(f"Stability Score: {stability['stability_score']}/{stability['max_score']}")
        print(f"Leader Consistency: {stability['leader_consistency']} leaders typical")
        print(f"Average Healthy Nodes: {stability['avg_healthy_nodes']:.1f}")
        
        if stability['issues']:
            print("Issues Detected:")
            for issue in stability['issues']:
                print(f"  - {issue}")
        else:
            print("No stability issues detected")
            
        print("\n4. DATA CONSISTENCY VERIFICATION")
        print("-" * 40)
        await self.test_data_consistency_verification()
        
        print("\n5. NETWORK PARTITION SIMULATION")
        print("-" * 40)
        await self.test_network_partition_simulation()
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE")
        print("="*60)
        
        # Summary recommendations
        print("\nRECOMMENDations:")
        if current_snapshot.healthy_count == 3 and current_snapshot.leader_count == 1:
            print("✓ Cluster is operating optimally")
        elif current_snapshot.healthy_count >= 2:
            print("⚠ Cluster has majority but some nodes are down")
        else:
            print("✗ Cluster is in critical state - insufficient nodes")
            
        if stability['stability_score'] >= 80:
            print("✓ Consensus protocol is stable")
        elif stability['stability_score'] >= 60:
            print("⚠ Consensus protocol has minor issues")
        else:
            print("✗ Consensus protocol needs attention")


async def main():
    """Run comprehensive Raft analysis."""
    analyzer = RaftAnalyzer()
    
    try:
        await analyzer.initialize()
        await analyzer.generate_comprehensive_report()
    finally:
        await analyzer.cleanup()


if __name__ == "__main__":
    asyncio.run(main())