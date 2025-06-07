"""
5-Node Raft cluster manager with brain split testing capabilities.
Demonstrates how Raft consensus handles network partitions and prevents split-brain scenarios.
"""

import subprocess
import time
import signal
import sys
import asyncio
import aiohttp
import json
from typing import Dict, List, Set


class FiveNodeCluster:
    def __init__(self):
        self.nodes = {
            'node1': {'port': 3000, 'process': None},
            'node2': {'port': 4000, 'process': None},
            'node3': {'port': 5000, 'process': None},
            'node4': {'port': 6000, 'process': None},
            'node5': {'port': 7000, 'process': None}
        }
        self.partitions = []
        self.session = None
        
    def start_cluster(self):
        """Start all 5 nodes in the cluster."""
        print("Starting 5-node Raft cluster...")
        print("=" * 50)
        
        for node_id, config in self.nodes.items():
            try:
                cmd = [sys.executable, 'main.py', node_id]
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                self.nodes[node_id]['process'] = process
                print(f"✓ Started {node_id} on port {config['port']}")
                time.sleep(1)  # Stagger startup
                
            except Exception as e:
                print(f"✗ Failed to start {node_id}: {e}")
                
        print(f"\n🚀 5-node cluster started successfully!")
        print("Cluster Configuration:")
        for node_id, config in self.nodes.items():
            print(f"  {node_id}: http://127.0.0.1:{config['port']}")
            
        print(f"\nMajority Quorum: 3 nodes (can tolerate 2 failures)")
        print(f"Brain Split Protection: Requires 3+ nodes for leader election")
        
    async def get_cluster_status(self) -> Dict:
        """Get status of all nodes in the cluster."""
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        status = {}
        for node_id, config in self.nodes.items():
            try:
                async with self.session.get(
                    f"http://127.0.0.1:{config['port']}/status",
                    timeout=aiohttp.ClientTimeout(total=2)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status[node_id] = {
                            'healthy': True,
                            'state': data.get('raft', {}).get('state', 'unknown'),
                            'term': data.get('raft', {}).get('term', 0),
                            'is_leader': data.get('raft', {}).get('is_leader', False),
                            'port': config['port']
                        }
                    else:
                        status[node_id] = {'healthy': False, 'port': config['port']}
            except:
                status[node_id] = {'healthy': False, 'port': config['port']}
                
        return status
        
    async def analyze_cluster_state(self):
        """Analyze current cluster state and consensus."""
        status = await self.get_cluster_status()
        
        healthy_nodes = [node for node, info in status.items() if info.get('healthy', False)]
        leaders = [node for node, info in status.items() if info.get('is_leader', False)]
        followers = [node for node, info in status.items() 
                    if info.get('healthy', False) and info.get('state') == 'follower']
        candidates = [node for node, info in status.items() 
                     if info.get('healthy', False) and info.get('state') == 'candidate']
        
        terms = [info.get('term', 0) for info in status.values() if info.get('healthy', False)]
        max_term = max(terms) if terms else 0
        
        print(f"\n📊 CLUSTER STATE ANALYSIS")
        print(f"Healthy Nodes: {len(healthy_nodes)}/5 {healthy_nodes}")
        print(f"Leaders: {len(leaders)} {leaders}")
        print(f"Followers: {len(followers)} {followers}")
        print(f"Candidates: {len(candidates)} {candidates}")
        print(f"Current Term: {max_term}")
        print(f"Has Majority: {'✓' if len(healthy_nodes) >= 3 else '✗'}")
        
        return {
            'healthy_count': len(healthy_nodes),
            'leader_count': len(leaders),
            'has_majority': len(healthy_nodes) >= 3,
            'consensus_state': 'healthy' if len(leaders) == 1 and len(healthy_nodes) >= 3 else 'degraded'
        }
        
    async def simulate_network_partition(self, partition_groups: List[List[str]]):
        """Simulate network partition by stopping specific nodes."""
        print(f"\n🔪 SIMULATING NETWORK PARTITION")
        print(f"Partition Groups: {partition_groups}")
        
        # Stop nodes not in the largest partition
        largest_partition = max(partition_groups, key=len)
        nodes_to_stop = []
        
        for group in partition_groups:
            if group != largest_partition:
                nodes_to_stop.extend(group)
                
        for node_id in nodes_to_stop:
            if self.nodes[node_id]['process']:
                try:
                    self.nodes[node_id]['process'].terminate()
                    self.nodes[node_id]['process'].wait(timeout=3)
                    print(f"✓ Partitioned {node_id} (simulated network isolation)")
                except:
                    self.nodes[node_id]['process'].kill()
                    
        await asyncio.sleep(3)  # Allow time for partition effects
        
        print(f"Remaining active partition: {largest_partition}")
        return await self.analyze_cluster_state()
        
    async def test_brain_split_scenarios(self):
        """Test various brain split scenarios."""
        print(f"\n🧠 BRAIN SPLIT SCENARIO TESTING")
        print("=" * 50)
        
        scenarios = [
            {
                'name': '3-2 Split',
                'description': 'Majority vs minority partition',
                'partitions': [['node1', 'node2', 'node3'], ['node4', 'node5']],
                'expected': 'Majority partition (3 nodes) should continue operating'
            },
            {
                'name': '2-2-1 Split',
                'description': 'No majority partition',
                'partitions': [['node1', 'node2'], ['node3', 'node4'], ['node5']],
                'expected': 'No partition can achieve majority, cluster unavailable'
            },
            {
                'name': '4-1 Split',
                'description': 'Super majority vs isolated node',
                'partitions': [['node1', 'node2', 'node3', 'node4'], ['node5']],
                'expected': 'Large partition continues normal operation'
            }
        ]
        
        for i, scenario in enumerate(scenarios, 1):
            print(f"\n[{i}/{len(scenarios)}] Testing: {scenario['name']}")
            print(f"Description: {scenario['description']}")
            print(f"Expected: {scenario['expected']}")
            print("-" * 40)
            
            # Restart cluster for clean test
            await self.restart_cluster()
            await asyncio.sleep(5)  # Allow cluster to stabilize
            
            # Get baseline
            baseline = await self.analyze_cluster_state()
            
            # Simulate partition
            result = await self.simulate_network_partition(scenario['partitions'])
            
            # Analyze results
            print(f"Result: {self.interpret_partition_result(scenario, result)}")
            
            input("\nPress Enter to continue to next scenario...")
            
    def interpret_partition_result(self, scenario, result):
        """Interpret the partition test results."""
        if scenario['name'] == '3-2 Split':
            if result['has_majority'] and result['leader_count'] <= 1:
                return "✓ PASSED - Majority partition operational, minority isolated"
            else:
                return "✗ FAILED - Incorrect behavior under partition"
                
        elif scenario['name'] == '2-2-1 Split':
            if not result['has_majority'] and result['leader_count'] == 0:
                return "✓ PASSED - No majority, cluster correctly unavailable"
            else:
                return "✗ FAILED - Should not have leader without majority"
                
        elif scenario['name'] == '4-1 Split':
            if result['has_majority'] and result['leader_count'] == 1:
                return "✓ PASSED - Super majority continues operation"
            else:
                return "✗ FAILED - Super majority should maintain operation"
                
        return "? UNKNOWN - Unexpected result"
        
    async def restart_cluster(self):
        """Restart the entire cluster."""
        print("\n🔄 Restarting cluster...")
        
        # Stop all nodes
        for node_id, config in self.nodes.items():
            if config['process']:
                try:
                    config['process'].terminate()
                    config['process'].wait(timeout=3)
                except:
                    config['process'].kill()
                config['process'] = None
                
        await asyncio.sleep(2)
        
        # Start all nodes
        self.start_cluster()
        
    async def test_data_consistency_across_partitions(self):
        """Test data consistency behavior during partitions."""
        print(f"\n📝 DATA CONSISTENCY TESTING")
        print("=" * 40)
        
        # Ensure cluster is healthy
        await self.restart_cluster()
        await asyncio.sleep(5)
        
        # Write test data
        test_key = f"partition_test_{int(time.time())}"
        test_value = "before_partition"
        
        print(f"Writing test data: {test_key} = {test_value}")
        
        try:
            async with self.session.post(
                f"http://127.0.0.1:3000/cache/{test_key}",
                json={'value': test_value},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status == 200:
                    print("✓ Data written successfully")
                else:
                    print("✗ Failed to write initial data")
                    return
        except Exception as e:
            print(f"✗ Error writing data: {e}")
            return
            
        await asyncio.sleep(2)  # Allow replication
        
        # Create 3-2 partition
        print("\nCreating 3-2 network partition...")
        await self.simulate_network_partition([['node1', 'node2', 'node3'], ['node4', 'node5']])
        
        # Try to write to majority partition
        try:
            async with self.session.post(
                f"http://127.0.0.1:3000/cache/partition_write",
                json={'value': 'written_during_partition'},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status == 200:
                    print("✓ Write succeeded in majority partition")
                else:
                    print("✗ Write failed in majority partition")
        except Exception as e:
            print(f"? Write error in majority partition: {e}")
            
        # Try to read from majority partition
        try:
            async with self.session.get(
                f"http://127.0.0.1:3000/cache/{test_key}",
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print(f"✓ Read from majority: {data.get('value')}")
                else:
                    print("✗ Read failed from majority partition")
        except Exception as e:
            print(f"? Read error from majority partition: {e}")
            
    def cleanup(self):
        """Clean shutdown of all processes."""
        print("\n🛑 Shutting down cluster...")
        
        for node_id, config in self.nodes.items():
            if config['process']:
                try:
                    config['process'].terminate()
                    config['process'].wait(timeout=3)
                    print(f"✓ Stopped {node_id}")
                except:
                    config['process'].kill()
                    print(f"⚠ Force killed {node_id}")
                    
        if self.session:
            asyncio.create_task(self.session.close())
            
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.cleanup()
        sys.exit(0)


async def main():
    """Main demonstration of 5-node cluster and brain split scenarios."""
    cluster = FiveNodeCluster()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, cluster.signal_handler)
    signal.signal(signal.SIGTERM, cluster.signal_handler)
    
    try:
        print("5-NODE RAFT CLUSTER BRAIN SPLIT DEMONSTRATION")
        print("=" * 60)
        
        # Start the cluster
        cluster.start_cluster()
        
        print("\nWaiting for cluster to stabilize...")
        await asyncio.sleep(10)
        
        # Initial analysis
        await cluster.analyze_cluster_state()
        
        print("\nWhat would you like to test?")
        print("1. Brain split scenarios")
        print("2. Data consistency during partitions") 
        print("3. Interactive cluster analysis")
        print("4. Exit")
        
        while True:
            choice = input("\nEnter choice (1-4): ").strip()
            
            if choice == '1':
                await cluster.test_brain_split_scenarios()
            elif choice == '2':
                await cluster.test_data_consistency_across_partitions()
            elif choice == '3':
                await cluster.analyze_cluster_state()
            elif choice == '4':
                break
            else:
                print("Invalid choice, please enter 1-4")
                
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        cluster.cleanup()


if __name__ == "__main__":
    asyncio.run(main())