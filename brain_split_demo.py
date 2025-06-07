"""
Brain Split Prevention Demonstration
Shows how Raft consensus prevents split-brain scenarios in distributed systems.
"""

import asyncio
import aiohttp
import json
import time
import subprocess
import signal
import sys


class BrainSplitDemo:
    def __init__(self):
        self.nodes = {
            'node1': 3000,
            'node2': 4000,
            'node3': 5000
        }
        self.session = None
        
    async def initialize(self):
        self.session = aiohttp.ClientSession()
        
    async def cleanup(self):
        if self.session:
            await self.session.close()
            
    async def get_cluster_state(self):
        """Get current state of all nodes."""
        state = {}
        for node_id, port in self.nodes.items():
            try:
                async with self.session.get(f'http://127.0.0.1:{port}/status',
                                           timeout=aiohttp.ClientTimeout(total=1)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        state[node_id] = {
                            'healthy': True,
                            'state': data.get('raft', {}).get('state', 'unknown'),
                            'term': data.get('raft', {}).get('term', 0),
                            'is_leader': data.get('raft', {}).get('is_leader', False),
                            'port': port
                        }
                    else:
                        state[node_id] = {'healthy': False, 'port': port}
            except:
                state[node_id] = {'healthy': False, 'port': port}
        return state
        
    def analyze_state(self, state):
        """Analyze cluster state for consensus properties."""
        healthy = [n for n, s in state.items() if s.get('healthy', False)]
        leaders = [n for n, s in state.items() if s.get('is_leader', False)]
        followers = [n for n, s in state.items() if s.get('state') == 'follower']
        candidates = [n for n, s in state.items() if s.get('state') == 'candidate']
        
        has_majority = len(healthy) >= 2  # For 3 nodes, need 2
        
        return {
            'healthy_count': len(healthy),
            'healthy_nodes': healthy,
            'leader_count': len(leaders),
            'leaders': leaders,
            'followers': followers,
            'candidates': candidates,
            'has_majority': has_majority,
            'can_make_progress': has_majority and len(leaders) <= 1
        }
        
    def print_state_analysis(self, analysis):
        """Print formatted cluster state analysis."""
        print(f"Healthy Nodes: {analysis['healthy_count']}/3 {analysis['healthy_nodes']}")
        print(f"Leaders: {analysis['leader_count']} {analysis['leaders']}")
        print(f"Followers: {len(analysis['followers'])} {analysis['followers']}")
        print(f"Candidates: {len(analysis['candidates'])} {analysis['candidates']}")
        print(f"Has Majority: {'Yes' if analysis['has_majority'] else 'No'}")
        print(f"Can Make Progress: {'Yes' if analysis['can_make_progress'] else 'No'}")
        
        if analysis['leader_count'] > 1:
            print("⚠ SPLIT-BRAIN DETECTED!")
        elif analysis['leader_count'] == 1 and analysis['has_majority']:
            print("✓ Healthy consensus state")
        elif not analysis['has_majority']:
            print("⚠ No majority - cluster unavailable")
        else:
            print("? Transitional state")
            
    async def test_write_operation(self, node_port, key, value):
        """Test if a node can accept write operations."""
        try:
            async with self.session.post(f'http://127.0.0.1:{node_port}/cache/{key}',
                                        json={'value': value},
                                        timeout=aiohttp.ClientTimeout(total=2)) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return True, result.get('message', 'Success')
                else:
                    result = await resp.json()
                    return False, result.get('error', f'HTTP {resp.status}')
        except Exception as e:
            return False, str(e)
            
    async def demonstrate_brain_split_prevention(self):
        """Demonstrate how Raft prevents brain split scenarios."""
        print("BRAIN SPLIT PREVENTION DEMONSTRATION")
        print("=" * 50)
        
        # Step 1: Show normal operation
        print("\n1. NORMAL CLUSTER OPERATION")
        print("-" * 30)
        state = await self.get_cluster_state()
        analysis = self.analyze_state(state)
        self.print_state_analysis(analysis)
        
        if not analysis['can_make_progress']:
            print("\nCluster not ready - waiting for leader election...")
            await asyncio.sleep(5)
            state = await self.get_cluster_state()
            analysis = self.analyze_state(state)
            self.print_state_analysis(analysis)
            
        # Test write operation
        if analysis['leaders']:
            leader_port = self.nodes[analysis['leaders'][0]]
            success, message = await self.test_write_operation(
                leader_port, 'demo_key', 'normal_operation'
            )
            print(f"Write test: {'Success' if success else 'Failed'} - {message}")
            
        print("\n2. SIMULATING NETWORK PARTITION")
        print("-" * 35)
        print("We'll simulate killing one node to create a 2-1 split")
        print("Expected: 2-node majority continues, 1 isolated node stops")
        
        # Simulate partition by killing node via dashboard
        try:
            async with self.session.post('http://127.0.0.1:8080/api/test',
                                        json={'type': 'kill_node', 'node': 'node3'},
                                        timeout=aiohttp.ClientTimeout(total=5)) as resp:
                print("\nSimulated node3 failure (network partition)")
        except:
            print("\nCould not simulate via dashboard - node may already be isolated")
            
        await asyncio.sleep(3)
        
        # Analyze post-partition state
        print("\nPost-partition cluster state:")
        state = await self.get_cluster_state()
        analysis = self.analyze_state(state)
        self.print_state_analysis(analysis)
        
        # Test write operations on remaining nodes
        print("\nTesting write operations on remaining nodes:")
        for node_id, port in self.nodes.items():
            if analysis['healthy_nodes'] and node_id in analysis['healthy_nodes']:
                success, message = await self.test_write_operation(
                    port, f'partition_test_{node_id}', f'value_from_{node_id}'
                )
                print(f"  {node_id}: {'Success' if success else 'Failed'} - {message}")
                
        print("\n3. BRAIN SPLIT PREVENTION ANALYSIS")
        print("-" * 40)
        
        if analysis['leader_count'] <= 1:
            print("✓ NO SPLIT-BRAIN: Only one or zero leaders detected")
        else:
            print("✗ SPLIT-BRAIN DETECTED: Multiple leaders exist!")
            
        if analysis['has_majority'] and analysis['can_make_progress']:
            print("✓ MAJORITY OPERATION: Cluster continues with majority nodes")
        else:
            print("✗ CLUSTER UNAVAILABLE: No majority consensus")
            
        print("\n4. RECOVERY SIMULATION")
        print("-" * 25)
        print("Attempting to restart the failed node...")
        
        try:
            async with self.session.post('http://127.0.0.1:8080/api/test',
                                        json={'type': 'restart_node', 'node': 'node3'},
                                        timeout=aiohttp.ClientTimeout(total=8)) as resp:
                print("Recovery command sent")
        except:
            print("Could not send recovery command")
            
        await asyncio.sleep(4)
        
        print("\nPost-recovery cluster state:")
        final_state = await self.get_cluster_state()
        final_analysis = self.analyze_state(final_state)
        self.print_state_analysis(final_analysis)
        
        print("\n5. KEY INSIGHTS")
        print("-" * 15)
        print("• Raft prevents split-brain by requiring majority votes")
        print("• Only partitions with majority can elect leaders")
        print("• Minority partitions become read-only")
        print("• System availability = majority nodes operational")
        print("• Recovery is automatic when network heals")
        
    async def demonstrate_5_node_benefits(self):
        """Explain benefits of 5-node vs 3-node clusters."""
        print("\n" + "=" * 60)
        print("5-NODE CLUSTER ADVANTAGES")
        print("=" * 60)
        
        scenarios = [
            {
                'name': '3-Node Cluster',
                'total': 3,
                'majority': 2,
                'fault_tolerance': 1,
                'scenarios': [
                    '2-1 split: Majority continues',
                    '1-1-1 split: Impossible (only 3 nodes)',
                    'Single failure: Still operational'
                ]
            },
            {
                'name': '5-Node Cluster', 
                'total': 5,
                'majority': 3,
                'fault_tolerance': 2,
                'scenarios': [
                    '3-2 split: Majority continues',
                    '4-1 split: Super majority continues',
                    '2-2-1 split: No majority, cluster stops',
                    'Two failures: Still operational'
                ]
            }
        ]
        
        for scenario in scenarios:
            print(f"\n{scenario['name']}:")
            print(f"  Total nodes: {scenario['total']}")
            print(f"  Majority required: {scenario['majority']}")
            print(f"  Fault tolerance: {scenario['fault_tolerance']} node(s)")
            print(f"  Availability: {(scenario['majority']/scenario['total'])*100:.1f}% of nodes needed")
            print("  Partition scenarios:")
            for s in scenario['scenarios']:
                print(f"    • {s}")
                
        print(f"\nRecommendation:")
        print(f"• Use 3 nodes for cost efficiency and simple setups")
        print(f"• Use 5+ nodes for high availability and fault tolerance")
        print(f"• Always use odd numbers to prevent tie votes")


async def main():
    """Main demonstration entry point."""
    demo = BrainSplitDemo()
    
    try:
        await demo.initialize()
        await demo.demonstrate_brain_split_prevention()
        await demo.demonstrate_5_node_benefits()
        
    except KeyboardInterrupt:
        print("\nDemo interrupted")
    finally:
        await demo.cleanup()


if __name__ == "__main__":
    asyncio.run(main())