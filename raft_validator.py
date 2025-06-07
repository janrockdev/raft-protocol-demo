"""
Raft consensus protocol validator - focused tests for distributed cache cluster.
"""

import asyncio
import aiohttp
import json
import time
import subprocess
import os
import signal


class RaftValidator:
    def __init__(self):
        self.nodes = {
            'node1': 3000,
            'node2': 4000, 
            'node3': 5000
        }
        self.session = None
        
    async def init_session(self):
        self.session = aiohttp.ClientSession()
        
    async def close_session(self):
        if self.session:
            await self.session.close()
            
    async def get_node_status(self, port):
        """Get status from a node."""
        try:
            async with self.session.get(f'http://127.0.0.1:{port}/status', 
                                       timeout=aiohttp.ClientTimeout(total=1)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except:
            pass
        return None
        
    async def get_cluster_state(self):
        """Get complete cluster state."""
        states = {}
        for name, port in self.nodes.items():
            states[name] = await self.get_node_status(port)
        return states
        
    async def find_leader(self):
        """Find current leader."""
        states = await self.get_cluster_state()
        for name, state in states.items():
            if state and state.get('raft', {}).get('is_leader', False):
                return name, self.nodes[name]
        return None, None
        
    async def test_basic_raft_state(self):
        """Test 1: Basic Raft state validation."""
        print("Test 1: Basic Raft State Validation")
        
        states = await self.get_cluster_state()
        healthy_nodes = [name for name, state in states.items() if state is not None]
        
        if len(healthy_nodes) < 3:
            print(f"  FAILED: Only {len(healthy_nodes)}/3 nodes healthy")
            return False
            
        # Check for exactly one leader
        leaders = []
        followers = []
        
        for name, state in states.items():
            if state:
                raft_state = state.get('raft', {}).get('state')
                if raft_state == 'leader':
                    leaders.append(name)
                elif raft_state == 'follower':
                    followers.append(name)
                    
        if len(leaders) != 1:
            print(f"  FAILED: Expected 1 leader, found {len(leaders)}: {leaders}")
            return False
            
        if len(followers) != 2:
            print(f"  FAILED: Expected 2 followers, found {len(followers)}: {followers}")
            return False
            
        print(f"  PASSED: Leader={leaders[0]}, Followers={followers}")
        return True
        
    async def test_data_operations(self):
        """Test 2: Basic data operations."""
        print("Test 2: Data Operations")
        
        leader_name, leader_port = await self.find_leader()
        if not leader_name:
            print("  FAILED: No leader found")
            return False
            
        # Test SET operation
        test_key = f"raft_test_{int(time.time())}"
        test_value = "raft_test_value"
        
        try:
            async with self.session.post(f'http://127.0.0.1:{leader_port}/cache/{test_key}',
                                        json={'value': test_value},
                                        timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status != 200:
                    print(f"  FAILED: SET operation failed with status {resp.status}")
                    return False
        except Exception as e:
            print(f"  FAILED: SET operation error: {e}")
            return False
            
        # Wait for replication
        await asyncio.sleep(1)
        
        # Test GET operation from all nodes
        success_count = 0
        for name, port in self.nodes.items():
            try:
                async with self.session.get(f'http://127.0.0.1:{port}/cache/{test_key}',
                                           timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('value') == test_value:
                            success_count += 1
            except:
                pass
                
        if success_count < 2:  # Majority
            print(f"  FAILED: Data only replicated to {success_count}/3 nodes")
            return False
            
        print(f"  PASSED: Data replicated to {success_count}/3 nodes")
        return True
        
    async def test_term_consistency(self):
        """Test 3: Term consistency across nodes."""
        print("Test 3: Term Consistency")
        
        states = await self.get_cluster_state()
        terms = []
        
        for name, state in states.items():
            if state:
                term = state.get('raft', {}).get('term', 0)
                terms.append(term)
                
        if not terms:
            print("  FAILED: No terms found")
            return False
            
        # All nodes should have the same term or very close
        max_term = max(terms)
        min_term = min(terms)
        
        if max_term - min_term > 1:
            print(f"  FAILED: Term variance too high: {min_term}-{max_term}")
            return False
            
        print(f"  PASSED: Terms consistent: {min_term}-{max_term}")
        return True
        
    async def test_performance_baseline(self):
        """Test 4: Performance baseline."""
        print("Test 4: Performance Baseline")
        
        leader_name, leader_port = await self.find_leader()
        if not leader_name:
            print("  FAILED: No leader found")
            return False
            
        # Perform 10 operations and measure
        start_time = time.time()
        successful = 0
        
        for i in range(10):
            try:
                async with self.session.post(f'http://127.0.0.1:{leader_port}/cache/perf_{i}',
                                            json={'value': f'perf_value_{i}'},
                                            timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        successful += 1
            except:
                pass
                
        duration = time.time() - start_time
        ops_per_sec = successful / duration if duration > 0 else 0
        
        if successful < 8:  # 80% success rate
            print(f"  FAILED: Low success rate: {successful}/10")
            return False
            
        print(f"  PASSED: {successful}/10 ops, {ops_per_sec:.1f} ops/sec")
        return True
        
    async def test_leader_heartbeat(self):
        """Test 5: Leader heartbeat mechanism."""
        print("Test 5: Leader Heartbeat")
        
        leader_name, leader_port = await self.find_leader()
        if not leader_name:
            print("  FAILED: No leader found")
            return False
            
        # Get initial states
        initial_states = await self.get_cluster_state()
        initial_terms = {name: state.get('raft', {}).get('term', 0) 
                        for name, state in initial_states.items() if state}
        
        # Wait and check again
        await asyncio.sleep(3)
        
        new_states = await self.get_cluster_state()
        new_terms = {name: state.get('raft', {}).get('term', 0) 
                    for name, state in new_states.items() if state}
        
        # Leader should still be the same
        new_leader_name, _ = await self.find_leader()
        
        if new_leader_name != leader_name:
            print(f"  FAILED: Leader changed unexpectedly: {leader_name} -> {new_leader_name}")
            return False
            
        # Terms should be stable
        for name in initial_terms:
            if name in new_terms and new_terms[name] > initial_terms[name] + 1:
                print(f"  FAILED: Unexpected term increase on {name}")
                return False
                
        print(f"  PASSED: Leader {leader_name} stable, terms consistent")
        return True
        
    async def run_all_tests(self):
        """Run all validation tests."""
        print("Raft Consensus Protocol Validator")
        print("=" * 40)
        
        await self.init_session()
        
        tests = [
            self.test_basic_raft_state,
            self.test_data_operations, 
            self.test_term_consistency,
            self.test_performance_baseline,
            self.test_leader_heartbeat
        ]
        
        passed = 0
        total = len(tests)
        
        for test in tests:
            try:
                result = await test()
                if result:
                    passed += 1
                print()
            except Exception as e:
                print(f"  ERROR: {e}")
                print()
                
        await self.close_session()
        
        print("=" * 40)
        print(f"Results: {passed}/{total} tests passed")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        
        if passed == total:
            print("All Raft consensus tests PASSED")
        else:
            print("Some tests FAILED - check cluster configuration")


async def main():
    validator = RaftValidator()
    await validator.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())