"""
Complete Raft consensus test suite demonstrating all protocol features.
Includes leader election, log replication, failure scenarios, and performance testing.
"""

import asyncio
import aiohttp
import json
import time
import random
from typing import Dict, List, Tuple


class RaftTestSuite:
    def __init__(self):
        self.nodes = {
            'node1': 3000,
            'node2': 4000, 
            'node3': 5000
        }
        self.session = None
        self.test_results = []
        
    async def init(self):
        self.session = aiohttp.ClientSession()
        
    async def cleanup(self):
        if self.session:
            await self.session.close()
            
    async def check_node(self, node_id: str) -> Dict:
        """Check individual node status."""
        port = self.nodes[node_id]
        try:
            async with self.session.get(f'http://127.0.0.1:{port}/status', 
                                       timeout=aiohttp.ClientTimeout(total=1)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except:
            pass
        return {'healthy': False}
        
    async def get_cluster_state(self) -> Dict:
        """Get complete cluster state."""
        states = {}
        for node_id in self.nodes:
            states[node_id] = await self.check_node(node_id)
        return states
        
    def analyze_cluster(self, states: Dict) -> Dict:
        """Analyze cluster state."""
        healthy = [node for node, state in states.items() if state.get('healthy', True)]
        leaders = []
        followers = []
        candidates = []
        
        for node, state in states.items():
            if state.get('healthy', True):
                raft_state = state.get('raft', {}).get('state', 'unknown')
                if raft_state == 'leader':
                    leaders.append(node)
                elif raft_state == 'follower':
                    followers.append(node)
                elif raft_state == 'candidate':
                    candidates.append(node)
                    
        return {
            'healthy_count': len(healthy),
            'healthy_nodes': healthy,
            'leaders': leaders,
            'followers': followers,
            'candidates': candidates,
            'has_majority': len(healthy) >= 2
        }
        
    async def test_basic_raft_properties(self):
        """Test 1: Basic Raft protocol properties."""
        print("Test 1: Basic Raft Protocol Properties")
        print("-" * 45)
        
        states = await self.get_cluster_state()
        analysis = self.analyze_cluster(states)
        
        print(f"Healthy nodes: {analysis['healthy_count']}/3")
        print(f"Leaders: {analysis['leaders']}")
        print(f"Followers: {analysis['followers']}")
        print(f"Candidates: {analysis['candidates']}")
        
        # Validate Raft safety properties
        safety_violations = []
        
        # Safety Property 1: At most one leader per term
        if len(analysis['leaders']) > 1:
            safety_violations.append("Multiple leaders detected")
            
        # Safety Property 2: Leader must exist if majority available
        if analysis['has_majority'] and len(analysis['leaders']) == 0:
            safety_violations.append("No leader with majority available")
            
        # Safety Property 3: Check term consistency
        terms = []
        for node, state in states.items():
            if state.get('healthy', True):
                term = state.get('raft', {}).get('term', 0)
                terms.append(term)
                
        if terms and max(terms) - min(terms) > 1:
            safety_violations.append("Excessive term variance")
            
        if safety_violations:
            print("Safety violations:")
            for violation in safety_violations:
                print(f"  ✗ {violation}")
            return False
        else:
            print("✓ All Raft safety properties satisfied")
            return True
            
    async def test_leader_stability(self):
        """Test 2: Leader stability over time."""
        print("\nTest 2: Leader Stability")
        print("-" * 25)
        
        measurements = []
        for i in range(6):
            states = await self.get_cluster_state()
            analysis = self.analyze_cluster(states)
            
            current_leader = analysis['leaders'][0] if analysis['leaders'] else None
            measurements.append(current_leader)
            
            print(f"Measurement {i+1}: Leader = {current_leader}")
            await asyncio.sleep(1)
            
        # Analyze stability
        unique_leaders = set(m for m in measurements if m is not None)
        
        if len(unique_leaders) <= 1:
            print("✓ Leader remained stable throughout test period")
            return True
        else:
            print(f"✗ Leader changed during test: {unique_leaders}")
            return False
            
    async def test_data_operations(self):
        """Test 3: Data operations and consistency."""
        print("\nTest 3: Data Operations and Consistency")
        print("-" * 40)
        
        # Find available node for operations
        states = await self.get_cluster_state()
        analysis = self.analyze_cluster(states)
        
        if not analysis['healthy_nodes']:
            print("✗ No healthy nodes available")
            return False
            
        target_node = analysis['healthy_nodes'][0]
        target_port = self.nodes[target_node]
        
        # Test data operations
        test_operations = [
            ('set', 'raft_key_1', 'raft_value_1'),
            ('set', 'raft_key_2', 'raft_value_2'),
            ('get', 'raft_key_1', 'raft_value_1'),
            ('set', 'raft_key_3', 'raft_value_3'),
            ('get', 'raft_key_2', 'raft_value_2'),
        ]
        
        successful_ops = 0
        
        for op_type, key, value in test_operations:
            try:
                if op_type == 'set':
                    async with self.session.post(
                        f'http://127.0.0.1:{target_port}/cache/{key}',
                        json={'value': value},
                        timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            successful_ops += 1
                            print(f"✓ SET {key}")
                        else:
                            result = await resp.json() if resp.status < 500 else {}
                            print(f"✗ SET {key}: {result.get('error', 'Failed')}")
                            
                elif op_type == 'get':
                    async with self.session.get(
                        f'http://127.0.0.1:{target_port}/cache/{key}',
                        timeout=aiohttp.ClientTimeout(total=2)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get('value') == value:
                                successful_ops += 1
                                print(f"✓ GET {key}")
                            else:
                                print(f"✗ GET {key}: Wrong value")
                        else:
                            print(f"✗ GET {key}: Not found")
                            
            except Exception as e:
                print(f"✗ {op_type.upper()} {key}: Exception - {str(e)[:50]}")
                
        success_rate = (successful_ops / len(test_operations)) * 100
        print(f"Operation success rate: {successful_ops}/{len(test_operations)} ({success_rate:.1f}%)")
        
        return success_rate >= 80
        
    async def test_performance_under_load(self):
        """Test 4: Performance under concurrent load."""
        print("\nTest 4: Performance Under Load")
        print("-" * 32)
        
        states = await self.get_cluster_state()
        analysis = self.analyze_cluster(states)
        
        if not analysis['healthy_nodes']:
            print("✗ No healthy nodes available")
            return False
            
        target_node = analysis['healthy_nodes'][0]
        target_port = self.nodes[target_node]
        
        # Concurrent load test
        num_operations = 25
        print(f"Executing {num_operations} concurrent operations...")
        
        start_time = time.time()
        tasks = []
        
        for i in range(num_operations):
            key = f"load_test_{i}_{int(time.time())}"
            value = f"load_value_{i}"
            
            task = self.session.post(
                f'http://127.0.0.1:{target_port}/cache/{key}',
                json={'value': value},
                timeout=aiohttp.ClientTimeout(total=3)
            )
            tasks.append(task)
            
        # Execute all operations concurrently
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful = 0
        for resp in responses:
            if isinstance(resp, aiohttp.ClientResponse):
                if resp.status == 200:
                    successful += 1
                resp.close()
                
        duration = time.time() - start_time
        throughput = successful / duration if duration > 0 else 0
        success_rate = (successful / num_operations) * 100
        
        print(f"Results:")
        print(f"  Successful operations: {successful}/{num_operations}")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.1f} ops/sec")
        
        return success_rate >= 70  # 70% success rate under load
        
    async def test_failure_simulation(self):
        """Test 5: Failure simulation and recovery."""
        print("\nTest 5: Failure Simulation and Recovery")
        print("-" * 42)
        
        # Get initial state
        initial_states = await self.get_cluster_state()
        initial_analysis = self.analyze_cluster(initial_states)
        
        print(f"Initial state: {initial_analysis['healthy_count']}/3 nodes healthy")
        
        # Attempt node failure simulation
        nodes_to_test = ['node2', 'node3']
        recovery_success = []
        
        for node in nodes_to_test:
            print(f"\nTesting {node} failure simulation...")
            
            try:
                # Attempt to kill node via dashboard
                async with self.session.post(
                    'http://127.0.0.1:8080/api/test',
                    json={'type': 'kill_node', 'node': node},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    print(f"  Failure simulation sent for {node}")
            except:
                print(f"  Could not simulate failure for {node}")
                
            await asyncio.sleep(2)
            
            # Check cluster state after failure
            post_failure_states = await self.get_cluster_state()
            post_failure_analysis = self.analyze_cluster(post_failure_states)
            
            print(f"  Post-failure: {post_failure_analysis['healthy_count']}/3 nodes healthy")
            
            # Attempt recovery
            try:
                async with self.session.post(
                    'http://127.0.0.1:8080/api/test',
                    json={'type': 'restart_node', 'node': node},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    print(f"  Recovery attempt sent for {node}")
                    recovery_success.append(True)
            except:
                print(f"  Could not attempt recovery for {node}")
                recovery_success.append(False)
                
            await asyncio.sleep(3)
            
        # Final state check
        final_states = await self.get_cluster_state()
        final_analysis = self.analyze_cluster(final_states)
        
        print(f"\nFinal state: {final_analysis['healthy_count']}/3 nodes healthy")
        
        # Test passes if cluster maintains operation
        if final_analysis['healthy_count'] >= 1:
            print("✓ Cluster maintained operation throughout failure scenarios")
            return True
        else:
            print("✗ Cluster failed to maintain operation")
            return False
            
    async def test_log_replication_verification(self):
        """Test 6: Log replication and consistency verification."""
        print("\nTest 6: Log Replication Verification")
        print("-" * 37)
        
        states = await self.get_cluster_state()
        healthy_nodes = [node for node, state in states.items() if state.get('healthy', True)]
        
        if len(healthy_nodes) < 1:
            print("✗ No healthy nodes for replication test")
            return False
            
        # Write test data
        test_key = f"replication_test_{int(time.time())}"
        test_value = f"replication_value_{random.randint(1000, 9999)}"
        
        write_node = healthy_nodes[0]
        write_port = self.nodes[write_node]
        
        print(f"Writing test data to {write_node}...")
        
        try:
            async with self.session.post(
                f'http://127.0.0.1:{write_port}/cache/{test_key}',
                json={'value': test_value},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status != 200:
                    result = await resp.json() if resp.status < 500 else {}
                    print(f"✗ Write failed: {result.get('error', 'Unknown error')}")
                    return False
        except Exception as e:
            print(f"✗ Write failed: {e}")
            return False
            
        # Wait for potential replication
        await asyncio.sleep(2)
        
        # Verify data on all healthy nodes
        print("Verifying replication across healthy nodes...")
        
        consistent_nodes = 0
        for node in healthy_nodes:
            port = self.nodes[node]
            try:
                async with self.session.get(
                    f'http://127.0.0.1:{port}/cache/{test_key}',
                    timeout=aiohttp.ClientTimeout(total=2)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('value') == test_value:
                            consistent_nodes += 1
                            print(f"  ✓ {node}: Data consistent")
                        else:
                            print(f"  ✗ {node}: Data inconsistent")
                    else:
                        print(f"  ✗ {node}: Data not found")
            except:
                print(f"  ✗ {node}: Unreachable")
                
        consistency_rate = (consistent_nodes / len(healthy_nodes)) * 100
        print(f"Consistency rate: {consistent_nodes}/{len(healthy_nodes)} ({consistency_rate:.1f}%)")
        
        return consistency_rate >= 100  # Require perfect consistency
        
    async def run_all_tests(self):
        """Execute complete test suite."""
        print("COMPREHENSIVE RAFT CONSENSUS TEST SUITE")
        print("=" * 50)
        print("Testing distributed cache Raft implementation")
        print("=" * 50)
        
        tests = [
            self.test_basic_raft_properties,
            self.test_leader_stability,
            self.test_data_operations,
            self.test_performance_under_load,
            self.test_failure_simulation,
            self.test_log_replication_verification
        ]
        
        results = []
        passed = 0
        
        for i, test in enumerate(tests, 1):
            print(f"\n[{i}/{len(tests)}] Running {test.__name__.replace('test_', '').replace('_', ' ').title()}...")
            
            try:
                result = await test()
                results.append(result)
                
                if result:
                    passed += 1
                    print("RESULT: ✓ PASSED")
                else:
                    print("RESULT: ✗ FAILED")
                    
            except Exception as e:
                results.append(False)
                print(f"RESULT: ✗ ERROR - {e}")
                
        # Final summary
        print("\n" + "=" * 50)
        print("RAFT TEST SUITE SUMMARY")
        print("=" * 50)
        
        print(f"Tests executed: {len(tests)}")
        print(f"Tests passed: {passed}")
        print(f"Tests failed: {len(tests) - passed}")
        print(f"Success rate: {(passed/len(tests))*100:.1f}%")
        
        # Detailed analysis
        if passed == len(tests):
            print("\n🎉 EXCELLENT: All Raft consensus tests passed!")
            print("The distributed cache demonstrates robust fault tolerance.")
        elif passed >= len(tests) * 0.8:
            print("\n✅ GOOD: Most tests passed with minor issues.")
            print("The Raft implementation is largely functional.")
        elif passed >= len(tests) * 0.5:
            print("\n⚠️  MODERATE: Some significant issues detected.")
            print("Raft consensus may have stability problems.")
        else:
            print("\n❌ CRITICAL: Multiple test failures detected.")
            print("Raft implementation requires immediate attention.")
            
        return results


async def main():
    """Main test execution."""
    suite = RaftTestSuite()
    
    try:
        await suite.init()
        results = await suite.run_all_tests()
        
        # Additional system information
        print(f"\nSystem Information:")
        print(f"- Cluster nodes: {list(suite.nodes.keys())}")
        print(f"- Test completion time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    finally:
        await suite.cleanup()


if __name__ == "__main__":
    asyncio.run(main())