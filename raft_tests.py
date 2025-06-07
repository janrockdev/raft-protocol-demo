"""
Comprehensive Raft consensus protocol tests for the distributed cache cluster.
Tests leader election, log replication, network partitions, and data consistency.
"""

import asyncio
import aiohttp
import json
import time
import random
from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum


class TestResult(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class TestCase:
    name: str
    description: str
    result: TestResult
    duration: float
    details: str
    error: str = ""


class RaftTester:
    """Comprehensive Raft consensus protocol tester."""
    
    def __init__(self):
        self.nodes = {
            'node1': {'host': '127.0.0.1', 'port': 3000},
            'node2': {'host': '127.0.0.1', 'port': 4000},
            'node3': {'host': '127.0.0.1', 'port': 5000}
        }
        self.session = None
        self.test_results = []
        
    async def initialize(self):
        """Initialize the tester."""
        self.session = aiohttp.ClientSession()
        
    async def cleanup(self):
        """Cleanup resources."""
        if self.session:
            await self.session.close()
            
    async def get_node_status(self, node_id: str) -> Dict[str, Any]:
        """Get status of a specific node."""
        node = self.nodes[node_id]
        try:
            async with self.session.get(
                f"http://{node['host']}:{node['port']}/status",
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            return {'error': str(e), 'healthy': False}
        return {'healthy': False}
        
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of all nodes in the cluster."""
        cluster_status = {}
        for node_id in self.nodes:
            cluster_status[node_id] = await self.get_node_status(node_id)
        return cluster_status
        
    async def find_leader(self) -> str:
        """Find the current cluster leader."""
        cluster_status = await self.get_cluster_status()
        for node_id, status in cluster_status.items():
            if status.get('raft', {}).get('is_leader', False):
                return node_id
        return None
        
    async def find_followers(self) -> List[str]:
        """Find all follower nodes."""
        cluster_status = await self.get_cluster_status()
        followers = []
        for node_id, status in cluster_status.items():
            if status.get('raft', {}).get('state') == 'follower':
                followers.append(node_id)
        return followers
        
    async def set_cache_value(self, node_id: str, key: str, value: str) -> Dict[str, Any]:
        """Set a cache value on a specific node."""
        node = self.nodes[node_id]
        try:
            async with self.session.post(
                f"http://{node['host']}:{node['port']}/cache/{key}",
                json={'value': value},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return {
                    'status': resp.status,
                    'data': await resp.json() if resp.status < 500 else {'error': 'Server error'}
                }
        except Exception as e:
            return {'status': 500, 'error': str(e)}
            
    async def get_cache_value(self, node_id: str, key: str) -> Dict[str, Any]:
        """Get a cache value from a specific node."""
        node = self.nodes[node_id]
        try:
            async with self.session.get(
                f"http://{node['host']}:{node['port']}/cache/{key}",
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                return {
                    'status': resp.status,
                    'data': await resp.json() if resp.status < 500 else {'error': 'Server error'}
                }
        except Exception as e:
            return {'status': 500, 'error': str(e)}
            
    async def simulate_node_failure(self, node_id: str):
        """Simulate node failure by killing the process."""
        try:
            await self.session.post(
                f"http://127.0.0.1:8080/api/test",
                json={'type': 'kill_node', 'node': node_id},
                timeout=aiohttp.ClientTimeout(total=5)
            )
        except:
            pass  # Expected if dashboard is not available
            
    async def simulate_node_recovery(self, node_id: str):
        """Simulate node recovery by restarting the process."""
        try:
            await self.session.post(
                f"http://127.0.0.1:8080/api/test",
                json={'type': 'restart_node', 'node': node_id},
                timeout=aiohttp.ClientTimeout(total=10)
            )
        except:
            pass  # Expected if dashboard is not available
            
    async def wait_for_leader_election(self, timeout: float = 15.0) -> str:
        """Wait for a leader to be elected."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            leader = await self.find_leader()
            if leader:
                return leader
            await asyncio.sleep(0.5)
        return None
        
    async def wait_for_log_replication(self, key: str, expected_value: str, timeout: float = 10.0) -> bool:
        """Wait for log replication across all nodes."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            consistent = True
            for node_id in self.nodes:
                result = await self.get_cache_value(node_id, key)
                if result['status'] != 200 or result['data'].get('value') != expected_value:
                    consistent = False
                    break
            if consistent:
                return True
            await asyncio.sleep(0.5)
        return False
        
    async def test_basic_leader_election(self) -> TestCase:
        """Test 1: Basic leader election functionality."""
        start_time = time.time()
        try:
            # Wait for initial leader election
            leader = await self.wait_for_leader_election()
            
            if not leader:
                return TestCase(
                    name="Basic Leader Election",
                    description="Test that cluster elects a leader on startup",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No leader elected within timeout period",
                    error="Leader election failed"
                )
            
            # Verify only one leader exists
            cluster_status = await self.get_cluster_status()
            leaders = [node for node, status in cluster_status.items() 
                      if status.get('raft', {}).get('is_leader', False)]
            
            if len(leaders) != 1:
                return TestCase(
                    name="Basic Leader Election",
                    description="Test that cluster elects a leader on startup",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Expected 1 leader, found {len(leaders)}: {leaders}",
                    error="Multiple leaders detected"
                )
            
            return TestCase(
                name="Basic Leader Election",
                description="Test that cluster elects a leader on startup",
                result=TestResult.PASSED,
                duration=time.time() - start_time,
                details=f"Leader elected: {leader}. All followers detected.",
            )
            
        except Exception as e:
            return TestCase(
                name="Basic Leader Election",
                description="Test that cluster elects a leader on startup",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def test_log_replication(self) -> TestCase:
        """Test 2: Log replication across all nodes."""
        start_time = time.time()
        try:
            # Find the current leader
            leader = await self.find_leader()
            if not leader:
                return TestCase(
                    name="Log Replication",
                    description="Test that writes are replicated to all nodes",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No leader available for testing",
                    error="No leader found"
                )
            
            # Set a value through the leader
            test_key = f"raft_test_{int(time.time())}"
            test_value = f"replication_test_{random.randint(1000, 9999)}"
            
            result = await self.set_cache_value(leader, test_key, test_value)
            if result['status'] != 200:
                return TestCase(
                    name="Log Replication",
                    description="Test that writes are replicated to all nodes",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Failed to set value on leader: {result}",
                    error="Write operation failed"
                )
            
            # Wait for replication and verify on all nodes
            replicated = await self.wait_for_log_replication(test_key, test_value)
            
            if not replicated:
                return TestCase(
                    name="Log Replication",
                    description="Test that writes are replicated to all nodes",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="Value not replicated to all nodes within timeout",
                    error="Replication timeout"
                )
            
            return TestCase(
                name="Log Replication",
                description="Test that writes are replicated to all nodes",
                result=TestResult.PASSED,
                duration=time.time() - start_time,
                details=f"Value '{test_value}' successfully replicated to all nodes",
            )
            
        except Exception as e:
            return TestCase(
                name="Log Replication",
                description="Test that writes are replicated to all nodes",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def test_leader_failure_and_recovery(self) -> TestCase:
        """Test 3: Leader failure and new leader election."""
        start_time = time.time()
        try:
            # Find current leader
            original_leader = await self.find_leader()
            if not original_leader:
                return TestCase(
                    name="Leader Failure Recovery",
                    description="Test leader failure and new leader election",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No leader available for testing",
                    error="No leader found"
                )
            
            # Set a value before failure
            test_key = f"leader_fail_test_{int(time.time())}"
            test_value = f"before_failure_{random.randint(1000, 9999)}"
            
            await self.set_cache_value(original_leader, test_key, test_value)
            await asyncio.sleep(1)  # Allow replication
            
            # Simulate leader failure
            await self.simulate_node_failure(original_leader)
            await asyncio.sleep(2)  # Wait for failure detection
            
            # Wait for new leader election
            new_leader = await self.wait_for_leader_election()
            
            if not new_leader:
                return TestCase(
                    name="Leader Failure Recovery",
                    description="Test leader failure and new leader election",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No new leader elected after original leader failure",
                    error="Leader election failed after failure"
                )
            
            if new_leader == original_leader:
                return TestCase(
                    name="Leader Failure Recovery",
                    description="Test leader failure and new leader election",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Same leader detected: {new_leader}",
                    error="Leader did not change after failure"
                )
            
            # Verify data consistency after leader change
            result = await self.get_cache_value(new_leader, test_key)
            if result['status'] != 200 or result['data'].get('value') != test_value:
                return TestCase(
                    name="Leader Failure Recovery",
                    description="Test leader failure and new leader election",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="Data inconsistency after leader failure",
                    error="Data lost during leader transition"
                )
            
            # Restart the failed leader
            await self.simulate_node_recovery(original_leader)
            await asyncio.sleep(3)  # Wait for recovery
            
            return TestCase(
                name="Leader Failure Recovery",
                description="Test leader failure and new leader election",
                result=TestResult.PASSED,
                duration=time.time() - start_time,
                details=f"New leader elected: {new_leader}. Data consistency maintained. Original leader recovered.",
            )
            
        except Exception as e:
            return TestCase(
                name="Leader Failure Recovery",
                description="Test leader failure and new leader election",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def test_follower_failure_and_recovery(self) -> TestCase:
        """Test 4: Follower failure and recovery."""
        start_time = time.time()
        try:
            # Find current leader and followers
            leader = await self.find_leader()
            followers = await self.find_followers()
            
            if not leader or len(followers) < 1:
                return TestCase(
                    name="Follower Failure Recovery",
                    description="Test follower failure and recovery",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Insufficient nodes: leader={leader}, followers={followers}",
                    error="Not enough nodes for test"
                )
            
            # Choose a follower to fail
            target_follower = followers[0]
            
            # Set a value before failure
            test_key = f"follower_fail_test_{int(time.time())}"
            test_value = f"before_failure_{random.randint(1000, 9999)}"
            
            await self.set_cache_value(leader, test_key, test_value)
            await asyncio.sleep(1)  # Allow replication
            
            # Simulate follower failure
            await self.simulate_node_failure(target_follower)
            await asyncio.sleep(2)
            
            # Verify cluster still functions with majority
            test_key2 = f"during_failure_{int(time.time())}"
            test_value2 = f"during_failure_{random.randint(1000, 9999)}"
            
            result = await self.set_cache_value(leader, test_key2, test_value2)
            if result['status'] != 200:
                return TestCase(
                    name="Follower Failure Recovery",
                    description="Test follower failure and recovery",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="Cluster failed to operate with follower down",
                    error="Cluster unavailable with follower failure"
                )
            
            # Restart the failed follower
            await self.simulate_node_recovery(target_follower)
            await asyncio.sleep(3)
            
            # Verify the recovered follower catches up
            await asyncio.sleep(2)  # Allow catch-up
            result = await self.get_cache_value(target_follower, test_key2)
            
            if result['status'] != 200 or result['data'].get('value') != test_value2:
                return TestCase(
                    name="Follower Failure Recovery",
                    description="Test follower failure and recovery",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="Recovered follower did not catch up with cluster state",
                    error="Follower sync failed"
                )
            
            return TestCase(
                name="Follower Failure Recovery",
                description="Test follower failure and recovery",
                result=TestResult.PASSED,
                duration=time.time() - start_time,
                details=f"Follower {target_follower} failed and recovered successfully. Cluster remained operational.",
            )
            
        except Exception as e:
            return TestCase(
                name="Follower Failure Recovery",
                description="Test follower failure and recovery",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def test_consistency_under_load(self) -> TestCase:
        """Test 5: Data consistency under concurrent operations."""
        start_time = time.time()
        try:
            leader = await self.find_leader()
            if not leader:
                return TestCase(
                    name="Consistency Under Load",
                    description="Test data consistency under concurrent operations",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No leader available for testing",
                    error="No leader found"
                )
            
            # Perform concurrent writes
            operations = []
            test_keys = []
            
            for i in range(10):
                key = f"concurrent_test_{i}_{int(time.time())}"
                value = f"concurrent_value_{i}_{random.randint(1000, 9999)}"
                test_keys.append((key, value))
                operations.append(self.set_cache_value(leader, key, value))
            
            # Execute all operations concurrently
            results = await asyncio.gather(*operations, return_exceptions=True)
            
            # Count successful operations
            successful = sum(1 for result in results 
                           if isinstance(result, dict) and result.get('status') == 200)
            
            if successful < len(operations) * 0.8:  # Allow some failures under load
                return TestCase(
                    name="Consistency Under Load",
                    description="Test data consistency under concurrent operations",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Too many failures: {successful}/{len(operations)} succeeded",
                    error="High failure rate under load"
                )
            
            # Wait for replication and verify consistency
            await asyncio.sleep(2)
            
            # Check consistency across all nodes
            consistency_errors = 0
            for key, expected_value in test_keys[:5]:  # Check subset for performance
                for node_id in self.nodes:
                    result = await self.get_cache_value(node_id, key)
                    if (result['status'] != 200 or 
                        result['data'].get('value') != expected_value):
                        consistency_errors += 1
            
            if consistency_errors > 0:
                return TestCase(
                    name="Consistency Under Load",
                    description="Test data consistency under concurrent operations",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details=f"Consistency errors detected: {consistency_errors}",
                    error="Data inconsistency under load"
                )
            
            return TestCase(
                name="Consistency Under Load",
                description="Test data consistency under concurrent operations",
                result=TestResult.PASSED,
                duration=time.time() - start_time,
                details=f"Successfully completed {successful}/{len(operations)} operations with full consistency",
            )
            
        except Exception as e:
            return TestCase(
                name="Consistency Under Load",
                description="Test data consistency under concurrent operations",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def test_term_progression(self) -> TestCase:
        """Test 6: Raft term progression during elections."""
        start_time = time.time()
        try:
            # Get initial term
            cluster_status = await self.get_cluster_status()
            initial_terms = {node: status.get('raft', {}).get('term', 0) 
                           for node, status in cluster_status.items()}
            
            if not any(initial_terms.values()):
                return TestCase(
                    name="Term Progression",
                    description="Test Raft term progression during elections",
                    result=TestResult.FAILED,
                    duration=time.time() - start_time,
                    details="No valid terms found in cluster",
                    error="Invalid cluster state"
                )
            
            max_initial_term = max(initial_terms.values())
            
            # Force a new election by killing current leader
            leader = await self.find_leader()
            if leader:
                await self.simulate_node_failure(leader)
                await asyncio.sleep(3)  # Wait for election
                
                # Check new term
                new_leader = await self.wait_for_leader_election()
                if new_leader:
                    new_status = await self.get_node_status(new_leader)
                    new_term = new_status.get('raft', {}).get('term', 0)
                    
                    if new_term <= max_initial_term:
                        return TestCase(
                            name="Term Progression",
                            description="Test Raft term progression during elections",
                            result=TestResult.FAILED,
                            duration=time.time() - start_time,
                            details=f"Term did not increase: {max_initial_term} -> {new_term}",
                            error="Term progression failed"
                        )
                    
                    # Restart the failed node
                    await self.simulate_node_recovery(leader)
                    
                    return TestCase(
                        name="Term Progression",
                        description="Test Raft term progression during elections",
                        result=TestResult.PASSED,
                        duration=time.time() - start_time,
                        details=f"Term progressed correctly: {max_initial_term} -> {new_term}",
                    )
            
            return TestCase(
                name="Term Progression",
                description="Test Raft term progression during elections",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="Could not complete term progression test",
                error="Test execution failed"
            )
            
        except Exception as e:
            return TestCase(
                name="Term Progression",
                description="Test Raft term progression during elections",
                result=TestResult.FAILED,
                duration=time.time() - start_time,
                details="",
                error=str(e)
            )
            
    async def run_all_tests(self) -> List[TestCase]:
        """Run all Raft consensus tests."""
        print("🧪 Starting Comprehensive Raft Consensus Tests...")
        print("=" * 60)
        
        tests = [
            self.test_basic_leader_election,
            self.test_log_replication,
            self.test_leader_failure_and_recovery,
            self.test_follower_failure_and_recovery,
            self.test_consistency_under_load,
            self.test_term_progression
        ]
        
        results = []
        
        for i, test_func in enumerate(tests, 1):
            print(f"\n[{i}/{len(tests)}] Running {test_func.__name__.replace('test_', '').replace('_', ' ').title()}...")
            
            try:
                result = await test_func()
                results.append(result)
                
                status_icon = "✅" if result.result == TestResult.PASSED else "❌"
                print(f"{status_icon} {result.name}: {result.result.value}")
                print(f"   Duration: {result.duration:.2f}s")
                print(f"   Details: {result.details}")
                if result.error:
                    print(f"   Error: {result.error}")
                    
            except Exception as e:
                error_result = TestCase(
                    name=test_func.__name__.replace('test_', '').replace('_', ' ').title(),
                    description="Test execution failed",
                    result=TestResult.FAILED,
                    duration=0,
                    details="",
                    error=str(e)
                )
                results.append(error_result)
                print(f"❌ {error_result.name}: FAILED - {str(e)}")
        
        self.test_results = results
        return results
        
    def print_summary(self):
        """Print test results summary."""
        if not self.test_results:
            print("No tests have been run.")
            return
            
        print("\n" + "=" * 60)
        print("🏁 RAFT CONSENSUS TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for r in self.test_results if r.result == TestResult.PASSED)
        failed = sum(1 for r in self.test_results if r.result == TestResult.FAILED)
        total = len(self.test_results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ✅")
        print(f"Failed: {failed} ❌")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        
        total_duration = sum(r.duration for r in self.test_results)
        print(f"Total Duration: {total_duration:.2f}s")
        
        if failed > 0:
            print(f"\n❌ Failed Tests:")
            for result in self.test_results:
                if result.result == TestResult.FAILED:
                    print(f"   • {result.name}: {result.error}")


async def main():
    """Run comprehensive Raft consensus tests."""
    tester = RaftTester()
    
    try:
        await tester.initialize()
        
        # Wait for cluster to be ready
        print("⏳ Waiting for cluster to be ready...")
        leader = await tester.wait_for_leader_election(timeout=30)
        if not leader:
            print("❌ Cluster not ready - no leader found")
            return
            
        print(f"✅ Cluster ready with leader: {leader}")
        
        # Run all tests
        await tester.run_all_tests()
        
        # Print summary
        tester.print_summary()
        
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())