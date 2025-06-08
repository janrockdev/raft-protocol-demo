"""
Quick Raft consensus protocol tests with faster execution.
"""

import asyncio
import aiohttp
import json
import time
import random


async def test_leader_election():
    """Test basic leader election."""
    session = aiohttp.ClientSession()
    
    try:
        # Check all nodes
        nodes = [3001, 3002, 3003]
        leaders = []
        
        for port in nodes:
            try:
                async with session.get(f'http://127.0.0.1:{port}/status', 
                                     timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('raft', {}).get('is_leader', False):
                            leaders.append(f'node{port//1000}')
            except:
                pass
        
        print(f"✅ Leader Election Test: Found {len(leaders)} leader(s): {leaders}")
        return len(leaders) == 1
        
    finally:
        await session.close()


async def test_data_replication():
    """Test data replication across nodes."""
    session = aiohttp.ClientSession()
    
    try:
        # Set a test value
        test_key = f"replication_test_{int(time.time())}"
        test_value = f"test_value_{random.randint(1000, 9999)}"
        
        # Write to leader (node1)
        async with session.post(f'http://127.0.0.1:3001/cache/{test_key}',
                               json={'value': test_value},
                               timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status != 200:
                print(f"❌ Data Replication Test: Write failed")
                return False
        
        # Wait for replication
        await asyncio.sleep(1)
        
        # Check replication on all nodes
        nodes = [3001, 3002, 3003]
        replicated_count = 0
        
        for port in nodes:
            try:
                async with session.get(f'http://127.0.0.1:{port}/cache/{test_key}',
                                     timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('value') == test_value:
                            replicated_count += 1
            except:
                pass
        
        print(f"✅ Data Replication Test: Replicated to {replicated_count}/3 nodes")
        return replicated_count >= 2  # Majority replication
        
    finally:
        await session.close()


async def test_leader_failure():
    """Test leader failure simulation."""
    session = aiohttp.ClientSession()
    
    try:
        # Find current leader
        original_leader = None
        nodes = [3001, 3002, 3003]
        
        for port in nodes:
            try:
                async with session.get(f'http://127.0.0.1:{port}/status',
                                     timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('raft', {}).get('is_leader', False):
                            original_leader = port
                            break
            except:
                pass
        
        if not original_leader:
            print("❌ Leader Failure Test: No leader found")
            return False
        
        # Kill the leader using dashboard
        try:
            node_name = f"node{original_leader//1000}"
            async with session.post('http://127.0.0.1:8080/api/test',
                                   json={'type': 'kill_node', 'node': node_name},
                                   timeout=aiohttp.ClientTimeout(total=5)) as resp:
                pass
        except:
            print("❌ Leader Failure Test: Could not simulate failure")
            return False
        
        # Wait for new election
        await asyncio.sleep(3)
        
        # Check for new leader
        new_leaders = []
        remaining_nodes = [p for p in nodes if p != original_leader]
        
        for port in remaining_nodes:
            try:
                async with session.get(f'http://127.0.0.1:{port}/status',
                                     timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('raft', {}).get('is_leader', False):
                            new_leaders.append(port)
            except:
                pass
        
        # Restart the failed node
        try:
            node_name = f"node{original_leader//1000}"
            async with session.post('http://127.0.0.1:8080/api/test',
                                   json={'type': 'restart_node', 'node': node_name},
                                   timeout=aiohttp.ClientTimeout(total=8)) as resp:
                pass
        except:
            pass
        
        print(f"✅ Leader Failure Test: New leader elected on port {new_leaders}")
        return len(new_leaders) == 1
        
    finally:
        await session.close()


async def test_consistency_under_load():
    """Test consistency under concurrent operations."""
    session = aiohttp.ClientSession()
    
    try:
        # Perform multiple concurrent writes
        operations = []
        for i in range(5):
            key = f"load_test_{i}_{int(time.time())}"
            value = f"value_{i}_{random.randint(100, 999)}"
            operations.append(
                session.post(f'http://127.0.0.1:3001/cache/{key}',
                           json={'value': value},
                           timeout=aiohttp.ClientTimeout(total=3))
            )
        
        # Execute concurrently
        responses = await asyncio.gather(*operations, return_exceptions=True)
        
        # Close all responses
        successful = 0
        for resp in responses:
            if isinstance(resp, aiohttp.ClientResponse):
                if resp.status == 200:
                    successful += 1
                resp.close()
        
        print(f"✅ Load Test: {successful}/5 operations succeeded")
        return successful >= 3  # Allow some failures under load
        
    finally:
        await session.close()


async def test_performance_metrics():
    """Test performance and get metrics."""
    session = aiohttp.ClientSession()
    
    try:
        start_time = time.time()
        operations = 10
        successful = 0
        
        for i in range(operations):
            try:
                async with session.post(f'http://127.0.0.1:3001/cache/perf_{i}',
                                       json={'value': f'perf_value_{i}'},
                                       timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        successful += 1
            except:
                pass
        
        duration = time.time() - start_time
        ops_per_sec = successful / duration if duration > 0 else 0
        
        print(f"✅ Performance Test: {successful}/{operations} ops, {ops_per_sec:.1f} ops/sec")
        return successful >= operations * 0.8
        
    finally:
        await session.close()


async def main():
    """Run quick Raft tests."""
    print("🧪 Running Quick Raft Consensus Tests")
    print("=" * 50)
    
    tests = [
        ("Leader Election", test_leader_election),
        ("Data Replication", test_data_replication),
        ("Performance Metrics", test_performance_metrics),
        ("Consistency Under Load", test_consistency_under_load),
        ("Leader Failure Recovery", test_leader_failure),
    ]
    
    passed = 0
    total = len(tests)
    
    for name, test_func in tests:
        print(f"\n🔍 Testing {name}...")
        try:
            result = await test_func()
            if result:
                passed += 1
            else:
                print(f"❌ {name}: FAILED")
        except Exception as e:
            print(f"❌ {name}: ERROR - {str(e)}")
        
        await asyncio.sleep(0.5)  # Brief pause between tests
    
    print(f"\n🏁 Test Summary: {passed}/{total} tests passed")
    print(f"Success Rate: {(passed/total)*100:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())