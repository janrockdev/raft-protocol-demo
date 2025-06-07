"""
Comprehensive Raft consensus demonstration and testing suite.
"""

import asyncio
import aiohttp
import json
import time
import random


async def demonstrate_raft_consensus():
    """Demonstrate key Raft consensus features."""
    session = aiohttp.ClientSession()
    
    print("🔍 Raft Consensus Protocol Demonstration")
    print("=" * 50)
    
    try:
        # Test 1: Cluster State Analysis
        print("\n1. CLUSTER STATE ANALYSIS")
        print("-" * 30)
        
        nodes = {'node1': 3000, 'node2': 4000, 'node3': 5000}
        cluster_state = {}
        
        for name, port in nodes.items():
            try:
                async with session.get(f'http://127.0.0.1:{port}/status', 
                                     timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        cluster_state[name] = data
                        raft_info = data.get('raft', {})
                        print(f"{name}: {raft_info.get('state', 'unknown')} | Term: {raft_info.get('term', 0)} | Leader: {raft_info.get('is_leader', False)}")
            except Exception as e:
                print(f"{name}: OFFLINE ({e})")
        
        leader = None
        followers = []
        for name, state in cluster_state.items():
            if state.get('raft', {}).get('is_leader', False):
                leader = name
            elif state.get('raft', {}).get('state') == 'follower':
                followers.append(name)
        
        print(f"\nCluster Status: Leader={leader}, Followers={followers}")
        
        # Test 2: Data Consistency Verification  
        print("\n2. DATA CONSISTENCY VERIFICATION")
        print("-" * 35)
        
        if leader:
            leader_port = nodes[leader]
            test_key = f"consistency_test_{int(time.time())}"
            test_value = f"distributed_value_{random.randint(1000, 9999)}"
            
            # Write to leader
            async with session.post(f'http://127.0.0.1:{leader_port}/cache/{test_key}',
                                   json={'value': test_value},
                                   timeout=aiohttp.ClientTimeout(total=3)) as resp:
                write_status = resp.status
                write_data = await resp.json()
            
            print(f"Write to {leader}: Status {write_status}")
            print(f"Response: {write_data.get('message', 'No message')}")
            
            # Wait for replication
            await asyncio.sleep(1.5)
            
            # Verify on all nodes
            print("\nReplication verification:")
            for name, port in nodes.items():
                try:
                    async with session.get(f'http://127.0.0.1:{port}/cache/{test_key}',
                                         timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get('value') == test_value:
                                print(f"  {name}: ✓ Consistent")
                            else:
                                print(f"  {name}: ✗ Inconsistent")
                        else:
                            print(f"  {name}: ✗ Error {resp.status}")
                except:
                    print(f"  {name}: ✗ Unreachable")
        
        # Test 3: Performance Under Load
        print("\n3. PERFORMANCE UNDER LOAD")
        print("-" * 27)
        
        if leader:
            leader_port = nodes[leader]
            operations = 20
            start_time = time.time()
            successful = 0
            failed = 0
            
            print(f"Executing {operations} concurrent operations...")
            
            # Create concurrent operations
            tasks = []
            for i in range(operations):
                key = f"load_test_{i}_{int(time.time())}"
                value = f"load_value_{i}"
                task = session.post(f'http://127.0.0.1:{leader_port}/cache/{key}',
                                  json={'value': value},
                                  timeout=aiohttp.ClientTimeout(total=3))
                tasks.append(task)
            
            # Execute concurrently
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for resp in responses:
                if isinstance(resp, aiohttp.ClientResponse):
                    if resp.status == 200:
                        successful += 1
                    else:
                        failed += 1
                    resp.close()
                else:
                    failed += 1
            
            duration = time.time() - start_time
            throughput = successful / duration if duration > 0 else 0
            
            print(f"Results: {successful} successful, {failed} failed")
            print(f"Duration: {duration:.2f}s")
            print(f"Throughput: {throughput:.1f} ops/sec")
            print(f"Success Rate: {(successful/operations)*100:.1f}%")
        
        # Test 4: Leader Failover Simulation
        print("\n4. LEADER FAILOVER SIMULATION")
        print("-" * 33)
        
        if leader:
            print(f"Current leader: {leader}")
            
            # Attempt to simulate leader failure via dashboard
            try:
                async with session.post('http://127.0.0.1:8080/api/test',
                                       json={'type': 'kill_node', 'node': leader},
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    print(f"Leader failure simulation initiated")
            except:
                print("Could not simulate failure via dashboard")
            
            # Wait for election
            await asyncio.sleep(4)
            
            # Check for new leader
            print("Checking for new leader...")
            new_leader = None
            remaining_nodes = {name: port for name, port in nodes.items() if name != leader}
            
            for name, port in remaining_nodes.items():
                try:
                    async with session.get(f'http://127.0.0.1:{port}/status',
                                         timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get('raft', {}).get('is_leader', False):
                                new_leader = name
                                new_term = data.get('raft', {}).get('term', 0)
                                print(f"New leader elected: {new_leader} (term {new_term})")
                                break
                except:
                    continue
            
            if not new_leader:
                print("No new leader detected - cluster may be recovering")
            
            # Attempt to restart failed node
            try:
                async with session.post('http://127.0.0.1:8080/api/test',
                                       json={'type': 'restart_node', 'node': leader},
                                       timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    print(f"Attempting to restart {leader}")
            except:
                print(f"Could not restart {leader} via dashboard")
        
        # Test 5: Network Partition Tolerance
        print("\n5. NETWORK PARTITION TOLERANCE")
        print("-" * 34)
        
        # Test majority operation
        healthy_count = len([name for name, port in nodes.items() 
                           if await check_node_health(session, port)])
        
        print(f"Healthy nodes: {healthy_count}/3")
        
        if healthy_count >= 2:
            print("✓ Majority available - cluster operational")
            
            # Test write operation
            available_nodes = []
            for name, port in nodes.items():
                if await check_node_health(session, port):
                    available_nodes.append((name, port))
            
            if available_nodes:
                test_node_name, test_port = available_nodes[0]
                test_key = f"partition_test_{int(time.time())}"
                test_value = "partition_tolerance_test"
                
                try:
                    async with session.post(f'http://127.0.0.1:{test_port}/cache/{test_key}',
                                           json={'value': test_value},
                                           timeout=aiohttp.ClientTimeout(total=3)) as resp:
                        if resp.status == 200:
                            print("✓ Write operation successful with majority")
                        else:
                            print("✗ Write operation failed")
                except:
                    print("✗ Write operation timed out")
        else:
            print("✗ No majority - cluster unavailable")
        
        print("\n" + "=" * 50)
        print("🏁 RAFT CONSENSUS DEMONSTRATION COMPLETE")
        print("=" * 50)
        
    finally:
        await session.close()


async def check_node_health(session, port):
    """Check if a node is healthy."""
    try:
        async with session.get(f'http://127.0.0.1:{port}/health',
                              timeout=aiohttp.ClientTimeout(total=1)) as resp:
            return resp.status == 200
    except:
        return False


async def run_mini_stress_test():
    """Run a focused stress test on the Raft cluster."""
    session = aiohttp.ClientSession()
    
    print("\n🚀 MINI STRESS TEST")
    print("-" * 20)
    
    try:
        # Find leader
        leader_port = None
        nodes = [3000, 4000, 5000]
        
        for port in nodes:
            try:
                async with session.get(f'http://127.0.0.1:{port}/status',
                                     timeout=aiohttp.ClientTimeout(total=1)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('raft', {}).get('is_leader', False):
                            leader_port = port
                            break
            except:
                continue
        
        if not leader_port:
            print("No leader found for stress test")
            return
        
        print(f"Running stress test on leader (port {leader_port})")
        
        # Burst test: 50 operations as fast as possible
        operations = 50
        start_time = time.time()
        successful = 0
        
        tasks = []
        for i in range(operations):
            key = f"stress_{i}_{int(time.time())}"
            value = f"stress_value_{i}"
            task = session.post(f'http://127.0.0.1:{leader_port}/cache/{key}',
                              json={'value': value},
                              timeout=aiohttp.ClientTimeout(total=2))
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for resp in responses:
            if isinstance(resp, aiohttp.ClientResponse):
                if resp.status == 200:
                    successful += 1
                resp.close()
        
        duration = time.time() - start_time
        throughput = successful / duration if duration > 0 else 0
        
        print(f"Stress Test Results:")
        print(f"  Operations: {successful}/{operations} successful")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Peak Throughput: {throughput:.1f} ops/sec")
        print(f"  Success Rate: {(successful/operations)*100:.1f}%")
        
    finally:
        await session.close()


async def main():
    """Run comprehensive Raft demonstration."""
    await demonstrate_raft_consensus()
    await run_mini_stress_test()


if __name__ == "__main__":
    asyncio.run(main())