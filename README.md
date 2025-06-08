# Raft Protocol

## Description

## Screenshots
![alt text](cache1.png "Dashboard (part#1)")
![alt text](cache2.png "Dashboard (part#2)")

## Python Virtual Environment Setup
```shell
sudo apt install python3.12-venv
python3 -m venv env
source env/bin/activate

pip install aiohttp
pip install aiohttp-cors
```

## Install Dependencies
```shell
pip install -r requirements.txt
```

## Running the Raft Cache Cluster
To run the Raft Cache cluster, you can use the provided `start_cluster.py` script. This script will start three nodes on different ports (3001, 3002, and 3003) to form a Raft cluster.
```shell
python main.py node1
python main.py node2
python main.py node3
# or
python start_cluster.py
```

## Testing the Raft Cache Cluster
To test the Raft Cache cluster, you can use the provided `test.py` script. This script will perform basic cache operations and verify data consistency across the nodes.

Basic Cache Operations
1. Check cluster health:
```shell
curl http://127.0.0.1:3001/health
```

2. View cluster status:
```shell
curl http://127.0.0.1:3001/status
```

3. Set a key-value pair:
```shell
curl -X POST http://127.0.0.1:3001/cache/username \
  -H 'Content-Type: application/json' \
  -d '{"value": "john_doe"}'
```

4. Set a key with TTL (expires in 60 seconds):
```shell
curl -X POST http://127.0.0.1:3001/cache/session123 \
  -H 'Content-Type: application/json' \
  -d '{"value": "active_session", "ttl": 60}'
```

5. Get a value:
```shell
curl http://127.0.0.1:3001/cache/username
```

6. List all keys:
```shell
curl http://127.0.0.1:3001/cache
```

7. Delete a key:
```shell
curl -X DELETE http://127.0.0.1:3001/cache/username
```

8. Clear all cache:
```shell
curl -X DELETE http://127.0.0.1:3001/cache
```

9. View cache statistics:
```shell
curl http://127.0.0.1:3001/stats
```

Test Data Consistency Across Nodes
Read from different nodes (should return same data):
```shell
curl http://127.0.0.1:3001/cache/mykey
curl http://127.0.0.1:3002/cache/mykey
curl http://127.0.0.1:3003/cache/mykey
```

## Testing Node Failures and Recovery with Raft Consensus
The cache uses Raft consensus protocol, so all write operations must go through the leader (node1 on port 3001), but you can read from any node to verify data consistency across the cluster.

Here's how to comprehensively test node failure and recovery in your Raft cluster:

Complete Node Failure Testing Guide
1. Current Cluster State:

# Check all nodes status
curl -s http://127.0.0.1:3001/status | jq '.raft.state'
curl -s http://127.0.0.1:3002/status | jq '.raft.state' 
curl -s http://127.0.0.1:3003/status | jq '.raft.state'
2. Test Follower Node Failure:

# Kill node2 (follower)
kill $(ps aux | grep "python main.py node2" | grep -v grep | awk '{print $2}')
# Verify node2 is down
curl -s http://127.0.0.1:3002/health || echo "Node 2 failed"
# Check if cluster still functions (2 nodes = majority)
curl -X POST http://127.0.0.1:3001/cache/after_node2_failure \
  -H 'Content-Type: application/json' \
  -d '{"value": "cluster_still_works"}'
3. Test Leader Node Failure:

# Kill the current leader (node1)
kill $(ps aux | grep "python main.py node1" | grep -v grep | awk '{print $2}')
# Wait for new leader election (5-10 seconds)
sleep 10
# Check new leader on remaining nodes
curl -s http://127.0.0.1:3002/status | jq '.raft.state'
curl -s http://127.0.0.1:3003/status | jq '.raft.state'
4. Test Majority Failure (Cluster Becomes Unavailable):

# Kill two nodes (lose majority)
kill $(ps aux | grep "python main.py node2" | grep -v grep | awk '{print $2}')
kill $(ps aux | grep "python main.py node3" | grep -v grep | awk '{print $2}')
# Try write operation (should fail - no majority)
curl -X POST http://127.0.0.1:3001/cache/no_majority \
  -H 'Content-Type: application/json' \
  -d '{"value": "should_fail"}'
5. Test Node Recovery:

# Restart failed nodes
python main.py node2 &
python main.py node3 &
# Wait for rejoin
sleep 5
# Verify all nodes are back
curl -s http://127.0.0.1:3001/health
curl -s http://127.0.0.1:3002/health  
curl -s http://127.0.0.1:3003/health
6. Test Split-Brain Prevention:

# Partition the network (simulate network failure)
# This would require iptables in real scenarios
# For testing, kill nodes strategically to force elections
# Force multiple elections by restarting nodes at different times
kill $(ps aux | grep "python main.py" | grep -v grep | awk '{print $2}')
python main.py node1 &
sleep 2
python main.py node2 &
sleep 2  
python main.py node3 &
7. Monitor Election Process:

# Watch term numbers increase during elections
while true; do
  echo "=== Election Monitor ==="
  curl -s http://127.0.0.1:3001/status | jq '.raft | {state, term, is_leader}' 2>/dev/null || echo "Node 1: DOWN"
  curl -s http://127.0.0.1:3002/status | jq '.raft | {state, term, is_leader}' 2>/dev/null || echo "Node 2: DOWN"
  curl -s http://127.0.0.1:3003/status | jq '.raft | {state, term, is_leader}' 2>/dev/null || echo "Node 3: DOWN"
  sleep 2
done
8. Test Data Persistence Through Failures:

# Set data before failure
curl -X POST http://127.0.0.1:3001/cache/persistent_data \
  -H 'Content-Type: application/json' \
  -d '{"value": "should_survive_failure"}'
# Restart entire cluster
kill $(ps aux | grep "python main.py" | grep -v grep | awk '{print $2}')
python start_cluster.py &
# Wait and verify data survived
sleep 10
curl -s http://127.0.0.1:3001/cache/persistent_data
9. Performance During Failures:

# Test write latency during node failures
for i in {1..10}; do
  time curl -X POST http://127.0.0.1:3001/cache/perf_test_$i \
    -H 'Content-Type: application/json' \
    -d '{"value": "performance_test"}'
done
Expected Raft Behaviors:

Follower failure: Cluster continues normally with majority (2/3 nodes)
Leader failure: New election occurs, new leader elected within seconds
Majority failure: Cluster becomes read-only, no writes accepted
Recovery: Nodes catch up via log replication when rejoining
Split-brain prevention: Only one leader per term, guaranteed by majority votes
The cluster demonstrates proper Raft consensus with fault tolerance, automatic failover, and data consistency guarantees.