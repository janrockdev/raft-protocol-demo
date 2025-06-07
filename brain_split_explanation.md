# Raft Consensus with 5 Nodes: Brain Split Prevention

## How Raft Prevents Brain Split

### Majority Consensus Rule
- **5 nodes require 3 for majority** (quorum)
- Only partitions with 3+ nodes can elect leaders
- Partitions with <3 nodes cannot make progress

### Network Partition Scenarios

#### 1. 3-2 Split (Safe)
```
Partition A: [Node1, Node2, Node3] ← Has majority (3/5)
Partition B: [Node4, Node5]        ← No majority (2/5)
```
**Result:** 
- Partition A continues normal operation
- Partition B becomes read-only, cannot elect leader
- No split-brain possible

#### 2. 4-1 Split (Safe)
```
Partition A: [Node1, Node2, Node3, Node4] ← Super majority (4/5)
Partition B: [Node5]                      ← Isolated (1/5)
```
**Result:**
- Partition A operates normally with high availability
- Partition B is completely isolated
- Excellent fault tolerance

#### 3. 2-2-1 Split (Unavailable)
```
Partition A: [Node1, Node2] ← No majority (2/5)
Partition B: [Node3, Node4] ← No majority (2/5)  
Partition C: [Node5]        ← No majority (1/5)
```
**Result:**
- No partition can achieve majority
- Entire cluster becomes unavailable
- Prevents split-brain by refusing to operate

## Brain Split Prevention Mechanisms

### 1. Vote Counting
```python
def can_become_leader(votes_received, total_nodes):
    required_votes = (total_nodes // 2) + 1
    return votes_received >= required_votes

# For 5 nodes: need 3 votes minimum
# For 3 nodes: need 2 votes minimum
```

### 2. Term Advancement
- Each election increments term number
- Nodes only vote once per term
- Higher terms invalidate lower terms

### 3. Log Consistency Checks
- Leaders must have most up-to-date log
- Followers only accept entries from current leader
- Prevents data divergence

## Advantages of 5-Node vs 3-Node

| Aspect | 3-Node Cluster | 5-Node Cluster |
|--------|---------------|----------------|
| Fault Tolerance | 1 node failure | 2 node failures |
| Availability | 66.7% nodes needed | 60% nodes needed |
| Recovery Time | Faster | Slightly slower |
| Resource Usage | Lower | Higher |
| Split-Brain Risk | Low | Very Low |

## Real-World Network Partition Examples

### Cloud Provider Outages
```
Region A: [Node1, Node2, Node3] ← Continues serving
Region B: [Node4, Node5]        ← Waits for reconnection
```

### Data Center Splits
```
DC Primary:   [Node1, Node2, Node3] ← Active
DC Secondary: [Node4, Node5]        ← Standby
```

### Switch/Router Failures
```
Network Segment 1: [Node1, Node2, Node3] ← Operational
Network Segment 2: [Node4, Node5]        ← Isolated
```

## Testing Brain Split Scenarios

The cluster can be tested with these partition scenarios:

1. **Graceful Partition**: Stop nodes to simulate network isolation
2. **Data Consistency**: Verify writes only succeed in majority partition
3. **Leader Election**: Confirm only majority partitions elect leaders
4. **Recovery**: Test behavior when partitions reconnect

## Configuration Recommendations

### For High Availability
- Use 5 or 7 nodes (odd numbers prevent ties)
- Distribute across availability zones
- Monitor network connectivity
- Set appropriate election timeouts

### For Cost Efficiency
- Use 3 nodes for smaller deployments
- Consider read replicas for scaling reads
- Use geographic distribution strategically

## Monitoring Brain Split Prevention

Key metrics to monitor:
- **Leader Count**: Should always be 0 or 1
- **Term Progression**: Should advance during elections only
- **Partition Detection**: Network connectivity between nodes
- **Quorum Status**: Whether cluster has majority
- **Write Acceptance**: Only majority partitions accept writes