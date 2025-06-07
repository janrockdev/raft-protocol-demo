"""Raft consensus protocol implementation for distributed key-value cache."""

import asyncio
import json
import logging
import random
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
import aiohttp
from aiohttp import web
import hashlib

from config import (
    ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, 
    HEARTBEAT_INTERVAL, RPC_TIMEOUT
)


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry:
    """Represents a single log entry in the Raft log."""
    
    def __init__(self, term: int, command: Dict[str, Any], index: int = 0):
        self.term = term
        self.command = command
        self.index = index
        self.committed = False
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict:
        return {
            'term': self.term,
            'command': self.command,
            'index': self.index,
            'committed': self.committed,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'LogEntry':
        entry = cls(data['term'], data['command'], data['index'])
        entry.committed = data.get('committed', False)
        entry.timestamp = data.get('timestamp', time.time())
        return entry


class RaftNode:
    """Implementation of a Raft consensus node."""
    
    def __init__(self, node_id: str, host: str, port: int, cluster_nodes: Dict[str, Dict]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cluster_nodes = cluster_nodes
        
        # Raft state
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self._get_random_election_timeout()
        
        # Tasks
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.election_task: Optional[asyncio.Task] = None
        
        # HTTP session for inter-node communication
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Callbacks
        self.on_commit_callback = None
        
        self.logger = logging.getLogger(f'RaftNode-{node_id}')
        
    def _get_random_election_timeout(self) -> float:
        """Generate a random election timeout to avoid split votes."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    
    async def initialize(self):
        """Initialize the Raft node."""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=RPC_TIMEOUT))
        
        # Initialize leader state for all nodes
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                self.next_index[node_id] = len(self.log) + 1
                self.match_index[node_id] = 0
        
        # Start election timeout task
        self.election_task = asyncio.create_task(self._election_timeout_loop())
        
        self.logger.info(f"Raft node {self.node_id} initialized")
    
    async def shutdown(self):
        """Shutdown the Raft node."""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.election_task:
            self.election_task.cancel()
        if self.session:
            await self.session.close()
    
    async def _election_timeout_loop(self):
        """Monitor election timeout and trigger elections."""
        while True:
            try:
                await asyncio.sleep(0.01)  # Check every 10ms
                
                if self.state == NodeState.LEADER:
                    continue
                
                current_time = time.time()
                if current_time - self.last_heartbeat > self.election_timeout:
                    self.logger.info(f"Election timeout reached, starting election")
                    await self._start_election()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in election timeout loop: {e}")
    
    async def _start_election(self):
        """Start a new election."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._get_random_election_timeout()
        
        self.logger.info(f"Starting election for term {self.current_term}")
        
        # Request votes from other nodes
        votes = 1  # Vote for self
        vote_tasks = []
        
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._request_vote(node_id, node_info))
                vote_tasks.append(task)
        
        # Wait for vote responses
        if vote_tasks:
            results = await asyncio.gather(*vote_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, bool) and result:
                    votes += 1
        
        # Check if we won the election
        majority = (len(self.cluster_nodes) // 2) + 1
        if votes >= majority and self.state == NodeState.CANDIDATE:
            await self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
            self.logger.info(f"Election failed, received {votes} votes, needed {majority}")
    
    async def _request_vote(self, node_id: str, node_info: Dict) -> bool:
        """Request vote from a specific node."""
        try:
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
            
            data = {
                'term': self.current_term,
                'candidate_id': self.node_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            }
            
            url = f"http://{node_info['host']}:{node_info['port']}/raft/request_vote"
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if result['term'] > self.current_term:
                        self.current_term = result['term']
                        self.state = NodeState.FOLLOWER
                        self.voted_for = None
                    return result.get('vote_granted', False)
                    
        except Exception as e:
            self.logger.warning(f"Failed to request vote from {node_id}: {e}")
        
        return False
    
    async def _become_leader(self):
        """Become the leader of the cluster."""
        self.state = NodeState.LEADER
        self.logger.info(f"Became leader for term {self.current_term}")
        
        # Initialize leader state
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                self.next_index[node_id] = len(self.log) + 1
                self.match_index[node_id] = 0
        
        # Start sending heartbeats
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to followers."""
        while self.state == NodeState.LEADER:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
    
    async def _send_heartbeats(self):
        """Send heartbeat messages to all followers."""
        tasks = []
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._send_append_entries(node_id, node_info))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_append_entries(self, node_id: str, node_info: Dict):
        """Send append entries RPC to a specific node."""
        try:
            prev_log_index = self.next_index[node_id] - 1
            prev_log_term = 0
            if prev_log_index > 0 and prev_log_index <= len(self.log):
                prev_log_term = self.log[prev_log_index - 1].term
            
            # Get entries to send
            entries = []
            if self.next_index[node_id] <= len(self.log):
                entries = [entry.to_dict() for entry in self.log[self.next_index[node_id] - 1:]]
            
            data = {
                'term': self.current_term,
                'leader_id': self.node_id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commit_index
            }
            
            url = f"http://{node_info['host']}:{node_info['port']}/raft/append_entries"
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if result['term'] > self.current_term:
                        self.current_term = result['term']
                        self.state = NodeState.FOLLOWER
                        self.voted_for = None
                        if self.heartbeat_task:
                            self.heartbeat_task.cancel()
                        return
                    
                    if result['success']:
                        # Update next_index and match_index
                        if entries:
                            self.match_index[node_id] = prev_log_index + len(entries)
                            self.next_index[node_id] = self.match_index[node_id] + 1
                    else:
                        # Decrement next_index and retry
                        self.next_index[node_id] = max(1, self.next_index[node_id] - 1)
                        
        except Exception as e:
            self.logger.warning(f"Failed to send append entries to {node_id}: {e}")
    
    async def handle_request_vote(self, request_data: Dict) -> Dict:
        """Handle incoming request vote RPC."""
        term = request_data['term']
        candidate_id = request_data['candidate_id']
        last_log_index = request_data['last_log_index']
        last_log_term = request_data['last_log_term']
        
        vote_granted = False
        
        # Update term if necessary
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = NodeState.FOLLOWER
        
        # Check if we can grant the vote
        if (term >= self.current_term and 
            (self.voted_for is None or self.voted_for == candidate_id)):
            
            # Check if candidate's log is at least as up-to-date as ours
            our_last_log_index = len(self.log)
            our_last_log_term = self.log[-1].term if self.log else 0
            
            log_ok = (last_log_term > our_last_log_term or 
                     (last_log_term == our_last_log_term and last_log_index >= our_last_log_index))
            
            if log_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()  # Reset election timeout
        
        self.logger.debug(f"Vote request from {candidate_id}, term {term}: {'granted' if vote_granted else 'denied'}")
        
        return {
            'term': self.current_term,
            'vote_granted': vote_granted
        }
    
    async def handle_append_entries(self, request_data: Dict) -> Dict:
        """Handle incoming append entries RPC."""
        term = request_data['term']
        leader_id = request_data['leader_id']
        prev_log_index = request_data['prev_log_index']
        prev_log_term = request_data['prev_log_term']
        entries = request_data['entries']
        leader_commit = request_data['leader_commit']
        
        success = False
        
        # Update term if necessary
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        
        if term >= self.current_term:
            self.state = NodeState.FOLLOWER
            self.last_heartbeat = time.time()
            
            # Check if previous log entry matches
            if (prev_log_index == 0 or 
                (prev_log_index <= len(self.log) and 
                 self.log[prev_log_index - 1].term == prev_log_term)):
                
                success = True
                
                # Remove conflicting entries and append new ones
                if entries:
                    # Convert entries back to LogEntry objects
                    new_entries = [LogEntry.from_dict(entry) for entry in entries]
                    
                    # Remove conflicting entries
                    if prev_log_index < len(self.log):
                        self.log = self.log[:prev_log_index]
                    
                    # Append new entries
                    self.log.extend(new_entries)
                
                # Update commit index
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.log))
                    await self._apply_committed_entries()
        
        return {
            'term': self.current_term,
            'success': success
        }
    
    async def _apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            entry.committed = True
            
            if self.on_commit_callback:
                await self.on_commit_callback(entry.command)
    
    async def propose_command(self, command: Dict[str, Any]) -> bool:
        """Propose a new command to the cluster."""
        if self.state != NodeState.LEADER:
            return False
        
        # Add entry to log
        entry = LogEntry(self.current_term, command, len(self.log) + 1)
        self.log.append(entry)
        
        self.logger.debug(f"Proposed command: {command}")
        
        # Try to replicate to majority
        return await self._replicate_entry(entry)
    
    async def _replicate_entry(self, entry: LogEntry) -> bool:
        """Replicate a log entry to the majority of nodes."""
        # Send to all followers
        tasks = []
        for node_id, node_info in self.cluster_nodes.items():
            if node_id != self.node_id:
                task = asyncio.create_task(self._send_append_entries(node_id, node_info))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if majority has replicated
        replicated_count = 1  # Leader counts as one
        for node_id in self.cluster_nodes:
            if node_id != self.node_id and self.match_index[node_id] >= entry.index:
                replicated_count += 1
        
        majority = (len(self.cluster_nodes) // 2) + 1
        if replicated_count >= majority:
            # Update commit index
            if entry.index > self.commit_index:
                self.commit_index = entry.index
                await self._apply_committed_entries()
            return True
        
        return False
    
    def get_status(self) -> Dict:
        """Get the current status of the Raft node."""
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.current_term,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'is_leader': self.state == NodeState.LEADER
        }
