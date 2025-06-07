"""Configuration settings for the Raft KV Cache cluster."""

import os

# Node configuration
NODES = {
    'node1': {'host': '127.0.0.1', 'port': 3000},
    'node2': {'host': '127.0.0.1', 'port': 4000},
    'node3': {'host': '127.0.0.1', 'port': 5000}
}

# Raft timing configuration (in seconds)
ELECTION_TIMEOUT_MIN = 0.15
ELECTION_TIMEOUT_MAX = 0.3
HEARTBEAT_INTERVAL = 0.05
RPC_TIMEOUT = 2.0

# Cache configuration
MAX_CACHE_SIZE = 10000
PERSISTENCE_INTERVAL = 30  # seconds

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
