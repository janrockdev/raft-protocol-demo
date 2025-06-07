"""High-performance key-value cache with persistence support."""

import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional
import hashlib
import os

from config import MAX_CACHE_SIZE, PERSISTENCE_INTERVAL


class CacheEntry:
    """Represents a single cache entry with metadata."""
    
    def __init__(self, key: str, value: Any, ttl: Optional[float] = None):
        self.key = key
        self.value = value
        self.created_at = time.time()
        self.accessed_at = self.created_at
        self.access_count = 0
        self.ttl = ttl
        self.expires_at = self.created_at + ttl if ttl else None
    
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def access(self):
        """Mark the entry as accessed."""
        self.accessed_at = time.time()
        self.access_count += 1
    
    def to_dict(self) -> Dict:
        """Convert entry to dictionary for serialization."""
        return {
            'key': self.key,
            'value': self.value,
            'created_at': self.created_at,
            'accessed_at': self.accessed_at,
            'access_count': self.access_count,
            'ttl': self.ttl,
            'expires_at': self.expires_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CacheEntry':
        """Create entry from dictionary."""
        entry = cls(data['key'], data['value'], data.get('ttl'))
        entry.created_at = data.get('created_at', time.time())
        entry.accessed_at = data.get('accessed_at', entry.created_at)
        entry.access_count = data.get('access_count', 0)
        entry.expires_at = data.get('expires_at')
        return entry


class DistributedCache:
    """High-performance distributed key-value cache."""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.cache: Dict[str, CacheEntry] = {}
        self.max_size = MAX_CACHE_SIZE
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'evictions': 0
        }
        
        # Persistence
        self.persistence_file = f"cache_{node_id}.json"
        self.persistence_task: Optional[asyncio.Task] = None
        self.dirty = False
        
        self.logger = logging.getLogger(f'Cache-{node_id}')
        
    async def initialize(self):
        """Initialize the cache."""
        await self._load_from_disk()
        
        # Start periodic persistence task
        self.persistence_task = asyncio.create_task(self._persistence_loop())
        
        self.logger.info(f"Cache initialized with {len(self.cache)} entries")
    
    async def shutdown(self):
        """Shutdown the cache."""
        if self.persistence_task:
            self.persistence_task.cancel()
        
        # Save final state
        await self._save_to_disk()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        if key not in self.cache:
            self.stats['misses'] += 1
            return None
        
        entry = self.cache[key]
        
        # Check if expired
        if entry.is_expired():
            del self.cache[key]
            self.stats['misses'] += 1
            self.dirty = True
            return None
        
        # Update access statistics
        entry.access()
        self.stats['hits'] += 1
        
        return entry.value
    
    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set a value in the cache."""
        try:
            # Check if we need to evict entries
            if len(self.cache) >= self.max_size and key not in self.cache:
                await self._evict_entries()
            
            # Create new entry
            entry = CacheEntry(key, value, ttl)
            self.cache[key] = entry
            
            self.stats['sets'] += 1
            self.dirty = True
            
            self.logger.debug(f"Set key '{key}' with value type {type(value)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting key '{key}': {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete a value from the cache."""
        if key in self.cache:
            del self.cache[key]
            self.stats['deletes'] += 1
            self.dirty = True
            self.logger.debug(f"Deleted key '{key}'")
            return True
        return False
    
    async def _evict_entries(self):
        """Evict entries using LRU policy."""
        if len(self.cache) == 0:
            return
        
        # Find the least recently used entry
        lru_key = min(self.cache.keys(), key=lambda k: self.cache[k].accessed_at)
        
        del self.cache[lru_key]
        self.stats['evictions'] += 1
        self.dirty = True
        
        self.logger.debug(f"Evicted key '{lru_key}' (LRU)")
    
    async def clear(self) -> bool:
        """Clear all entries from the cache."""
        self.cache.clear()
        self.dirty = True
        self.logger.info("Cache cleared")
        return True
    
    def size(self) -> int:
        """Get the current size of the cache."""
        return len(self.cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hit_rate': round(hit_rate, 2),
            **self.stats
        }
    
    async def _persistence_loop(self):
        """Periodic persistence to disk."""
        while True:
            try:
                await asyncio.sleep(PERSISTENCE_INTERVAL)
                if self.dirty:
                    await self._save_to_disk()
                    self.dirty = False
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in persistence loop: {e}")
    
    async def _save_to_disk(self):
        """Save cache state to disk."""
        try:
            # Clean expired entries before saving
            await self._cleanup_expired()
            
            data = {
                'cache': {key: entry.to_dict() for key, entry in self.cache.items()},
                'stats': self.stats,
                'timestamp': time.time()
            }
            
            # Write atomically using temporary file
            temp_file = f"{self.persistence_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            os.rename(temp_file, self.persistence_file)
            self.logger.debug(f"Saved {len(self.cache)} entries to disk")
            
        except Exception as e:
            self.logger.error(f"Error saving cache to disk: {e}")
    
    async def _load_from_disk(self):
        """Load cache state from disk."""
        try:
            if not os.path.exists(self.persistence_file):
                return
            
            with open(self.persistence_file, 'r') as f:
                data = json.load(f)
            
            # Restore cache entries
            cache_data = data.get('cache', {})
            for key, entry_data in cache_data.items():
                entry = CacheEntry.from_dict(entry_data)
                if not entry.is_expired():
                    self.cache[key] = entry
            
            # Restore statistics
            self.stats.update(data.get('stats', {}))
            
            self.logger.info(f"Loaded {len(self.cache)} entries from disk")
            
        except Exception as e:
            self.logger.error(f"Error loading cache from disk: {e}")
    
    async def _cleanup_expired(self):
        """Remove expired entries from cache."""
        expired_keys = []
        current_time = time.time()
        
        for key, entry in self.cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            self.logger.debug(f"Cleaned up {len(expired_keys)} expired entries")
    
    async def apply_command(self, command: Dict[str, Any]):
        """Apply a Raft command to the cache."""
        try:
            operation = command.get('operation')
            key = command.get('key')
            
            if operation == 'set':
                value = command.get('value')
                ttl = command.get('ttl')
                await self.set(key, value, ttl)
                
            elif operation == 'delete':
                await self.delete(key)
                
            elif operation == 'clear':
                await self.clear()
                
            else:
                self.logger.warning(f"Unknown operation: {operation}")
                
        except Exception as e:
            self.logger.error(f"Error applying command {command}: {e}")
    
    def get_all_keys(self) -> list:
        """Get all keys in the cache."""
        # Clean expired entries first
        expired_keys = []
        for key, entry in self.cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        return list(self.cache.keys())
