"""
Distributed Caching System
In-memory caching with TTL, LRU eviction, and cache-aside pattern
"""
import time
import threading
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, TypeVar, Generic
from collections import OrderedDict
from enum import Enum
import functools

logger = logging.getLogger("cache")

T = TypeVar('T')


class EvictionPolicy(Enum):
    """Cache eviction policies"""
    LRU = "lru"        # Least Recently Used
    LFU = "lfu"        # Least Frequently Used
    FIFO = "fifo"      # First In First Out
    TTL = "ttl"        # Time To Live only


@dataclass
class CacheEntry:
    """Single cache entry"""
    key: str
    value: Any
    created_at: float
    expires_at: Optional[float]
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    size_bytes: int = 0
    tags: List[str] = field(default_factory=list)


@dataclass
class CacheStats:
    """Cache statistics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    sets: int = 0
    deletes: int = 0
    total_bytes: int = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0


class CacheNamespace:
    """
    Cache namespace with isolated entries
    
    Features:
    - TTL-based expiration
    - LRU eviction
    - Tag-based invalidation
    - Size limits
    """
    
    def __init__(
        self,
        name: str,
        max_size: int = 10000,
        default_ttl_seconds: int = 300,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        max_memory_bytes: int = 100 * 1024 * 1024  # 100MB
    ):
        self.name = name
        self.max_size = max_size
        self.default_ttl = default_ttl_seconds
        self.eviction_policy = eviction_policy
        self.max_memory_bytes = max_memory_bytes
        
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()
        
        logger.info(f"Cache namespace '{name}' initialized (max_size={max_size})")
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of value"""
        try:
            return len(json.dumps(value))
        except:
            return len(str(value))
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry has expired"""
        if entry.expires_at is None:
            return False
        return time.time() > entry.expires_at
    
    def _evict_if_needed(self):
        """Evict entries if cache is full"""
        # Check memory limit
        while self._stats.total_bytes > self.max_memory_bytes and self._cache:
            self._evict_one()
        
        # Check size limit
        while len(self._cache) >= self.max_size:
            self._evict_one()
    
    def _evict_one(self):
        """Evict a single entry based on policy"""
        if not self._cache:
            return
        
        if self.eviction_policy == EvictionPolicy.LRU:
            # Remove least recently used (first item in OrderedDict)
            key = next(iter(self._cache))
        
        elif self.eviction_policy == EvictionPolicy.LFU:
            # Remove least frequently used
            key = min(self._cache.keys(), key=lambda k: self._cache[k].access_count)
        
        elif self.eviction_policy == EvictionPolicy.FIFO:
            # Remove oldest
            key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
        
        else:  # TTL - remove expired or oldest
            expired = [k for k, v in self._cache.items() if self._is_expired(v)]
            if expired:
                key = expired[0]
            else:
                key = next(iter(self._cache))
        
        entry = self._cache.pop(key)
        self._stats.total_bytes -= entry.size_bytes
        self._stats.evictions += 1
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache"""
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._stats.misses += 1
                return default
            
            if self._is_expired(entry):
                del self._cache[key]
                self._stats.total_bytes -= entry.size_bytes
                self._stats.expirations += 1
                self._stats.misses += 1
                return default
            
            # Update access stats
            entry.access_count += 1
            entry.last_accessed = time.time()
            
            # Move to end for LRU
            if self.eviction_policy == EvictionPolicy.LRU:
                self._cache.move_to_end(key)
            
            self._stats.hits += 1
            return entry.value
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Set value in cache"""
        with self._lock:
            # Remove existing entry if present
            if key in self._cache:
                old_entry = self._cache.pop(key)
                self._stats.total_bytes -= old_entry.size_bytes
            
            # Evict if needed
            self._evict_if_needed()
            
            # Calculate TTL
            ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
            expires_at = time.time() + ttl if ttl > 0 else None
            
            # Create entry
            size = self._estimate_size(value)
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                expires_at=expires_at,
                size_bytes=size,
                tags=tags or []
            )
            
            self._cache[key] = entry
            self._stats.total_bytes += size
            self._stats.sets += 1
            
            return True
    
    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        with self._lock:
            if key in self._cache:
                entry = self._cache.pop(key)
                self._stats.total_bytes -= entry.size_bytes
                self._stats.deletes += 1
                return True
            return False
    
    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a specific tag"""
        with self._lock:
            keys_to_delete = [
                k for k, v in self._cache.items()
                if tag in v.tags
            ]
            
            for key in keys_to_delete:
                entry = self._cache.pop(key)
                self._stats.total_bytes -= entry.size_bytes
                self._stats.deletes += 1
            
            return len(keys_to_delete)
    
    def clear(self):
        """Clear all entries"""
        with self._lock:
            self._cache.clear()
            self._stats.total_bytes = 0
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries"""
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items()
                if self._is_expired(v)
            ]
            
            for key in expired_keys:
                entry = self._cache.pop(key)
                self._stats.total_bytes -= entry.size_bytes
                self._stats.expirations += 1
            
            return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            return {
                "name": self.name,
                "entries": len(self._cache),
                "max_size": self.max_size,
                "memory_bytes": self._stats.total_bytes,
                "max_memory_bytes": self.max_memory_bytes,
                "hits": self._stats.hits,
                "misses": self._stats.misses,
                "hit_rate": self._stats.hit_rate,
                "evictions": self._stats.evictions,
                "expirations": self._stats.expirations,
                "sets": self._stats.sets,
                "deletes": self._stats.deletes,
                "eviction_policy": self.eviction_policy.value,
                "default_ttl_seconds": self.default_ttl
            }
    
    def keys(self) -> List[str]:
        """Get all keys"""
        with self._lock:
            return list(self._cache.keys())
    
    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired"""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            if self._is_expired(entry):
                return False
            return True


class CacheManager:
    """
    Distributed Cache Manager
    
    Features:
    - Multiple namespaces
    - Cache-aside pattern support
    - Memoization decorator
    - Global invalidation
    """
    
    def __init__(self):
        self._namespaces: Dict[str, CacheNamespace] = {}
        self._lock = threading.RLock()
        self._initialize_default_namespaces()
        
        logger.info("Cache Manager initialized")
    
    def _initialize_default_namespaces(self):
        """Initialize default cache namespaces"""
        defaults = [
            ("emails", 5000, 600, EvictionPolicy.LRU),
            ("classifications", 10000, 300, EvictionPolicy.LRU),
            ("predictions", 5000, 180, EvictionPolicy.TTL),
            ("search_results", 1000, 120, EvictionPolicy.LRU),
            ("analytics", 500, 60, EvictionPolicy.TTL),
            ("responses", 2000, 300, EvictionPolicy.LRU),
            ("entities", 5000, 600, EvictionPolicy.LRU),
            ("security_scans", 2000, 300, EvictionPolicy.LRU),
            ("user_sessions", 10000, 3600, EvictionPolicy.LRU),
            ("api_responses", 5000, 120, EvictionPolicy.TTL),
        ]
        
        for name, max_size, ttl, policy in defaults:
            self.create_namespace(name, max_size, ttl, policy)
    
    def create_namespace(
        self,
        name: str,
        max_size: int = 10000,
        default_ttl: int = 300,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    ) -> CacheNamespace:
        """Create a new cache namespace"""
        with self._lock:
            if name not in self._namespaces:
                self._namespaces[name] = CacheNamespace(
                    name=name,
                    max_size=max_size,
                    default_ttl_seconds=default_ttl,
                    eviction_policy=eviction_policy
                )
            return self._namespaces[name]
    
    def get_namespace(self, name: str) -> Optional[CacheNamespace]:
        """Get a cache namespace"""
        return self._namespaces.get(name)
    
    def get(self, namespace: str, key: str, default: Any = None) -> Any:
        """Get value from namespace"""
        ns = self._namespaces.get(namespace)
        if ns:
            return ns.get(key, default)
        return default
    
    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Set value in namespace"""
        ns = self._namespaces.get(namespace)
        if ns:
            return ns.set(key, value, ttl_seconds, tags)
        return False
    
    def delete(self, namespace: str, key: str) -> bool:
        """Delete value from namespace"""
        ns = self._namespaces.get(namespace)
        if ns:
            return ns.delete(key)
        return False
    
    def cache_aside(
        self,
        namespace: str,
        key: str,
        loader: Callable[[], T],
        ttl_seconds: Optional[int] = None
    ) -> T:
        """
        Cache-aside pattern: get from cache or load and cache
        
        Args:
            namespace: Cache namespace
            key: Cache key
            loader: Function to load data if not cached
            ttl_seconds: Optional TTL override
            
        Returns:
            Cached or freshly loaded value
        """
        # Try cache first
        value = self.get(namespace, key)
        if value is not None:
            return value
        
        # Load and cache
        value = loader()
        self.set(namespace, key, value, ttl_seconds)
        return value
    
    def invalidate_pattern(self, namespace: str, pattern: str) -> int:
        """Invalidate keys matching pattern"""
        ns = self._namespaces.get(namespace)
        if not ns:
            return 0
        
        import fnmatch
        keys_to_delete = [
            k for k in ns.keys()
            if fnmatch.fnmatch(k, pattern)
        ]
        
        count = 0
        for key in keys_to_delete:
            if ns.delete(key):
                count += 1
        
        return count
    
    def invalidate_all(self, namespace: str = None):
        """Clear all caches or specific namespace"""
        with self._lock:
            if namespace:
                ns = self._namespaces.get(namespace)
                if ns:
                    ns.clear()
            else:
                for ns in self._namespaces.values():
                    ns.clear()
    
    def cleanup_all(self) -> Dict[str, int]:
        """Cleanup expired entries in all namespaces"""
        results = {}
        for name, ns in self._namespaces.items():
            results[name] = ns.cleanup_expired()
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all namespaces"""
        stats = {}
        total_hits = 0
        total_misses = 0
        total_entries = 0
        total_memory = 0
        
        for name, ns in self._namespaces.items():
            ns_stats = ns.get_stats()
            stats[name] = ns_stats
            total_hits += ns_stats["hits"]
            total_misses += ns_stats["misses"]
            total_entries += ns_stats["entries"]
            total_memory += ns_stats["memory_bytes"]
        
        return {
            "summary": {
                "total_namespaces": len(self._namespaces),
                "total_entries": total_entries,
                "total_memory_bytes": total_memory,
                "total_memory_mb": total_memory / (1024 * 1024),
                "total_hits": total_hits,
                "total_misses": total_misses,
                "overall_hit_rate": (total_hits / (total_hits + total_misses) * 100) if (total_hits + total_misses) > 0 else 0
            },
            "namespaces": stats
        }


# Memoization decorator
def memoize(
    namespace: str = "default",
    ttl_seconds: int = 300,
    key_builder: Optional[Callable] = None
):
    """
    Decorator for memoizing function results
    
    Usage:
        @memoize("my_cache", ttl_seconds=60)
        def expensive_function(arg1, arg2):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key
            if key_builder:
                cache_key = key_builder(*args, **kwargs)
            else:
                key_parts = [func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
            
            # Try cache
            cache_manager = get_cache_manager()
            cached = cache_manager.get(namespace, cache_key)
            if cached is not None:
                return cached
            
            # Execute and cache
            result = func(*args, **kwargs)
            cache_manager.set(namespace, cache_key, result, ttl_seconds)
            return result
        
        return wrapper
    return decorator


# Global instance
_cache_manager = CacheManager()


def get_cache_manager() -> CacheManager:
    """Get global cache manager"""
    return _cache_manager
