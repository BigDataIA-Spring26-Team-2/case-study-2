"""
Redis caching service for PE Org-AI-R Platform.
"""
import json
import functools
import hashlib
import redis.asyncio as redis
from typing import Optional, Any, Callable
from datetime import timedelta
import os


class RedisService:
    """Async Redis cache service."""
    
    def __init__(
        self, 
        host: str = os.getenv("REDIS_HOST", "localhost"),
        port: int = int(os.getenv("REDIS_PORT", "6379")),
        db: int = int(os.getenv("REDIS_DB", "0"))
    ):
        self.host = host
        self.port = port
        self.db = db
        self._client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Initialize Redis connection."""
        if not self._client:
            self._client = await redis.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                decode_responses=True
            )
    
    async def disconnect(self):
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get cached value by key."""
        if not self._client:
            await self.connect()
        
        value = await self._client.get(key)
        if value:
            print(f"[CACHE HIT] {key}")
            return json.loads(value)
        print(f"[CACHE MISS] {key}")   
        return None
    
    async def set(
        self, 
        key: str, 
        value: Any, 
        ttl: Optional[int] = 300  # 5 min default
    ) -> bool:
        """Set cache value with optional TTL (seconds)."""
        if not self._client:
            await self.connect()
        
        serialized = json.dumps(value, default=str)
        
        if ttl:
            result= await self._client.setex(key, ttl, serialized)
            print(f"➜ [CACHE SET] {key} (TTL: {ttl}s)")
            return result
        result= await self._client.set(key, serialized)
        print(f"➜ [CACHE SET] {key} (no TTL)")
        return result

    
    async def delete(self, key: str) -> bool:
        """Delete cache key."""
        if not self._client:
            await self.connect()
        
        return await self._client.delete(key) > 0
    
    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern (e.g., 'company:*')."""
        if not self._client:
            await self.connect()
        
        keys = await self._client.keys(pattern)
        if keys:
            count= await self._client.delete(*keys)
            print(f"[CACHE INVALIDATE] {pattern} - deleted {count} keys") 
            return count
        print(f"[CACHE INVALIDATE] {pattern} - no keys found")
        return 0
    
    async def clear_all(self) -> bool:
        """Clear entire cache (use with caution)."""
        if not self._client:
            await self.connect()
        
        return await self._client.flushdb()
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self._client:
            await self.connect()
        
        return await self._client.exists(key) > 0
    
    async def ping(self) -> bool:
        """Health check."""
        try:
            if not self._client:
                await self.connect()
            return await self._client.ping()
        except:
            return False
    
    async def cache_query(key: str, query_func, ttl: int = 300):
        """Manual cache helper for raw SQL queries."""
        cached = await redis_service.get(key)
        if cached is not None:
            print(f"[CACHE HIT] {key}")
            return cached
    
        print(f"[CACHE MISS] {key}")
        result = await query_func()
    
        if result is not None:
            await redis_service.set(key, result, ttl=ttl)
    
        return result


# Global instance
redis_service = RedisService()


# ============================================================================
# DECORATORS
# ============================================================================

def cache(ttl: int = 300, key_prefix: str = None):
    """Cache decorator - handles Pydantic models correctly."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            prefix = key_prefix or func.__name__
            
            args_str = json.dumps([str(a) for a in args[1:]], sort_keys=True, default=str)
            kwargs_str = json.dumps(kwargs, sort_keys=True, default=str)
            key_hash = hashlib.md5(f"{args_str}{kwargs_str}".encode()).hexdigest()[:8]
            
            cache_key = f"{prefix}:{key_hash}"
            
            cached = await redis_service.get(cache_key)
            if cached is not None:
                print(f"[CACHE HIT] {cache_key}")
                return cached
            
            print(f"[CACHE MISS] {cache_key}")
            result = await func(*args, **kwargs)
            
            if result is not None:
                # Convert Pydantic models to dict before caching
                if hasattr(result, 'model_dump'):
                    cache_data = result.model_dump(mode='json')
                elif isinstance(result, list) and result and hasattr(result[0], 'model_dump'):
                    cache_data = [item.model_dump(mode='json') for item in result]
                elif isinstance(result, tuple):
                    # Handle (list, count) tuples from list methods
                    items, count = result
                    if items and hasattr(items[0], 'model_dump'):
                        cache_data = ([item.model_dump(mode='json') for item in items], count)
                    else:
                        cache_data = result
                else:
                    cache_data = result
                
                await redis_service.set(cache_key, cache_data, ttl=ttl)
            
            return result
        
        return wrapper
    return decorator


def invalidate_cache(*patterns: str):
    """
    Invalidate cache decorator - clears cache on mutations.
    
    Usage:
        @invalidate_cache("company:*", "companies:list:*")
        async def update_company(self, company_id: UUID, data):
            # Snowflake update here
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Execute the function first
            result = await func(*args, **kwargs)
            
            # Then invalidate cache patterns
            for pattern in patterns:
                deleted = await redis_service.delete_pattern(pattern)
                print(f"[CACHE INVALIDATE] {pattern} - deleted {deleted} keys")  # Debug
            
            return result
        
        return wrapper
    return decorator