"""
Shared Redis client for caching and real-time data
"""

import redis
import json
import logging
from typing import Any, Optional, Dict
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        self.default_ttl = 3600  # 1 hour default TTL
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in Redis with optional TTL"""
        try:
            serialized_value = json.dumps(value, default=str)
            ttl = ttl or self.default_ttl
            return self.client.setex(key, ttl, serialized_value)
        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from Redis"""
        try:
            value = self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Delete a key from Redis"""
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in Redis"""
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Failed to check existence of key {key}: {e}")
            return False
    
    def set_hash(self, key: str, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set a hash in Redis"""
        try:
            # Serialize all values in the mapping
            serialized_mapping = {k: json.dumps(v, default=str) for k, v in mapping.items()}
            result = self.client.hset(key, mapping=serialized_mapping)
            if ttl:
                self.client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to set hash {key}: {e}")
            return False
    
    def get_hash(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a hash from Redis"""
        try:
            hash_data = self.client.hgetall(key)
            if hash_data:
                return {k: json.loads(v) for k, v in hash_data.items()}
            return None
        except Exception as e:
            logger.error(f"Failed to get hash {key}: {e}")
            return None
    
    def cache_risk_assessment(self, region: str, sector: str, assessment: Dict[str, Any], ttl: int = 1800):
        """Cache risk assessment data (30 minutes TTL)"""
        key = f"risk_assessment:{region}:{sector}"
        return self.set(key, assessment, ttl)
    
    def get_cached_risk_assessment(self, region: str, sector: str) -> Optional[Dict[str, Any]]:
        """Get cached risk assessment"""
        key = f"risk_assessment:{region}:{sector}"
        return self.get(key)
    
    def cache_ml_prediction(self, model_name: str, input_hash: str, prediction: Dict[str, Any], ttl: int = 3600):
        """Cache ML prediction (1 hour TTL)"""
        key = f"ml_prediction:{model_name}:{input_hash}"
        return self.set(key, prediction, ttl)
    
    def get_cached_ml_prediction(self, model_name: str, input_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached ML prediction"""
        key = f"ml_prediction:{model_name}:{input_hash}"
        return self.get(key)
    
    def store_real_time_data(self, data_type: str, data: Dict[str, Any], ttl: int = 300):
        """Store real-time data (5 minutes TTL)"""
        timestamp = datetime.utcnow().isoformat()
        key = f"realtime:{data_type}:{timestamp}"
        return self.set(key, data, ttl)
    
    def get_recent_real_time_data(self, data_type: str, minutes: int = 60) -> list:
        """Get recent real-time data"""
        try:
            pattern = f"realtime:{data_type}:*"
            keys = self.client.keys(pattern)
            
            # Filter keys by timestamp (last N minutes)
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
            recent_data = []
            
            for key in keys:
                timestamp_str = key.split(':')[-1]
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                    if timestamp >= cutoff_time:
                        data = self.get(key)
                        if data:
                            data['timestamp'] = timestamp_str
                            recent_data.append(data)
                except ValueError:
                    continue
            
            return sorted(recent_data, key=lambda x: x['timestamp'], reverse=True)
        except Exception as e:
            logger.error(f"Failed to get recent real-time data: {e}")
            return []
    
    def health_check(self) -> bool:
        """Check Redis connection health"""
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

# Cache key patterns
class CacheKeys:
    RISK_ASSESSMENT = "risk_assessment:{region}:{sector}"
    ML_PREDICTION = "ml_prediction:{model}:{hash}"
    REAL_TIME_DATA = "realtime:{type}:{timestamp}"
    BUSINESS_PROFILE = "business_profile:{id}"
    RECENT_EVENTS = "recent_events:{hours}"
