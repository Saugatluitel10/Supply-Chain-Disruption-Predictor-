"""
Data Collection Service - Enhanced with real-world data sources and processing pipeline
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import asyncio
import logging
import json
import os
import hashlib
import re
from typing import List, Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

# Import shared modules
import sys
sys.path.append('/app/shared')
from database import get_db, SupplyChainEvent
from redis_client import RedisClient
from message_queue import MessageQueue, EventType

# Import data sources
from data_sources import DataSourceOrchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Collection Service", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()
data_orchestrator = DataSourceOrchestrator()

# Initialize scheduler
scheduler = AsyncIOScheduler()

class DataProcessor:
    """Enhanced data processing pipeline with validation and normalization"""
    
    def __init__(self):
        self.processed_hashes = set()  # For duplicate detection
        self.location_mappings = self._load_location_mappings()
        self.sector_mappings = self._load_sector_mappings()
    
    def _load_location_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Load standardized location mappings"""
        return {
            "los angeles": {"standard_name": "Los Angeles", "country": "US", "region": "North America", "coordinates": [34.0522, -118.2437]},
            "shanghai": {"standard_name": "Shanghai", "country": "CN", "region": "Asia", "coordinates": [31.2304, 121.4737]},
            "singapore": {"standard_name": "Singapore", "country": "SG", "region": "Asia", "coordinates": [1.3521, 103.8198]},
            "rotterdam": {"standard_name": "Rotterdam", "country": "NL", "region": "Europe", "coordinates": [51.9244, 4.4777]},
            "hamburg": {"standard_name": "Hamburg", "country": "DE", "region": "Europe", "coordinates": [53.5511, 9.9937]},
            "suez canal": {"standard_name": "Suez Canal", "country": "EG", "region": "Middle East", "coordinates": [30.0444, 32.3499]},
            "panama canal": {"standard_name": "Panama Canal", "country": "PA", "region": "Central America", "coordinates": [9.0820, -79.7737]},
            "china": {"standard_name": "China", "country": "CN", "region": "Asia", "coordinates": [35.8617, 104.1954]},
            "usa": {"standard_name": "United States", "country": "US", "region": "North America", "coordinates": [37.0902, -95.7129]},
            "germany": {"standard_name": "Germany", "country": "DE", "region": "Europe", "coordinates": [51.1657, 10.4515]}
        }
    
    def _load_sector_mappings(self) -> Dict[str, str]:
        """Load standardized sector mappings"""
        return {
            "auto": "automotive",
            "car": "automotive",
            "vehicle": "automotive",
            "chip": "electronics",
            "semiconductor": "electronics",
            "tech": "electronics",
            "oil": "energy",
            "gas": "energy",
            "power": "energy",
            "food": "agriculture",
            "farming": "agriculture",
            "crop": "agriculture",
            "ship": "transportation",
            "port": "transportation",
            "logistics": "transportation",
            "freight": "transportation",
            "factory": "manufacturing",
            "production": "manufacturing",
            "plant": "manufacturing"
        }
    
    def validate_event_data(self, event: Dict[str, Any]) -> bool:
        """Validate event data quality and completeness"""
        required_fields = ["title", "description", "severity"]
        
        # Check required fields
        for field in required_fields:
            if not event.get(field):
                logger.warning(f"Event missing required field: {field}")
                return False
        
        # Validate severity range
        severity = event.get("severity", 0)
        if not isinstance(severity, (int, float)) or not 0 <= severity <= 1:
            logger.warning(f"Invalid severity value: {severity}")
            return False
        
        # Validate title and description length
        if len(event.get("title", "")) < 10 or len(event.get("description", "")) < 20:
            logger.warning("Event title or description too short")
            return False
        
        return True
    
    def detect_duplicate(self, event: Dict[str, Any]) -> bool:
        """Detect duplicate events using content hashing"""
        # Create hash from title, description, and location
        content = f"{event.get('title', '')}{event.get('description', '')}{event.get('location', '')}"
        content_hash = hashlib.md5(content.encode()).hexdigest()
        
        if content_hash in self.processed_hashes:
            return True
        
        self.processed_hashes.add(content_hash)
        return False
    
    def preprocess_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,!?;:-]', '', text)
        
        # Normalize common abbreviations
        abbreviations = {
            r'\bUS\b': 'United States',
            r'\bUK\b': 'United Kingdom',
            r'\bEU\b': 'European Union',
            r'\bCEO\b': 'Chief Executive Officer',
            r'\bGDP\b': 'Gross Domestic Product',
            r'\bCPI\b': 'Consumer Price Index'
        }
        
        for abbrev, full_form in abbreviations.items():
            text = re.sub(abbrev, full_form, text, flags=re.IGNORECASE)
        
        return text
    
    def standardize_location(self, location: str) -> Dict[str, Any]:
        """Standardize location information"""
        if not location:
            return {"standard_name": "", "country": "", "region": "", "coordinates": [0, 0]}
        
        location_lower = location.lower().strip()
        
        # Direct mapping lookup
        if location_lower in self.location_mappings:
            return self.location_mappings[location_lower]
        
        # Fuzzy matching for partial matches
        for key, value in self.location_mappings.items():
            if key in location_lower or location_lower in key:
                return value
        
        # Default fallback
        return {
            "standard_name": location.title(),
            "country": "",
            "region": "",
            "coordinates": [0, 0]
        }
    
    def standardize_sectors(self, sectors: List[str]) -> List[str]:
        """Standardize sector names"""
        if not sectors:
            return []
        
        standardized = set()
        for sector in sectors:
            sector_lower = sector.lower().strip()
            
            # Direct mapping
            if sector_lower in self.sector_mappings:
                standardized.add(self.sector_mappings[sector_lower])
            else:
                # Check for partial matches
                for key, value in self.sector_mappings.items():
                    if key in sector_lower:
                        standardized.add(value)
                        break
                else:
                    # Keep original if no mapping found
                    standardized.add(sector_lower)
        
        return list(standardized)
    
    def align_timestamp(self, event: Dict[str, Any]) -> str:
        """Ensure consistent timestamp format across all events"""
        # Try to extract timestamp from various fields
        timestamp_fields = ['published_at', 'date', 'timestamp', 'created_at']
        
        for field in timestamp_fields:
            if field in event and event[field]:
                try:
                    # Parse various timestamp formats
                    timestamp_str = str(event[field])
                    
                    # ISO format
                    if 'T' in timestamp_str:
                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    # Date only
                    elif '-' in timestamp_str and len(timestamp_str) == 10:
                        dt = datetime.strptime(timestamp_str, '%Y-%m-%d')
                    # Other common formats
                    else:
                        # Try multiple formats
                        formats = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']
                        dt = None
                        for fmt in formats:
                            try:
                                dt = datetime.strptime(timestamp_str, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if dt is None:
                            raise ValueError("Unable to parse timestamp")
                    
                    return dt.isoformat()
                
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse timestamp {event[field]}: {e}")
                    continue
        
        # Default to current time if no valid timestamp found
        return datetime.utcnow().isoformat()
    
    def process_event(self, event: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
        """Process a single event through the complete pipeline"""
        try:
            # Step 1: Validate data quality
            if not self.validate_event_data(event):
                return None
            
            # Step 2: Check for duplicates
            if self.detect_duplicate(event):
                logger.info(f"Duplicate event detected from {source}, skipping")
                return None
            
            # Step 3: Text preprocessing
            event['title'] = self.preprocess_text(event.get('title', ''))
            event['description'] = self.preprocess_text(event.get('description', ''))
            
            # Step 4: Geographic standardization
            location_info = self.standardize_location(event.get('location', ''))
            event['location_standardized'] = location_info
            
            # Step 5: Sector standardization
            sectors = event.get('impact_sectors', [])
            event['impact_sectors'] = self.standardize_sectors(sectors)
            
            # Step 6: Time series alignment
            event['timestamp'] = self.align_timestamp(event)
            
            # Step 7: Add metadata
            event['source'] = source
            event['processed_at'] = datetime.utcnow().isoformat()
            event['data_quality_score'] = self._calculate_quality_score(event)
            
            return event
            
        except Exception as e:
            logger.error(f"Error processing event from {source}: {e}")
            return None
    
    def _calculate_quality_score(self, event: Dict[str, Any]) -> float:
        """Calculate data quality score for the event"""
        score = 0.0
        
        # Title quality (0-0.2)
        title_len = len(event.get('title', ''))
        score += min(0.2, title_len / 100)
        
        # Description quality (0-0.3)
        desc_len = len(event.get('description', ''))
        score += min(0.3, desc_len / 200)
        
        # Location standardization (0-0.2)
        if event.get('location_standardized', {}).get('country'):
            score += 0.2
        
        # Sector identification (0-0.2)
        if event.get('impact_sectors'):
            score += min(0.2, len(event['impact_sectors']) * 0.05)
        
        # Timestamp validity (0-0.1)
        if event.get('timestamp'):
            score += 0.1
        
        return min(1.0, score)

# Initialize data processor
data_processor = DataProcessor()

class ScheduledDataCollector:
    """Automated data collection with optimal scheduling"""
    
    def __init__(self):
        self.collection_stats = {
            "total_collected": 0,
            "last_collection": None,
            "errors": 0,
            "duplicates_filtered": 0
        }
    
    async def collect_and_process_all_data(self):
        """Main data collection and processing workflow"""
        logger.info("Starting scheduled data collection from all sources")
        
        try:
            # Collect data from all sources
            collected_data = await data_orchestrator.collect_all_data()
            
            all_processed_events = []
            
            # Process each data source
            for source_name, events in collected_data.items():
                logger.info(f"Processing {len(events)} events from {source_name}")
                
                for event in events:
                    processed_event = data_processor.process_event(event, source_name)
                    if processed_event:
                        all_processed_events.append(processed_event)
                        
                        # Store in database
                        await self._store_event_in_database(processed_event)
                        
                        # Cache in Redis
                        await self._cache_event_in_redis(processed_event)
                        
                        # Publish to message queue
                        await self._publish_event_to_queue(processed_event)
            
            # Update collection statistics
            self.collection_stats["total_collected"] += len(all_processed_events)
            self.collection_stats["last_collection"] = datetime.utcnow().isoformat()
            
            logger.info(f"Data collection completed. Processed {len(all_processed_events)} events")
            
            return all_processed_events
            
        except Exception as e:
            self.collection_stats["errors"] += 1
            logger.error(f"Error in scheduled data collection: {e}")
            return []
    
    async def _store_event_in_database(self, event: Dict[str, Any]):
        """Store processed event in PostgreSQL database"""
        try:
            db = next(get_db())
            
            db_event = SupplyChainEvent(
                title=event.get('title', ''),
                description=event.get('description', ''),
                event_type=event.get('source', 'unknown'),
                severity=event.get('severity', 0.0),
                location=event.get('location_standardized', {}).get('standard_name', ''),
                impact_sectors=event.get('impact_sectors', []),
                raw_data=event,
                created_at=datetime.fromisoformat(event.get('timestamp', datetime.utcnow().isoformat()))
            )
            
            db.add(db_event)
            db.commit()
            db.refresh(db_event)
            
            # Add database ID to event
            event['database_id'] = str(db_event.id)
            
        except Exception as e:
            logger.error(f"Error storing event in database: {e}")
        finally:
            db.close()
    
    async def _cache_event_in_redis(self, event: Dict[str, Any]):
        """Cache processed event in Redis for fast access"""
        try:
            # Cache by location
            location = event.get('location_standardized', {}).get('standard_name', '')
            if location:
                await redis_client.cache_real_time_data(f"location:{location}", event, ttl=3600)
            
            # Cache by sectors
            for sector in event.get('impact_sectors', []):
                await redis_client.cache_real_time_data(f"sector:{sector}", event, ttl=3600)
            
            # Cache recent events
            await redis_client.cache_real_time_data("recent_events", event, ttl=1800)
            
        except Exception as e:
            logger.error(f"Error caching event in Redis: {e}")
    
    async def _publish_event_to_queue(self, event: Dict[str, Any]):
        """Publish processed event to message queue for downstream processing"""
        try:
            # Determine event type for routing
            severity = event.get('severity', 0.0)
            
            if severity >= 0.8:
                event_type = EventType.HIGH_RISK_DETECTED
            elif severity >= 0.6:
                event_type = EventType.MEDIUM_RISK_DETECTED
            else:
                event_type = EventType.DATA_COLLECTED
            
            await message_queue.publish_event(event_type, event)
            
        except Exception as e:
            logger.error(f"Error publishing event to queue: {e}")

# Initialize scheduled collector
scheduled_collector = ScheduledDataCollector()

# Scheduler configuration
@app.on_event("startup")
async def startup_event():
    """Configure and start the scheduler on application startup"""
    logger.info("Starting Data Collection Service scheduler")
    
    # Schedule high-frequency data collection (every 15 minutes)
    scheduler.add_job(
        scheduled_collector.collect_and_process_all_data,
        trigger=IntervalTrigger(minutes=15),
        id="high_frequency_collection",
        name="High Frequency Data Collection",
        replace_existing=True
    )
    
    # Schedule comprehensive data collection (every 2 hours)
    scheduler.add_job(
        scheduled_collector.collect_and_process_all_data,
        trigger=IntervalTrigger(hours=2),
        id="comprehensive_collection",
        name="Comprehensive Data Collection",
        replace_existing=True
    )
    
    # Schedule daily cleanup and maintenance (at 2 AM)
    scheduler.add_job(
        cleanup_old_data,
        trigger=CronTrigger(hour=2, minute=0),
        id="daily_cleanup",
        name="Daily Data Cleanup",
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown scheduler gracefully"""
    scheduler.shutdown()
    logger.info("Scheduler shutdown completed")

async def cleanup_old_data():
    """Clean up old processed hashes and temporary data"""
    try:
        # Clear old processed hashes (keep last 10000)
        if len(data_processor.processed_hashes) > 10000:
            # Keep only the most recent hashes (simplified approach)
            data_processor.processed_hashes = set(list(data_processor.processed_hashes)[-5000:])
        
        # Clean up old Redis cache entries
        await redis_client.cleanup_expired_data()
        
        logger.info("Daily cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Error during daily cleanup: {e}")

# API Endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    api_status = data_orchestrator.get_api_status()
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "collection_stats": scheduled_collector.collection_stats,
        "api_configurations": api_status,
        "scheduler_status": "running" if scheduler.running else "stopped"
    }

@app.post("/collect")
async def trigger_manual_collection(background_tasks: BackgroundTasks):
    """Manually trigger data collection"""
    background_tasks.add_task(scheduled_collector.collect_and_process_all_data)
    
    return {
        "message": "Data collection triggered",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/sources/status")
async def get_data_sources_status():
    """Get status of all configured data sources"""
    return {
        "configured_sources": data_orchestrator.get_api_status(),
        "last_collection": scheduled_collector.collection_stats.get("last_collection"),
        "total_events_collected": scheduled_collector.collection_stats.get("total_collected", 0),
        "collection_errors": scheduled_collector.collection_stats.get("errors", 0)
    }

@app.get("/events/recent")
async def get_recent_events(limit: int = 50):
    """Get recently collected and processed events"""
    try:
        # Get from Redis cache first
        cached_events = await redis_client.get_cached_data("recent_events")
        
        if cached_events:
            return {
                "events": cached_events[:limit],
                "source": "cache",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # Fallback to database
        db = next(get_db())
        events = db.query(SupplyChainEvent).order_by(SupplyChainEvent.created_at.desc()).limit(limit).all()
        
        event_data = []
        for event in events:
            event_data.append({
                "id": str(event.id),
                "title": event.title,
                "description": event.description,
                "severity": event.severity,
                "location": event.location,
                "impact_sectors": event.impact_sectors,
                "created_at": event.created_at.isoformat(),
                "event_type": event.event_type
            })
        
        return {
            "events": event_data,
            "source": "database",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error retrieving recent events: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve recent events")

@app.get("/stats/collection")
async def get_collection_statistics():
    """Get detailed collection statistics and metrics"""
    try:
        # Get API configuration status
        api_status = data_orchestrator.get_api_status()
        configured_apis = sum(1 for status in api_status.values() if status)
        
        # Get scheduler job information
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            })
        
        return {
            "collection_stats": scheduled_collector.collection_stats,
            "api_configuration": {
                "total_apis": len(api_status),
                "configured_apis": configured_apis,
                "api_details": api_status
            },
            "scheduler_info": {
                "status": "running" if scheduler.running else "stopped",
                "jobs": jobs
            },
            "data_processor_stats": {
                "processed_hashes_count": len(data_processor.processed_hashes),
                "location_mappings": len(data_processor.location_mappings),
                "sector_mappings": len(data_processor.sector_mappings)
            }
        }
        
    except Exception as e:
        logger.error(f"Error retrieving collection statistics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve statistics")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
