"""
Data Collection Service - Collects news, weather, and economic data
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
import asyncio
import httpx
import logging
from datetime import datetime, timedelta
import json
import hashlib
import sys
import os
from typing import List, Dict, Any

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import SessionLocal, SupplyChainEvent
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Collection Service", version="1.0.0")

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

class DataCollector:
    def __init__(self):
        self.news_sources = [
            "https://newsapi.org/v2/everything",
            "https://api.gdeltproject.org/api/v2/doc/doc"
        ]
        self.weather_apis = [
            "https://api.openweathermap.org/data/2.5/weather",
            "https://api.weatherapi.com/v1/current.json"
        ]
        self.economic_apis = [
            "https://api.worldbank.org/v2/country/all/indicator",
            "https://api.tradingeconomics.com/markets"
        ]
    
    async def collect_news_data(self) -> List[Dict[str, Any]]:
        """Collect supply chain related news"""
        news_data = []
        
        # Simulate news collection (replace with actual API calls)
        supply_chain_keywords = [
            "supply chain disruption", "logistics", "shipping delays",
            "port congestion", "semiconductor shortage", "raw materials",
            "trade war", "sanctions", "factory closure"
        ]
        
        try:
            # Mock news data for demonstration
            mock_news = [
                {
                    "title": "Major Port Strike Affects Global Shipping",
                    "description": "Dock workers at three major ports have gone on strike, potentially disrupting global supply chains for weeks.",
                    "source": "Reuters",
                    "location": "Los Angeles, CA",
                    "severity": 0.8,
                    "impact_sectors": ["automotive", "electronics", "retail"],
                    "url": "https://example.com/news/1"
                },
                {
                    "title": "Semiconductor Factory Fire Causes Production Halt",
                    "description": "A fire at a major semiconductor manufacturing facility has halted production, affecting chip supply globally.",
                    "source": "TechNews",
                    "location": "Taiwan",
                    "severity": 0.9,
                    "impact_sectors": ["electronics", "automotive", "telecommunications"],
                    "url": "https://example.com/news/2"
                },
                {
                    "title": "New Trade Agreement Reduces Tariffs",
                    "description": "A new bilateral trade agreement is expected to reduce supply chain costs for manufacturers.",
                    "source": "Economic Times",
                    "location": "Global",
                    "severity": 0.3,
                    "impact_sectors": ["manufacturing", "retail"],
                    "url": "https://example.com/news/3"
                }
            ]
            
            news_data.extend(mock_news)
            logger.info(f"Collected {len(mock_news)} news items")
            
        except Exception as e:
            logger.error(f"Error collecting news data: {e}")
        
        return news_data
    
    async def collect_weather_data(self) -> List[Dict[str, Any]]:
        """Collect weather data that might affect supply chains"""
        weather_data = []
        
        try:
            # Mock weather data for demonstration
            mock_weather = [
                {
                    "title": "Hurricane Warning Issued for Gulf Coast",
                    "description": "Category 3 hurricane approaching major shipping ports, expected landfall in 48 hours.",
                    "location": "Gulf of Mexico",
                    "severity": 0.85,
                    "impact_sectors": ["oil_gas", "shipping", "agriculture"],
                    "weather_type": "hurricane"
                },
                {
                    "title": "Severe Winter Storm Affecting Transportation",
                    "description": "Blizzard conditions causing widespread transportation delays across the Midwest.",
                    "location": "Midwest USA",
                    "severity": 0.6,
                    "impact_sectors": ["transportation", "agriculture", "retail"],
                    "weather_type": "winter_storm"
                }
            ]
            
            weather_data.extend(mock_weather)
            logger.info(f"Collected {len(mock_weather)} weather events")
            
        except Exception as e:
            logger.error(f"Error collecting weather data: {e}")
        
        return weather_data
    
    async def collect_economic_data(self) -> List[Dict[str, Any]]:
        """Collect economic indicators affecting supply chains"""
        economic_data = []
        
        try:
            # Mock economic data for demonstration
            mock_economic = [
                {
                    "title": "Inflation Rate Reaches 5-Year High",
                    "description": "Consumer price index shows significant increase, affecting raw material costs.",
                    "severity": 0.7,
                    "impact_sectors": ["manufacturing", "retail", "construction"],
                    "indicator_type": "inflation"
                },
                {
                    "title": "Currency Devaluation Affects Import Costs",
                    "description": "Local currency weakens against major trading partners, increasing import costs.",
                    "severity": 0.6,
                    "impact_sectors": ["import_export", "manufacturing"],
                    "indicator_type": "currency"
                }
            ]
            
            economic_data.extend(mock_economic)
            logger.info(f"Collected {len(mock_economic)} economic indicators")
            
        except Exception as e:
            logger.error(f"Error collecting economic data: {e}")
        
        return economic_data
    
    async def store_events(self, events: List[Dict[str, Any]], event_type: str):
        """Store collected events in database and cache"""
        db = SessionLocal()
        try:
            stored_events = []
            
            for event_data in events:
                # Create database record
                event = SupplyChainEvent(
                    event_type=event_type,
                    title=event_data["title"],
                    description=event_data["description"],
                    source=event_data.get("source", "Unknown"),
                    location=event_data.get("location", ""),
                    severity=event_data.get("severity", 0.5),
                    impact_sectors=event_data.get("impact_sectors", []),
                    timestamp=datetime.utcnow(),
                    processed=False
                )
                
                db.add(event)
                db.flush()  # Get the ID
                
                # Store in Redis for real-time access
                redis_client.store_real_time_data(
                    f"event_{event_type}",
                    {
                        "id": str(event.id),
                        "title": event.title,
                        "severity": event.severity,
                        "location": event.location,
                        "impact_sectors": event.impact_sectors
                    }
                )
                
                stored_events.append({
                    "id": str(event.id),
                    "type": event_type,
                    "title": event.title,
                    "severity": event.severity
                })
            
            db.commit()
            
            # Publish event to message queue
            if stored_events:
                message_queue.publish_event(
                    exchange=Exchanges.SUPPLY_CHAIN_EVENTS,
                    routing_key=EventTypes.DATA_COLLECTED,
                    message={
                        "event_type": event_type,
                        "events_count": len(stored_events),
                        "events": stored_events,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            
            logger.info(f"Stored {len(stored_events)} {event_type} events")
            return stored_events
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error storing events: {e}")
            raise
        finally:
            db.close()

# Initialize data collector
data_collector = DataCollector()

@app.get("/")
async def root():
    return {"message": "Data Collection Service", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "data-collector",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/collect")
async def trigger_data_collection(background_tasks: BackgroundTasks):
    """Trigger data collection process"""
    background_tasks.add_task(collect_all_data)
    return {
        "message": "Data collection started",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/collect/news")
async def collect_news(background_tasks: BackgroundTasks):
    """Collect news data"""
    background_tasks.add_task(collect_news_data)
    return {"message": "News collection started"}

@app.post("/collect/weather")
async def collect_weather(background_tasks: BackgroundTasks):
    """Collect weather data"""
    background_tasks.add_task(collect_weather_data)
    return {"message": "Weather collection started"}

@app.post("/collect/economic")
async def collect_economic(background_tasks: BackgroundTasks):
    """Collect economic data"""
    background_tasks.add_task(collect_economic_data)
    return {"message": "Economic data collection started"}

async def collect_all_data():
    """Collect all types of data"""
    try:
        logger.info("Starting comprehensive data collection")
        
        # Collect all data types concurrently
        news_task = asyncio.create_task(data_collector.collect_news_data())
        weather_task = asyncio.create_task(data_collector.collect_weather_data())
        economic_task = asyncio.create_task(data_collector.collect_economic_data())
        
        news_data, weather_data, economic_data = await asyncio.gather(
            news_task, weather_task, economic_task
        )
        
        # Store all collected data
        await data_collector.store_events(news_data, "news")
        await data_collector.store_events(weather_data, "weather")
        await data_collector.store_events(economic_data, "economic")
        
        total_events = len(news_data) + len(weather_data) + len(economic_data)
        logger.info(f"Data collection completed. Total events: {total_events}")
        
    except Exception as e:
        logger.error(f"Error in data collection: {e}")

async def collect_news_data():
    """Background task to collect news data"""
    try:
        news_data = await data_collector.collect_news_data()
        await data_collector.store_events(news_data, "news")
    except Exception as e:
        logger.error(f"Error collecting news data: {e}")

async def collect_weather_data():
    """Background task to collect weather data"""
    try:
        weather_data = await data_collector.collect_weather_data()
        await data_collector.store_events(weather_data, "weather")
    except Exception as e:
        logger.error(f"Error collecting weather data: {e}")

async def collect_economic_data():
    """Background task to collect economic data"""
    try:
        economic_data = await data_collector.collect_economic_data()
        await data_collector.store_events(economic_data, "economic")
    except Exception as e:
        logger.error(f"Error collecting economic data: {e}")

# Start periodic data collection
@app.on_event("startup")
async def startup_event():
    """Start periodic data collection on service startup"""
    logger.info("Data Collection Service starting up")
    
    # Initial data collection
    asyncio.create_task(collect_all_data())
    
    # Schedule periodic collection (every 30 minutes)
    async def periodic_collection():
        while True:
            await asyncio.sleep(1800)  # 30 minutes
            await collect_all_data()
    
    asyncio.create_task(periodic_collection())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
