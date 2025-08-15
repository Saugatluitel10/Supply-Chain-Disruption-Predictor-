"""
Data Sources Service - External data integration for supply chain intelligence
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import json
import os
from dataclasses import dataclass
import sys

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import SessionLocal, SupplyChainEvent
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Sources Service", version="1.0.0")

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

@dataclass
class DataSourceConfig:
    """Configuration for external data sources"""
    news_api_key: str = os.getenv('NEWS_API_KEY', '')
    openweather_api_key: str = os.getenv('OPENWEATHER_API_KEY', '')
    fred_api_key: str = os.getenv('FRED_API_KEY', '')
    marine_traffic_api_key: str = os.getenv('MARINE_TRAFFIC_API_KEY', '')

class NewsDataCollector:
    """Collect news data from multiple sources"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_urls = {
            'newsapi': 'https://newsapi.org/v2',
            'google_news': 'https://newsapi.org/v2',  # Using NewsAPI for Google News
        }
        
    async def fetch_supply_chain_news(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch supply chain related news"""
        try:
            # Search terms for supply chain disruptions
            search_terms = [
                'supply chain disruption',
                'port congestion',
                'shipping delays',
                'logistics crisis',
                'trade war',
                'manufacturing shortage',
                'semiconductor shortage'
            ]
            
            all_articles = []
            
            for term in search_terms:
                url = f"{self.base_urls['newsapi']}/everything"
                params = {
                    'q': term,
                    'apiKey': self.api_key,
                    'language': 'en',
                    'sortBy': 'publishedAt',
                    'from': (datetime.now() - timedelta(days=7)).isoformat(),
                    'pageSize': 20
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        articles = data.get('articles', [])
                        
                        for article in articles:
                            processed_article = {
                                'source': 'newsapi',
                                'title': article.get('title', ''),
                                'description': article.get('description', ''),
                                'url': article.get('url', ''),
                                'published_at': article.get('publishedAt', ''),
                                'search_term': term,
                                'sentiment_score': None,  # To be analyzed later
                                'relevance_score': None,  # To be calculated
                                'impact_level': 'unknown'
                            }
                            all_articles.append(processed_article)
                    
                    # Rate limiting
                    await asyncio.sleep(0.5)
            
            return all_articles
            
        except Exception as e:
            logger.error(f"Error fetching news data: {e}")
            return []

class WeatherDataCollector:
    """Collect weather data for supply chain impact analysis"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = 'https://api.openweathermap.org/data/2.5'
        
        # Major ports and logistics hubs
        self.critical_locations = [
            {'name': 'Los Angeles Port', 'lat': 33.7361, 'lon': -118.2640},
            {'name': 'Long Beach Port', 'lat': 33.7701, 'lon': -118.2137},
            {'name': 'New York Port', 'lat': 40.6892, 'lon': -74.0445},
            {'name': 'Shanghai Port', 'lat': 31.2304, 'lon': 121.4737},
            {'name': 'Singapore Port', 'lat': 1.2966, 'lon': 103.8764},
            {'name': 'Rotterdam Port', 'lat': 51.9225, 'lon': 4.4792},
            {'name': 'Hamburg Port', 'lat': 53.5511, 'lon': 9.9937},
            {'name': 'Suez Canal', 'lat': 30.5852, 'lon': 32.2654}
        ]
    
    async def fetch_weather_data(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch current weather and forecasts for critical locations"""
        try:
            weather_data = []
            
            for location in self.critical_locations:
                # Current weather
                current_url = f"{self.base_url}/weather"
                current_params = {
                    'lat': location['lat'],
                    'lon': location['lon'],
                    'appid': self.api_key,
                    'units': 'metric'
                }
                
                async with session.get(current_url, params=current_params) as response:
                    if response.status == 200:
                        current_data = await response.json()
                        
                        # Forecast data
                        forecast_url = f"{self.base_url}/forecast"
                        async with session.get(forecast_url, params=current_params) as forecast_response:
                            if forecast_response.status == 200:
                                forecast_data = await forecast_response.json()
                                
                                processed_data = {
                                    'location': location['name'],
                                    'coordinates': {'lat': location['lat'], 'lon': location['lon']},
                                    'current_weather': {
                                        'temperature': current_data['main']['temp'],
                                        'humidity': current_data['main']['humidity'],
                                        'pressure': current_data['main']['pressure'],
                                        'wind_speed': current_data['wind']['speed'],
                                        'wind_direction': current_data['wind'].get('deg', 0),
                                        'weather_condition': current_data['weather'][0]['main'],
                                        'description': current_data['weather'][0]['description'],
                                        'visibility': current_data.get('visibility', 0)
                                    },
                                    'forecast': self._process_forecast(forecast_data),
                                    'risk_assessment': self._assess_weather_risk(current_data, forecast_data),
                                    'timestamp': datetime.now().isoformat()
                                }
                                weather_data.append(processed_data)
                
                # Rate limiting
                await asyncio.sleep(0.2)
            
            return weather_data
            
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            return []
    
    def _process_forecast(self, forecast_data: Dict) -> List[Dict]:
        """Process 5-day forecast data"""
        forecasts = []
        for item in forecast_data.get('list', [])[:10]:  # Next 10 periods (30 hours)
            forecasts.append({
                'datetime': item['dt_txt'],
                'temperature': item['main']['temp'],
                'weather_condition': item['weather'][0]['main'],
                'wind_speed': item['wind']['speed'],
                'precipitation_probability': item.get('pop', 0) * 100
            })
        return forecasts
    
    def _assess_weather_risk(self, current: Dict, forecast: Dict) -> Dict:
        """Assess weather-related supply chain risks"""
        risk_factors = {
            'severe_weather_risk': 'low',
            'wind_risk': 'low',
            'precipitation_risk': 'low',
            'visibility_risk': 'low',
            'overall_risk': 'low'
        }
        
        # Check current conditions
        wind_speed = current['wind']['speed']
        weather_condition = current['weather'][0]['main'].lower()
        visibility = current.get('visibility', 10000)
        
        # Wind risk assessment
        if wind_speed > 15:
            risk_factors['wind_risk'] = 'high'
        elif wind_speed > 10:
            risk_factors['wind_risk'] = 'medium'
        
        # Severe weather check
        if weather_condition in ['thunderstorm', 'snow', 'extreme']:
            risk_factors['severe_weather_risk'] = 'high'
        elif weather_condition in ['rain', 'drizzle']:
            risk_factors['severe_weather_risk'] = 'medium'
        
        # Visibility risk
        if visibility < 1000:
            risk_factors['visibility_risk'] = 'high'
        elif visibility < 5000:
            risk_factors['visibility_risk'] = 'medium'
        
        # Check forecast for upcoming risks
        forecast_items = forecast.get('list', [])[:8]  # Next 24 hours
        for item in forecast_items:
            if item.get('pop', 0) > 0.7:  # High precipitation probability
                risk_factors['precipitation_risk'] = 'high'
                break
            elif item.get('pop', 0) > 0.4:
                risk_factors['precipitation_risk'] = 'medium'
        
        # Overall risk calculation
        high_risks = sum(1 for risk in risk_factors.values() if risk == 'high')
        medium_risks = sum(1 for risk in risk_factors.values() if risk == 'medium')
        
        if high_risks >= 2:
            risk_factors['overall_risk'] = 'high'
        elif high_risks >= 1 or medium_risks >= 2:
            risk_factors['overall_risk'] = 'medium'
        
        return risk_factors

class EconomicDataCollector:
    """Collect economic indicators affecting supply chains"""
    
    def __init__(self, fred_api_key: str):
        self.fred_api_key = fred_api_key
        self.fred_base_url = 'https://api.stlouisfed.org/fred'
        
        # Key economic indicators
        self.indicators = {
            'GDP': 'GDP',
            'unemployment_rate': 'UNRATE',
            'inflation_rate': 'CPIAUCSL',
            'interest_rate': 'FEDFUNDS',
            'dollar_index': 'DTWEXBGS',
            'oil_price': 'DCOILWTICO',
            'baltic_dry_index': 'BDIY',  # Shipping costs indicator
            'manufacturing_pmi': 'NAPM',
            'consumer_confidence': 'UMCSENT'
        }
    
    async def fetch_economic_data(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch economic indicators"""
        try:
            economic_data = []
            
            for indicator_name, series_id in self.indicators.items():
                url = f"{self.fred_base_url}/series/observations"
                params = {
                    'series_id': series_id,
                    'api_key': self.fred_api_key,
                    'file_type': 'json',
                    'limit': 12,  # Last 12 observations
                    'sort_order': 'desc'
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        observations = data.get('observations', [])
                        
                        if observations:
                            latest = observations[0]
                            trend_data = self._calculate_trend(observations)
                            
                            processed_data = {
                                'indicator': indicator_name,
                                'series_id': series_id,
                                'latest_value': float(latest['value']) if latest['value'] != '.' else None,
                                'date': latest['date'],
                                'trend': trend_data,
                                'impact_assessment': self._assess_economic_impact(indicator_name, trend_data),
                                'timestamp': datetime.now().isoformat()
                            }
                            economic_data.append(processed_data)
                
                # Rate limiting for FRED API
                await asyncio.sleep(0.1)
            
            return economic_data
            
        except Exception as e:
            logger.error(f"Error fetching economic data: {e}")
            return []
    
    def _calculate_trend(self, observations: List[Dict]) -> Dict:
        """Calculate trend from historical observations"""
        values = []
        for obs in observations:
            if obs['value'] != '.':
                values.append(float(obs['value']))
        
        if len(values) < 2:
            return {'direction': 'unknown', 'magnitude': 0, 'volatility': 'unknown'}
        
        # Simple trend calculation
        recent_avg = sum(values[:3]) / min(3, len(values))
        older_avg = sum(values[3:6]) / min(3, len(values[3:]))
        
        if older_avg == 0:
            change_pct = 0
        else:
            change_pct = ((recent_avg - older_avg) / older_avg) * 100
        
        direction = 'increasing' if change_pct > 1 else 'decreasing' if change_pct < -1 else 'stable'
        
        # Calculate volatility
        if len(values) >= 3:
            volatility = 'high' if max(values[:3]) - min(values[:3]) > recent_avg * 0.1 else 'low'
        else:
            volatility = 'unknown'
        
        return {
            'direction': direction,
            'magnitude': abs(change_pct),
            'volatility': volatility,
            'change_percentage': change_pct
        }
    
    def _assess_economic_impact(self, indicator: str, trend: Dict) -> Dict:
        """Assess supply chain impact of economic indicators"""
        impact_mapping = {
            'oil_price': {
                'increasing': 'negative',  # Higher costs
                'decreasing': 'positive',  # Lower costs
                'stable': 'neutral'
            },
            'dollar_index': {
                'increasing': 'mixed',  # Affects imports/exports differently
                'decreasing': 'mixed',
                'stable': 'neutral'
            },
            'baltic_dry_index': {
                'increasing': 'negative',  # Higher shipping costs
                'decreasing': 'positive',  # Lower shipping costs
                'stable': 'neutral'
            },
            'manufacturing_pmi': {
                'increasing': 'positive',  # Economic growth
                'decreasing': 'negative',  # Economic slowdown
                'stable': 'neutral'
            }
        }
        
        direction = trend['direction']
        default_impact = {'impact': 'neutral', 'severity': 'low'}
        
        if indicator in impact_mapping:
            impact = impact_mapping[indicator].get(direction, 'neutral')
            severity = 'high' if trend['magnitude'] > 5 else 'medium' if trend['magnitude'] > 2 else 'low'
            return {'impact': impact, 'severity': severity}
        
        return default_impact

class ShippingDataCollector:
    """Collect shipping and logistics data"""
    
    def __init__(self, marine_traffic_api_key: str):
        self.marine_traffic_api_key = marine_traffic_api_key
        self.marine_traffic_base_url = 'https://services.marinetraffic.com/api'
        
        # Major ports for monitoring
        self.major_ports = [
            {'name': 'Los Angeles', 'area_id': 'USLAX'},
            {'name': 'Long Beach', 'area_id': 'USLGB'},
            {'name': 'New York', 'area_id': 'USNYC'},
            {'name': 'Shanghai', 'area_id': 'CNSHA'},
            {'name': 'Singapore', 'area_id': 'SGSIN'},
            {'name': 'Rotterdam', 'area_id': 'NLRTM'}
        ]
    
    async def fetch_shipping_data(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch shipping and port data"""
        try:
            shipping_data = []
            
            # For demo purposes, we'll simulate shipping data
            # In production, you would use actual Marine Traffic API calls
            for port in self.major_ports:
                simulated_data = {
                    'port_name': port['name'],
                    'port_code': port['area_id'],
                    'vessel_count': {
                        'total': 150 + hash(port['name']) % 100,
                        'cargo_ships': 80 + hash(port['name']) % 50,
                        'container_ships': 40 + hash(port['name']) % 30,
                        'tankers': 30 + hash(port['name']) % 20
                    },
                    'congestion_level': self._calculate_congestion_level(port['name']),
                    'average_wait_time': 2.5 + (hash(port['name']) % 10) / 2,  # hours
                    'berth_utilization': 0.75 + (hash(port['name']) % 25) / 100,
                    'recent_arrivals': 25 + hash(port['name']) % 15,
                    'recent_departures': 22 + hash(port['name']) % 12,
                    'risk_assessment': self._assess_port_risk(port['name']),
                    'timestamp': datetime.now().isoformat()
                }
                shipping_data.append(simulated_data)
            
            return shipping_data
            
        except Exception as e:
            logger.error(f"Error fetching shipping data: {e}")
            return []
    
    def _calculate_congestion_level(self, port_name: str) -> str:
        """Calculate port congestion level"""
        # Simulate congestion based on port characteristics
        congestion_score = hash(port_name) % 100
        
        if congestion_score > 80:
            return 'high'
        elif congestion_score > 50:
            return 'medium'
        else:
            return 'low'
    
    def _assess_port_risk(self, port_name: str) -> Dict:
        """Assess port-related supply chain risks"""
        risk_score = hash(port_name) % 100
        
        return {
            'congestion_risk': 'high' if risk_score > 70 else 'medium' if risk_score > 40 else 'low',
            'weather_risk': 'medium' if risk_score % 3 == 0 else 'low',
            'labor_risk': 'high' if risk_score % 7 == 0 else 'low',
            'infrastructure_risk': 'medium' if risk_score % 5 == 0 else 'low',
            'overall_risk': 'high' if risk_score > 80 else 'medium' if risk_score > 50 else 'low'
        }

class DataSourcesOrchestrator:
    """Orchestrate data collection from all sources"""
    
    def __init__(self):
        self.config = DataSourceConfig()
        self.news_collector = NewsDataCollector(self.config.news_api_key)
        self.weather_collector = WeatherDataCollector(self.config.openweather_api_key)
        self.economic_collector = EconomicDataCollector(self.config.fred_api_key)
        self.shipping_collector = ShippingDataCollector(self.config.marine_traffic_api_key)
    
    async def collect_all_data(self) -> Dict[str, Any]:
        """Collect data from all sources"""
        async with aiohttp.ClientSession() as session:
            try:
                # Collect data from all sources concurrently
                news_task = self.news_collector.fetch_supply_chain_news(session)
                weather_task = self.weather_collector.fetch_weather_data(session)
                economic_task = self.economic_collector.fetch_economic_data(session)
                shipping_task = self.shipping_collector.fetch_shipping_data(session)
                
                news_data, weather_data, economic_data, shipping_data = await asyncio.gather(
                    news_task, weather_task, economic_task, shipping_task,
                    return_exceptions=True
                )
                
                # Handle any exceptions
                if isinstance(news_data, Exception):
                    logger.error(f"News data collection failed: {news_data}")
                    news_data = []
                
                if isinstance(weather_data, Exception):
                    logger.error(f"Weather data collection failed: {weather_data}")
                    weather_data = []
                
                if isinstance(economic_data, Exception):
                    logger.error(f"Economic data collection failed: {economic_data}")
                    economic_data = []
                
                if isinstance(shipping_data, Exception):
                    logger.error(f"Shipping data collection failed: {shipping_data}")
                    shipping_data = []
                
                collected_data = {
                    'news': news_data,
                    'weather': weather_data,
                    'economic': economic_data,
                    'shipping': shipping_data,
                    'collection_timestamp': datetime.now().isoformat(),
                    'data_quality': self._assess_data_quality(news_data, weather_data, economic_data, shipping_data)
                }
                
                # Store in Redis for caching
                await self._cache_data(collected_data)
                
                # Publish to message queue for other services
                await self._publish_data_update(collected_data)
                
                return collected_data
                
            except Exception as e:
                logger.error(f"Error in data collection orchestration: {e}")
                return {
                    'news': [],
                    'weather': [],
                    'economic': [],
                    'shipping': [],
                    'collection_timestamp': datetime.now().isoformat(),
                    'error': str(e)
                }
    
    def _assess_data_quality(self, news_data: List, weather_data: List, economic_data: List, shipping_data: List) -> Dict:
        """Assess the quality and completeness of collected data"""
        return {
            'news_data_points': len(news_data),
            'weather_data_points': len(weather_data),
            'economic_data_points': len(economic_data),
            'shipping_data_points': len(shipping_data),
            'completeness_score': min(100, (len(news_data) + len(weather_data) + len(economic_data) + len(shipping_data)) / 4 * 10),
            'freshness': 'current'  # Data is collected in real-time
        }
    
    async def _cache_data(self, data: Dict):
        """Cache collected data in Redis"""
        try:
            cache_key = f"external_data:{datetime.now().strftime('%Y%m%d_%H')}"
            redis_client.set(cache_key, json.dumps(data), ex=3600)  # Cache for 1 hour
            logger.info(f"Data cached with key: {cache_key}")
        except Exception as e:
            logger.error(f"Error caching data: {e}")
    
    async def _publish_data_update(self, data: Dict):
        """Publish data update to message queue"""
        try:
            message = {
                'event_type': 'external_data_update',
                'timestamp': datetime.now().isoformat(),
                'data_summary': {
                    'news_articles': len(data.get('news', [])),
                    'weather_locations': len(data.get('weather', [])),
                    'economic_indicators': len(data.get('economic', [])),
                    'shipping_ports': len(data.get('shipping', []))
                }
            }
            
            message_queue.publish_message(
                exchange=Exchanges.EVENTS,
                routing_key='data.external.update',
                message=message
            )
            logger.info("Published external data update event")
        except Exception as e:
            logger.error(f"Error publishing data update: {e}")

# Initialize orchestrator
data_orchestrator = DataSourcesOrchestrator()

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"service": "Data Sources Service", "status": "healthy", "version": "1.0.0"}

@app.post("/collect/all")
async def collect_all_data(background_tasks: BackgroundTasks):
    """Trigger data collection from all sources"""
    try:
        data = await data_orchestrator.collect_all_data()
        return {
            "status": "success",
            "message": "Data collection completed",
            "summary": data.get('data_quality', {}),
            "timestamp": data.get('collection_timestamp')
        }
    except Exception as e:
        logger.error(f"Error in data collection endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collect/news")
async def collect_news_data():
    """Collect only news data"""
    try:
        async with aiohttp.ClientSession() as session:
            news_data = await data_orchestrator.news_collector.fetch_supply_chain_news(session)
        
        return {
            "status": "success",
            "data": news_data,
            "count": len(news_data),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error collecting news data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collect/weather")
async def collect_weather_data():
    """Collect only weather data"""
    try:
        async with aiohttp.ClientSession() as session:
            weather_data = await data_orchestrator.weather_collector.fetch_weather_data(session)
        
        return {
            "status": "success",
            "data": weather_data,
            "count": len(weather_data),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error collecting weather data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collect/economic")
async def collect_economic_data():
    """Collect only economic data"""
    try:
        async with aiohttp.ClientSession() as session:
            economic_data = await data_orchestrator.economic_collector.fetch_economic_data(session)
        
        return {
            "status": "success",
            "data": economic_data,
            "count": len(economic_data),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error collecting economic data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/collect/shipping")
async def collect_shipping_data():
    """Collect only shipping data"""
    try:
        async with aiohttp.ClientSession() as session:
            shipping_data = await data_orchestrator.shipping_collector.fetch_shipping_data(session)
        
        return {
            "status": "success",
            "data": shipping_data,
            "count": len(shipping_data),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error collecting shipping data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/cached")
async def get_cached_data():
    """Retrieve cached external data"""
    try:
        cache_key = f"external_data:{datetime.now().strftime('%Y%m%d_%H')}"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return {
                "status": "success",
                "data": json.loads(cached_data),
                "source": "cache"
            }
        else:
            return {
                "status": "no_cache",
                "message": "No cached data available for current hour"
            }
    except Exception as e:
        logger.error(f"Error retrieving cached data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
