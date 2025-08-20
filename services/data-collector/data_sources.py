"""
Real-world data sources integration for supply chain monitoring
"""

import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class DataSourceConfig:
    """Configuration for data sources"""
    news_api_key: str = os.getenv("NEWS_API_KEY", "")
    openweather_api_key: str = os.getenv("OPENWEATHER_API_KEY", "")
    fred_api_key: str = os.getenv("FRED_API_KEY", "")
    trading_economics_key: str = os.getenv("TRADING_ECONOMICS_KEY", "")
    marine_traffic_key: str = os.getenv("MARINE_TRAFFIC_KEY", "")

class NewsDataSource:
    """News data source integration"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.supply_chain_keywords = [
            "supply chain", "logistics", "shipping", "port", "container",
            "semiconductor", "chip shortage", "raw materials", "trade war",
            "sanctions", "factory", "manufacturing", "disruption", "bottleneck",
            "freight", "cargo", "warehouse", "inventory", "supplier"
        ]
    
    async def collect_newsapi_data(self) -> List[Dict[str, Any]]:
        """Collect data from NewsAPI"""
        if not self.config.news_api_key:
            logger.warning("NewsAPI key not configured")
            return []
        
        try:
            async with httpx.AsyncClient() as client:
                # Search for supply chain related news
                query = " OR ".join(self.supply_chain_keywords[:5])  # Limit query length
                
                response = await client.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q": query,
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 50,
                        "from": (datetime.utcnow() - timedelta(days=1)).isoformat(),
                        "apiKey": self.config.news_api_key
                    },
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get("articles", [])
                    
                    processed_articles = []
                    for article in articles:
                        processed_article = {
                            "title": article.get("title", ""),
                            "description": article.get("description", ""),
                            "source": article.get("source", {}).get("name", "NewsAPI"),
                            "location": self._extract_location(article.get("content", "")),
                            "severity": self._calculate_news_severity(article),
                            "impact_sectors": self._identify_sectors(article),
                            "url": article.get("url", ""),
                            "published_at": article.get("publishedAt", ""),
                            "raw_data": article
                        }
                        processed_articles.append(processed_article)
                    
                    logger.info(f"Collected {len(processed_articles)} articles from NewsAPI")
                    return processed_articles
                else:
                    logger.error(f"NewsAPI request failed: {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error collecting NewsAPI data: {e}")
            return []
    
    async def collect_google_news_data(self) -> List[Dict[str, Any]]:
        """Collect data from Google News RSS"""
        try:
            async with httpx.AsyncClient() as client:
                processed_articles = []
                
                for keyword in self.supply_chain_keywords[:3]:  # Limit requests
                    response = await client.get(
                        f"https://news.google.com/rss/search",
                        params={"q": keyword, "hl": "en", "gl": "US", "ceid": "US:en"},
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        # Parse RSS XML
                        root = ET.fromstring(response.content)
                        
                        for item in root.findall(".//item")[:10]:  # Limit per keyword
                            title = item.find("title").text if item.find("title") is not None else ""
                            description = item.find("description").text if item.find("description") is not None else ""
                            link = item.find("link").text if item.find("link") is not None else ""
                            pub_date = item.find("pubDate").text if item.find("pubDate") is not None else ""
                            
                            article = {
                                "title": title,
                                "description": description,
                                "source": "Google News",
                                "location": self._extract_location(description),
                                "severity": self._calculate_news_severity({"title": title, "description": description}),
                                "impact_sectors": self._identify_sectors({"title": title, "description": description}),
                                "url": link,
                                "published_at": pub_date,
                                "keyword": keyword
                            }
                            processed_articles.append(article)
                
                logger.info(f"Collected {len(processed_articles)} articles from Google News")
                return processed_articles
                
        except Exception as e:
            logger.error(f"Error collecting Google News data: {e}")
            return []
    
    def _calculate_news_severity(self, article: Dict[str, Any]) -> float:
        """Calculate severity score based on article content"""
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()
        
        high_severity_terms = [
            "crisis", "emergency", "shutdown", "collapse", "disaster", "critical",
            "severe", "major disruption", "widespread", "catastrophic", "halt",
            "suspended", "closed", "blocked", "strike", "war", "conflict"
        ]
        
        medium_severity_terms = [
            "delay", "shortage", "disruption", "impact", "affected", "reduced",
            "limited", "concern", "warning", "risk", "challenge", "problem"
        ]
        
        severity = 0.3  # Base severity
        
        for term in high_severity_terms:
            if term in text:
                severity += 0.15
        
        for term in medium_severity_terms:
            if term in text:
                severity += 0.08
        
        return min(1.0, severity)
    
    def _identify_sectors(self, article: Dict[str, Any]) -> List[str]:
        """Identify affected sectors from article content"""
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()
        
        sector_keywords = {
            "automotive": ["car", "auto", "vehicle", "toyota", "ford", "gm", "tesla"],
            "electronics": ["chip", "semiconductor", "electronics", "apple", "samsung", "intel"],
            "energy": ["oil", "gas", "energy", "power", "electricity", "renewable"],
            "agriculture": ["food", "agriculture", "farming", "crop", "grain", "livestock"],
            "retail": ["retail", "consumer", "shopping", "walmart", "amazon", "store"],
            "manufacturing": ["factory", "plant", "production", "manufacturing", "industrial"],
            "transportation": ["shipping", "logistics", "freight", "cargo", "port", "airline"],
            "pharmaceuticals": ["drug", "medicine", "pharmaceutical", "vaccine", "medical"]
        }
        
        identified_sectors = []
        for sector, keywords in sector_keywords.items():
            if any(keyword in text for keyword in keywords):
                identified_sectors.append(sector)
        
        return identified_sectors
    
    def _extract_location(self, text: str) -> str:
        """Extract location information from text"""
        if not text:
            return ""
        
        # Common location patterns
        locations = [
            "china", "taiwan", "japan", "korea", "singapore", "vietnam", "thailand",
            "germany", "france", "italy", "spain", "uk", "netherlands", "poland",
            "usa", "canada", "mexico", "brazil", "argentina",
            "suez canal", "panama canal", "strait of hormuz", "malacca strait",
            "los angeles", "long beach", "shanghai", "rotterdam", "hamburg"
        ]
        
        text_lower = text.lower()
        for location in locations:
            if location in text_lower:
                return location.title()
        
        return ""

class WeatherDataSource:
    """Weather data source integration"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.critical_ports = [
            {"name": "Los Angeles", "lat": 33.7701, "lon": -118.1937},
            {"name": "Long Beach", "lat": 33.7701, "lon": -118.1937},
            {"name": "Shanghai", "lat": 31.2304, "lon": 121.4737},
            {"name": "Singapore", "lat": 1.2966, "lon": 103.7764},
            {"name": "Rotterdam", "lat": 51.9225, "lon": 4.4792},
            {"name": "Hamburg", "lat": 53.5511, "lon": 9.9937},
            {"name": "Antwerp", "lat": 51.2194, "lon": 4.4025},
            {"name": "Suez Canal", "lat": 30.0444, "lon": 32.3499}
        ]
    
    async def collect_weather_data(self) -> List[Dict[str, Any]]:
        """Collect weather data from OpenWeatherMap"""
        if not self.config.openweather_api_key:
            logger.warning("OpenWeatherMap API key not configured")
            return []
        
        try:
            weather_events = []
            
            async with httpx.AsyncClient() as client:
                for port in self.critical_ports:
                    # Current weather
                    current_response = await client.get(
                        "https://api.openweathermap.org/data/2.5/weather",
                        params={
                            "lat": port["lat"],
                            "lon": port["lon"],
                            "appid": self.config.openweather_api_key,
                            "units": "metric"
                        },
                        timeout=30.0
                    )
                    
                    if current_response.status_code == 200:
                        current_data = current_response.json()
                        
                        # Check for severe weather conditions
                        weather_event = self._process_weather_data(current_data, port["name"])
                        if weather_event:
                            weather_events.append(weather_event)
                    
                    # Weather alerts
                    alerts_response = await client.get(
                        "https://api.openweathermap.org/data/2.5/onecall",
                        params={
                            "lat": port["lat"],
                            "lon": port["lon"],
                            "appid": self.config.openweather_api_key,
                            "exclude": "minutely,hourly,daily"
                        },
                        timeout=30.0
                    )
                    
                    if alerts_response.status_code == 200:
                        alerts_data = alerts_response.json()
                        alerts = alerts_data.get("alerts", [])
                        
                        for alert in alerts:
                            weather_event = {
                                "title": f"Weather Alert: {alert.get('event', 'Unknown')} - {port['name']}",
                                "description": alert.get("description", ""),
                                "location": port["name"],
                                "severity": self._calculate_weather_severity(alert),
                                "impact_sectors": ["transportation", "shipping", "logistics"],
                                "weather_type": alert.get("event", "").lower(),
                                "start_time": datetime.fromtimestamp(alert.get("start", 0)).isoformat(),
                                "end_time": datetime.fromtimestamp(alert.get("end", 0)).isoformat(),
                                "raw_data": alert
                            }
                            weather_events.append(weather_event)
                    
                    # Small delay to respect API limits
                    await asyncio.sleep(0.1)
            
            logger.info(f"Collected {len(weather_events)} weather events")
            return weather_events
            
        except Exception as e:
            logger.error(f"Error collecting weather data: {e}")
            return []
    
    def _process_weather_data(self, data: Dict[str, Any], location: str) -> Optional[Dict[str, Any]]:
        """Process current weather data for severe conditions"""
        weather = data.get("weather", [{}])[0]
        main = data.get("main", {})
        wind = data.get("wind", {})
        
        weather_id = weather.get("id", 0)
        wind_speed = wind.get("speed", 0)
        
        # Check for severe weather conditions
        severe_conditions = []
        severity = 0.0
        
        # Thunderstorms (200-299)
        if 200 <= weather_id < 300:
            severe_conditions.append("thunderstorm")
            severity = max(severity, 0.7)
        
        # Snow (600-699)
        elif 600 <= weather_id < 700:
            severe_conditions.append("snow")
            severity = max(severity, 0.6)
        
        # Extreme weather (900+)
        elif weather_id >= 900:
            severe_conditions.append("extreme weather")
            severity = max(severity, 0.9)
        
        # High wind speeds (> 15 m/s = ~34 mph)
        if wind_speed > 15:
            severe_conditions.append("high winds")
            severity = max(severity, 0.6)
        
        # Very high wind speeds (> 25 m/s = ~56 mph)
        if wind_speed > 25:
            severity = max(severity, 0.8)
        
        if severe_conditions and severity > 0.5:
            return {
                "title": f"Severe Weather: {', '.join(severe_conditions).title()} - {location}",
                "description": f"Current conditions: {weather.get('description', '')}. Wind speed: {wind_speed} m/s",
                "location": location,
                "severity": severity,
                "impact_sectors": ["transportation", "shipping", "logistics", "agriculture"],
                "weather_type": weather.get("main", "").lower(),
                "temperature": main.get("temp", 0),
                "wind_speed": wind_speed,
                "raw_data": data
            }
        
        return None
    
    def _calculate_weather_severity(self, alert: Dict[str, Any]) -> float:
        """Calculate weather alert severity"""
        event = alert.get("event", "").lower()
        
        high_severity_events = [
            "hurricane", "typhoon", "cyclone", "tornado", "blizzard",
            "ice storm", "severe thunderstorm", "flash flood"
        ]
        
        medium_severity_events = [
            "winter storm", "heavy snow", "high wind", "flood",
            "heat wave", "cold wave", "fog"
        ]
        
        if any(term in event for term in high_severity_events):
            return 0.9
        elif any(term in event for term in medium_severity_events):
            return 0.7
        else:
            return 0.5

class EconomicDataSource:
    """Economic indicators data source"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.economic_indicators = [
            {"series_id": "GDPC1", "name": "GDP", "impact": "high"},
            {"series_id": "CPIAUCSL", "name": "Consumer Price Index", "impact": "high"},
            {"series_id": "UNRATE", "name": "Unemployment Rate", "impact": "medium"},
            {"series_id": "DEXUSEU", "name": "USD/EUR Exchange Rate", "impact": "high"},
            {"series_id": "DEXCHUS", "name": "USD/CNY Exchange Rate", "impact": "high"},
            {"series_id": "DCOILWTICO", "name": "Crude Oil Prices", "impact": "high"},
            {"series_id": "GOLDAMGBD228NLBM", "name": "Gold Prices", "impact": "medium"}
        ]
    
    async def collect_fred_data(self) -> List[Dict[str, Any]]:
        """Collect data from Federal Reserve Economic Data (FRED)"""
        if not self.config.fred_api_key:
            logger.warning("FRED API key not configured")
            return []
        
        try:
            economic_events = []
            
            async with httpx.AsyncClient() as client:
                for indicator in self.economic_indicators:
                    response = await client.get(
                        "https://api.stlouisfed.org/fred/series/observations",
                        params={
                            "series_id": indicator["series_id"],
                            "api_key": self.config.fred_api_key,
                            "file_type": "json",
                            "limit": 10,
                            "sort_order": "desc"
                        },
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        observations = data.get("observations", [])
                        
                        if len(observations) >= 2:
                            latest = observations[0]
                            previous = observations[1]
                            
                            # Calculate change
                            try:
                                latest_value = float(latest.get("value", 0))
                                previous_value = float(previous.get("value", 0))
                                
                                if previous_value != 0:
                                    change_percent = ((latest_value - previous_value) / previous_value) * 100
                                    
                                    # Generate event if significant change
                                    if abs(change_percent) > 2.0:  # 2% threshold
                                        economic_event = {
                                            "title": f"Economic Indicator Alert: {indicator['name']}",
                                            "description": f"{indicator['name']} changed by {change_percent:.2f}% from {previous_value} to {latest_value}",
                                            "severity": self._calculate_economic_severity(change_percent, indicator["impact"]),
                                            "impact_sectors": self._get_economic_impact_sectors(indicator["series_id"]),
                                            "indicator_name": indicator["name"],
                                            "series_id": indicator["series_id"],
                                            "current_value": latest_value,
                                            "previous_value": previous_value,
                                            "change_percent": change_percent,
                                            "date": latest.get("date", ""),
                                            "raw_data": {"latest": latest, "previous": previous}
                                        }
                                        economic_events.append(economic_event)
                            except (ValueError, TypeError):
                                continue
                    
                    # Small delay to respect API limits
                    await asyncio.sleep(0.2)
            
            logger.info(f"Collected {len(economic_events)} economic indicators")
            return economic_events
            
        except Exception as e:
            logger.error(f"Error collecting FRED data: {e}")
            return []
    
    def _calculate_economic_severity(self, change_percent: float, impact_level: str) -> float:
        """Calculate economic event severity"""
        base_severity = abs(change_percent) / 100  # Convert percentage to decimal
        
        impact_multipliers = {
            "high": 1.5,
            "medium": 1.0,
            "low": 0.7
        }
        
        multiplier = impact_multipliers.get(impact_level, 1.0)
        severity = min(1.0, base_severity * multiplier)
        
        return max(0.3, severity)  # Minimum severity of 0.3
    
    def _get_economic_impact_sectors(self, series_id: str) -> List[str]:
        """Get sectors impacted by economic indicator"""
        sector_mapping = {
            "GDPC1": ["manufacturing", "retail", "finance", "construction"],
            "CPIAUCSL": ["retail", "manufacturing", "agriculture", "energy"],
            "UNRATE": ["retail", "manufacturing", "construction", "finance"],
            "DEXUSEU": ["import_export", "manufacturing", "retail"],
            "DEXCHUS": ["import_export", "manufacturing", "electronics"],
            "DCOILWTICO": ["energy", "transportation", "manufacturing", "agriculture"],
            "GOLDAMGBD228NLBM": ["finance", "manufacturing", "electronics"]
        }
        
        return sector_mapping.get(series_id, ["general"])

class ShippingDataSource:
    """Shipping and logistics data source"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.major_ports = [
            {"name": "Los Angeles", "country": "US", "mmsi_area": "366"},
            {"name": "Long Beach", "country": "US", "mmsi_area": "366"},
            {"name": "Shanghai", "country": "CN", "mmsi_area": "412"},
            {"name": "Singapore", "country": "SG", "mmsi_area": "563"},
            {"name": "Rotterdam", "country": "NL", "mmsi_area": "244"},
            {"name": "Hamburg", "country": "DE", "mmsi_area": "211"}
        ]
    
    async def collect_marine_traffic_data(self) -> List[Dict[str, Any]]:
        """Collect shipping data from Marine Traffic API"""
        if not self.config.marine_traffic_key:
            logger.warning("Marine Traffic API key not configured")
            return []
        
        try:
            shipping_events = []
            
            async with httpx.AsyncClient() as client:
                # Get port congestion data (simplified approach)
                for port in self.major_ports:
                    # This is a simplified example - actual Marine Traffic API has different endpoints
                    response = await client.get(
                        "https://services.marinetraffic.com/api/exportvessels/v:8",
                        params={
                            "key": self.config.marine_traffic_key,
                            "timespan": 60,  # Last 60 minutes
                            "mmsi": port["mmsi_area"] + "000000",  # Simplified MMSI pattern
                            "protocol": "json"
                        },
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        vessels = data if isinstance(data, list) else []
                        
                        # Analyze vessel density for congestion
                        if len(vessels) > 50:  # Threshold for congestion
                            shipping_event = {
                                "title": f"Port Congestion Alert: {port['name']}",
                                "description": f"High vessel density detected at {port['name']} port with {len(vessels)} vessels in area",
                                "location": port["name"],
                                "severity": min(0.9, len(vessels) / 100),  # Scale based on vessel count
                                "impact_sectors": ["shipping", "logistics", "manufacturing", "retail"],
                                "port_name": port["name"],
                                "vessel_count": len(vessels),
                                "country": port["country"],
                                "raw_data": {"vessel_count": len(vessels), "port_info": port}
                            }
                            shipping_events.append(shipping_event)
                    
                    # Delay to respect API limits
                    await asyncio.sleep(1.0)
            
            logger.info(f"Collected {len(shipping_events)} shipping events")
            return shipping_events
            
        except Exception as e:
            logger.error(f"Error collecting Marine Traffic data: {e}")
            return []

class DataSourceOrchestrator:
    """Orchestrates all data sources"""
    
    def __init__(self):
        self.config = DataSourceConfig()
        self.news_source = NewsDataSource(self.config)
        self.weather_source = WeatherDataSource(self.config)
        self.economic_source = EconomicDataSource(self.config)
        self.shipping_source = ShippingDataSource(self.config)
    
    async def collect_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Collect data from all sources concurrently"""
        logger.info("Starting comprehensive data collection from all sources")
        
        try:
            # Run all data collection tasks concurrently
            tasks = [
                self.news_source.collect_newsapi_data(),
                self.news_source.collect_google_news_data(),
                self.weather_source.collect_weather_data(),
                self.economic_source.collect_fred_data(),
                self.shipping_source.collect_marine_traffic_data()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            collected_data = {
                "news_api": results[0] if not isinstance(results[0], Exception) else [],
                "google_news": results[1] if not isinstance(results[1], Exception) else [],
                "weather": results[2] if not isinstance(results[2], Exception) else [],
                "economic": results[3] if not isinstance(results[3], Exception) else [],
                "shipping": results[4] if not isinstance(results[4], Exception) else []
            }
            
            # Log any exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    source_names = ["news_api", "google_news", "weather", "economic", "shipping"]
                    logger.error(f"Error in {source_names[i]} collection: {result}")
            
            total_events = sum(len(events) for events in collected_data.values())
            logger.info(f"Data collection completed. Total events collected: {total_events}")
            
            return collected_data
            
        except Exception as e:
            logger.error(f"Error in data collection orchestration: {e}")
            return {
                "news_api": [],
                "google_news": [],
                "weather": [],
                "economic": [],
                "shipping": []
            }
    
    def get_api_status(self) -> Dict[str, bool]:
        """Check which APIs are configured"""
        return {
            "news_api": bool(self.config.news_api_key),
            "openweather": bool(self.config.openweather_api_key),
            "fred": bool(self.config.fred_api_key),
            "trading_economics": bool(self.config.trading_economics_key),
            "marine_traffic": bool(self.config.marine_traffic_key)
        }
