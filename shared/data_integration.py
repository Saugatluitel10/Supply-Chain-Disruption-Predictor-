"""
Data Integration Client - Interface for external data sources
"""

import aiohttp
import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import os

logger = logging.getLogger(__name__)

@dataclass
class ExternalDataSummary:
    """Summary of external data collection"""
    news_articles: int
    weather_locations: int
    economic_indicators: int
    shipping_ports: int
    collection_timestamp: str
    data_quality_score: float

class DataIntegrationClient:
    """Client for accessing external data sources service"""
    
    def __init__(self, data_sources_url: str = "http://data-sources:8005"):
        self.base_url = data_sources_url
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def collect_all_data(self) -> Dict[str, Any]:
        """Trigger collection of all external data sources"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.post(f"{self.base_url}/collect/all") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Failed to collect all data: {response.status}")
                    return {"status": "error", "message": f"HTTP {response.status}"}
        except Exception as e:
            logger.error(f"Error collecting all data: {e}")
            return {"status": "error", "message": str(e)}
    
    async def get_news_data(self) -> List[Dict]:
        """Get supply chain news data"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.post(f"{self.base_url}/collect/news") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    logger.error(f"Failed to get news data: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error getting news data: {e}")
            return []
    
    async def get_weather_data(self) -> List[Dict]:
        """Get weather data for critical locations"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.post(f"{self.base_url}/collect/weather") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    logger.error(f"Failed to get weather data: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error getting weather data: {e}")
            return []
    
    async def get_economic_data(self) -> List[Dict]:
        """Get economic indicators data"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.post(f"{self.base_url}/collect/economic") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    logger.error(f"Failed to get economic data: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error getting economic data: {e}")
            return []
    
    async def get_shipping_data(self) -> List[Dict]:
        """Get shipping and logistics data"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.post(f"{self.base_url}/collect/shipping") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    logger.error(f"Failed to get shipping data: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error getting shipping data: {e}")
            return []
    
    async def get_cached_data(self) -> Optional[Dict]:
        """Get cached external data"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(f"{self.base_url}/data/cached") as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'success':
                        return data.get('data')
                    else:
                        return None
                else:
                    logger.error(f"Failed to get cached data: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error getting cached data: {e}")
            return None
    
    async def get_data_summary(self) -> ExternalDataSummary:
        """Get summary of available external data"""
        try:
            cached_data = await self.get_cached_data()
            
            if cached_data:
                quality = cached_data.get('data_quality', {})
                return ExternalDataSummary(
                    news_articles=quality.get('news_data_points', 0),
                    weather_locations=quality.get('weather_data_points', 0),
                    economic_indicators=quality.get('economic_data_points', 0),
                    shipping_ports=quality.get('shipping_data_points', 0),
                    collection_timestamp=cached_data.get('collection_timestamp', ''),
                    data_quality_score=quality.get('completeness_score', 0)
                )
            else:
                # If no cached data, trigger collection and return summary
                result = await self.collect_all_data()
                summary = result.get('summary', {})
                return ExternalDataSummary(
                    news_articles=0,
                    weather_locations=0,
                    economic_indicators=0,
                    shipping_ports=0,
                    collection_timestamp=result.get('timestamp', ''),
                    data_quality_score=0
                )
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return ExternalDataSummary(
                news_articles=0,
                weather_locations=0,
                economic_indicators=0,
                shipping_ports=0,
                collection_timestamp=datetime.now().isoformat(),
                data_quality_score=0
            )

class ExternalDataAnalyzer:
    """Analyze external data for supply chain insights"""
    
    @staticmethod
    def analyze_news_sentiment(news_data: List[Dict]) -> Dict[str, Any]:
        """Analyze sentiment and impact of news articles"""
        if not news_data:
            return {"overall_sentiment": "neutral", "risk_indicators": [], "impact_score": 0}
        
        # Simple keyword-based sentiment analysis
        negative_keywords = [
            'disruption', 'delay', 'shortage', 'crisis', 'blockade', 'strike',
            'congestion', 'breakdown', 'failure', 'shortage', 'conflict'
        ]
        
        positive_keywords = [
            'improvement', 'efficiency', 'solution', 'recovery', 'growth',
            'expansion', 'investment', 'innovation', 'agreement'
        ]
        
        sentiment_scores = []
        risk_indicators = []
        
        for article in news_data:
            title = article.get('title', '').lower()
            description = article.get('description', '').lower()
            content = f"{title} {description}"
            
            negative_count = sum(1 for keyword in negative_keywords if keyword in content)
            positive_count = sum(1 for keyword in positive_keywords if keyword in content)
            
            if negative_count > positive_count:
                sentiment_scores.append(-1)
                if negative_count >= 2:  # Multiple negative indicators
                    risk_indicators.append({
                        'source': article.get('source', 'unknown'),
                        'title': article.get('title', ''),
                        'risk_level': 'high' if negative_count >= 3 else 'medium',
                        'keywords_found': [kw for kw in negative_keywords if kw in content]
                    })
            elif positive_count > negative_count:
                sentiment_scores.append(1)
            else:
                sentiment_scores.append(0)
        
        # Calculate overall sentiment
        if sentiment_scores:
            avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
            if avg_sentiment < -0.3:
                overall_sentiment = "negative"
            elif avg_sentiment > 0.3:
                overall_sentiment = "positive"
            else:
                overall_sentiment = "neutral"
        else:
            overall_sentiment = "neutral"
            avg_sentiment = 0
        
        return {
            "overall_sentiment": overall_sentiment,
            "sentiment_score": avg_sentiment,
            "risk_indicators": risk_indicators,
            "impact_score": min(100, max(0, (abs(avg_sentiment) * 100))),
            "articles_analyzed": len(news_data)
        }
    
    @staticmethod
    def analyze_weather_risks(weather_data: List[Dict]) -> Dict[str, Any]:
        """Analyze weather-related supply chain risks"""
        if not weather_data:
            return {"overall_risk": "unknown", "high_risk_locations": [], "risk_score": 0}
        
        high_risk_locations = []
        risk_scores = []
        
        for location_data in weather_data:
            location = location_data.get('location', 'Unknown')
            risk_assessment = location_data.get('risk_assessment', {})
            overall_risk = risk_assessment.get('overall_risk', 'low')
            
            # Convert risk levels to numeric scores
            risk_score_map = {'low': 1, 'medium': 2, 'high': 3}
            location_risk_score = risk_score_map.get(overall_risk, 1)
            risk_scores.append(location_risk_score)
            
            if overall_risk in ['high', 'medium']:
                high_risk_locations.append({
                    'location': location,
                    'risk_level': overall_risk,
                    'risk_factors': risk_assessment,
                    'current_weather': location_data.get('current_weather', {})
                })
        
        # Calculate overall risk
        if risk_scores:
            avg_risk_score = sum(risk_scores) / len(risk_scores)
            if avg_risk_score >= 2.5:
                overall_risk = "high"
            elif avg_risk_score >= 1.5:
                overall_risk = "medium"
            else:
                overall_risk = "low"
        else:
            overall_risk = "unknown"
            avg_risk_score = 0
        
        return {
            "overall_risk": overall_risk,
            "risk_score": (avg_risk_score / 3) * 100,  # Convert to 0-100 scale
            "high_risk_locations": high_risk_locations,
            "locations_analyzed": len(weather_data)
        }
    
    @staticmethod
    def analyze_economic_trends(economic_data: List[Dict]) -> Dict[str, Any]:
        """Analyze economic trends affecting supply chains"""
        if not economic_data:
            return {"trend_analysis": "insufficient_data", "risk_indicators": [], "impact_score": 0}
        
        trend_analysis = {}
        risk_indicators = []
        impact_scores = []
        
        # Key indicators that directly affect supply chains
        critical_indicators = {
            'oil_price': {'weight': 0.3, 'negative_trend': 'increasing'},
            'baltic_dry_index': {'weight': 0.25, 'negative_trend': 'increasing'},
            'dollar_index': {'weight': 0.2, 'negative_trend': 'increasing'},
            'manufacturing_pmi': {'weight': 0.15, 'negative_trend': 'decreasing'},
            'inflation_rate': {'weight': 0.1, 'negative_trend': 'increasing'}
        }
        
        for indicator_data in economic_data:
            indicator = indicator_data.get('indicator', '')
            trend = indicator_data.get('trend', {})
            impact_assessment = indicator_data.get('impact_assessment', {})
            
            if indicator in critical_indicators:
                config = critical_indicators[indicator]
                direction = trend.get('direction', 'stable')
                magnitude = trend.get('magnitude', 0)
                
                # Assess if trend is negative for supply chains
                is_negative_trend = direction == config['negative_trend']
                
                if is_negative_trend and magnitude > 5:  # Significant change
                    risk_indicators.append({
                        'indicator': indicator,
                        'trend_direction': direction,
                        'magnitude': magnitude,
                        'impact': impact_assessment.get('impact', 'neutral'),
                        'severity': impact_assessment.get('severity', 'low')
                    })
                    
                    # Calculate weighted impact score
                    severity_multiplier = {'low': 1, 'medium': 2, 'high': 3}
                    impact_score = (magnitude * config['weight'] * 
                                  severity_multiplier.get(impact_assessment.get('severity', 'low'), 1))
                    impact_scores.append(impact_score)
                
                trend_analysis[indicator] = {
                    'direction': direction,
                    'magnitude': magnitude,
                    'impact_on_supply_chain': 'negative' if is_negative_trend else 'positive'
                }
        
        # Calculate overall impact
        overall_impact_score = sum(impact_scores) if impact_scores else 0
        
        if overall_impact_score > 15:
            trend_summary = "high_negative_impact"
        elif overall_impact_score > 8:
            trend_summary = "moderate_negative_impact"
        elif overall_impact_score > 3:
            trend_summary = "low_negative_impact"
        else:
            trend_summary = "stable_conditions"
        
        return {
            "trend_analysis": trend_summary,
            "detailed_trends": trend_analysis,
            "risk_indicators": risk_indicators,
            "impact_score": min(100, overall_impact_score * 5),  # Scale to 0-100
            "indicators_analyzed": len(economic_data)
        }
    
    @staticmethod
    def analyze_shipping_congestion(shipping_data: List[Dict]) -> Dict[str, Any]:
        """Analyze shipping congestion and logistics risks"""
        if not shipping_data:
            return {"congestion_status": "unknown", "congested_ports": [], "risk_score": 0}
        
        congested_ports = []
        congestion_scores = []
        
        for port_data in shipping_data:
            port_name = port_data.get('port_name', 'Unknown')
            congestion_level = port_data.get('congestion_level', 'low')
            wait_time = port_data.get('average_wait_time', 0)
            berth_utilization = port_data.get('berth_utilization', 0)
            risk_assessment = port_data.get('risk_assessment', {})
            
            # Calculate congestion score
            congestion_score_map = {'low': 1, 'medium': 2, 'high': 3}
            base_score = congestion_score_map.get(congestion_level, 1)
            
            # Adjust score based on wait time and berth utilization
            if wait_time > 5:  # More than 5 hours wait
                base_score += 1
            if berth_utilization > 0.9:  # Over 90% utilization
                base_score += 1
            
            congestion_scores.append(min(5, base_score))  # Cap at 5
            
            if congestion_level in ['high', 'medium'] or wait_time > 3:
                congested_ports.append({
                    'port_name': port_name,
                    'congestion_level': congestion_level,
                    'wait_time_hours': wait_time,
                    'berth_utilization': berth_utilization,
                    'risk_factors': risk_assessment
                })
        
        # Calculate overall congestion status
        if congestion_scores:
            avg_congestion = sum(congestion_scores) / len(congestion_scores)
            if avg_congestion >= 3.5:
                congestion_status = "high_congestion"
            elif avg_congestion >= 2.5:
                congestion_status = "moderate_congestion"
            else:
                congestion_status = "normal_operations"
        else:
            congestion_status = "unknown"
            avg_congestion = 0
        
        return {
            "congestion_status": congestion_status,
            "congested_ports": congested_ports,
            "risk_score": (avg_congestion / 5) * 100,  # Convert to 0-100 scale
            "ports_analyzed": len(shipping_data)
        }

# Convenience function for getting comprehensive external data analysis
async def get_comprehensive_external_analysis() -> Dict[str, Any]:
    """Get comprehensive analysis of all external data sources"""
    try:
        async with DataIntegrationClient() as client:
            # Get all data
            news_data = await client.get_news_data()
            weather_data = await client.get_weather_data()
            economic_data = await client.get_economic_data()
            shipping_data = await client.get_shipping_data()
            
            # Analyze each data source
            analyzer = ExternalDataAnalyzer()
            
            news_analysis = analyzer.analyze_news_sentiment(news_data)
            weather_analysis = analyzer.analyze_weather_risks(weather_data)
            economic_analysis = analyzer.analyze_economic_trends(economic_data)
            shipping_analysis = analyzer.analyze_shipping_congestion(shipping_data)
            
            # Calculate overall risk score
            risk_components = [
                news_analysis.get('impact_score', 0) * 0.25,
                weather_analysis.get('risk_score', 0) * 0.25,
                economic_analysis.get('impact_score', 0) * 0.3,
                shipping_analysis.get('risk_score', 0) * 0.2
            ]
            
            overall_risk_score = sum(risk_components)
            
            if overall_risk_score >= 70:
                overall_risk_level = "high"
            elif overall_risk_score >= 40:
                overall_risk_level = "medium"
            else:
                overall_risk_level = "low"
            
            return {
                "overall_risk_level": overall_risk_level,
                "overall_risk_score": overall_risk_score,
                "news_analysis": news_analysis,
                "weather_analysis": weather_analysis,
                "economic_analysis": economic_analysis,
                "shipping_analysis": shipping_analysis,
                "data_freshness": datetime.now().isoformat(),
                "analysis_summary": {
                    "total_news_articles": len(news_data),
                    "weather_locations_monitored": len(weather_data),
                    "economic_indicators_tracked": len(economic_data),
                    "ports_monitored": len(shipping_data)
                }
            }
    
    except Exception as e:
        logger.error(f"Error in comprehensive external analysis: {e}")
        return {
            "overall_risk_level": "unknown",
            "overall_risk_score": 0,
            "error": str(e),
            "data_freshness": datetime.now().isoformat()
        }
