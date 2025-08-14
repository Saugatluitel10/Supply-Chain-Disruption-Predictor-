"""
Data Collector Module - Gathers data from various sources
"""

import requests
import feedparser
import os
import json
from datetime import datetime, timedelta
from textblob import TextBlob
import yfinance as yf
from newsapi import NewsApiClient
import re

class DataCollector:
    def __init__(self):
        self.news_api_key = os.getenv('NEWS_API_KEY')
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
        # Initialize News API client if key is available
        self.news_client = NewsApiClient(api_key=self.news_api_key) if self.news_api_key else None
        
        # Supply chain related keywords
        self.supply_chain_keywords = [
            'supply chain', 'logistics', 'shipping', 'port congestion', 'freight',
            'container shortage', 'semiconductor shortage', 'raw materials',
            'manufacturing delay', 'trade disruption', 'customs delay',
            'factory closure', 'strike', 'labor shortage', 'fuel shortage'
        ]
        
        # Critical regions for supply chain
        self.critical_regions = [
            'China', 'Taiwan', 'South Korea', 'Japan', 'Singapore',
            'Germany', 'Netherlands', 'United States', 'Mexico',
            'Suez Canal', 'Panama Canal', 'Strait of Hormuz'
        ]

    def collect_news(self):
        """Collect supply chain related news"""
        news_data = []
        
        try:
            # Method 1: Use News API if available
            if self.news_client:
                news_data.extend(self._collect_from_news_api())
            
            # Method 2: RSS feeds as backup
            news_data.extend(self._collect_from_rss_feeds())
            
            # Process and score news items
            processed_news = []
            for item in news_data:
                processed_item = self._process_news_item(item)
                if processed_item['severity'] > 0.3:  # Only include significant news
                    processed_news.append(processed_item)
            
            return processed_news[:50]  # Limit to 50 most relevant items
            
        except Exception as e:
            print(f"Error collecting news data: {e}")
            return []

    def _collect_from_news_api(self):
        """Collect from News API"""
        news_items = []
        
        try:
            # Search for supply chain related articles
            for keyword in self.supply_chain_keywords[:5]:  # Limit API calls
                articles = self.news_client.get_everything(
                    q=keyword,
                    language='en',
                    sort_by='publishedAt',
                    from_param=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
                )
                
                for article in articles.get('articles', [])[:10]:
                    news_items.append({
                        'title': article['title'],
                        'description': article['description'] or '',
                        'source': article['source']['name'],
                        'url': article['url'],
                        'published_at': article['publishedAt']
                    })
        
        except Exception as e:
            print(f"Error with News API: {e}")
        
        return news_items

    def _collect_from_rss_feeds(self):
        """Collect from RSS feeds as backup"""
        rss_feeds = [
            'https://www.supplychaindive.com/feeds/',
            'https://www.freightwaves.com/news/feed',
            'https://www.logisticsmgmt.com/rss_feeds/all_news',
            'https://feeds.reuters.com/reuters/businessNews'
        ]
        
        news_items = []
        
        for feed_url in rss_feeds:
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:10]:  # Limit items per feed
                    # Check if entry is supply chain related
                    text_content = f"{entry.title} {entry.get('summary', '')}"
                    if self._is_supply_chain_related(text_content):
                        news_items.append({
                            'title': entry.title,
                            'description': entry.get('summary', ''),
                            'source': feed.feed.get('title', 'RSS Feed'),
                            'url': entry.get('link', ''),
                            'published_at': entry.get('published', '')
                        })
            except Exception as e:
                print(f"Error parsing RSS feed {feed_url}: {e}")
                continue
        
        return news_items

    def _is_supply_chain_related(self, text):
        """Check if text is related to supply chain"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.supply_chain_keywords)

    def _process_news_item(self, item):
        """Process and score a news item"""
        # Combine title and description for analysis
        full_text = f"{item['title']} {item['description']}"
        
        # Sentiment analysis
        blob = TextBlob(full_text)
        sentiment = blob.sentiment.polarity
        
        # Calculate severity based on keywords and sentiment
        severity = self._calculate_news_severity(full_text, sentiment)
        
        # Extract location if possible
        location = self._extract_location(full_text)
        
        return {
            'title': item['title'],
            'description': item['description'],
            'source': item['source'],
            'location': location,
            'severity': severity,
            'sentiment': sentiment,
            'url': item.get('url', ''),
            'published_at': item.get('published_at', '')
        }

    def _calculate_news_severity(self, text, sentiment):
        """Calculate severity score for news item"""
        severity = 0.3  # Base severity
        
        # High impact keywords
        high_impact_keywords = [
            'shutdown', 'closure', 'strike', 'blocked', 'suspended',
            'shortage', 'disruption', 'delay', 'crisis', 'emergency'
        ]
        
        # Medium impact keywords
        medium_impact_keywords = [
            'congestion', 'bottleneck', 'slow', 'reduced', 'limited'
        ]
        
        text_lower = text.lower()
        
        # Increase severity based on keywords
        for keyword in high_impact_keywords:
            if keyword in text_lower:
                severity += 0.2
        
        for keyword in medium_impact_keywords:
            if keyword in text_lower:
                severity += 0.1
        
        # Adjust based on sentiment (negative sentiment = higher severity)
        if sentiment < -0.3:
            severity += 0.2
        elif sentiment < 0:
            severity += 0.1
        
        # Check for critical regions
        for region in self.critical_regions:
            if region.lower() in text_lower:
                severity += 0.15
                break
        
        return min(severity, 1.0)  # Cap at 1.0

    def _extract_location(self, text):
        """Extract location from text"""
        for region in self.critical_regions:
            if region.lower() in text.lower():
                return region
        return ''

    def collect_weather(self):
        """Collect severe weather data affecting supply chains"""
        weather_data = []
        
        if not self.weather_api_key:
            return self._get_mock_weather_data()
        
        try:
            # Major ports and logistics hubs
            key_locations = [
                {'name': 'Los Angeles', 'lat': 34.0522, 'lon': -118.2437},
                {'name': 'Shanghai', 'lat': 31.2304, 'lon': 121.4737},
                {'name': 'Singapore', 'lat': 1.3521, 'lon': 103.8198},
                {'name': 'Rotterdam', 'lat': 51.9244, 'lon': 4.4777},
                {'name': 'Hamburg', 'lat': 53.5511, 'lon': 9.9937}
            ]
            
            for location in key_locations:
                weather_info = self._get_weather_for_location(location)
                if weather_info:
                    weather_data.append(weather_info)
        
        except Exception as e:
            print(f"Error collecting weather data: {e}")
            return self._get_mock_weather_data()
        
        return weather_data

    def _get_weather_for_location(self, location):
        """Get weather data for specific location"""
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': location['lat'],
                'lon': location['lon'],
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if response.status_code == 200:
                weather_condition = data['weather'][0]['main'].lower()
                wind_speed = data['wind']['speed']
                
                # Determine if weather could affect supply chain
                severity = self._calculate_weather_severity(weather_condition, wind_speed)
                
                if severity > 0.4:  # Only report significant weather
                    return {
                        'title': f"Severe Weather Alert: {location['name']}",
                        'description': f"{data['weather'][0]['description'].title()} with wind speeds of {wind_speed} m/s",
                        'location': location['name'],
                        'severity': severity,
                        'weather_condition': weather_condition,
                        'wind_speed': wind_speed
                    }
        
        except Exception as e:
            print(f"Error getting weather for {location['name']}: {e}")
        
        return None

    def _calculate_weather_severity(self, condition, wind_speed):
        """Calculate severity of weather impact on supply chain"""
        severity = 0.0
        
        # High impact weather conditions
        if condition in ['thunderstorm', 'snow', 'extreme']:
            severity = 0.8
        elif condition in ['rain', 'drizzle']:
            severity = 0.4
        elif condition in ['fog', 'mist']:
            severity = 0.3
        
        # High wind speeds affect shipping
        if wind_speed > 15:  # Strong winds
            severity += 0.3
        elif wind_speed > 10:
            severity += 0.2
        
        return min(severity, 1.0)

    def _get_mock_weather_data(self):
        """Return mock weather data when API is not available"""
        return [
            {
                'title': 'Storm Warning: Pacific Shipping Routes',
                'description': 'Severe storm conditions expected to affect Pacific shipping lanes',
                'location': 'Pacific Ocean',
                'severity': 0.7,
                'weather_condition': 'storm',
                'wind_speed': 18
            }
        ]

    def collect_economic(self):
        """Collect economic indicators affecting supply chains"""
        economic_data = []
        
        try:
            # Oil prices (affects transportation costs)
            oil_data = self._get_oil_prices()
            if oil_data:
                economic_data.append(oil_data)
            
            # Shipping rates (Baltic Dry Index proxy)
            shipping_data = self._get_shipping_indicators()
            if shipping_data:
                economic_data.append(shipping_data)
            
            # Currency fluctuations
            currency_data = self._get_currency_indicators()
            economic_data.extend(currency_data)
        
        except Exception as e:
            print(f"Error collecting economic data: {e}")
            return self._get_mock_economic_data()
        
        return economic_data

    def _get_oil_prices(self):
        """Get oil price data"""
        try:
            # Get crude oil prices
            oil = yf.Ticker("CL=F")  # Crude Oil Futures
            hist = oil.history(period="5d")
            
            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                change_percent = ((current_price - previous_price) / previous_price) * 100
                
                severity = min(abs(change_percent) / 10, 1.0)  # 10% change = max severity
                
                return {
                    'title': f'Oil Price Movement: {change_percent:.1f}%',
                    'description': f'Crude oil prices {"increased" if change_percent > 0 else "decreased"} by {abs(change_percent):.1f}% to ${current_price:.2f}',
                    'severity': severity,
                    'indicator': 'oil_price',
                    'value': current_price,
                    'change_percent': change_percent
                }
        except Exception as e:
            print(f"Error getting oil prices: {e}")
        
        return None

    def _get_shipping_indicators(self):
        """Get shipping cost indicators"""
        try:
            # Use shipping company stock as proxy for shipping costs
            maersk = yf.Ticker("AMKBY")  # Maersk
            hist = maersk.history(period="5d")
            
            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                change_percent = ((current_price - previous_price) / previous_price) * 100
                
                severity = min(abs(change_percent) / 5, 0.8)  # 5% change = high severity
                
                return {
                    'title': f'Shipping Cost Indicator: {change_percent:.1f}%',
                    'description': f'Shipping indicators suggest costs have {"increased" if change_percent > 0 else "decreased"} by {abs(change_percent):.1f}%',
                    'severity': severity,
                    'indicator': 'shipping_cost',
                    'change_percent': change_percent
                }
        except Exception as e:
            print(f"Error getting shipping indicators: {e}")
        
        return None

    def _get_currency_indicators(self):
        """Get currency fluctuation data"""
        currency_data = []
        
        try:
            # Major currency pairs affecting trade
            currency_pairs = ['EURUSD=X', 'USDJPY=X', 'USDCNY=X']
            
            for pair in currency_pairs:
                ticker = yf.Ticker(pair)
                hist = ticker.history(period="5d")
                
                if not hist.empty:
                    current_rate = hist['Close'].iloc[-1]
                    previous_rate = hist['Close'].iloc[-2] if len(hist) > 1 else current_rate
                    change_percent = ((current_rate - previous_rate) / previous_rate) * 100
                    
                    if abs(change_percent) > 2:  # Only report significant changes
                        severity = min(abs(change_percent) / 5, 0.7)
                        
                        currency_data.append({
                            'title': f'Currency Fluctuation: {pair.replace("=X", "")}',
                            'description': f'{pair.replace("=X", "")} {"strengthened" if change_percent > 0 else "weakened"} by {abs(change_percent):.1f}%',
                            'severity': severity,
                            'indicator': 'currency',
                            'pair': pair,
                            'change_percent': change_percent
                        })
        
        except Exception as e:
            print(f"Error getting currency data: {e}")
        
        return currency_data

    def _get_mock_economic_data(self):
        """Return mock economic data when APIs are not available"""
        return [
            {
                'title': 'Oil Price Surge: +5.2%',
                'description': 'Crude oil prices increased by 5.2% due to geopolitical tensions',
                'severity': 0.6,
                'indicator': 'oil_price',
                'change_percent': 5.2
            },
            {
                'title': 'Shipping Costs Rising',
                'description': 'Container shipping rates increased by 8% this week',
                'severity': 0.7,
                'indicator': 'shipping_cost',
                'change_percent': 8.0
            }
        ]
