"""
AI Analyzer Module - Uses AI/ML to analyze supply chain disruptions
"""

import os
import json
import numpy as np
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from datetime import datetime, timedelta
import re

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

class AIAnalyzer:
    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        
        # Industry sectors and their supply chain dependencies
        self.industry_dependencies = {
            'automotive': ['semiconductors', 'steel', 'aluminum', 'rubber', 'plastics'],
            'electronics': ['semiconductors', 'rare earth metals', 'lithium', 'copper'],
            'pharmaceuticals': ['active ingredients', 'packaging materials', 'chemicals'],
            'food_beverage': ['agricultural products', 'packaging', 'transportation'],
            'textiles': ['cotton', 'synthetic fibers', 'dyes', 'chemicals'],
            'construction': ['steel', 'cement', 'lumber', 'copper'],
            'energy': ['oil', 'natural gas', 'solar panels', 'wind turbines'],
            'retail': ['consumer goods', 'packaging', 'transportation']
        }
        
        # Regional supply chain hubs
        self.supply_hubs = {
            'Asia-Pacific': ['China', 'Taiwan', 'South Korea', 'Japan', 'Singapore', 'Vietnam'],
            'Europe': ['Germany', 'Netherlands', 'Italy', 'France', 'UK'],
            'North America': ['United States', 'Mexico', 'Canada'],
            'Middle East': ['UAE', 'Saudi Arabia', 'Qatar'],
            'Africa': ['South Africa', 'Egypt', 'Morocco']
        }
        
        # Initialize TF-IDF vectorizer for text analysis
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        
        # Risk patterns learned from historical data
        self.risk_patterns = self._initialize_risk_patterns()

    def _initialize_risk_patterns(self):
        """Initialize risk patterns based on historical supply chain disruptions"""
        return {
            'port_congestion': {
                'keywords': ['port', 'congestion', 'backlog', 'delay', 'container'],
                'impact_multiplier': 1.2,
                'affected_sectors': ['automotive', 'electronics', 'retail']
            },
            'weather_disruption': {
                'keywords': ['storm', 'hurricane', 'flood', 'earthquake', 'typhoon'],
                'impact_multiplier': 1.5,
                'affected_sectors': ['food_beverage', 'energy', 'construction']
            },
            'geopolitical_tension': {
                'keywords': ['sanctions', 'trade war', 'border', 'tariff', 'embargo'],
                'impact_multiplier': 1.3,
                'affected_sectors': ['electronics', 'automotive', 'energy']
            },
            'labor_shortage': {
                'keywords': ['strike', 'labor', 'workers', 'shortage', 'union'],
                'impact_multiplier': 1.1,
                'affected_sectors': ['automotive', 'construction', 'food_beverage']
            },
            'raw_material_shortage': {
                'keywords': ['shortage', 'scarcity', 'limited supply', 'raw material'],
                'impact_multiplier': 1.4,
                'affected_sectors': ['electronics', 'automotive', 'construction']
            }
        }

    def analyze_event_impact(self, event):
        """Analyze the potential impact of a supply chain event"""
        try:
            # Extract key information from event
            event_text = f"{event.title} {event.description}"
            
            # Perform sentiment analysis
            sentiment = self._analyze_sentiment(event_text)
            
            # Identify risk patterns
            risk_pattern = self._identify_risk_pattern(event_text)
            
            # Calculate impact score
            impact_score = self._calculate_impact_score(event, sentiment, risk_pattern)
            
            # Identify affected sectors
            affected_sectors = self._identify_affected_sectors(event_text, risk_pattern)
            
            # Generate predictions
            predictions = self._generate_predictions(event, impact_score, affected_sectors)
            
            return {
                'event_id': event.id,
                'impact_score': impact_score,
                'sentiment': sentiment,
                'risk_pattern': risk_pattern,
                'affected_sectors': affected_sectors,
                'predictions': predictions,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            print(f"Error analyzing event impact: {e}")
            return None

    def _analyze_sentiment(self, text):
        """Analyze sentiment of the text"""
        blob = TextBlob(text)
        return {
            'polarity': blob.sentiment.polarity,
            'subjectivity': blob.sentiment.subjectivity
        }

    def _identify_risk_pattern(self, text):
        """Identify which risk pattern the event matches"""
        text_lower = text.lower()
        
        best_match = None
        best_score = 0
        
        for pattern_name, pattern_data in self.risk_patterns.items():
            score = 0
            for keyword in pattern_data['keywords']:
                if keyword in text_lower:
                    score += 1
            
            # Normalize score by number of keywords
            normalized_score = score / len(pattern_data['keywords'])
            
            if normalized_score > best_score:
                best_score = normalized_score
                best_match = pattern_name
        
        return {
            'pattern': best_match,
            'confidence': best_score
        } if best_match else None

    def _calculate_impact_score(self, event, sentiment, risk_pattern):
        """Calculate overall impact score for the event"""
        base_score = event.severity or 0.5
        
        # Adjust based on sentiment (negative sentiment increases impact)
        sentiment_adjustment = 0
        if sentiment['polarity'] < -0.3:
            sentiment_adjustment = 0.2
        elif sentiment['polarity'] < 0:
            sentiment_adjustment = 0.1
        
        # Adjust based on risk pattern
        pattern_adjustment = 0
        if risk_pattern and risk_pattern['pattern']:
            pattern_data = self.risk_patterns[risk_pattern['pattern']]
            pattern_adjustment = (pattern_data['impact_multiplier'] - 1) * risk_pattern['confidence']
        
        # Calculate final score
        impact_score = base_score + sentiment_adjustment + pattern_adjustment
        
        return min(impact_score, 1.0)  # Cap at 1.0

    def _identify_affected_sectors(self, text, risk_pattern):
        """Identify which industry sectors are likely to be affected"""
        affected_sectors = []
        text_lower = text.lower()
        
        # Check for direct sector mentions
        for sector in self.industry_dependencies.keys():
            sector_keywords = [sector.replace('_', ' '), sector]
            if any(keyword in text_lower for keyword in sector_keywords):
                affected_sectors.append(sector)
        
        # Check for material/dependency mentions
        for sector, dependencies in self.industry_dependencies.items():
            for dependency in dependencies:
                if dependency in text_lower:
                    if sector not in affected_sectors:
                        affected_sectors.append(sector)
        
        # Add sectors based on risk pattern
        if risk_pattern and risk_pattern['pattern']:
            pattern_sectors = self.risk_patterns[risk_pattern['pattern']]['affected_sectors']
            for sector in pattern_sectors:
                if sector not in affected_sectors:
                    affected_sectors.append(sector)
        
        return affected_sectors[:5]  # Limit to top 5 sectors

    def _generate_predictions(self, event, impact_score, affected_sectors):
        """Generate predictions about supply chain disruptions"""
        predictions = []
        
        # Time-based predictions
        if impact_score > 0.7:
            timeline = "1-2 weeks"
            severity = "High"
        elif impact_score > 0.5:
            timeline = "2-4 weeks"
            severity = "Medium"
        else:
            timeline = "4-8 weeks"
            severity = "Low"
        
        # Generate sector-specific predictions
        for sector in affected_sectors:
            prediction = {
                'sector': sector,
                'severity': severity,
                'timeline': timeline,
                'impact_description': self._generate_sector_impact_description(sector, event),
                'confidence': min(impact_score + 0.1, 1.0)
            }
            predictions.append(prediction)
        
        return predictions

    def _generate_sector_impact_description(self, sector, event):
        """Generate human-readable impact description for a sector"""
        sector_impacts = {
            'automotive': 'Production delays and increased costs for vehicle manufacturing',
            'electronics': 'Component shortages affecting device production and pricing',
            'pharmaceuticals': 'Potential delays in drug manufacturing and distribution',
            'food_beverage': 'Supply chain disruptions affecting food availability and pricing',
            'textiles': 'Material shortages impacting clothing and textile production',
            'construction': 'Building material shortages and project delays',
            'energy': 'Potential energy supply disruptions and price volatility',
            'retail': 'Inventory shortages and delivery delays for consumer goods'
        }
        
        return sector_impacts.get(sector, f'Supply chain disruptions affecting {sector} sector')

    def analyze_business_impact(self, business_profile):
        """Analyze potential impact on a specific business"""
        try:
            # Parse business profile data
            industry = business_profile.industry
            key_suppliers = json.loads(business_profile.key_suppliers) if business_profile.key_suppliers else []
            supply_regions = json.loads(business_profile.supply_regions) if business_profile.supply_regions else []
            critical_materials = json.loads(business_profile.critical_materials) if business_profile.critical_materials else []
            
            # Calculate risk exposure
            risk_exposure = self._calculate_business_risk_exposure(
                industry, key_suppliers, supply_regions, critical_materials
            )
            
            # Generate recommendations
            recommendations = self._generate_business_recommendations(risk_exposure)
            
            # Calculate overall risk score
            overall_risk = self._calculate_overall_business_risk(risk_exposure)
            
            return {
                'business_name': business_profile.business_name,
                'industry': industry,
                'overall_risk_score': overall_risk,
                'risk_exposure': risk_exposure,
                'recommendations': recommendations,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            print(f"Error analyzing business impact: {e}")
            return None

    def _calculate_business_risk_exposure(self, industry, suppliers, regions, materials):
        """Calculate risk exposure for a business"""
        risk_factors = {}
        
        # Industry-specific risks
        if industry in self.industry_dependencies:
            dependencies = self.industry_dependencies[industry]
            risk_factors['material_dependency'] = {
                'score': len(dependencies) * 0.1,
                'description': f'Dependent on {len(dependencies)} critical materials',
                'materials': dependencies
            }
        
        # Regional concentration risk
        region_risk = 0
        high_risk_regions = ['China', 'Taiwan', 'South Korea']  # Example high-risk regions
        
        for region in regions:
            if region in high_risk_regions:
                region_risk += 0.2
        
        if region_risk > 0:
            risk_factors['regional_concentration'] = {
                'score': min(region_risk, 0.8),
                'description': f'High concentration in {len([r for r in regions if r in high_risk_regions])} high-risk regions',
                'regions': [r for r in regions if r in high_risk_regions]
            }
        
        # Supplier concentration risk
        if len(suppliers) < 3:
            risk_factors['supplier_concentration'] = {
                'score': 0.6,
                'description': 'Limited supplier diversity increases risk',
                'supplier_count': len(suppliers)
            }
        elif len(suppliers) < 5:
            risk_factors['supplier_concentration'] = {
                'score': 0.3,
                'description': 'Moderate supplier concentration',
                'supplier_count': len(suppliers)
            }
        
        return risk_factors

    def _calculate_overall_business_risk(self, risk_exposure):
        """Calculate overall risk score for business"""
        if not risk_exposure:
            return 0.3  # Default moderate risk
        
        total_risk = 0
        risk_count = 0
        
        for risk_factor in risk_exposure.values():
            total_risk += risk_factor['score']
            risk_count += 1
        
        return min(total_risk / max(risk_count, 1), 1.0) if risk_count > 0 else 0.3

    def _generate_business_recommendations(self, risk_exposure):
        """Generate recommendations based on risk exposure"""
        recommendations = []
        
        for risk_type, risk_data in risk_exposure.items():
            if risk_type == 'material_dependency':
                recommendations.append({
                    'category': 'Supply Diversification',
                    'priority': 'High',
                    'action': 'Identify alternative suppliers for critical materials',
                    'timeline': '1-3 months'
                })
            
            elif risk_type == 'regional_concentration':
                recommendations.append({
                    'category': 'Geographic Diversification',
                    'priority': 'High',
                    'action': 'Diversify supply base across multiple regions',
                    'timeline': '3-6 months'
                })
            
            elif risk_type == 'supplier_concentration':
                recommendations.append({
                    'category': 'Supplier Diversification',
                    'priority': 'Medium',
                    'action': 'Expand supplier network to reduce concentration risk',
                    'timeline': '2-4 months'
                })
        
        # Add general recommendations
        recommendations.extend([
            {
                'category': 'Inventory Management',
                'priority': 'Medium',
                'action': 'Maintain strategic inventory buffers for critical materials',
                'timeline': 'Ongoing'
            },
            {
                'category': 'Monitoring',
                'priority': 'High',
                'action': 'Implement continuous supply chain monitoring',
                'timeline': 'Immediate'
            }
        ])
        
        return recommendations

    def cluster_similar_events(self, events):
        """Cluster similar supply chain events for pattern recognition"""
        if len(events) < 2:
            return []
        
        try:
            # Prepare text data
            event_texts = [f"{event.title} {event.description}" for event in events]
            
            # Vectorize text
            tfidf_matrix = self.vectorizer.fit_transform(event_texts)
            
            # Determine optimal number of clusters
            n_clusters = min(5, len(events) // 2 + 1)
            
            # Perform clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = kmeans.fit_predict(tfidf_matrix)
            
            # Group events by cluster
            clusters = {}
            for i, label in enumerate(cluster_labels):
                if label not in clusters:
                    clusters[label] = []
                clusters[label].append({
                    'event': events[i],
                    'similarity_score': 1.0  # Placeholder
                })
            
            return clusters
            
        except Exception as e:
            print(f"Error clustering events: {e}")
            return []

    def predict_cascade_effects(self, primary_event, related_events):
        """Predict cascade effects from a primary disruption"""
        try:
            cascade_predictions = []
            
            # Analyze primary event
            primary_analysis = self.analyze_event_impact(primary_event)
            if not primary_analysis:
                return []
            
            # Identify potential cascade sectors
            primary_sectors = primary_analysis['affected_sectors']
            
            # For each affected sector, predict secondary effects
            for sector in primary_sectors:
                if sector in self.industry_dependencies:
                    dependencies = self.industry_dependencies[sector]
                    
                    for dependency in dependencies:
                        cascade_prediction = {
                            'trigger_sector': sector,
                            'affected_resource': dependency,
                            'cascade_probability': min(primary_analysis['impact_score'] * 0.7, 0.9),
                            'estimated_timeline': '2-6 weeks',
                            'potential_impact': f'Secondary shortage of {dependency} affecting multiple industries'
                        }
                        cascade_predictions.append(cascade_prediction)
            
            return cascade_predictions[:10]  # Limit to top 10 predictions
            
        except Exception as e:
            print(f"Error predicting cascade effects: {e}")
            return []
