"""
Risk Calculator Module - Calculates supply chain risk scores and assessments
"""

import json
import math
from datetime import datetime, timedelta
from collections import defaultdict

class RiskCalculator:
    def __init__(self):
        # Regional risk weights based on supply chain importance and vulnerability
        self.regional_weights = {
            'China': 0.9,
            'Taiwan': 0.8,
            'South Korea': 0.7,
            'Japan': 0.6,
            'Singapore': 0.7,
            'Germany': 0.6,
            'Netherlands': 0.5,
            'United States': 0.7,
            'Mexico': 0.5,
            'Vietnam': 0.6,
            'India': 0.6,
            'Thailand': 0.5,
            'Malaysia': 0.5,
            'Global': 0.8  # Default for global events
        }
        
        # Sector vulnerability scores
        self.sector_vulnerability = {
            'automotive': 0.8,
            'electronics': 0.9,
            'pharmaceuticals': 0.7,
            'food_beverage': 0.6,
            'textiles': 0.5,
            'construction': 0.6,
            'energy': 0.8,
            'retail': 0.7,
            'aerospace': 0.8,
            'chemicals': 0.7
        }
        
        # Event type impact multipliers
        self.event_multipliers = {
            'news': 1.0,
            'weather': 1.2,
            'economic': 1.1,
            'geopolitical': 1.3,
            'natural_disaster': 1.5,
            'cyber_attack': 1.4,
            'pandemic': 1.6
        }
        
        # Time decay factors for risk assessment
        self.time_decay_factors = {
            'immediate': 1.0,      # 0-24 hours
            'short_term': 0.8,     # 1-7 days
            'medium_term': 0.6,    # 1-4 weeks
            'long_term': 0.4       # 1+ months
        }

    def calculate_risks(self, event):
        """Calculate risk assessments for different regions and sectors based on an event"""
        risk_assessments = []
        
        try:
            # Determine event characteristics
            event_severity = event.severity or 0.5
            event_location = event.location or 'Global'
            event_type = event.event_type or 'news'
            
            # Get base risk multiplier for event type
            type_multiplier = self.event_multipliers.get(event_type, 1.0)
            
            # Calculate regional risks
            regional_risks = self._calculate_regional_risks(
                event_severity, event_location, type_multiplier, event
            )
            
            # Calculate sector risks
            sector_risks = self._calculate_sector_risks(
                event_severity, event_location, type_multiplier, event
            )
            
            # Combine regional and sector risks
            for region_risk in regional_risks:
                for sector_risk in sector_risks:
                    combined_risk = self._combine_risks(region_risk, sector_risk)
                    if combined_risk['risk_level'] > 0.3:  # Only include significant risks
                        risk_assessments.append(combined_risk)
            
            # Sort by risk level and return top assessments
            risk_assessments.sort(key=lambda x: x['risk_level'], reverse=True)
            return risk_assessments[:20]  # Limit to top 20 assessments
            
        except Exception as e:
            print(f"Error calculating risks for event {event.id}: {e}")
            return []

    def _calculate_regional_risks(self, severity, location, multiplier, event):
        """Calculate risk levels for different regions"""
        regional_risks = []
        
        # Direct impact on event location
        if location and location in self.regional_weights:
            direct_risk = severity * self.regional_weights[location] * multiplier
            regional_risks.append({
                'region': location,
                'risk_level': min(direct_risk, 1.0),
                'impact_type': 'direct',
                'description': f'Direct impact from {event.event_type} event'
            })
        
        # Indirect impacts on connected regions
        connected_regions = self._get_connected_regions(location)
        for region in connected_regions:
            if region in self.regional_weights:
                # Reduce risk for indirect impact
                indirect_risk = severity * self.regional_weights[region] * multiplier * 0.6
                regional_risks.append({
                    'region': region,
                    'risk_level': min(indirect_risk, 0.8),
                    'impact_type': 'indirect',
                    'description': f'Indirect impact through supply chain connections'
                })
        
        return regional_risks

    def _get_connected_regions(self, primary_region):
        """Get regions connected to the primary region through supply chains"""
        # Simplified supply chain connectivity mapping
        connections = {
            'China': ['Taiwan', 'South Korea', 'Japan', 'Singapore', 'United States'],
            'Taiwan': ['China', 'South Korea', 'Japan', 'United States'],
            'Germany': ['Netherlands', 'France', 'Italy', 'Poland'],
            'United States': ['Mexico', 'Canada', 'China', 'Germany'],
            'Singapore': ['Malaysia', 'Thailand', 'Indonesia', 'China'],
            'Japan': ['China', 'South Korea', 'Taiwan', 'United States']
        }
        
        return connections.get(primary_region, [])

    def _calculate_sector_risks(self, severity, location, multiplier, event):
        """Calculate risk levels for different sectors"""
        sector_risks = []
        
        # Analyze event text to identify relevant sectors
        event_text = f"{event.title} {event.description}".lower()
        relevant_sectors = self._identify_relevant_sectors(event_text)
        
        # If no specific sectors identified, use general impact
        if not relevant_sectors:
            relevant_sectors = list(self.sector_vulnerability.keys())[:5]
        
        for sector in relevant_sectors:
            if sector in self.sector_vulnerability:
                sector_risk = severity * self.sector_vulnerability[sector] * multiplier
                
                # Adjust based on event type and sector combination
                sector_risk = self._apply_sector_event_adjustments(sector_risk, sector, event.event_type)
                
                sector_risks.append({
                    'sector': sector,
                    'risk_level': min(sector_risk, 1.0),
                    'vulnerability': self.sector_vulnerability[sector],
                    'description': f'Impact on {sector} sector from {event.event_type} disruption'
                })
        
        return sector_risks

    def _identify_relevant_sectors(self, event_text):
        """Identify sectors relevant to the event based on text analysis"""
        relevant_sectors = []
        
        # Sector keywords mapping
        sector_keywords = {
            'automotive': ['car', 'vehicle', 'auto', 'automotive', 'toyota', 'ford', 'gm'],
            'electronics': ['chip', 'semiconductor', 'electronics', 'computer', 'phone', 'apple', 'samsung'],
            'pharmaceuticals': ['drug', 'medicine', 'pharmaceutical', 'vaccine', 'pfizer', 'healthcare'],
            'food_beverage': ['food', 'agriculture', 'crop', 'grain', 'meat', 'dairy', 'beverage'],
            'textiles': ['textile', 'clothing', 'fabric', 'cotton', 'fashion', 'apparel'],
            'construction': ['construction', 'building', 'cement', 'steel', 'lumber', 'housing'],
            'energy': ['oil', 'gas', 'energy', 'power', 'electricity', 'renewable', 'solar'],
            'retail': ['retail', 'store', 'shopping', 'consumer', 'walmart', 'amazon']
        }
        
        for sector, keywords in sector_keywords.items():
            if any(keyword in event_text for keyword in keywords):
                relevant_sectors.append(sector)
        
        return relevant_sectors

    def _apply_sector_event_adjustments(self, base_risk, sector, event_type):
        """Apply sector-specific adjustments based on event type"""
        adjustments = {
            'weather': {
                'food_beverage': 1.3,
                'energy': 1.2,
                'construction': 1.2
            },
            'economic': {
                'automotive': 1.2,
                'electronics': 1.1,
                'retail': 1.3
            },
            'geopolitical': {
                'electronics': 1.4,
                'automotive': 1.2,
                'energy': 1.3
            }
        }
        
        if event_type in adjustments and sector in adjustments[event_type]:
            return base_risk * adjustments[event_type][sector]
        
        return base_risk

    def _combine_risks(self, region_risk, sector_risk):
        """Combine regional and sector risks into a comprehensive assessment"""
        # Calculate combined risk level
        combined_risk_level = (region_risk['risk_level'] + sector_risk['risk_level']) / 2
        
        # Apply interaction effects
        if region_risk['impact_type'] == 'direct':
            combined_risk_level *= 1.1  # Boost for direct regional impact
        
        # Generate risk factors
        risk_factors = self._generate_risk_factors(region_risk, sector_risk)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            region_risk['region'], 
            sector_risk['sector'], 
            combined_risk_level
        )
        
        return {
            'region': region_risk['region'],
            'sector': sector_risk['sector'],
            'risk_level': min(combined_risk_level, 1.0),
            'risk_factors': json.dumps(risk_factors),
            'recommendations': recommendations
        }

    def _generate_risk_factors(self, region_risk, sector_risk):
        """Generate detailed risk factors"""
        factors = []
        
        # Regional factors
        if region_risk['risk_level'] > 0.6:
            factors.append({
                'type': 'regional',
                'description': f"High exposure in {region_risk['region']}",
                'severity': 'high'
            })
        
        # Sector factors
        if sector_risk['risk_level'] > 0.6:
            factors.append({
                'type': 'sectoral',
                'description': f"High vulnerability in {sector_risk['sector']} sector",
                'severity': 'high'
            })
        
        # Interaction factors
        if region_risk['risk_level'] > 0.5 and sector_risk['risk_level'] > 0.5:
            factors.append({
                'type': 'interaction',
                'description': f"Combined regional and sectoral exposure amplifies risk",
                'severity': 'medium'
            })
        
        return factors

    def _generate_recommendations(self, region, sector, risk_level):
        """Generate risk mitigation recommendations"""
        recommendations = []
        
        if risk_level > 0.7:
            recommendations.extend([
                "Immediate action required: Review and activate contingency plans",
                f"Diversify suppliers away from {region} if heavily concentrated",
                "Increase inventory buffers for critical materials",
                "Establish alternative supply routes"
            ])
        elif risk_level > 0.5:
            recommendations.extend([
                "Monitor situation closely and prepare contingency measures",
                f"Assess supplier concentration in {region}",
                "Review contracts for force majeure clauses",
                "Consider temporary inventory increases"
            ])
        else:
            recommendations.extend([
                "Continue monitoring for escalation",
                "Review supplier risk assessments",
                "Maintain standard inventory levels"
            ])
        
        # Sector-specific recommendations
        sector_recommendations = {
            'automotive': ["Consider alternative semiconductor sources", "Review just-in-time delivery schedules"],
            'electronics': ["Secure component inventory", "Evaluate design alternatives"],
            'pharmaceuticals': ["Ensure API supply security", "Review regulatory compliance"],
            'food_beverage': ["Monitor commodity prices", "Secure packaging materials"],
            'energy': ["Review fuel supply contracts", "Consider renewable alternatives"]
        }
        
        if sector in sector_recommendations:
            recommendations.extend(sector_recommendations[sector])
        
        return "; ".join(recommendations[:5])  # Limit to 5 key recommendations

    def calculate_portfolio_risk(self, business_profiles):
        """Calculate aggregated risk for a portfolio of businesses"""
        try:
            if not business_profiles:
                return {'overall_risk': 0.3, 'risk_distribution': {}}
            
            total_risk = 0
            risk_distribution = defaultdict(float)
            
            for profile in business_profiles:
                # Get industry risk
                industry = profile.industry
                industry_risk = self.sector_vulnerability.get(industry, 0.5)
                
                # Get regional risk
                supply_regions = json.loads(profile.supply_regions) if profile.supply_regions else []
                regional_risk = 0
                for region in supply_regions:
                    regional_risk += self.regional_weights.get(region, 0.5)
                regional_risk = regional_risk / max(len(supply_regions), 1)
                
                # Combined business risk
                business_risk = (industry_risk + regional_risk) / 2
                total_risk += business_risk
                
                # Update distribution
                risk_distribution[industry] += business_risk
            
            overall_risk = total_risk / len(business_profiles)
            
            return {
                'overall_risk': min(overall_risk, 1.0),
                'risk_distribution': dict(risk_distribution),
                'business_count': len(business_profiles)
            }
            
        except Exception as e:
            print(f"Error calculating portfolio risk: {e}")
            return {'overall_risk': 0.5, 'risk_distribution': {}}

    def calculate_time_adjusted_risk(self, base_risk, event_timestamp):
        """Adjust risk based on time elapsed since event"""
        try:
            now = datetime.utcnow()
            time_diff = now - event_timestamp
            
            if time_diff <= timedelta(hours=24):
                time_factor = self.time_decay_factors['immediate']
            elif time_diff <= timedelta(days=7):
                time_factor = self.time_decay_factors['short_term']
            elif time_diff <= timedelta(weeks=4):
                time_factor = self.time_decay_factors['medium_term']
            else:
                time_factor = self.time_decay_factors['long_term']
            
            return base_risk * time_factor
            
        except Exception as e:
            print(f"Error calculating time-adjusted risk: {e}")
            return base_risk

    def get_risk_summary(self, risk_assessments):
        """Generate a summary of risk assessments"""
        if not risk_assessments:
            return {
                'total_assessments': 0,
                'high_risk_count': 0,
                'medium_risk_count': 0,
                'low_risk_count': 0,
                'top_risk_regions': [],
                'top_risk_sectors': []
            }
        
        high_risk = [r for r in risk_assessments if r.risk_level > 0.7]
        medium_risk = [r for r in risk_assessments if 0.4 <= r.risk_level <= 0.7]
        low_risk = [r for r in risk_assessments if r.risk_level < 0.4]
        
        # Count by region and sector
        region_counts = defaultdict(int)
        sector_counts = defaultdict(int)
        
        for assessment in risk_assessments:
            region_counts[assessment.region] += 1
            sector_counts[assessment.sector] += 1
        
        # Get top regions and sectors
        top_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'total_assessments': len(risk_assessments),
            'high_risk_count': len(high_risk),
            'medium_risk_count': len(medium_risk),
            'low_risk_count': len(low_risk),
            'top_risk_regions': [{'region': r[0], 'count': r[1]} for r in top_regions],
            'top_risk_sectors': [{'sector': s[0], 'count': s[1]} for s in top_sectors]
        }
