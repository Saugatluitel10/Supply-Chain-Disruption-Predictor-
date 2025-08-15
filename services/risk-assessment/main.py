"""
Risk Assessment Service - Business logic for risk calculations
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
import logging
from datetime import datetime, timedelta
import json
import sys
import os
from typing import Dict, Any, List

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import SessionLocal, SupplyChainEvent, RiskAssessment, BusinessProfile
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Risk Assessment Service", version="1.0.0")

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

class RiskCalculator:
    def __init__(self):
        self.risk_factors = {
            "geopolitical": 0.3,
            "weather": 0.25,
            "economic": 0.2,
            "infrastructure": 0.15,
            "supplier": 0.1
        }
        
        self.sector_weights = {
            "electronics": 0.9,
            "automotive": 0.85,
            "pharmaceuticals": 0.8,
            "energy": 0.75,
            "agriculture": 0.7,
            "retail": 0.6,
            "manufacturing": 0.65,
            "transportation": 0.7,
            "finance": 0.5
        }
        
        self.region_weights = {
            "asia": 0.8,
            "europe": 0.6,
            "north_america": 0.5,
            "south_america": 0.7,
            "africa": 0.75,
            "middle_east": 0.85,
            "oceania": 0.55
        }
    
    def calculate_event_risk(self, event: SupplyChainEvent) -> List[Dict[str, Any]]:
        """Calculate risk assessments for an event across different regions and sectors"""
        try:
            risk_assessments = []
            
            # Get affected sectors from event
            impact_sectors = event.impact_sectors if event.impact_sectors else ["general"]
            
            # Calculate risk for each sector
            for sector in impact_sectors:
                # Calculate base risk
                base_risk = self._calculate_base_risk(event)
                
                # Apply sector-specific multiplier
                sector_multiplier = self.sector_weights.get(sector.lower(), 0.6)
                sector_risk = min(1.0, base_risk * sector_multiplier)
                
                # Calculate regional risks
                regional_risks = self._calculate_regional_risks(event, sector_risk)
                
                for region, risk_level in regional_risks.items():
                    risk_factors = self._identify_risk_factors(event, sector, region)
                    recommendations = self._generate_recommendations(risk_level, risk_factors, sector)
                    
                    assessment = {
                        "region": region,
                        "sector": sector,
                        "risk_level": risk_level,
                        "risk_factors": risk_factors,
                        "recommendations": recommendations,
                        "confidence_score": self._calculate_confidence(event, sector, region),
                        "event_id": str(event.id),
                        "event_severity": event.severity
                    }
                    
                    risk_assessments.append(assessment)
            
            return risk_assessments
            
        except Exception as e:
            logger.error(f"Error calculating event risk: {e}")
            raise
    
    def calculate_business_risk(self, business_profile: BusinessProfile, recent_events: List[SupplyChainEvent]) -> Dict[str, Any]:
        """Calculate risk assessment for a specific business"""
        try:
            business_risks = []
            overall_risk = 0.0
            
            # Analyze each recent event's impact on the business
            for event in recent_events:
                event_risk = self._calculate_business_event_risk(business_profile, event)
                if event_risk["risk_level"] > 0.1:  # Only include significant risks
                    business_risks.append(event_risk)
                    overall_risk = max(overall_risk, event_risk["risk_level"])
            
            # Calculate aggregate risk factors
            risk_factors = self._aggregate_business_risk_factors(business_profile, business_risks)
            
            # Generate business-specific recommendations
            recommendations = self._generate_business_recommendations(overall_risk, risk_factors, business_profile)
            
            return {
                "business_id": str(business_profile.id),
                "business_name": business_profile.business_name,
                "overall_risk_level": overall_risk,
                "risk_category": self._categorize_risk(overall_risk),
                "individual_risks": business_risks,
                "aggregated_risk_factors": risk_factors,
                "recommendations": recommendations,
                "assessment_timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error calculating business risk: {e}")
            raise
    
    def _calculate_base_risk(self, event: SupplyChainEvent) -> float:
        """Calculate base risk level for an event"""
        # Start with event severity
        base_risk = event.severity or 0.5
        
        # Apply event type multiplier
        event_type_multipliers = {
            "weather": 0.8,
            "economic": 0.7,
            "news": 0.6,
            "geopolitical": 0.9
        }
        
        type_multiplier = event_type_multipliers.get(event.event_type.lower(), 0.6)
        base_risk *= type_multiplier
        
        # Apply location-based risk
        location_risk = self._get_location_risk(event.location or "")
        base_risk = (base_risk + location_risk) / 2
        
        return min(1.0, base_risk)
    
    def _calculate_regional_risks(self, event: SupplyChainEvent, base_risk: float) -> Dict[str, float]:
        """Calculate risk levels for different regions"""
        regional_risks = {}
        
        # If event has specific location, calculate proximity-based risk
        event_location = (event.location or "").lower()
        
        for region, weight in self.region_weights.items():
            if region in event_location or any(country in event_location for country in self._get_region_countries(region)):
                # Direct impact - higher risk
                regional_risks[region] = min(1.0, base_risk * 1.2)
            else:
                # Indirect impact - reduced risk
                regional_risks[region] = base_risk * weight * 0.7
        
        return regional_risks
    
    def _calculate_business_event_risk(self, business_profile: BusinessProfile, event: SupplyChainEvent) -> Dict[str, Any]:
        """Calculate how a specific event affects a business"""
        # Base event risk
        base_risk = self._calculate_base_risk(event)
        
        # Business-specific factors
        supply_regions = business_profile.supply_regions or []
        critical_materials = business_profile.critical_materials or []
        industry = business_profile.industry or ""
        
        # Calculate exposure factors
        region_exposure = self._calculate_region_exposure(supply_regions, event.location or "")
        sector_exposure = self._calculate_sector_exposure(industry, event.impact_sectors or [])
        material_exposure = self._calculate_material_exposure(critical_materials, event)
        
        # Calculate business-specific risk
        business_risk = base_risk * (1 + region_exposure * 0.5 + sector_exposure * 0.4 + material_exposure * 0.3)
        business_risk = min(1.0, business_risk)
        
        return {
            "event_id": str(event.id),
            "event_title": event.title,
            "event_type": event.event_type,
            "risk_level": business_risk,
            "exposure_factors": {
                "region_exposure": region_exposure,
                "sector_exposure": sector_exposure,
                "material_exposure": material_exposure
            },
            "timestamp": event.timestamp.isoformat()
        }
    
    def _identify_risk_factors(self, event: SupplyChainEvent, sector: str, region: str) -> Dict[str, Any]:
        """Identify specific risk factors"""
        risk_factors = {
            "event_severity": event.severity or 0.5,
            "sector_vulnerability": self.sector_weights.get(sector.lower(), 0.6),
            "regional_stability": 1 - self.region_weights.get(region.lower(), 0.6),
            "event_type": event.event_type,
            "location_risk": self._get_location_risk(event.location or "")
        }
        
        # Add specific factors based on event type
        if event.event_type == "weather":
            risk_factors["weather_severity"] = event.severity
            risk_factors["seasonal_factor"] = self._get_seasonal_factor()
        elif event.event_type == "economic":
            risk_factors["economic_impact"] = event.severity
            risk_factors["market_volatility"] = 0.6  # Mock value
        
        return risk_factors
    
    def _generate_recommendations(self, risk_level: float, risk_factors: Dict[str, Any], sector: str) -> List[str]:
        """Generate recommendations based on risk level and factors"""
        recommendations = []
        
        if risk_level >= 0.8:
            recommendations.extend([
                "Immediate action required - activate crisis management protocols",
                "Diversify suppliers immediately to reduce dependency",
                "Increase inventory buffers for critical components",
                "Establish direct communication with key suppliers"
            ])
        elif risk_level >= 0.6:
            recommendations.extend([
                "High risk detected - monitor situation closely",
                "Review and update contingency plans",
                "Consider alternative sourcing options",
                "Increase safety stock levels"
            ])
        elif risk_level >= 0.4:
            recommendations.extend([
                "Moderate risk - maintain vigilance",
                "Review supplier performance and reliability",
                "Consider risk mitigation strategies"
            ])
        else:
            recommendations.append("Low risk - continue normal operations with standard monitoring")
        
        # Sector-specific recommendations
        if sector.lower() == "electronics":
            recommendations.append("Monitor semiconductor supply chain closely")
        elif sector.lower() == "automotive":
            recommendations.append("Track just-in-time delivery schedules")
        elif sector.lower() == "pharmaceuticals":
            recommendations.append("Ensure regulatory compliance across supply chain")
        
        return recommendations
    
    def _generate_business_recommendations(self, risk_level: float, risk_factors: Dict[str, Any], business_profile: BusinessProfile) -> List[str]:
        """Generate business-specific recommendations"""
        recommendations = []
        
        # Risk-level based recommendations
        if risk_level >= 0.7:
            recommendations.extend([
                f"Critical risk alert for {business_profile.business_name}",
                "Activate emergency supply chain protocols immediately",
                "Contact all key suppliers to assess their status",
                "Consider expedited shipping for critical materials"
            ])
        elif risk_level >= 0.5:
            recommendations.extend([
                "Elevated risk detected - increase monitoring frequency",
                "Review inventory levels for critical materials",
                "Prepare alternative sourcing strategies"
            ])
        
        # Industry-specific recommendations
        industry = business_profile.industry.lower()
        if "manufacturing" in industry:
            recommendations.append("Review production schedules and capacity planning")
        elif "retail" in industry:
            recommendations.append("Assess inventory levels and customer demand patterns")
        
        return recommendations
    
    def _calculate_confidence(self, event: SupplyChainEvent, sector: str, region: str) -> float:
        """Calculate confidence score for risk assessment"""
        confidence = 0.7  # Base confidence
        
        # Adjust based on data quality
        if event.severity is not None:
            confidence += 0.1
        if event.location:
            confidence += 0.1
        if event.impact_sectors:
            confidence += 0.1
        
        return min(1.0, confidence)
    
    def _get_location_risk(self, location: str) -> float:
        """Get risk score based on location"""
        high_risk_keywords = ["war", "conflict", "strike", "disaster", "hurricane", "earthquake"]
        location_lower = location.lower()
        
        for keyword in high_risk_keywords:
            if keyword in location_lower:
                return 0.8
        
        return 0.4
    
    def _get_region_countries(self, region: str) -> List[str]:
        """Get countries for a region"""
        region_countries = {
            "asia": ["china", "japan", "korea", "taiwan", "singapore", "thailand", "vietnam"],
            "europe": ["germany", "france", "italy", "spain", "uk", "netherlands"],
            "north_america": ["usa", "canada", "mexico"],
            "south_america": ["brazil", "argentina", "chile"],
            "africa": ["south africa", "nigeria", "egypt"],
            "middle_east": ["saudi arabia", "uae", "israel", "turkey"],
            "oceania": ["australia", "new zealand"]
        }
        return region_countries.get(region, [])
    
    def _calculate_region_exposure(self, supply_regions: List[str], event_location: str) -> float:
        """Calculate exposure based on supply regions"""
        if not supply_regions or not event_location:
            return 0.0
        
        event_location_lower = event_location.lower()
        max_exposure = 0.0
        
        for region in supply_regions:
            if region.lower() in event_location_lower:
                max_exposure = max(max_exposure, 1.0)
            elif any(word in event_location_lower for word in region.lower().split()):
                max_exposure = max(max_exposure, 0.5)
        
        return max_exposure
    
    def _calculate_sector_exposure(self, industry: str, impact_sectors: List[str]) -> float:
        """Calculate exposure based on industry and impacted sectors"""
        if not industry or not impact_sectors:
            return 0.0
        
        industry_lower = industry.lower()
        
        for sector in impact_sectors:
            if sector.lower() in industry_lower:
                return 1.0
        
        return 0.0
    
    def _calculate_material_exposure(self, critical_materials: List[str], event: SupplyChainEvent) -> float:
        """Calculate exposure based on critical materials"""
        if not critical_materials:
            return 0.0
        
        # Simple keyword matching in event description
        event_text = (event.description or "").lower()
        
        for material in critical_materials:
            if material.lower() in event_text:
                return 0.8
        
        return 0.2
    
    def _aggregate_business_risk_factors(self, business_profile: BusinessProfile, business_risks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate risk factors across all business risks"""
        if not business_risks:
            return {}
        
        total_region_exposure = sum(risk["exposure_factors"]["region_exposure"] for risk in business_risks)
        total_sector_exposure = sum(risk["exposure_factors"]["sector_exposure"] for risk in business_risks)
        total_material_exposure = sum(risk["exposure_factors"]["material_exposure"] for risk in business_risks)
        
        count = len(business_risks)
        
        return {
            "average_region_exposure": total_region_exposure / count,
            "average_sector_exposure": total_sector_exposure / count,
            "average_material_exposure": total_material_exposure / count,
            "total_events_affecting": count,
            "supply_chain_diversity": len(business_profile.supply_regions or []),
            "industry_vulnerability": self.sector_weights.get(business_profile.industry.lower(), 0.6)
        }
    
    def _categorize_risk(self, risk_level: float) -> str:
        """Categorize risk level"""
        if risk_level >= 0.8:
            return "critical"
        elif risk_level >= 0.6:
            return "high"
        elif risk_level >= 0.4:
            return "medium"
        else:
            return "low"
    
    def _get_seasonal_factor(self) -> float:
        """Get seasonal risk factor"""
        # Mock seasonal factor - in production, this would be more sophisticated
        return 0.6

# Initialize risk calculator
risk_calculator = RiskCalculator()

@app.get("/")
async def root():
    return {"message": "Risk Assessment Service", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "risk-assessment",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/analyze")
async def analyze_risk(analysis_request: Dict[str, Any]):
    """Analyze risk for specific scenario"""
    try:
        analysis_type = analysis_request.get("type", "event")
        
        if analysis_type == "event":
            event_data = analysis_request.get("event_data", {})
            
            # Create temporary event object for analysis
            temp_event = SupplyChainEvent(
                event_type=event_data.get("event_type", "unknown"),
                title=event_data.get("title", ""),
                description=event_data.get("description", ""),
                location=event_data.get("location", ""),
                severity=event_data.get("severity", 0.5),
                impact_sectors=event_data.get("impact_sectors", [])
            )
            
            risk_assessments = risk_calculator.calculate_event_risk(temp_event)
            
            return {
                "analysis_type": "event",
                "risk_assessments": risk_assessments,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        elif analysis_type == "business":
            business_id = analysis_request.get("business_id")
            
            if not business_id:
                raise HTTPException(status_code=400, detail="Business ID required for business analysis")
            
            # Get business profile from database
            db = SessionLocal()
            try:
                business_profile = db.query(BusinessProfile).filter(BusinessProfile.id == business_id).first()
                if not business_profile:
                    raise HTTPException(status_code=404, detail="Business profile not found")
                
                # Get recent events
                recent_events = db.query(SupplyChainEvent).filter(
                    SupplyChainEvent.timestamp >= datetime.utcnow() - timedelta(hours=48)
                ).all()
                
                business_risk = risk_calculator.calculate_business_risk(business_profile, recent_events)
                
                return {
                    "analysis_type": "business",
                    "business_risk": business_risk,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            finally:
                db.close()
        
        else:
            raise HTTPException(status_code=400, detail="Invalid analysis type")
            
    except Exception as e:
        logger.error(f"Error analyzing risk: {e}")
        raise HTTPException(status_code=500, detail="Risk analysis failed")

@app.post("/process-events")
async def process_unprocessed_events(background_tasks: BackgroundTasks):
    """Process unprocessed events for risk assessment"""
    background_tasks.add_task(process_events_background)
    return {"message": "Event processing started"}

async def process_events_background():
    """Background task to process unprocessed events"""
    try:
        db = SessionLocal()
        
        # Get unprocessed events
        unprocessed_events = db.query(SupplyChainEvent).filter(
            SupplyChainEvent.processed == False
        ).all()
        
        logger.info(f"Processing {len(unprocessed_events)} unprocessed events")
        
        for event in unprocessed_events:
            try:
                # Calculate risk assessments
                risk_assessments = risk_calculator.calculate_event_risk(event)
                
                # Store risk assessments in database
                for assessment_data in risk_assessments:
                    risk_assessment = RiskAssessment(
                        region=assessment_data["region"],
                        sector=assessment_data["sector"],
                        risk_level=assessment_data["risk_level"],
                        risk_factors=assessment_data["risk_factors"],
                        recommendations=assessment_data["recommendations"],
                        confidence_score=assessment_data["confidence_score"]
                    )
                    db.add(risk_assessment)
                
                # Cache risk assessments
                for assessment_data in risk_assessments:
                    redis_client.cache_risk_assessment(
                        assessment_data["region"],
                        assessment_data["sector"],
                        assessment_data
                    )
                
                # Mark event as processed
                event.processed = True
                
                # Publish risk calculation event
                message_queue.publish_event(
                    exchange=Exchanges.RISK_ALERTS,
                    routing_key=EventTypes.RISK_CALCULATED,
                    message={
                        "event_id": str(event.id),
                        "risk_assessments": risk_assessments,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
            except Exception as e:
                logger.error(f"Error processing event {event.id}: {e}")
                continue
        
        db.commit()
        logger.info(f"Completed processing {len(unprocessed_events)} events")
        
    except Exception as e:
        logger.error(f"Error in background event processing: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
