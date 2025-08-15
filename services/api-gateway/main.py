"""
API Gateway - Central entry point for all microservices
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import httpx
import logging
from datetime import datetime, timedelta
import sys
import os

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import get_db, SupplyChainEvent, RiskAssessment, BusinessProfile, Alert
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Supply Chain Predictor API Gateway", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

# Service URLs
SERVICES = {
    "data_collector": "http://data-collector:8001",
    "ml_inference": "http://ml-inference:8002",
    "risk_assessment": "http://risk-assessment:8003",
    "notification": "http://notification-service:8004"
}

@app.get("/")
async def root():
    return {"message": "Supply Chain Predictor API Gateway", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "api_gateway": "healthy",
        "redis": redis_client.health_check(),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Check service health
    async with httpx.AsyncClient() as client:
        for service_name, service_url in SERVICES.items():
            try:
                response = await client.get(f"{service_url}/health", timeout=5.0)
                health_status[service_name] = "healthy" if response.status_code == 200 else "unhealthy"
            except Exception:
                health_status[service_name] = "unreachable"
    
    return health_status

# Dashboard endpoints
@app.get("/api/dashboard/overview")
async def get_dashboard_overview(db: Session = Depends(get_db)):
    """Get dashboard overview data"""
    try:
        # Check cache first
        cached_data = redis_client.get("dashboard_overview")
        if cached_data:
            return cached_data
        
        # Get recent events count
        recent_events_count = db.query(SupplyChainEvent).filter(
            SupplyChainEvent.timestamp >= datetime.utcnow() - timedelta(hours=24)
        ).count()
        
        # Get high-risk assessments count
        high_risk_count = db.query(RiskAssessment).filter(
            RiskAssessment.risk_level >= 0.7,
            RiskAssessment.timestamp >= datetime.utcnow() - timedelta(hours=24)
        ).count()
        
        # Get active alerts count
        active_alerts_count = db.query(Alert).filter(Alert.status == "active").count()
        
        # Get business profiles count
        business_profiles_count = db.query(BusinessProfile).count()
        
        overview_data = {
            "recent_events": recent_events_count,
            "high_risk_assessments": high_risk_count,
            "active_alerts": active_alerts_count,
            "business_profiles": business_profiles_count,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        # Cache for 5 minutes
        redis_client.set("dashboard_overview", overview_data, 300)
        
        return overview_data
    except Exception as e:
        logger.error(f"Error getting dashboard overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/events/recent")
async def get_recent_events(limit: int = 20, db: Session = Depends(get_db)):
    """Get recent supply chain events"""
    try:
        events = db.query(SupplyChainEvent).filter(
            SupplyChainEvent.timestamp >= datetime.utcnow() - timedelta(hours=48)
        ).order_by(SupplyChainEvent.severity.desc()).limit(limit).all()
        
        events_data = []
        for event in events:
            events_data.append({
                "id": str(event.id),
                "type": event.event_type,
                "title": event.title,
                "description": event.description,
                "location": event.location,
                "severity": event.severity,
                "impact_sectors": event.impact_sectors,
                "timestamp": event.timestamp.isoformat(),
                "source": event.source
            })
        
        return {"events": events_data, "total": len(events_data)}
    except Exception as e:
        logger.error(f"Error getting recent events: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/risk-assessments/recent")
async def get_recent_risk_assessments(limit: int = 20, db: Session = Depends(get_db)):
    """Get recent risk assessments"""
    try:
        assessments = db.query(RiskAssessment).filter(
            RiskAssessment.timestamp >= datetime.utcnow() - timedelta(hours=24)
        ).order_by(RiskAssessment.risk_level.desc()).limit(limit).all()
        
        assessments_data = []
        for assessment in assessments:
            assessments_data.append({
                "id": str(assessment.id),
                "region": assessment.region,
                "sector": assessment.sector,
                "risk_level": assessment.risk_level,
                "risk_factors": assessment.risk_factors,
                "recommendations": assessment.recommendations,
                "confidence_score": assessment.confidence_score,
                "timestamp": assessment.timestamp.isoformat()
            })
        
        return {"assessments": assessments_data, "total": len(assessments_data)}
    except Exception as e:
        logger.error(f"Error getting recent risk assessments: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Business profile endpoints
@app.get("/api/business-profiles")
async def get_business_profiles(db: Session = Depends(get_db)):
    """Get all business profiles"""
    try:
        profiles = db.query(BusinessProfile).all()
        profiles_data = []
        
        for profile in profiles:
            profiles_data.append({
                "id": str(profile.id),
                "business_name": profile.business_name,
                "industry": profile.industry,
                "key_suppliers": profile.key_suppliers,
                "supply_regions": profile.supply_regions,
                "critical_materials": profile.critical_materials,
                "risk_tolerance": profile.risk_tolerance,
                "created_at": profile.created_at.isoformat()
            })
        
        return {"profiles": profiles_data}
    except Exception as e:
        logger.error(f"Error getting business profiles: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/business-profiles")
async def create_business_profile(profile_data: dict, db: Session = Depends(get_db)):
    """Create a new business profile"""
    try:
        profile = BusinessProfile(
            business_name=profile_data["business_name"],
            industry=profile_data["industry"],
            key_suppliers=profile_data.get("key_suppliers", []),
            supply_regions=profile_data.get("supply_regions", []),
            critical_materials=profile_data.get("critical_materials", []),
            risk_tolerance=profile_data.get("risk_tolerance", 0.5),
            notification_preferences=profile_data.get("notification_preferences", {})
        )
        
        db.add(profile)
        db.commit()
        db.refresh(profile)
        
        return {
            "id": str(profile.id),
            "business_name": profile.business_name,
            "industry": profile.industry,
            "created_at": profile.created_at.isoformat()
        }
    except Exception as e:
        logger.error(f"Error creating business profile: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Proxy endpoints to microservices
@app.post("/api/data-collection/trigger")
async def trigger_data_collection(background_tasks: BackgroundTasks):
    """Trigger data collection process"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{SERVICES['data_collector']}/collect")
            return response.json()
    except Exception as e:
        logger.error(f"Error triggering data collection: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger data collection")

@app.post("/api/ml-inference/predict")
async def get_ml_prediction(prediction_data: dict):
    """Get ML prediction"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SERVICES['ml_inference']}/predict",
                json=prediction_data
            )
            return response.json()
    except Exception as e:
        logger.error(f"Error getting ML prediction: {e}")
        raise HTTPException(status_code=500, detail="Failed to get ML prediction")

@app.post("/api/risk-assessment/analyze")
async def analyze_risk(analysis_data: dict):
    """Analyze risk for specific scenario"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SERVICES['risk_assessment']}/analyze",
                json=analysis_data
            )
            return response.json()
    except Exception as e:
        logger.error(f"Error analyzing risk: {e}")
        raise HTTPException(status_code=500, detail="Failed to analyze risk")

@app.get("/api/alerts/active")
async def get_active_alerts(db: Session = Depends(get_db)):
    """Get active alerts"""
    try:
        alerts = db.query(Alert).filter(Alert.status == "active").order_by(Alert.created_at.desc()).all()
        
        alerts_data = []
        for alert in alerts:
            alerts_data.append({
                "id": str(alert.id),
                "business_profile_id": str(alert.business_profile_id) if alert.business_profile_id else None,
                "alert_type": alert.alert_type,
                "title": alert.title,
                "message": alert.message,
                "severity": alert.severity,
                "status": alert.status,
                "metadata": alert.metadata,
                "created_at": alert.created_at.isoformat()
            })
        
        return {"alerts": alerts_data}
    except Exception as e:
        logger.error(f"Error getting active alerts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
