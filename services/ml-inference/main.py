"""
ML Inference Service - AI analysis and predictions
"""

from fastapi import FastAPI, HTTPException
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
import joblib
import logging
from datetime import datetime, timedelta
import json
import hashlib
import sys
import os
from typing import Dict, Any, List

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import SessionLocal, SupplyChainEvent, MLPrediction
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ML Inference Service", version="1.0.0")

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

class MLInferenceEngine:
    def __init__(self):
        self.models = {}
        self.scalers = {}
        self.initialize_models()
    
    def initialize_models(self):
        """Initialize ML models"""
        try:
            # Risk prediction model
            self.models['risk_predictor'] = RandomForestClassifier(
                n_estimators=100,
                random_state=42
            )
            
            # Impact severity model
            self.models['impact_predictor'] = GradientBoostingRegressor(
                n_estimators=100,
                random_state=42
            )
            
            # Disruption duration model
            self.models['duration_predictor'] = RandomForestClassifier(
                n_estimators=100,
                random_state=42
            )
            
            # Initialize scalers
            self.scalers['risk_scaler'] = StandardScaler()
            self.scalers['impact_scaler'] = StandardScaler()
            self.scalers['duration_scaler'] = StandardScaler()
            
            # Train models with mock data (in production, load pre-trained models)
            self._train_mock_models()
            
            logger.info("ML models initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing ML models: {e}")
            raise
    
    def _train_mock_models(self):
        """Train models with mock data for demonstration"""
        try:
            # Generate mock training data
            np.random.seed(42)
            n_samples = 1000
            
            # Features: severity, location_risk, sector_vulnerability, economic_indicator
            X_risk = np.random.rand(n_samples, 4)
            y_risk = (X_risk[:, 0] * 0.4 + X_risk[:, 1] * 0.3 + X_risk[:, 2] * 0.2 + X_risk[:, 3] * 0.1 > 0.5).astype(int)
            
            X_impact = np.random.rand(n_samples, 5)
            y_impact = X_impact[:, 0] * 0.3 + X_impact[:, 1] * 0.25 + X_impact[:, 2] * 0.2 + X_impact[:, 3] * 0.15 + X_impact[:, 4] * 0.1
            
            X_duration = np.random.rand(n_samples, 4)
            y_duration = ((X_duration[:, 0] + X_duration[:, 1]) * 2).astype(int) % 4  # 0-3 classes
            
            # Train models
            self.models['risk_predictor'].fit(X_risk, y_risk)
            self.models['impact_predictor'].fit(X_impact, y_impact)
            self.models['duration_predictor'].fit(X_duration, y_duration)
            
            # Fit scalers
            self.scalers['risk_scaler'].fit(X_risk)
            self.scalers['impact_scaler'].fit(X_impact)
            self.scalers['duration_scaler'].fit(X_duration)
            
            logger.info("Mock models trained successfully")
            
        except Exception as e:
            logger.error(f"Error training mock models: {e}")
            raise
    
    def predict_risk(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict risk level for an event"""
        try:
            # Extract features from event data
            features = self._extract_risk_features(event_data)
            features_scaled = self.scalers['risk_scaler'].transform([features])
            
            # Make prediction
            risk_prob = self.models['risk_predictor'].predict_proba(features_scaled)[0]
            risk_prediction = self.models['risk_predictor'].predict(features_scaled)[0]
            
            return {
                "risk_level": float(risk_prob[1]),  # Probability of high risk
                "risk_category": "high" if risk_prediction == 1 else "low",
                "confidence": float(max(risk_prob)),
                "features_used": features
            }
            
        except Exception as e:
            logger.error(f"Error predicting risk: {e}")
            raise
    
    def predict_impact(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict impact severity"""
        try:
            # Extract features from event data
            features = self._extract_impact_features(event_data)
            features_scaled = self.scalers['impact_scaler'].transform([features])
            
            # Make prediction
            impact_score = self.models['impact_predictor'].predict(features_scaled)[0]
            
            return {
                "impact_score": float(max(0, min(1, impact_score))),  # Clamp to 0-1
                "impact_category": self._categorize_impact(impact_score),
                "affected_sectors": self._predict_affected_sectors(event_data),
                "features_used": features
            }
            
        except Exception as e:
            logger.error(f"Error predicting impact: {e}")
            raise
    
    def predict_duration(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict disruption duration"""
        try:
            # Extract features from event data
            features = self._extract_duration_features(event_data)
            features_scaled = self.scalers['duration_scaler'].transform([features])
            
            # Make prediction
            duration_class = self.models['duration_predictor'].predict(features_scaled)[0]
            duration_prob = self.models['duration_predictor'].predict_proba(features_scaled)[0]
            
            duration_mapping = {
                0: {"days": "1-7", "category": "short"},
                1: {"days": "8-30", "category": "medium"},
                2: {"days": "31-90", "category": "long"},
                3: {"days": "90+", "category": "extended"}
            }
            
            return {
                "duration_class": int(duration_class),
                "duration_range": duration_mapping[duration_class]["days"],
                "duration_category": duration_mapping[duration_class]["category"],
                "confidence": float(max(duration_prob)),
                "features_used": features
            }
            
        except Exception as e:
            logger.error(f"Error predicting duration: {e}")
            raise
    
    def analyze_business_impact(self, business_profile: Dict[str, Any], event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze impact on specific business"""
        try:
            # Calculate business-specific risk factors
            supply_regions = business_profile.get("supply_regions", [])
            critical_materials = business_profile.get("critical_materials", [])
            key_suppliers = business_profile.get("key_suppliers", [])
            
            event_location = event_data.get("location", "")
            event_sectors = event_data.get("impact_sectors", [])
            
            # Calculate overlap scores
            region_overlap = self._calculate_region_overlap(supply_regions, event_location)
            sector_overlap = self._calculate_sector_overlap(business_profile.get("industry", ""), event_sectors)
            
            # Base predictions
            risk_pred = self.predict_risk(event_data)
            impact_pred = self.predict_impact(event_data)
            duration_pred = self.predict_duration(event_data)
            
            # Adjust for business-specific factors
            business_risk = risk_pred["risk_level"] * (1 + region_overlap * 0.5 + sector_overlap * 0.3)
            business_impact = impact_pred["impact_score"] * (1 + region_overlap * 0.4 + sector_overlap * 0.4)
            
            # Clamp values
            business_risk = min(1.0, business_risk)
            business_impact = min(1.0, business_impact)
            
            return {
                "business_risk_level": float(business_risk),
                "business_impact_score": float(business_impact),
                "region_overlap": float(region_overlap),
                "sector_overlap": float(sector_overlap),
                "duration_prediction": duration_pred,
                "recommendations": self._generate_recommendations(business_risk, business_impact, business_profile),
                "risk_factors": {
                    "supply_chain_exposure": region_overlap,
                    "industry_vulnerability": sector_overlap,
                    "base_event_severity": event_data.get("severity", 0.5)
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing business impact: {e}")
            raise
    
    def _extract_risk_features(self, event_data: Dict[str, Any]) -> List[float]:
        """Extract features for risk prediction"""
        return [
            event_data.get("severity", 0.5),
            self._get_location_risk_score(event_data.get("location", "")),
            self._get_sector_vulnerability_score(event_data.get("impact_sectors", [])),
            self._get_economic_indicator_score()
        ]
    
    def _extract_impact_features(self, event_data: Dict[str, Any]) -> List[float]:
        """Extract features for impact prediction"""
        return [
            event_data.get("severity", 0.5),
            self._get_location_risk_score(event_data.get("location", "")),
            self._get_sector_vulnerability_score(event_data.get("impact_sectors", [])),
            len(event_data.get("impact_sectors", [])) / 10.0,  # Normalized sector count
            self._get_event_type_score(event_data.get("event_type", ""))
        ]
    
    def _extract_duration_features(self, event_data: Dict[str, Any]) -> List[float]:
        """Extract features for duration prediction"""
        return [
            event_data.get("severity", 0.5),
            self._get_event_type_score(event_data.get("event_type", "")),
            self._get_location_risk_score(event_data.get("location", "")),
            len(event_data.get("impact_sectors", [])) / 10.0
        ]
    
    def _get_location_risk_score(self, location: str) -> float:
        """Get risk score based on location"""
        high_risk_locations = ["taiwan", "china", "suez canal", "panama canal", "gulf of mexico"]
        location_lower = location.lower()
        
        for high_risk in high_risk_locations:
            if high_risk in location_lower:
                return 0.8
        
        return 0.4
    
    def _get_sector_vulnerability_score(self, sectors: List[str]) -> float:
        """Get vulnerability score based on affected sectors"""
        high_vulnerability_sectors = ["electronics", "automotive", "pharmaceuticals", "energy"]
        
        if not sectors:
            return 0.5
        
        vulnerability_scores = []
        for sector in sectors:
            if sector.lower() in high_vulnerability_sectors:
                vulnerability_scores.append(0.8)
            else:
                vulnerability_scores.append(0.4)
        
        return sum(vulnerability_scores) / len(vulnerability_scores)
    
    def _get_economic_indicator_score(self) -> float:
        """Get current economic indicator score"""
        # In production, this would fetch real economic data
        return 0.6
    
    def _get_event_type_score(self, event_type: str) -> float:
        """Get score based on event type"""
        event_scores = {
            "weather": 0.7,
            "economic": 0.5,
            "news": 0.6,
            "geopolitical": 0.8
        }
        return event_scores.get(event_type.lower(), 0.5)
    
    def _categorize_impact(self, impact_score: float) -> str:
        """Categorize impact score"""
        if impact_score >= 0.8:
            return "critical"
        elif impact_score >= 0.6:
            return "high"
        elif impact_score >= 0.4:
            return "medium"
        else:
            return "low"
    
    def _predict_affected_sectors(self, event_data: Dict[str, Any]) -> List[str]:
        """Predict which sectors will be affected"""
        base_sectors = event_data.get("impact_sectors", [])
        
        # Add related sectors based on event type
        event_type = event_data.get("event_type", "").lower()
        if event_type == "weather":
            base_sectors.extend(["agriculture", "transportation", "energy"])
        elif event_type == "economic":
            base_sectors.extend(["manufacturing", "retail", "finance"])
        
        return list(set(base_sectors))
    
    def _calculate_region_overlap(self, supply_regions: List[str], event_location: str) -> float:
        """Calculate overlap between supply regions and event location"""
        if not supply_regions or not event_location:
            return 0.0
        
        event_location_lower = event_location.lower()
        overlap_score = 0.0
        
        for region in supply_regions:
            if region.lower() in event_location_lower or event_location_lower in region.lower():
                overlap_score = max(overlap_score, 1.0)
            elif any(word in event_location_lower for word in region.lower().split()):
                overlap_score = max(overlap_score, 0.5)
        
        return overlap_score
    
    def _calculate_sector_overlap(self, business_industry: str, event_sectors: List[str]) -> float:
        """Calculate overlap between business industry and affected sectors"""
        if not business_industry or not event_sectors:
            return 0.0
        
        business_industry_lower = business_industry.lower()
        
        for sector in event_sectors:
            if sector.lower() in business_industry_lower or business_industry_lower in sector.lower():
                return 1.0
        
        return 0.0
    
    def _generate_recommendations(self, risk_level: float, impact_score: float, business_profile: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on risk and impact"""
        recommendations = []
        
        if risk_level >= 0.7:
            recommendations.append("Activate emergency supply chain protocols")
            recommendations.append("Contact alternative suppliers immediately")
        
        if impact_score >= 0.6:
            recommendations.append("Increase inventory buffers for critical materials")
            recommendations.append("Consider expedited shipping for essential components")
        
        if risk_level >= 0.5 and impact_score >= 0.5:
            recommendations.append("Monitor situation closely and prepare contingency plans")
            recommendations.append("Communicate with key stakeholders about potential delays")
        
        return recommendations

# Initialize ML engine
ml_engine = MLInferenceEngine()

@app.get("/")
async def root():
    return {"message": "ML Inference Service", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "ml-inference",
        "models_loaded": len(ml_engine.models),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/predict")
async def predict(prediction_request: Dict[str, Any]):
    """Make ML predictions"""
    try:
        prediction_type = prediction_request.get("type", "risk")
        event_data = prediction_request.get("event_data", {})
        
        # Generate input hash for caching
        input_str = json.dumps(prediction_request, sort_keys=True)
        input_hash = hashlib.md5(input_str.encode()).hexdigest()
        
        # Check cache first
        cached_prediction = redis_client.get_cached_ml_prediction(prediction_type, input_hash)
        if cached_prediction:
            return {"prediction": cached_prediction, "cached": True}
        
        # Make prediction
        if prediction_type == "risk":
            prediction = ml_engine.predict_risk(event_data)
        elif prediction_type == "impact":
            prediction = ml_engine.predict_impact(event_data)
        elif prediction_type == "duration":
            prediction = ml_engine.predict_duration(event_data)
        elif prediction_type == "business_impact":
            business_profile = prediction_request.get("business_profile", {})
            prediction = ml_engine.analyze_business_impact(business_profile, event_data)
        else:
            raise HTTPException(status_code=400, detail="Invalid prediction type")
        
        # Cache prediction
        redis_client.cache_ml_prediction(prediction_type, input_hash, prediction)
        
        # Store prediction in database
        db = SessionLocal()
        try:
            ml_prediction = MLPrediction(
                model_name=prediction_type,
                input_data=prediction_request,
                prediction=prediction,
                confidence_score=prediction.get("confidence", 0.0),
                model_version="1.0.0"
            )
            db.add(ml_prediction)
            db.commit()
        finally:
            db.close()
        
        # Publish prediction event
        message_queue.publish_event(
            exchange=Exchanges.ML_PREDICTIONS,
            routing_key=EventTypes.ML_PREDICTION,
            message={
                "prediction_type": prediction_type,
                "prediction": prediction,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {"prediction": prediction, "cached": False}
        
    except Exception as e:
        logger.error(f"Error making prediction: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")

@app.post("/analyze/business-impact")
async def analyze_business_impact(analysis_request: Dict[str, Any]):
    """Analyze impact on specific business"""
    try:
        business_profile = analysis_request.get("business_profile", {})
        event_data = analysis_request.get("event_data", {})
        
        if not business_profile or not event_data:
            raise HTTPException(status_code=400, detail="Business profile and event data required")
        
        analysis = ml_engine.analyze_business_impact(business_profile, event_data)
        
        return {"analysis": analysis}
        
    except Exception as e:
        logger.error(f"Error analyzing business impact: {e}")
        raise HTTPException(status_code=500, detail="Business impact analysis failed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
