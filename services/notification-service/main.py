"""
Notification Service - Real-time alerts and notifications
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
import asyncio
import logging
from datetime import datetime, timedelta
import json
import sys
import os
from typing import Dict, Any, List
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Add shared modules to path
sys.path.append('/app/shared')
sys.path.append('../../shared')

from database import SessionLocal, Alert, BusinessProfile, RiskAssessment
from redis_client import RedisClient
from message_queue import MessageQueue, EventTypes, Exchanges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Notification Service", version="1.0.0")

# Initialize clients
redis_client = RedisClient()
message_queue = MessageQueue()

class NotificationManager:
    def __init__(self):
        self.notification_channels = {
            "email": self._send_email_notification,
            "webhook": self._send_webhook_notification,
            "sms": self._send_sms_notification
        }
        
        self.alert_thresholds = {
            "critical": 0.8,
            "high": 0.6,
            "medium": 0.4,
            "low": 0.2
        }
        
        # Email configuration (mock for demo)
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.email_user = os.getenv("EMAIL_USER", "alerts@supplychain.com")
        self.email_password = os.getenv("EMAIL_PASSWORD", "password")
    
    async def process_risk_alert(self, risk_data: Dict[str, Any]):
        """Process risk assessment and generate alerts if necessary"""
        try:
            risk_assessments = risk_data.get("risk_assessments", [])
            
            for assessment in risk_assessments:
                risk_level = assessment.get("risk_level", 0.0)
                
                # Determine alert severity
                alert_severity = self._determine_alert_severity(risk_level)
                
                if alert_severity != "none":
                    # Create alert
                    alert_data = {
                        "alert_type": "risk_assessment",
                        "title": f"{alert_severity.title()} Risk Alert - {assessment.get('region', 'Unknown')} {assessment.get('sector', 'Sector')}",
                        "message": self._generate_risk_alert_message(assessment),
                        "severity": alert_severity,
                        "metadata": {
                            "risk_level": risk_level,
                            "region": assessment.get("region"),
                            "sector": assessment.get("sector"),
                            "event_id": assessment.get("event_id"),
                            "recommendations": assessment.get("recommendations", [])
                        }
                    }
                    
                    # Store alert in database
                    alert_id = await self._store_alert(alert_data)
                    
                    # Send notifications to relevant businesses
                    await self._notify_affected_businesses(alert_data, assessment)
                    
                    logger.info(f"Generated {alert_severity} alert for {assessment.get('region')} {assessment.get('sector')}")
            
        except Exception as e:
            logger.error(f"Error processing risk alert: {e}")
    
    async def process_business_impact_alert(self, impact_data: Dict[str, Any]):
        """Process business impact analysis and generate targeted alerts"""
        try:
            business_id = impact_data.get("business_id")
            analysis = impact_data.get("analysis", {})
            
            business_risk_level = analysis.get("business_risk_level", 0.0)
            alert_severity = self._determine_alert_severity(business_risk_level)
            
            if alert_severity != "none":
                alert_data = {
                    "business_profile_id": business_id,
                    "alert_type": "business_impact",
                    "title": f"Business Impact Alert - {alert_severity.title()} Risk Detected",
                    "message": self._generate_business_impact_message(analysis),
                    "severity": alert_severity,
                    "metadata": {
                        "business_risk_level": business_risk_level,
                        "impact_score": analysis.get("business_impact_score", 0.0),
                        "recommendations": analysis.get("recommendations", []),
                        "risk_factors": analysis.get("risk_factors", {})
                    }
                }
                
                # Store alert
                alert_id = await self._store_alert(alert_data)
                
                # Send notification to specific business
                await self._send_business_notification(business_id, alert_data)
                
                logger.info(f"Generated business impact alert for business {business_id}")
        
        except Exception as e:
            logger.error(f"Error processing business impact alert: {e}")
    
    async def _store_alert(self, alert_data: Dict[str, Any]) -> str:
        """Store alert in database"""
        db = SessionLocal()
        try:
            alert = Alert(
                business_profile_id=alert_data.get("business_profile_id"),
                alert_type=alert_data["alert_type"],
                title=alert_data["title"],
                message=alert_data["message"],
                severity=alert_data["severity"],
                status="active",
                metadata=alert_data["metadata"]
            )
            
            db.add(alert)
            db.commit()
            db.refresh(alert)
            
            # Cache alert for real-time access
            redis_client.store_real_time_data(
                "alert",
                {
                    "id": str(alert.id),
                    "title": alert.title,
                    "severity": alert.severity,
                    "alert_type": alert.alert_type,
                    "business_profile_id": str(alert.business_profile_id) if alert.business_profile_id else None
                }
            )
            
            return str(alert.id)
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error storing alert: {e}")
            raise
        finally:
            db.close()
    
    async def _notify_affected_businesses(self, alert_data: Dict[str, Any], assessment: Dict[str, Any]):
        """Notify businesses that might be affected by the risk"""
        db = SessionLocal()
        try:
            region = assessment.get("region", "").lower()
            sector = assessment.get("sector", "").lower()
            
            # Find businesses that might be affected
            affected_businesses = db.query(BusinessProfile).all()
            
            for business in affected_businesses:
                # Check if business is affected
                is_affected = self._is_business_affected(business, region, sector)
                
                if is_affected:
                    business_alert_data = alert_data.copy()
                    business_alert_data["business_profile_id"] = str(business.id)
                    
                    await self._send_business_notification(str(business.id), business_alert_data)
            
        except Exception as e:
            logger.error(f"Error notifying affected businesses: {e}")
        finally:
            db.close()
    
    async def _send_business_notification(self, business_id: str, alert_data: Dict[str, Any]):
        """Send notification to a specific business"""
        db = SessionLocal()
        try:
            business = db.query(BusinessProfile).filter(BusinessProfile.id == business_id).first()
            if not business:
                return
            
            notification_preferences = business.notification_preferences or {}
            
            # Send notifications based on preferences
            for channel, enabled in notification_preferences.items():
                if enabled and channel in self.notification_channels:
                    try:
                        await self.notification_channels[channel](business, alert_data)
                    except Exception as e:
                        logger.error(f"Error sending {channel} notification to business {business_id}: {e}")
            
            # If no preferences set, send email by default
            if not notification_preferences:
                try:
                    await self._send_email_notification(business, alert_data)
                except Exception as e:
                    logger.error(f"Error sending default email notification: {e}")
        
        except Exception as e:
            logger.error(f"Error sending business notification: {e}")
        finally:
            db.close()
    
    async def _send_email_notification(self, business: BusinessProfile, alert_data: Dict[str, Any]):
        """Send email notification"""
        try:
            # Mock email sending for demo
            logger.info(f"Sending email notification to {business.business_name}")
            logger.info(f"Subject: {alert_data['title']}")
            logger.info(f"Message: {alert_data['message']}")
            
            # In production, implement actual email sending
            # msg = MIMEMultipart()
            # msg['From'] = self.email_user
            # msg['To'] = business_email
            # msg['Subject'] = alert_data['title']
            # msg.attach(MIMEText(alert_data['message'], 'plain'))
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
    
    async def _send_webhook_notification(self, business: BusinessProfile, alert_data: Dict[str, Any]):
        """Send webhook notification"""
        try:
            # Mock webhook sending for demo
            logger.info(f"Sending webhook notification to {business.business_name}")
            logger.info(f"Webhook data: {json.dumps(alert_data, indent=2)}")
            
            # In production, implement actual webhook sending
            # async with httpx.AsyncClient() as client:
            #     await client.post(webhook_url, json=alert_data)
            
        except Exception as e:
            logger.error(f"Error sending webhook: {e}")
    
    async def _send_sms_notification(self, business: BusinessProfile, alert_data: Dict[str, Any]):
        """Send SMS notification"""
        try:
            # Mock SMS sending for demo
            logger.info(f"Sending SMS notification to {business.business_name}")
            logger.info(f"SMS: {alert_data['title']} - {alert_data['message'][:100]}...")
            
            # In production, implement actual SMS sending via Twilio or similar
            
        except Exception as e:
            logger.error(f"Error sending SMS: {e}")
    
    def _determine_alert_severity(self, risk_level: float) -> str:
        """Determine alert severity based on risk level"""
        if risk_level >= self.alert_thresholds["critical"]:
            return "critical"
        elif risk_level >= self.alert_thresholds["high"]:
            return "high"
        elif risk_level >= self.alert_thresholds["medium"]:
            return "medium"
        elif risk_level >= self.alert_thresholds["low"]:
            return "low"
        else:
            return "none"
    
    def _generate_risk_alert_message(self, assessment: Dict[str, Any]) -> str:
        """Generate risk alert message"""
        region = assessment.get("region", "Unknown Region")
        sector = assessment.get("sector", "Unknown Sector")
        risk_level = assessment.get("risk_level", 0.0)
        recommendations = assessment.get("recommendations", [])
        
        message = f"Risk Alert for {region} - {sector}\n\n"
        message += f"Risk Level: {risk_level:.2%}\n"
        message += f"Confidence: {assessment.get('confidence_score', 0.0):.2%}\n\n"
        
        if recommendations:
            message += "Recommendations:\n"
            for i, rec in enumerate(recommendations[:3], 1):  # Limit to top 3
                message += f"{i}. {rec}\n"
        
        message += f"\nGenerated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        
        return message
    
    def _generate_business_impact_message(self, analysis: Dict[str, Any]) -> str:
        """Generate business impact alert message"""
        business_risk = analysis.get("business_risk_level", 0.0)
        impact_score = analysis.get("business_impact_score", 0.0)
        recommendations = analysis.get("recommendations", [])
        
        message = f"Business Impact Alert\n\n"
        message += f"Risk Level: {business_risk:.2%}\n"
        message += f"Impact Score: {impact_score:.2%}\n\n"
        
        risk_factors = analysis.get("risk_factors", {})
        if risk_factors:
            message += "Risk Factors:\n"
            for factor, value in risk_factors.items():
                if isinstance(value, (int, float)):
                    message += f"• {factor.replace('_', ' ').title()}: {value:.2f}\n"
                else:
                    message += f"• {factor.replace('_', ' ').title()}: {value}\n"
            message += "\n"
        
        if recommendations:
            message += "Immediate Actions Required:\n"
            for i, rec in enumerate(recommendations[:3], 1):
                message += f"{i}. {rec}\n"
        
        message += f"\nGenerated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        
        return message
    
    def _is_business_affected(self, business: BusinessProfile, region: str, sector: str) -> bool:
        """Check if a business is affected by the risk"""
        # Check supply regions
        supply_regions = business.supply_regions or []
        for supply_region in supply_regions:
            if region in supply_region.lower() or supply_region.lower() in region:
                return True
        
        # Check industry/sector match
        business_industry = business.industry.lower()
        if sector in business_industry or business_industry in sector:
            return True
        
        return False

# Initialize notification manager
notification_manager = NotificationManager()

@app.get("/")
async def root():
    return {"message": "Notification Service", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "notification-service",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/send-alert")
async def send_alert(alert_request: Dict[str, Any]):
    """Send a custom alert"""
    try:
        alert_type = alert_request.get("type", "custom")
        
        if alert_type == "risk_assessment":
            await notification_manager.process_risk_alert(alert_request)
        elif alert_type == "business_impact":
            await notification_manager.process_business_impact_alert(alert_request)
        else:
            # Handle custom alerts
            alert_data = {
                "alert_type": "custom",
                "title": alert_request.get("title", "Custom Alert"),
                "message": alert_request.get("message", ""),
                "severity": alert_request.get("severity", "medium"),
                "metadata": alert_request.get("metadata", {})
            }
            
            alert_id = await notification_manager._store_alert(alert_data)
            
            # Send to specific business if specified
            business_id = alert_request.get("business_id")
            if business_id:
                await notification_manager._send_business_notification(business_id, alert_data)
        
        return {"message": "Alert sent successfully"}
        
    except Exception as e:
        logger.error(f"Error sending alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to send alert")

@app.get("/alerts/active")
async def get_active_alerts():
    """Get active alerts"""
    try:
        db = SessionLocal()
        alerts = db.query(Alert).filter(Alert.status == "active").order_by(Alert.created_at.desc()).limit(50).all()
        
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
        raise HTTPException(status_code=500, detail="Failed to get alerts")
    finally:
        db.close()

@app.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str):
    """Acknowledge an alert"""
    try:
        db = SessionLocal()
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        
        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        alert.acknowledged_at = datetime.utcnow()
        db.commit()
        
        return {"message": "Alert acknowledged"}
        
    except Exception as e:
        logger.error(f"Error acknowledging alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to acknowledge alert")
    finally:
        db.close()

@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Resolve an alert"""
    try:
        db = SessionLocal()
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        
        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        alert.status = "resolved"
        alert.resolved_at = datetime.utcnow()
        db.commit()
        
        return {"message": "Alert resolved"}
        
    except Exception as e:
        logger.error(f"Error resolving alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to resolve alert")
    finally:
        db.close()

# Message queue consumer for processing alerts
async def start_message_consumers():
    """Start message queue consumers"""
    try:
        def process_risk_alert(message):
            asyncio.create_task(notification_manager.process_risk_alert(message))
        
        def process_business_impact(message):
            asyncio.create_task(notification_manager.process_business_impact_alert(message))
        
        # Start consumers in background
        asyncio.create_task(asyncio.to_thread(
            message_queue.consume_events,
            "risk_alerts_queue",
            process_risk_alert,
            Exchanges.RISK_ALERTS,
            EventTypes.RISK_CALCULATED
        ))
        
        asyncio.create_task(asyncio.to_thread(
            message_queue.consume_events,
            "business_impact_queue",
            process_business_impact,
            Exchanges.ML_PREDICTIONS,
            EventTypes.BUSINESS_IMPACT
        ))
        
        logger.info("Message queue consumers started")
        
    except Exception as e:
        logger.error(f"Error starting message consumers: {e}")

@app.on_event("startup")
async def startup_event():
    """Start message consumers on service startup"""
    logger.info("Notification Service starting up")
    await start_message_consumers()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
