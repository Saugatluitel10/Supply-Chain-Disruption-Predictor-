"""
Supply Chain Disruption Predictor - Main Application
"""

from flask import Flask, render_template, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
import os
import threading
import schedule
import time
from datetime import datetime, timedelta

from data_collector import DataCollector
from ai_analyzer import AIAnalyzer
from risk_calculator import RiskCalculator

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///supply_chain.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Initialize core components
data_collector = DataCollector()
ai_analyzer = AIAnalyzer()
risk_calculator = RiskCalculator()

# Database Models
class SupplyChainEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    event_type = db.Column(db.String(50), nullable=False)  # news, weather, economic
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    source = db.Column(db.String(100))
    location = db.Column(db.String(100))
    severity = db.Column(db.Float)  # 0-1 scale
    impact_sectors = db.Column(db.Text)  # JSON string of affected sectors
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    processed = db.Column(db.Boolean, default=False)

class RiskAssessment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.String(100), nullable=False)
    sector = db.Column(db.String(100), nullable=False)
    risk_level = db.Column(db.Float, nullable=False)  # 0-1 scale
    risk_factors = db.Column(db.Text)  # JSON string of contributing factors
    recommendations = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class BusinessProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    business_name = db.Column(db.String(200), nullable=False)
    industry = db.Column(db.String(100), nullable=False)
    key_suppliers = db.Column(db.Text)  # JSON string
    supply_regions = db.Column(db.Text)  # JSON string
    critical_materials = db.Column(db.Text)  # JSON string
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Routes
@app.route('/')
def dashboard():
    """Main dashboard view"""
    return render_template('dashboard.html')

@app.route('/api/risk-overview')
def risk_overview():
    """Get current risk overview"""
    try:
        # Get recent risk assessments
        recent_assessments = RiskAssessment.query.filter(
            RiskAssessment.timestamp >= datetime.utcnow() - timedelta(hours=24)
        ).order_by(RiskAssessment.risk_level.desc()).limit(10).all()
        
        risk_data = []
        for assessment in recent_assessments:
            risk_data.append({
                'region': assessment.region,
                'sector': assessment.sector,
                'risk_level': assessment.risk_level,
                'risk_factors': assessment.risk_factors,
                'recommendations': assessment.recommendations,
                'timestamp': assessment.timestamp.isoformat()
            })
        
        return jsonify({
            'status': 'success',
            'data': risk_data,
            'last_updated': datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/recent-events')
def recent_events():
    """Get recent supply chain events"""
    try:
        events = SupplyChainEvent.query.filter(
            SupplyChainEvent.timestamp >= datetime.utcnow() - timedelta(hours=48)
        ).order_by(SupplyChainEvent.severity.desc()).limit(20).all()
        
        events_data = []
        for event in events:
            events_data.append({
                'id': event.id,
                'type': event.event_type,
                'title': event.title,
                'description': event.description,
                'location': event.location,
                'severity': event.severity,
                'impact_sectors': event.impact_sectors,
                'timestamp': event.timestamp.isoformat()
            })
        
        return jsonify({
            'status': 'success',
            'data': events_data
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/business-profile', methods=['GET', 'POST'])
def business_profile():
    """Manage business profile"""
    if request.method == 'POST':
        try:
            data = request.json
            profile = BusinessProfile(
                business_name=data['business_name'],
                industry=data['industry'],
                key_suppliers=data.get('key_suppliers', '[]'),
                supply_regions=data.get('supply_regions', '[]'),
                critical_materials=data.get('critical_materials', '[]')
            )
            db.session.add(profile)
            db.session.commit()
            
            return jsonify({'status': 'success', 'message': 'Profile created successfully'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    else:
        # GET request - return existing profiles
        profiles = BusinessProfile.query.all()
        profiles_data = []
        for profile in profiles:
            profiles_data.append({
                'id': profile.id,
                'business_name': profile.business_name,
                'industry': profile.industry,
                'key_suppliers': profile.key_suppliers,
                'supply_regions': profile.supply_regions,
                'critical_materials': profile.critical_materials
            })
        
        return jsonify({
            'status': 'success',
            'data': profiles_data
        })

@app.route('/api/analyze-impact', methods=['POST'])
def analyze_impact():
    """Analyze potential impact on specific business"""
    try:
        data = request.json
        business_id = data.get('business_id')
        
        if not business_id:
            return jsonify({'status': 'error', 'message': 'Business ID required'}), 400
        
        # Get business profile
        profile = BusinessProfile.query.get(business_id)
        if not profile:
            return jsonify({'status': 'error', 'message': 'Business profile not found'}), 404
        
        # Analyze impact using AI
        impact_analysis = ai_analyzer.analyze_business_impact(profile)
        
        return jsonify({
            'status': 'success',
            'data': impact_analysis
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def run_data_collection():
    """Background task to collect and process data"""
    print("Starting data collection...")
    
    # Collect news data
    news_data = data_collector.collect_news()
    for item in news_data:
        event = SupplyChainEvent(
            event_type='news',
            title=item['title'],
            description=item['description'],
            source=item['source'],
            location=item.get('location', ''),
            severity=item.get('severity', 0.5)
        )
        db.session.add(event)
    
    # Collect weather data
    weather_data = data_collector.collect_weather()
    for item in weather_data:
        event = SupplyChainEvent(
            event_type='weather',
            title=item['title'],
            description=item['description'],
            location=item['location'],
            severity=item['severity']
        )
        db.session.add(event)
    
    # Collect economic data
    economic_data = data_collector.collect_economic()
    for item in economic_data:
        event = SupplyChainEvent(
            event_type='economic',
            title=item['title'],
            description=item['description'],
            severity=item['severity']
        )
        db.session.add(event)
    
    db.session.commit()
    print(f"Data collection completed. Added {len(news_data + weather_data + economic_data)} events.")

def run_risk_analysis():
    """Background task to analyze risks"""
    print("Starting risk analysis...")
    
    # Get unprocessed events
    unprocessed_events = SupplyChainEvent.query.filter_by(processed=False).all()
    
    for event in unprocessed_events:
        # Calculate risk for different regions and sectors
        risk_assessments = risk_calculator.calculate_risks(event)
        
        for assessment in risk_assessments:
            risk_record = RiskAssessment(
                region=assessment['region'],
                sector=assessment['sector'],
                risk_level=assessment['risk_level'],
                risk_factors=assessment['risk_factors'],
                recommendations=assessment['recommendations']
            )
            db.session.add(risk_record)
        
        # Mark event as processed
        event.processed = True
    
    db.session.commit()
    print(f"Risk analysis completed for {len(unprocessed_events)} events.")

def schedule_tasks():
    """Schedule background tasks"""
    schedule.every(30).minutes.do(run_data_collection)
    schedule.every(15).minutes.do(run_risk_analysis)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        
        # Run initial data collection
        run_data_collection()
        run_risk_analysis()
        
        # Start background scheduler
        scheduler_thread = threading.Thread(target=schedule_tasks)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        app.run(debug=True, host='0.0.0.0', port=5000)
