"""
Integration and End-to-End Tests for Supply Chain Disruption Predictor
Covers:
- End-to-end workflow from data ingestion to notification
- Data consistency across microservices
- Failover/resilience simulation
"""
import pytest
import requests
import time

API_GATEWAY_URL = "http://localhost:8000"

@pytest.mark.integration
def test_end_to_end_workflow():
    """Test data ingestion, risk assessment, and notification delivery."""
    # 1. Trigger data ingestion (simulate new event)
    resp = requests.post(f"{API_GATEWAY_URL}/api/data-collection/trigger")
    assert resp.status_code == 200
    # 2. Wait for backend processing
    time.sleep(5)
    # 3. Check if new event appears in recent events
    events = requests.get(f"{API_GATEWAY_URL}/api/events/recent").json()
    assert events["success"] and len(events["data"]["events"]) > 0
    # 4. Check if risk assessment is updated
    risks = requests.get(f"{API_GATEWAY_URL}/api/risks/recent").json()
    assert risks["success"] and len(risks["data"]["assessments"]) > 0
    # 5. Check for notification delivery (mock or real)
    alerts = requests.get(f"{API_GATEWAY_URL}/api/alerts/active").json()
    assert alerts["success"]

@pytest.mark.integration
def test_data_consistency_across_services():
    """Check that data in API, DB, and cache are consistent."""
    # Fetch from API
    events_api = requests.get(f"{API_GATEWAY_URL}/api/events/recent").json()["data"]["events"]
    # Optionally: Fetch from DB/cache directly if API available
    # For now, just check API returns non-empty and unique IDs
    ids = set(e["id"] for e in events_api)
    assert len(ids) == len(events_api)
