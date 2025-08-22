"""
Performance and Failover Tests for Supply Chain Disruption Predictor
Covers:
- Load testing under concurrent user scenarios
- Failover and resilience
"""
import pytest
import requests
import threading
import time

API_GATEWAY_URL = "http://localhost:8000"

@pytest.mark.performance
def test_concurrent_requests():
    """Simulate concurrent users accessing dashboard and triggering data collection."""
    num_threads = 10
    results = []
    def worker():
        resp = requests.get(f"{API_GATEWAY_URL}/api/dashboard/overview")
        results.append(resp.status_code)
    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert all(code == 200 for code in results)

@pytest.mark.failover
def test_failover_recovery():
    """Simulate a service failure and check system recovery (manual step required)."""
    # This is a placeholder: in real CI, you would kill a container and check auto-recovery
    # For now, just check service is up
    resp = requests.get(f"{API_GATEWAY_URL}/health")
    assert resp.status_code == 200
