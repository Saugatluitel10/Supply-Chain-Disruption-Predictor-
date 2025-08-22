"""
Model Validation and Backtesting for Supply Chain Disruption Predictor
Covers:
- Backtesting against historical disruption events
- Precision-recall optimization
- Sensitivity analysis
- Business impact validation
"""
import pytest
import requests
import numpy as np

API_GATEWAY_URL = "http://localhost:8000"

@pytest.mark.modelvalidation
def test_backtesting_historical_events():
    """Backtest model predictions against historical events, calculate accuracy."""
    # Fetch historical events and predictions
    resp = requests.get(f"{API_GATEWAY_URL}/api/model/backtest").json()
    assert resp["success"]
    y_true = resp["data"]["actual_labels"]
    y_pred = resp["data"]["predicted_labels"]
    accuracy = np.mean(np.array(y_true) == np.array(y_pred))
    assert accuracy > 0.7  # Example threshold

@pytest.mark.modelvalidation
def test_precision_recall_optimization():
    """Test precision and recall, optimize threshold if possible."""
    resp = requests.get(f"{API_GATEWAY_URL}/api/model/backtest").json()
    y_true = resp["data"]["actual_labels"]
    y_pred = resp["data"]["predicted_labels"]
    from sklearn.metrics import precision_score, recall_score
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    assert precision > 0.6 and recall > 0.6

@pytest.mark.modelvalidation
def test_sensitivity_analysis():
    """Test model under extreme/edge input conditions."""
    extreme_inputs = [{"feature": 1e6}, {"feature": -1e6}, {"feature": 0}]
    for inp in extreme_inputs:
        resp = requests.post(f"{API_GATEWAY_URL}/api/model/predict", json=inp).json()
        assert resp["success"]

@pytest.mark.modelvalidation
def test_business_impact_validation():
    """Validate business impact metrics (cost savings, decision improvement)."""
    resp = requests.get(f"{API_GATEWAY_URL}/api/model/business_impact").json()
    assert resp["success"]
    assert resp["data"]["cost_savings"] >= 0
