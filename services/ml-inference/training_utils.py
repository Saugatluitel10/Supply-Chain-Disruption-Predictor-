"""
Training Utilities for Supply Chain Disruption Predictor
Includes:
- Labeled dataset creation from historical events
- Temporal cross-validation
- Class balancing for rare disruptions
- Bayesian hyperparameter optimization
"""

import pandas as pd
from typing import List, Tuple, Dict, Any
from sklearn.model_selection import TimeSeriesSplit
from imblearn.over_sampling import SMOTE
from skopt import BayesSearchCV
from sklearn.ensemble import RandomForestClassifier

# --- Labeled Dataset Creation ---
def create_labeled_dataset(events: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert list of historical disruption events to labeled DataFrame.
    Each event should have keys: 'features', 'label', 'timestamp', ...
    """
    return pd.DataFrame(events)

# --- Temporal Cross-Validation ---
def get_time_series_splits(df: pd.DataFrame, n_splits: int = 5) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Perform temporal cross-validation (TimeSeriesSplit) to avoid leakage.
    """
    tscv = TimeSeriesSplit(n_splits=n_splits)
    return list(tscv.split(df))

# --- Class Balancing ---
def balance_classes(X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Use SMOTE to balance rare disruption classes.
    """
    smote = SMOTE()
    X_res, y_res = smote.fit_resample(X, y)
    return X_res, y_res

# --- Bayesian Hyperparameter Optimization ---
def bayesian_hyperopt(X: pd.DataFrame, y: pd.Series, search_spaces: Dict[str, Any], cv=3) -> Any:
    """
    Use Bayesian optimization for hyperparameter tuning (example: RandomForest).
    """
    clf = RandomForestClassifier()
    opt = BayesSearchCV(clf, search_spaces, n_iter=20, cv=cv, n_jobs=-1)
    opt.fit(X, y)
    return opt.best_estimator_, opt.best_params_
