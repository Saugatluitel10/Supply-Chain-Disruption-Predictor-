import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to ${config.url}`);
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor
api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

// API endpoints
export const apiService = {
  // Dashboard endpoints
  getDashboardOverview: () => api.get('/api/dashboard/overview'),
  
  // Events endpoints
  getRecentEvents: (limit = 20) => api.get(`/api/events/recent?limit=${limit}`),
  
  // Risk assessments endpoints
  getRecentRiskAssessments: (limit = 20) => api.get(`/api/risk-assessments/recent?limit=${limit}`),
  
  // Business profiles endpoints
  getBusinessProfiles: () => api.get('/api/business-profiles'),
  createBusinessProfile: (profileData: any) => api.post('/api/business-profiles', profileData),
  
  // Alerts endpoints
  getActiveAlerts: () => api.get('/api/alerts/active'),
  acknowledgeAlert: (alertId: string) => api.post(`/api/alerts/${alertId}/acknowledge`),
  resolveAlert: (alertId: string) => api.post(`/api/alerts/${alertId}/resolve`),
  
  // Data collection endpoints
  triggerDataCollection: () => api.post('/api/data-collection/trigger'),
  
  // ML inference endpoints
  getMlPrediction: (predictionData: any) => api.post('/api/ml-inference/predict', predictionData),
  
  // Risk analysis endpoints
  analyzeRisk: (analysisData: any) => api.post('/api/risk-assessment/analyze', analysisData),
  
  // Health check
  getHealthStatus: () => api.get('/health'),
};

// Types
export interface DashboardOverview {
  recent_events: number;
  high_risk_assessments: number;
  active_alerts: number;
  business_profiles: number;
  last_updated: string;
}

export interface SupplyChainEvent {
  id: string;
  type: string;
  title: string;
  description: string;
  location: string;
  severity: number;
  impact_sectors: string[];
  timestamp: string;
  source: string;
}

export interface RiskAssessment {
  id: string;
  region: string;
  sector: string;
  risk_level: number;
  risk_factors: any;
  recommendations: string;
  confidence_score: number;
  timestamp: string;
}

export interface BusinessProfile {
  id: string;
  business_name: string;
  industry: string;
  key_suppliers: string[];
  supply_regions: string[];
  critical_materials: string[];
  risk_tolerance: number;
  created_at: string;
}

export interface Alert {
  id: string;
  business_profile_id?: string;
  alert_type: string;
  title: string;
  message: string;
  severity: string;
  status: string;
  metadata: any;
  created_at: string;
}

export default api;
