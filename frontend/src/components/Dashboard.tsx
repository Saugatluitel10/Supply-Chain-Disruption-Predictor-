import React from 'react';
import { useQuery } from 'react-query';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  CircularProgress,
  Alert,
  Button,
  Chip,
} from '@mui/material';
import {
  TrendingUp,
  Warning,
  Business,
  Notifications,
  Refresh,
} from '@mui/icons-material';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';
import { apiService, DashboardOverview, SupplyChainEvent, RiskAssessment } from '../services/api';
import { format } from 'date-fns';

const Dashboard: React.FC = () => {
  const {
    data: overview,
    isLoading: overviewLoading,
    error: overviewError,
    refetch: refetchOverview,
  } = useQuery<{ data: DashboardOverview }>('dashboard-overview', apiService.getDashboardOverview);

  const {
    data: recentEvents,
    isLoading: eventsLoading,
    error: eventsError,
  } = useQuery<{ data: { events: SupplyChainEvent[] } }>('recent-events', () => apiService.getRecentEvents(10));

  const {
    data: riskAssessments,
    isLoading: risksLoading,
    error: risksError,
  } = useQuery<{ data: { assessments: RiskAssessment[] } }>('recent-risks', () => apiService.getRecentRiskAssessments(10));

  const handleRefresh = () => {
    refetchOverview();
  };

  const handleTriggerDataCollection = async () => {
    try {
      await apiService.triggerDataCollection();
      // Refresh data after collection
      setTimeout(() => {
        refetchOverview();
      }, 2000);
    } catch (error) {
      console.error('Failed to trigger data collection:', error);
    }
  };

  const getSeverityColor = (severity: number) => {
    if (severity >= 0.8) return 'error';
    if (severity >= 0.6) return 'warning';
    if (severity >= 0.4) return 'info';
    return 'success';
  };

  const getRiskColor = (riskLevel: number) => {
    if (riskLevel >= 0.8) return '#f44336';
    if (riskLevel >= 0.6) return '#ff9800';
    if (riskLevel >= 0.4) return '#2196f3';
    return '#4caf50';
  };

  // Prepare chart data
  const eventChartData = recentEvents?.data.events.slice(0, 7).map((event, index) => ({
    name: `Event ${index + 1}`,
    severity: event.severity * 100,
    type: event.type,
  })) || [];

  const riskChartData = riskAssessments?.data.assessments.slice(0, 7).map((risk, index) => ({
    name: risk.region.substring(0, 10),
    risk: risk.risk_level * 100,
    sector: risk.sector,
  })) || [];

  if (overviewLoading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  if (overviewError) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        Failed to load dashboard data. Please try again.
      </Alert>
    );
  }

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          Supply Chain Dashboard
        </Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleRefresh}
            sx={{ mr: 2 }}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            onClick={handleTriggerDataCollection}
          >
            Collect Data
          </Button>
        </Box>
      </Box>

      {/* Overview Cards */}
      <Grid container spacing={3} mb={4}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center">
                <TrendingUp color="primary" sx={{ mr: 2 }} />
                <Box>
                  <Typography color="textSecondary" gutterBottom>
                    Recent Events
                  </Typography>
                  <Typography variant="h4">
                    {overview?.data.recent_events || 0}
                  </Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center">
                <Warning color="warning" sx={{ mr: 2 }} />
                <Box>
                  <Typography color="textSecondary" gutterBottom>
                    High Risk Assessments
                  </Typography>
                  <Typography variant="h4">
                    {overview?.data.high_risk_assessments || 0}
                  </Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center">
                <Notifications color="error" sx={{ mr: 2 }} />
                <Box>
                  <Typography color="textSecondary" gutterBottom>
                    Active Alerts
                  </Typography>
                  <Typography variant="h4">
                    {overview?.data.active_alerts || 0}
                  </Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center">
                <Business color="info" sx={{ mr: 2 }} />
                <Box>
                  <Typography color="textSecondary" gutterBottom>
                    Business Profiles
                  </Typography>
                  <Typography variant="h4">
                    {overview?.data.business_profiles || 0}
                  </Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Grid container spacing={3}>
        {/* Recent Events Chart */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Event Severity Trends
              </Typography>
              {eventsLoading ? (
                <Box display="flex" justifyContent="center" p={4}>
                  <CircularProgress />
                </Box>
              ) : eventsError ? (
                <Alert severity="error">Failed to load events data</Alert>
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={eventChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="severity" fill="#8884d8" />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Risk Assessment Chart */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Risk Level by Region
              </Typography>
              {risksLoading ? (
                <Box display="flex" justifyContent="center" p={4}>
                  <CircularProgress />
                </Box>
              ) : risksError ? (
                <Alert severity="error">Failed to load risk data</Alert>
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={riskChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="risk" stroke="#82ca9d" strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Recent Events List */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Recent Supply Chain Events
              </Typography>
              {eventsLoading ? (
                <CircularProgress />
              ) : eventsError ? (
                <Alert severity="error">Failed to load events</Alert>
              ) : (
                <Box>
                  {recentEvents?.data.events.slice(0, 5).map((event) => (
                    <Box key={event.id} mb={2} p={2} border={1} borderColor="grey.300" borderRadius={1}>
                      <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={1}>
                        <Typography variant="subtitle2" fontWeight="bold">
                          {event.title}
                        </Typography>
                        <Chip
                          label={`${(event.severity * 100).toFixed(0)}%`}
                          color={getSeverityColor(event.severity)}
                          size="small"
                        />
                      </Box>
                      <Typography variant="body2" color="textSecondary" mb={1}>
                        {event.description.substring(0, 100)}...
                      </Typography>
                      <Box display="flex" justifyContent="space-between" alignItems="center">
                        <Typography variant="caption" color="textSecondary">
                          {event.location} • {format(new Date(event.timestamp), 'MMM dd, HH:mm')}
                        </Typography>
                        <Chip label={event.type} size="small" variant="outlined" />
                      </Box>
                    </Box>
                  ))}
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Recent Risk Assessments */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Latest Risk Assessments
              </Typography>
              {risksLoading ? (
                <CircularProgress />
              ) : risksError ? (
                <Alert severity="error">Failed to load risk assessments</Alert>
              ) : (
                <Box>
                  {riskAssessments?.data.assessments.slice(0, 5).map((risk) => (
                    <Box key={risk.id} mb={2} p={2} border={1} borderColor="grey.300" borderRadius={1}>
                      <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                        <Typography variant="subtitle2" fontWeight="bold">
                          {risk.region} - {risk.sector}
                        </Typography>
                        <Box
                          sx={{
                            width: 12,
                            height: 12,
                            borderRadius: '50%',
                            backgroundColor: getRiskColor(risk.risk_level),
                          }}
                        />
                      </Box>
                      <Typography variant="body2" color="textSecondary" mb={1}>
                        Risk Level: {(risk.risk_level * 100).toFixed(1)}%
                      </Typography>
                      <Typography variant="caption" color="textSecondary">
                        {format(new Date(risk.timestamp), 'MMM dd, HH:mm')} • 
                        Confidence: {(risk.confidence_score * 100).toFixed(0)}%
                      </Typography>
                    </Box>
                  ))}
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {overview?.data.last_updated && (
        <Box mt={3} textAlign="center">
          <Typography variant="caption" color="textSecondary">
            Last updated: {format(new Date(overview.data.last_updated), 'MMM dd, yyyy HH:mm:ss')}
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default Dashboard;
