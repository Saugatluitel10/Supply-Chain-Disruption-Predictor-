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
  TextField,
  MenuItem,
  Stack,
} from '@mui/material';
import {
  TrendingUp,
  Warning,
  Business,
  Notifications,
  Refresh,
  Download,
  FilterAlt,
} from '@mui/icons-material';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';
import MapChart from './MapChart';
import { apiService, DashboardOverview, SupplyChainEvent, RiskAssessment } from '../services/api';
import { format } from 'date-fns';
import TimelineChart from './TimelineChart';
import { exportElementToPNG, exportToCSV } from '../utils/export';
import { useTranslation } from 'react-i18next';
import { Link as RouterLink } from 'react-router-dom';

const Dashboard: React.FC = () => {
  const { t } = useTranslation();
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

  const { data: alertsData } = useQuery<{ data: { alerts: any[] } }>(
    'active-alerts-mini',
    apiService.getActiveAlerts,
    { refetchInterval: 15000 }
  );

  const [selectedRegion, setSelectedRegion] = React.useState<string | null>(null);
  const [selectedIndustry, setSelectedIndustry] = React.useState<string | null>(null);

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

  const industryOptions = React.useMemo(() => {
    const sectors = new Set<string>();
    riskAssessments?.data.assessments.forEach((r) => sectors.add(r.sector));
    recentEvents?.data.events.forEach((e) => e.impact_sectors?.forEach((s) => sectors.add(s)));
    return Array.from(sectors).sort();
  }, [riskAssessments, recentEvents]);

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
          {t('dashboard_title')}
        </Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleRefresh}
            sx={{ mr: 2 }}
          >
            {t('refresh')}
          </Button>
          <Button
            variant="contained"
            onClick={handleTriggerDataCollection}
          >
            {t('collect_data')}
          </Button>
        </Box>
      </Box>

      {/* Filters */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'stretch', sm: 'center' }}>
            <Box display="flex" alignItems="center" gap={1} flex={1}>
              <FilterAlt color="action" />
              <Typography variant="subtitle1">{t('filters')}</Typography>
            </Box>
            <TextField
              label={t('region')}
              value={selectedRegion || ''}
              onChange={(e) => setSelectedRegion(e.target.value || null)}
              placeholder={t('region')}
              size="small"
              sx={{ minWidth: 200 }}
            />
            <TextField
              select
              label={t('industry')}
              value={selectedIndustry || ''}
              onChange={(e) => setSelectedIndustry(e.target.value || null)}
              size="small"
              sx={{ minWidth: 200 }}
            >
              <MenuItem value="">—</MenuItem>
              {industryOptions.map((opt) => (
                <MenuItem key={opt} value={opt}>{opt}</MenuItem>
              ))}
            </TextField>
            <Button onClick={() => { setSelectedRegion(null); setSelectedIndustry(null); }}>{t('clear')}</Button>
          </Stack>
        </CardContent>
      </Card>

      {/* Risk Heat Map */}
      <Grid container spacing={3} mb={4}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                {t('heatmap_title')}
              </Typography>
              <Box id="heatmap-card" sx={{ width: '100%', overflowX: 'auto' }}>
                <MapChart
                  data={[
                    { region: 'China', risk: 0.85 },
                    { region: 'United States', risk: 0.55 },
                    { region: 'India', risk: 0.45 },
                    { region: 'Germany', risk: 0.35 },
                    { region: 'Japan', risk: 0.4 },
                    { region: 'South Korea', risk: 0.5 },
                    { region: 'Netherlands', risk: 0.2 },
                    { region: 'Mexico', risk: 0.6 },
                    { region: 'Vietnam', risk: 0.65 },
                    { region: 'Thailand', risk: 0.5 },
                    { region: 'Malaysia', risk: 0.4 },
                    { region: 'Singapore', risk: 0.3 },
                    { region: 'Taiwan', risk: 0.7 },
                  ]}
                  onRegionSelect={(name) => setSelectedRegion(name)}
                  selectedRegion={selectedRegion}
                />
              </Box>
              <Box display="flex" justifyContent="flex-end" mt={1}>
                <Button size="small" startIcon={<Download />} onClick={() => exportElementToPNG('heatmap-card', 'risk-heatmap.png')}>
                  {t('export_png')}
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Overview Cards */}
      <Grid container spacing={3} mb={4}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center">
                <TrendingUp color="primary" sx={{ mr: 2 }} />
                <Box>
                  <Typography color="textSecondary" gutterBottom>
                    {t('recent_events')}
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
                    {t('high_risk_assessments')}
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
                    {t('active_alerts')}
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
                    {t('business_profiles')}
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
        {/* Timeline Visualization */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                <Typography variant="h6">{t('event_severity_trends')}</Typography>
                <Button size="small" startIcon={<Download />} onClick={() => exportElementToPNG('timeline-card', 'timeline.png')}>
                  {t('export_png')}
                </Button>
              </Box>
              {eventsLoading || risksLoading ? (
                <Box display="flex" justifyContent="center" p={4}><CircularProgress /></Box>
              ) : (
                <Box id="timeline-card">
                  <TimelineChart
                    events={recentEvents?.data.events || []}
                    risks={riskAssessments?.data.assessments || []}
                    regionFilter={selectedRegion}
                    industryFilter={selectedIndustry}
                  />
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Recent Events Chart */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                {t('event_severity_trends')}
              </Typography>
              {eventsLoading ? (
                <Box display="flex" justifyContent="center" p={4}>
                  <CircularProgress />
                </Box>
              ) : eventsError ? (
                <Alert severity="error">Failed to load events data</Alert>
              ) : (
                <Box id="events-chart">
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={eventChartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="severity" fill="#8884d8" />
                    </BarChart>
                  </ResponsiveContainer>
                  <Box display="flex" justifyContent="flex-end" mt={1}>
                    <Button size="small" startIcon={<Download />} onClick={() => exportElementToPNG('events-chart', 'events-chart.png')}>
                      {t('export_png')}
                    </Button>
                  </Box>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Risk Assessment Chart */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                {t('risk_level_by_region')}
              </Typography>
              {risksLoading ? (
                <Box display="flex" justifyContent="center" p={4}>
                  <CircularProgress />
                </Box>
              ) : risksError ? (
                <Alert severity="error">Failed to load risk data</Alert>
              ) : (
                <Box id="risk-chart">
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={riskChartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis />
                      <Tooltip />
                      <Line type="monotone" dataKey="risk" stroke="#82ca9d" strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                  <Box display="flex" justifyContent="flex-end" mt={1}>
                    <Button size="small" startIcon={<Download />} onClick={() => exportElementToPNG('risk-chart', 'risk-chart.png')}>
                      {t('export_png')}
                    </Button>
                  </Box>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Recent Events List */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                {t('recent_supply_chain_events')}
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
                  <Box display="flex" justifyContent="flex-end">
                    <Button size="small" startIcon={<Download />} onClick={() => {
                      const rows = (recentEvents?.data.events || []).map(e => ({
                        id: e.id,
                        title: e.title,
                        type: e.type,
                        location: e.location,
                        severity: (e.severity * 100).toFixed(0),
                        timestamp: e.timestamp,
                      }));
                      exportToCSV('events.csv', rows);
                    }}>{t('export_csv')}</Button>
                  </Box>
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
                {t('latest_risk_assessments')}
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
                  <Box display="flex" justifyContent="flex-end">
                    <Button size="small" startIcon={<Download />} onClick={() => {
                      const rows = (riskAssessments?.data.assessments || []).map(r => ({
                        id: r.id,
                        region: r.region,
                        sector: r.sector,
                        risk_level: (r.risk_level * 100).toFixed(1),
                        confidence: (r.confidence_score * 100).toFixed(0),
                        timestamp: r.timestamp,
                      }));
                      exportToCSV('risk-assessments.csv', rows);
                    }}>{t('export_csv')}</Button>
                  </Box>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Alert Center (Prioritized) */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                <Typography variant="h6">{t('active_alerts')}</Typography>
                <Button component={RouterLink} to="/alerts">View All</Button>
              </Box>
              <Box>
                {(alertsData?.data.alerts || [])
                  .slice()
                  .sort((a, b) => {
                    const order = { critical: 3, high: 2, medium: 1, low: 0 } as any;
                    return (order[b.severity] || 0) - (order[a.severity] || 0);
                  })
                  .slice(0, 5)
                  .map((alert) => (
                    <Box key={alert.id} mb={2} p={2} border={1} borderColor="grey.300" borderRadius={1}>
                      <Box display="flex" justifyContent="space-between" alignItems="center">
                        <Typography variant="subtitle2" fontWeight="bold">{alert.title}</Typography>
                        <Chip label={alert.severity} color={alert.severity === 'critical' ? 'error' : alert.severity === 'high' ? 'warning' : 'info'} size="small" />
                      </Box>
                      <Typography variant="body2" color="textSecondary">{alert.message}</Typography>
                    </Box>
                  ))}
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {overview?.data.last_updated && (
        <Box mt={3} textAlign="center">
          <Typography variant="caption" color="textSecondary">
            {t('last_updated')}: {format(new Date(overview.data.last_updated), 'MMM dd, yyyy HH:mm:ss')}
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default Dashboard;
