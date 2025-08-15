import React from 'react';
import { useQuery, useMutation, useQueryClient } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Chip,
  Alert as MuiAlert,
  CircularProgress,
  Button,
  IconButton,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import {
  Notifications,
  CheckCircle,
  ExpandMore,
  NotificationsActive,
} from '@mui/icons-material';
import { apiService, Alert } from '../services/api';
import { format } from 'date-fns';

const Alerts: React.FC = () => {
  const queryClient = useQueryClient();

  const {
    data: alerts,
    isLoading,
    error,
  } = useQuery<{ data: { alerts: Alert[] } }>('active-alerts', apiService.getActiveAlerts);

  const acknowledgeMutation = useMutation(apiService.acknowledgeAlert, {
    onSuccess: () => {
      queryClient.invalidateQueries('active-alerts');
    },
  });

  const resolveMutation = useMutation(apiService.resolveAlert, {
    onSuccess: () => {
      queryClient.invalidateQueries('active-alerts');
    },
  });

  const getSeverityColor = (severity: string) => {
    switch (severity.toLowerCase()) {
      case 'critical':
        return 'error' as const;
      case 'high':
        return 'warning' as const;
      case 'medium':
        return 'info' as const;
      case 'low':
        return 'success' as const;
      default:
        return 'default' as const;
    }
  };

  const getSeverityIcon = (severity: string) => {
    switch (severity.toLowerCase()) {
      case 'critical':
      case 'high':
        return <NotificationsActive color="error" />;
      default:
        return <Notifications color="primary" />;
    }
  };

  const getAlertTypeLabel = (alertType: string) => {
    switch (alertType) {
      case 'risk_assessment':
        return 'Risk Assessment';
      case 'business_impact':
        return 'Business Impact';
      case 'data_collection':
        return 'Data Collection';
      default:
        return alertType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }
  };

  const handleAcknowledge = (alertId: string) => {
    acknowledgeMutation.mutate(alertId);
  };

  const handleResolve = (alertId: string) => {
    resolveMutation.mutate(alertId);
  };

  if (isLoading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <MuiAlert severity="error" sx={{ mb: 2 }}>
        Failed to load alerts. Please try again.
      </MuiAlert>
    );
  }

  const activeAlerts = alerts?.data.alerts || [];
  const criticalAlerts = activeAlerts.filter(alert => alert.severity === 'critical');
  const highAlerts = activeAlerts.filter(alert => alert.severity === 'high');
  const otherAlerts = activeAlerts.filter(alert => !['critical', 'high'].includes(alert.severity));

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          Active Alerts
        </Typography>
        <Box display="flex" gap={1}>
          <Chip
            label={`${criticalAlerts.length} Critical`}
            color="error"
            variant={criticalAlerts.length > 0 ? "filled" : "outlined"}
          />
          <Chip
            label={`${highAlerts.length} High`}
            color="warning"
            variant={highAlerts.length > 0 ? "filled" : "outlined"}
          />
          <Chip
            label={`${otherAlerts.length} Other`}
            color="info"
            variant={otherAlerts.length > 0 ? "filled" : "outlined"}
          />
        </Box>
      </Box>

      {/* Critical Alerts */}
      {criticalAlerts.length > 0 && (
        <Box mb={4}>
          <Typography variant="h5" color="error" gutterBottom>
            Critical Alerts
          </Typography>
          <Grid container spacing={2}>
            {criticalAlerts.map((alert) => (
              <Grid item xs={12} key={alert.id}>
                <Card sx={{ borderLeft: 4, borderLeftColor: 'error.main' }}>
                  <CardContent>
                    <Box display="flex" justifyContent="space-between" alignItems="flex-start">
                      <Box display="flex" alignItems="flex-start" flex={1}>
                        {getSeverityIcon(alert.severity)}
                        <Box ml={2} flex={1}>
                          <Typography variant="h6" gutterBottom>
                            {alert.title}
                          </Typography>
                          <Typography variant="body2" color="textSecondary" paragraph>
                            {alert.message}
                          </Typography>
                          <Box display="flex" gap={1} mb={2}>
                            <Chip
                              label={getAlertTypeLabel(alert.alert_type)}
                              size="small"
                              variant="outlined"
                            />
                            <Chip
                              label={alert.severity}
                              color={getSeverityColor(alert.severity)}
                              size="small"
                            />
                          </Box>
                          <Typography variant="caption" color="textSecondary">
                            {format(new Date(alert.created_at), 'MMM dd, yyyy HH:mm:ss')}
                          </Typography>
                        </Box>
                      </Box>
                      <Box display="flex" flexDirection="column" gap={1}>
                        <Button
                          size="small"
                          variant="outlined"
                          onClick={() => handleAcknowledge(alert.id)}
                          disabled={acknowledgeMutation.isLoading}
                        >
                          Acknowledge
                        </Button>
                        <Button
                          size="small"
                          variant="contained"
                          color="success"
                          startIcon={<CheckCircle />}
                          onClick={() => handleResolve(alert.id)}
                          disabled={resolveMutation.isLoading}
                        >
                          Resolve
                        </Button>
                      </Box>
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Box>
      )}

      {/* High Priority Alerts */}
      {highAlerts.length > 0 && (
        <Box mb={4}>
          <Typography variant="h5" color="warning.main" gutterBottom>
            High Priority Alerts
          </Typography>
          <Grid container spacing={2}>
            {highAlerts.map((alert) => (
              <Grid item xs={12} md={6} key={alert.id}>
                <Card sx={{ borderLeft: 4, borderLeftColor: 'warning.main' }}>
                  <CardContent>
                    <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={2}>
                      <Box display="flex" alignItems="center">
                        {getSeverityIcon(alert.severity)}
                        <Typography variant="h6" ml={1}>
                          {alert.title}
                        </Typography>
                      </Box>
                      <Chip
                        label={alert.severity}
                        color={getSeverityColor(alert.severity)}
                        size="small"
                      />
                    </Box>
                    
                    <Typography variant="body2" color="textSecondary" paragraph>
                      {alert.message.length > 100 
                        ? `${alert.message.substring(0, 100)}...`
                        : alert.message
                      }
                    </Typography>
                    
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Typography variant="caption" color="textSecondary">
                        {format(new Date(alert.created_at), 'MMM dd, HH:mm')}
                      </Typography>
                      <Box display="flex" gap={1}>
                        <Button
                          size="small"
                          onClick={() => handleAcknowledge(alert.id)}
                        >
                          Acknowledge
                        </Button>
                        <Button
                          size="small"
                          variant="contained"
                          onClick={() => handleResolve(alert.id)}
                        >
                          Resolve
                        </Button>
                      </Box>
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Box>
      )}

      {/* Other Alerts */}
      {otherAlerts.length > 0 && (
        <Box>
          <Typography variant="h5" gutterBottom>
            Other Alerts
          </Typography>
          {otherAlerts.map((alert) => (
            <Accordion key={alert.id}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Box display="flex" alignItems="center" width="100%">
                  <Box display="flex" alignItems="center" flex={1}>
                    {getSeverityIcon(alert.severity)}
                    <Typography variant="subtitle1" ml={1}>
                      {alert.title}
                    </Typography>
                  </Box>
                  <Box display="flex" gap={1} mr={2}>
                    <Chip
                      label={getAlertTypeLabel(alert.alert_type)}
                      size="small"
                      variant="outlined"
                    />
                    <Chip
                      label={alert.severity}
                      color={getSeverityColor(alert.severity)}
                      size="small"
                    />
                  </Box>
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" color="textSecondary" paragraph>
                  {alert.message}
                </Typography>
                
                {alert.metadata && Object.keys(alert.metadata).length > 0 && (
                  <Box mb={2}>
                    <Typography variant="subtitle2" gutterBottom>
                      Additional Information
                    </Typography>
                    <Box display="flex" flexWrap="wrap" gap={0.5}>
                      {Object.entries(alert.metadata).slice(0, 5).map(([key, value]) => (
                        <Chip
                          key={key}
                          label={`${key.replace(/_/g, ' ')}: ${typeof value === 'object' ? JSON.stringify(value).substring(0, 20) + '...' : value}`}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </Box>
                )}
                
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="caption" color="textSecondary">
                    Created: {format(new Date(alert.created_at), 'MMM dd, yyyy HH:mm:ss')}
                  </Typography>
                  <Box display="flex" gap={1}>
                    <Button
                      size="small"
                      onClick={() => handleAcknowledge(alert.id)}
                      disabled={acknowledgeMutation.isLoading}
                    >
                      Acknowledge
                    </Button>
                    <Button
                      size="small"
                      variant="contained"
                      onClick={() => handleResolve(alert.id)}
                      disabled={resolveMutation.isLoading}
                    >
                      Resolve
                    </Button>
                  </Box>
                </Box>
              </AccordionDetails>
            </Accordion>
          ))}
        </Box>
      )}

      {activeAlerts.length === 0 && (
        <Box textAlign="center" py={8}>
          <CheckCircle sx={{ fontSize: 64, color: 'success.main', mb: 2 }} />
          <Typography variant="h6" color="textSecondary" gutterBottom>
            No Active Alerts
          </Typography>
          <Typography variant="body2" color="textSecondary">
            All alerts have been resolved. Your supply chain is operating normally.
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default Alerts;
