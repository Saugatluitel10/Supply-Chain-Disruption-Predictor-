import React from 'react';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Chip,
  Alert,
  CircularProgress,
  LinearProgress,
} from '@mui/material';
import { Assessment, TrendingUp } from '@mui/icons-material';
import { apiService, RiskAssessment } from '../services/api';
import { format } from 'date-fns';

const RiskAssessments: React.FC = () => {
  const {
    data: assessments,
    isLoading,
    error,
  } = useQuery<{ data: { assessments: RiskAssessment[] } }>('recent-risks', () => apiService.getRecentRiskAssessments(50));

  const getRiskLevel = (riskLevel: number) => {
    if (riskLevel >= 0.8) return { label: 'Critical', color: 'error' as const, bgColor: '#ffebee' };
    if (riskLevel >= 0.6) return { label: 'High', color: 'warning' as const, bgColor: '#fff3e0' };
    if (riskLevel >= 0.4) return { label: 'Medium', color: 'info' as const, bgColor: '#e3f2fd' };
    return { label: 'Low', color: 'success' as const, bgColor: '#e8f5e8' };
  };

  const getRiskColor = (riskLevel: number) => {
    if (riskLevel >= 0.8) return 'error';
    if (riskLevel >= 0.6) return 'warning';
    if (riskLevel >= 0.4) return 'info';
    return 'success';
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
      <Alert severity="error" sx={{ mb: 2 }}>
        Failed to load risk assessments. Please try again.
      </Alert>
    );
  }

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          Risk Assessments
        </Typography>
      </Box>

      <Grid container spacing={3}>
        {assessments?.data.assessments.map((assessment) => {
          const riskLevel = getRiskLevel(assessment.risk_level);
          
          return (
            <Grid item xs={12} md={6} lg={4} key={assessment.id}>
              <Card sx={{ backgroundColor: riskLevel.bgColor }}>
                <CardContent>
                  <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={2}>
                    <Box display="flex" alignItems="center">
                      <Assessment color="primary" sx={{ mr: 1 }} />
                      <Box>
                        <Typography variant="h6" component="h2">
                          {assessment.region}
                        </Typography>
                        <Typography variant="subtitle2" color="textSecondary">
                          {assessment.sector}
                        </Typography>
                      </Box>
                    </Box>
                    <Chip
                      label={riskLevel.label}
                      color={riskLevel.color}
                      size="small"
                    />
                  </Box>

                  <Box mb={2}>
                    <Typography variant="subtitle2" gutterBottom>
                      Risk Level: {(assessment.risk_level * 100).toFixed(1)}%
                    </Typography>
                    <LinearProgress
                      variant="determinate"
                      value={assessment.risk_level * 100}
                      color={getRiskColor(assessment.risk_level)}
                      sx={{ height: 8, borderRadius: 4 }}
                    />
                  </Box>

                  <Box mb={2}>
                    <Typography variant="subtitle2" gutterBottom>
                      Confidence Score: {(assessment.confidence_score * 100).toFixed(0)}%
                    </Typography>
                    <LinearProgress
                      variant="determinate"
                      value={assessment.confidence_score * 100}
                      color="info"
                      sx={{ height: 6, borderRadius: 3 }}
                    />
                  </Box>

                  {assessment.recommendations && (
                    <Box mb={2}>
                      <Typography variant="subtitle2" gutterBottom>
                        Recommendations
                      </Typography>
                      <Typography variant="body2" color="textSecondary">
                        {assessment.recommendations.length > 150 
                          ? `${assessment.recommendations.substring(0, 150)}...`
                          : assessment.recommendations
                        }
                      </Typography>
                    </Box>
                  )}

                  {assessment.risk_factors && (
                    <Box mb={2}>
                      <Typography variant="subtitle2" gutterBottom>
                        Risk Factors
                      </Typography>
                      <Box display="flex" flexWrap="wrap" gap={0.5}>
                        {Object.entries(assessment.risk_factors).slice(0, 3).map(([key, value]) => (
                          <Chip
                            key={key}
                            label={`${key.replace(/_/g, ' ')}: ${typeof value === 'number' ? (value * 100).toFixed(0) + '%' : value}`}
                            size="small"
                            variant="outlined"
                          />
                        ))}
                      </Box>
                    </Box>
                  )}

                  <Typography variant="caption" color="textSecondary">
                    Assessed: {format(new Date(assessment.timestamp), 'MMM dd, yyyy HH:mm')}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>

      {assessments?.data.assessments.length === 0 && (
        <Box textAlign="center" py={8}>
          <TrendingUp sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
          <Typography variant="h6" color="textSecondary" gutterBottom>
            No Risk Assessments Available
          </Typography>
          <Typography variant="body2" color="textSecondary">
            Risk assessments will appear here once data collection and analysis begins.
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default RiskAssessments;
