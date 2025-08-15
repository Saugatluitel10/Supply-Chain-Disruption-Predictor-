import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Grid,
  Chip,
  Alert,
  CircularProgress,
  IconButton,
} from '@mui/material';
import { Add, Business, Edit } from '@mui/icons-material';
import { apiService, BusinessProfile } from '../services/api';
import { format } from 'date-fns';

const BusinessProfiles: React.FC = () => {
  const [open, setOpen] = useState(false);
  const [formData, setFormData] = useState({
    business_name: '',
    industry: '',
    key_suppliers: '',
    supply_regions: '',
    critical_materials: '',
    risk_tolerance: 0.5,
  });

  const queryClient = useQueryClient();

  const {
    data: profiles,
    isLoading,
    error,
  } = useQuery<{ data: { profiles: BusinessProfile[] } }>('business-profiles', apiService.getBusinessProfiles);

  const createProfileMutation = useMutation(apiService.createBusinessProfile, {
    onSuccess: () => {
      queryClient.invalidateQueries('business-profiles');
      setOpen(false);
      resetForm();
    },
  });

  const handleSubmit = () => {
    const profileData = {
      ...formData,
      key_suppliers: formData.key_suppliers.split(',').map(s => s.trim()).filter(s => s),
      supply_regions: formData.supply_regions.split(',').map(s => s.trim()).filter(s => s),
      critical_materials: formData.critical_materials.split(',').map(s => s.trim()).filter(s => s),
    };

    createProfileMutation.mutate(profileData);
  };

  const resetForm = () => {
    setFormData({
      business_name: '',
      industry: '',
      key_suppliers: '',
      supply_regions: '',
      critical_materials: '',
      risk_tolerance: 0.5,
    });
  };

  const handleClose = () => {
    setOpen(false);
    resetForm();
  };

  const getRiskToleranceLabel = (tolerance: number) => {
    if (tolerance >= 0.8) return { label: 'High', color: 'success' as const };
    if (tolerance >= 0.5) return { label: 'Medium', color: 'warning' as const };
    return { label: 'Low', color: 'error' as const };
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
        Failed to load business profiles. Please try again.
      </Alert>
    );
  }

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          Business Profiles
        </Typography>
        <Button
          variant="contained"
          startIcon={<Add />}
          onClick={() => setOpen(true)}
        >
          Add Profile
        </Button>
      </Box>

      <Grid container spacing={3}>
        {profiles?.data.profiles.map((profile) => {
          const riskTolerance = getRiskToleranceLabel(profile.risk_tolerance);
          
          return (
            <Grid item xs={12} md={6} lg={4} key={profile.id}>
              <Card>
                <CardContent>
                  <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={2}>
                    <Box display="flex" alignItems="center">
                      <Business color="primary" sx={{ mr: 1 }} />
                      <Typography variant="h6" component="h2">
                        {profile.business_name}
                      </Typography>
                    </Box>
                    <IconButton size="small">
                      <Edit />
                    </IconButton>
                  </Box>

                  <Typography variant="body2" color="textSecondary" gutterBottom>
                    Industry: {profile.industry}
                  </Typography>

                  <Box mb={2}>
                    <Typography variant="subtitle2" gutterBottom>
                      Risk Tolerance
                    </Typography>
                    <Chip
                      label={riskTolerance.label}
                      color={riskTolerance.color}
                      size="small"
                    />
                  </Box>

                  {profile.supply_regions && profile.supply_regions.length > 0 && (
                    <Box mb={2}>
                      <Typography variant="subtitle2" gutterBottom>
                        Supply Regions
                      </Typography>
                      <Box display="flex" flexWrap="wrap" gap={0.5}>
                        {profile.supply_regions.slice(0, 3).map((region, index) => (
                          <Chip
                            key={index}
                            label={region}
                            size="small"
                            variant="outlined"
                          />
                        ))}
                        {profile.supply_regions.length > 3 && (
                          <Chip
                            label={`+${profile.supply_regions.length - 3} more`}
                            size="small"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    </Box>
                  )}

                  {profile.critical_materials && profile.critical_materials.length > 0 && (
                    <Box mb={2}>
                      <Typography variant="subtitle2" gutterBottom>
                        Critical Materials
                      </Typography>
                      <Box display="flex" flexWrap="wrap" gap={0.5}>
                        {profile.critical_materials.slice(0, 2).map((material, index) => (
                          <Chip
                            key={index}
                            label={material}
                            size="small"
                            color="secondary"
                            variant="outlined"
                          />
                        ))}
                        {profile.critical_materials.length > 2 && (
                          <Chip
                            label={`+${profile.critical_materials.length - 2} more`}
                            size="small"
                            color="secondary"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    </Box>
                  )}

                  <Typography variant="caption" color="textSecondary">
                    Created: {format(new Date(profile.created_at), 'MMM dd, yyyy')}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>

      {/* Add Profile Dialog */}
      <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
        <DialogTitle>Add Business Profile</DialogTitle>
        <DialogContent>
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Business Name"
                value={formData.business_name}
                onChange={(e) => setFormData({ ...formData, business_name: e.target.value })}
                required
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Industry"
                value={formData.industry}
                onChange={(e) => setFormData({ ...formData, industry: e.target.value })}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Key Suppliers (comma-separated)"
                value={formData.key_suppliers}
                onChange={(e) => setFormData({ ...formData, key_suppliers: e.target.value })}
                multiline
                rows={2}
                helperText="Enter supplier names separated by commas"
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Supply Regions (comma-separated)"
                value={formData.supply_regions}
                onChange={(e) => setFormData({ ...formData, supply_regions: e.target.value })}
                multiline
                rows={2}
                helperText="Enter regions separated by commas (e.g., Asia, Europe, North America)"
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Critical Materials (comma-separated)"
                value={formData.critical_materials}
                onChange={(e) => setFormData({ ...formData, critical_materials: e.target.value })}
                multiline
                rows={2}
                helperText="Enter critical materials separated by commas"
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Risk Tolerance"
                type="number"
                value={formData.risk_tolerance}
                onChange={(e) => setFormData({ ...formData, risk_tolerance: parseFloat(e.target.value) })}
                inputProps={{ min: 0, max: 1, step: 0.1 }}
                helperText="Risk tolerance level (0.0 = Low, 1.0 = High)"
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Cancel</Button>
          <Button
            onClick={handleSubmit}
            variant="contained"
            disabled={createProfileMutation.isLoading || !formData.business_name || !formData.industry}
          >
            {createProfileMutation.isLoading ? <CircularProgress size={20} /> : 'Create Profile'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default BusinessProfiles;
