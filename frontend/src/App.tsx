import React from 'react';
import { Routes, Route } from 'react-router-dom';
import { Box, AppBar, Toolbar, Typography, Container, Select, MenuItem } from '@mui/material';
import Dashboard from './components/Dashboard';
import BusinessProfiles from './components/BusinessProfiles';
import RiskAssessments from './components/RiskAssessments';
import Alerts from './components/Alerts';
import Navigation from './components/Navigation';
import { useTranslation } from 'react-i18next';

function App() {
  const { t, i18n } = useTranslation();
  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static" elevation={0}>
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            {t('app_title')}
          </Typography>
          <Select
            size="small"
            value={i18n.language?.startsWith('es') ? 'es' : 'en'}
            onChange={(e) => i18n.changeLanguage(e.target.value as string)}
            sx={{ color: 'inherit', borderColor: 'rgba(255,255,255,0.3)',
              '& .MuiSelect-icon': { color: 'inherit' },
              '& .MuiOutlinedInput-notchedOutline': { borderColor: 'rgba(255,255,255,0.3)' }
            }}
            variant="outlined"
          >
            <MenuItem value="en">EN</MenuItem>
            <MenuItem value="es">ES</MenuItem>
          </Select>
        </Toolbar>
      </AppBar>
      
      <Box sx={{ display: 'flex' }}>
        <Navigation />
        
        <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
          <Container maxWidth="xl">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/dashboard" element={<Dashboard />} />
              <Route path="/business-profiles" element={<BusinessProfiles />} />
              <Route path="/risk-assessments" element={<RiskAssessments />} />
              <Route path="/alerts" element={<Alerts />} />
            </Routes>
          </Container>
        </Box>
      </Box>
    </Box>
  );
}

export default App;
