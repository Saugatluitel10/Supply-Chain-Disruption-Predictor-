import React from 'react';
import { Routes, Route } from 'react-router-dom';
import { Box, AppBar, Toolbar, Typography, Container } from '@mui/material';
import Dashboard from './components/Dashboard';
import BusinessProfiles from './components/BusinessProfiles';
import RiskAssessments from './components/RiskAssessments';
import Alerts from './components/Alerts';
import Navigation from './components/Navigation';

function App() {
  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static" elevation={0}>
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Supply Chain Predictor
          </Typography>
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
