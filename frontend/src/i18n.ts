import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

const resources = {
  en: {
    translation: {
      app_title: 'Supply Chain Predictor',
      dashboard_title: 'Supply Chain Dashboard',
      refresh: 'Refresh',
      collect_data: 'Collect Data',
      heatmap_title: 'Global Disruption Risk Heat Map',
      recent_events: 'Recent Events',
      high_risk_assessments: 'High Risk Assessments',
      active_alerts: 'Active Alerts',
      business_profiles: 'Business Profiles',
      event_severity_trends: 'Event Severity Trends',
      risk_level_by_region: 'Risk Level by Region',
      recent_supply_chain_events: 'Recent Supply Chain Events',
      latest_risk_assessments: 'Latest Risk Assessments',
      last_updated: 'Last updated',
      filters: 'Filters',
      region: 'Region',
      industry: 'Industry',
      clear: 'Clear',
      export: 'Export',
      export_csv: 'Export CSV',
      export_png: 'Export PNG',
    },
  },
  es: {
    translation: {
      app_title: 'Predicción de Cadena de Suministro',
      dashboard_title: 'Panel de Cadena de Suministro',
      refresh: 'Actualizar',
      collect_data: 'Recolectar Datos',
      heatmap_title: 'Mapa de Calor de Riesgo Global',
      recent_events: 'Eventos Recientes',
      high_risk_assessments: 'Evaluaciones de Alto Riesgo',
      active_alerts: 'Alertas Activas',
      business_profiles: 'Perfiles de Negocio',
      event_severity_trends: 'Tendencias de Severidad de Eventos',
      risk_level_by_region: 'Nivel de Riesgo por Región',
      recent_supply_chain_events: 'Eventos Recientes de la Cadena de Suministro',
      latest_risk_assessments: 'Últimas Evaluaciones de Riesgo',
      last_updated: 'Última actualización',
      filters: 'Filtros',
      region: 'Región',
      industry: 'Industria',
      clear: 'Limpiar',
      export: 'Exportar',
      export_csv: 'Exportar CSV',
      export_png: 'Exportar PNG',
    },
  },
};

i18n
  .use(initReactI18next)
  .init({
    resources,
    lng: 'en',
    fallbackLng: 'en',
    interpolation: {
      escapeValue: false,
    },
  });

export default i18n;
