import React from 'react';
import { ComposableMap, Geographies, Geography } from 'react-simple-maps';
import { scaleLinear } from 'd3-scale';

// Example topojson for world map
const geoUrl = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json';

interface RegionRisk {
  region: string;
  risk: number; // 0 to 1
}

interface MapChartProps {
  data: RegionRisk[];
}

// Simple mapping from region name to ISO A3 code (should be expanded for real data)
const regionToISO: Record<string, string> = {
  'China': 'CHN',
  'United States': 'USA',
  'India': 'IND',
  'Germany': 'DEU',
  'Japan': 'JPN',
  'South Korea': 'KOR',
  'Netherlands': 'NLD',
  'Mexico': 'MEX',
  'Vietnam': 'VNM',
  'Thailand': 'THA',
  'Malaysia': 'MYS',
  'Singapore': 'SGP',
  'Taiwan': 'TWN',
  // Add more as needed
};

const riskColor = scaleLinear<string>()
  .domain([0, 0.4, 0.6, 0.8, 1])
  .range(['#e8f5e9', '#e3f2fd', '#fff3e0', '#ff9800', '#f44336']);

const MapChart: React.FC<MapChartProps> = ({ data }) => {
  // Map region risk to ISO code for coloring
  const riskByISO: Record<string, number> = {};
  data.forEach(({ region, risk }) => {
    const iso = regionToISO[region];
    if (iso) riskByISO[iso] = risk;
  });

  return (
    <ComposableMap projection="geoMercator" width={800} height={400}>
      <Geographies geography={geoUrl}>
        {({ geographies }) =>
          geographies.map((geo) => {
            const iso = geo.properties.ISO_A3;
            const risk = riskByISO[iso] ?? 0;
            return (
              <Geography
                key={geo.rsmKey}
                geography={geo}
                fill={riskColor(risk)}
                stroke="#FFF"
                style={{ outline: 'none' }}
              />
            );
          })
        }
      </Geographies>
    </ComposableMap>
  );
};

export default MapChart;
