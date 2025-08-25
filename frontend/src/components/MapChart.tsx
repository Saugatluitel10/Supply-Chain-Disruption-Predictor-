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
  onRegionSelect?: (region: string) => void;
  selectedRegion?: string | null;
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

const MapChart: React.FC<MapChartProps> = ({ data, onRegionSelect, selectedRegion }) => {
  const containerRef = React.useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = React.useState<number>(800);

  React.useEffect(() => {
    const handleResize = () => {
      const w = containerRef.current?.offsetWidth || 800;
      setWidth(w);
    };
    handleResize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Map region risk to ISO code for coloring
  const riskByISO: Record<string, number> = {};
  data.forEach(({ region, risk }) => {
    const iso = regionToISO[region];
    if (iso) riskByISO[iso] = risk;
  });

  const height = Math.max(240, Math.round(width * 0.5));

  return (
    <div ref={containerRef} style={{ width: '100%' }}>
      <ComposableMap projection="geoMercator" width={width} height={height}>
        <Geographies geography={geoUrl}>
          {({ geographies }) =>
            geographies.map((geo) => {
              const iso = geo.properties.ISO_A3;
              const name = geo.properties.NAME as string;
              const risk = riskByISO[iso] ?? 0;
              const isSelected = selectedRegion && name === selectedRegion;
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={riskColor(risk)}
                  stroke={isSelected ? '#000' : '#FFF'}
                  strokeWidth={isSelected ? 1.5 : 0.75}
                  style={{ outline: 'none', cursor: onRegionSelect ? 'pointer' : 'default' }}
                  onClick={onRegionSelect ? () => onRegionSelect(name) : undefined}
                  title={`${name}: ${(risk * 100).toFixed(0)}%`}
                />
              );
            })
          }
        </Geographies>
      </ComposableMap>
    </div>
  );
};

export default MapChart;
