import React from 'react';
import { ResponsiveContainer, ComposedChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Bar, Legend } from 'recharts';
import { SupplyChainEvent, RiskAssessment } from '../services/api';
import { format } from 'date-fns';

export interface TimelineChartProps {
  events: SupplyChainEvent[];
  risks: RiskAssessment[];
  regionFilter?: string | null;
  industryFilter?: string | null;
}

interface TimelinePoint {
  date: string; // ISO date (day)
  predictedRisk: number; // 0-100
  eventSeverity: number; // 0-100 aggregate
}

function groupByDay<T>(items: T[], getDate: (i: T) => Date) {
  const map = new Map<string, T[]>();
  items.forEach((item) => {
    const d = getDate(item);
    const key = format(d, 'yyyy-MM-dd');
    if (!map.has(key)) map.set(key, []);
    map.get(key)!.push(item);
  });
  return map;
}

const TimelineChart: React.FC<TimelineChartProps> = ({ events, risks, regionFilter, industryFilter }) => {
  const filteredEvents = React.useMemo(() => {
    return events.filter((e) => {
      const regionOk = !regionFilter || e.location?.toLowerCase().includes(regionFilter.toLowerCase());
      const industryOk = !industryFilter || e.impact_sectors?.includes(industryFilter);
      return regionOk && industryOk;
    });
  }, [events, regionFilter, industryFilter]);

  const filteredRisks = React.useMemo(() => {
    return risks.filter((r) => {
      const regionOk = !regionFilter || r.region?.toLowerCase().includes(regionFilter.toLowerCase());
      const industryOk = !industryFilter || r.sector === industryFilter;
      return regionOk && industryOk;
    });
  }, [risks, regionFilter, industryFilter]);

  const points: TimelinePoint[] = React.useMemo(() => {
    const eventByDay = groupByDay(filteredEvents, (e) => new Date(e.timestamp));
    const riskByDay = groupByDay(filteredRisks, (r) => new Date(r.timestamp));
    const allDays = new Set<string>([...eventByDay.keys(), ...riskByDay.keys()]);
    const data: TimelinePoint[] = Array.from(allDays)
      .sort()
      .map((day) => {
        const ev = eventByDay.get(day) || [];
        const rk = riskByDay.get(day) || [];
        const eventSeverity = ev.reduce((sum: number, e) => sum + (e.severity || 0) * 100, 0);
        const predictedRisk = rk.length > 0 ? (rk.reduce((sum, r) => sum + (r.risk_level || 0) * 100, 0) / rk.length) : 0;
        return { date: day, eventSeverity, predictedRisk };
      });
    // Ensure ordered by date
    return data.sort((a, b) => a.date.localeCompare(b.date));
  }, [filteredEvents, filteredRisks]);

  return (
    <ResponsiveContainer width="100%" height={300}>
      <ComposedChart data={points} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" tickFormatter={(d) => format(new Date(d), 'MMM dd')} minTickGap={24} />
        <YAxis yAxisId="left" label={{ value: 'Predicted Risk (%)', angle: -90, position: 'insideLeft' }} domain={[0, 100]} />
        <YAxis yAxisId="right" orientation="right" label={{ value: 'Event Severity (sum %)', angle: 90, position: 'insideRight' }} domain={[0, 'auto']} />
        <Tooltip labelFormatter={(d) => format(new Date(d), 'PP')} />
        <Legend />
        <Area yAxisId="left" type="monotone" dataKey="predictedRisk" name="Predicted Risk" stroke="#1976d2" fill="#90caf9" fillOpacity={0.5} />
        <Bar yAxisId="right" dataKey="eventSeverity" name="Event Severity" fill="#f44336" opacity={0.8} />
      </ComposedChart>
    </ResponsiveContainer>
  );
};

export default TimelineChart;
