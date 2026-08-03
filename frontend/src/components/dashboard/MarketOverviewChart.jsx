import { useState } from 'react'
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Card } from '../common/Card'
import { Button } from '../common/Button'
import { formatMarketNumber, formatPercent } from '../../utils/formatters'

const timeframes = ['1D', '1W', '1M']

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return <div className="market-chart-tooltip"><span>{label}</span><strong className="mono">{formatMarketNumber(payload[0].value)}</strong></div>
}

export function MarketOverviewChart({ overview }) {
  const [timeframe, setTimeframe] = useState('1D')
  const data = overview.seriesByRange[timeframe]
  return <section aria-labelledby="market-overview-heading"><div className="section-heading"><div><h2 id="market-overview-heading">Market overview</h2><p>{overview.label} <span className="metric-positive mono">{formatPercent(overview.change)}</span></p></div><div className="timeframe-controls" aria-label="Market overview timeframe">{timeframes.map((range) => <Button key={range} variant="ghost" className={`timeframe-button ${timeframe === range ? 'timeframe-button--active' : ''}`} onClick={() => setTimeframe(range)} aria-pressed={timeframe === range}>{range}</Button>)}</div></div><Card className="market-chart-card"><div className="market-chart" role="img" aria-label={`Demo ${overview.label} market movement over ${timeframe}`}><ResponsiveContainer width="100%" height="100%"><AreaChart data={data} margin={{ top: 10, right: 4, left: -22, bottom: 0 }}><defs><linearGradient id="market-area" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stopColor="#4c8dff" stopOpacity={0.24} /><stop offset="100%" stopColor="#4c8dff" stopOpacity={0} /></linearGradient></defs><CartesianGrid vertical={false} stroke="#252d3a" strokeDasharray="3 5" /><XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fill: '#667085', fontSize: 11 }} minTickGap={24} /><YAxis dataKey="value" axisLine={false} tickLine={false} tick={false} width={24} domain={['dataMin - 30', 'dataMax + 30']} /><Tooltip content={<ChartTooltip />} cursor={{ stroke: '#303a49', strokeWidth: 1 }} /><Area type="monotone" dataKey="value" stroke="#4c8dff" strokeWidth={2} fill="url(#market-area)" isAnimationActive={false} /></AreaChart></ResponsiveContainer></div></Card></section>
}
