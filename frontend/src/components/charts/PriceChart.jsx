import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Card } from '../common/Card'
import { Button } from '../common/Button'
import { LoadingState } from '../common/LoadingState'
import { EmptyState } from '../common/EmptyState'
import { formatCurrency } from '../../utils/formatters'
import './charts.css'

const timeframes = ['1M', '3M', '6M', '1Y', '5Y']

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return <div className="chart-tooltip"><span>{label}</span><strong className="mono">{formatCurrency(payload[0].value)}</strong></div>
}

export function PriceChart({ chartData, range, onRangeChange, loading, direction = 'positive' }) {
  const hexColor = direction === 'negative' ? '#f05252' : '#20c77a'
  
  return (
    <section aria-labelledby="price-chart-heading">
      <div className="section-heading">
        <h2 id="price-chart-heading">Price history</h2>
        <div className="timeframe-controls" aria-label="Price chart timeframe">
          {timeframes.map(r => (
            <Button key={r} variant="ghost" className={`timeframe-button ${range === r ? 'timeframe-button--active' : ''}`} onClick={() => onRangeChange(r)} aria-pressed={range === r} disabled={loading}>{r}</Button>
          ))}
        </div>
      </div>
      
      <Card className="price-chart-card">
        {loading ? (
          <LoadingState label="Loading chart data..." />
        ) : chartData.length === 0 ? (
          <EmptyState title="No chart data" description={`No historical data available for ${range}.`} />
        ) : (
          <div className="price-chart" role="img" aria-label={`Stock price movement over ${range}`}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 10, right: 4, left: -22, bottom: 0 }}>
                <defs>
                  <linearGradient id="stock-price-area" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor={hexColor} stopOpacity={0.24} />
                    <stop offset="100%" stopColor={hexColor} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid vertical={false} stroke="#252d3a" strokeDasharray="3 5" />
                <XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fill: '#667085', fontSize: 11 }} minTickGap={24} />
                <YAxis dataKey="close" axisLine={false} tickLine={false} tick={false} width={24} domain={['dataMin', 'dataMax']} />
                <Tooltip content={<ChartTooltip />} cursor={{ stroke: '#303a49', strokeWidth: 1 }} />
                <Area type="monotone" dataKey="close" stroke={hexColor} strokeWidth={2} fill="url(#stock-price-area)" isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </Card>
    </section>
  )
}
