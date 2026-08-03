import { Badge } from '../common/Badge'
import { Card } from '../common/Card'
import './stocks.css'

const interpretationTones = { Bullish: 'positive', Bearish: 'negative', Above: 'positive', Below: 'negative', Near: 'warning', Neutral: 'neutral', Moderate: 'warning', High: 'negative', Low: 'positive' }

export function TechnicalIndicators({ indicators }) {
  if (!indicators?.length) return null
  return <Card className="indicators-card"><div className="indicators-table" role="table" aria-label="Technical indicators">{indicators.map(({ name, value, interpretation }) => <div className="indicator-row" role="row" key={name}><span className="indicator-name" role="cell">{name}</span><span className="indicator-value mono" role="cell">{value}</span><span role="cell"><Badge tone={interpretationTones[interpretation] ?? 'neutral'}>{interpretation}</Badge></span></div>)}</div></Card>
}
