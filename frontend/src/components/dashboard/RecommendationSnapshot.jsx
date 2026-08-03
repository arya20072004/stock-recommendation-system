import { Card } from '../common/Card'

export function RecommendationSnapshot({ snapshot }) {
  const metrics = [{ label: 'Buy', value: snapshot.buy, tone: 'positive' }, { label: 'Hold', value: snapshot.hold, tone: 'warning' }, { label: 'Sell', value: snapshot.sell, tone: 'negative' }]
  return <section aria-labelledby="recommendation-snapshot-heading"><h2 id="recommendation-snapshot-heading" className="section-label">Recommendation snapshot</h2><Card className="recommendation-snapshot">{metrics.map(({ label, value, tone }) => <div key={label} className={`snapshot-metric snapshot-metric--${tone}`}><strong className="mono">{value}</strong><span>{label}</span></div>)}</Card></section>
}
