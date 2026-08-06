import { Card } from '../common/Card'

export function SentimentOverview({ meta }) {
  if (!meta) return null
  
  const counts = meta.sentiment_counts || { POSITIVE: 0, NEUTRAL: 0, NEGATIVE: 0, UNSCORED: 0 }
  const total = meta.total || 1 // Avoid divide by zero
  
  return (
    <Card className="sentiment-overview">
      <div className="section-heading">
        <div>
          <h2>Sentiment overview</h2>
          <p>Based on {meta.total || 0} articles</p>
        </div>
      </div>
      <div className="sentiment-overview__bar" aria-label="Sentiment distribution">
        <span className="sentiment-overview__segment sentiment-overview__segment--positive" style={{ width: `${(counts.POSITIVE / total) * 100}%` }} />
        <span className="sentiment-overview__segment sentiment-overview__segment--neutral" style={{ width: `${(counts.NEUTRAL / total) * 100}%` }} />
        <span className="sentiment-overview__segment sentiment-overview__segment--negative" style={{ width: `${(counts.NEGATIVE / total) * 100}%` }} />
      </div>
      <div className="sentiment-overview__legend">
        {['POSITIVE', 'NEUTRAL', 'NEGATIVE'].map((sentiment) => (
          <div key={sentiment}>
            <span className={`sentiment-dot sentiment-dot--${sentiment.toLowerCase()}`} />
            <strong>{Math.round(((counts[sentiment] || 0) / total) * 100)}%</strong>
            <span>{sentiment}</span>
          </div>
        ))}
      </div>
    </Card>
  )
}
