import { Card } from '../common/Card'

export function NewsSummaryCards({ meta }) {
  if (!meta) return null
  
  const counts = meta.sentiment_counts || { POSITIVE: 0, NEUTRAL: 0, NEGATIVE: 0, UNSCORED: 0 }
  const total = meta.total || 0
  
  // Find the dominant sentiment among POS, NEU, NEG
  const dominant = ['POSITIVE', 'NEUTRAL', 'NEGATIVE'].sort((a, b) => (counts[b] || 0) - (counts[a] || 0))[0]

  return (
    <section className="news-summary-grid" aria-label="News sentiment summary">
      <Card className="news-summary-card news-summary-card--overall">
        <span className="eyebrow">Market sentiment</span>
        <strong className={`sentiment-text sentiment-text--${dominant.toLowerCase()}`}>{dominant}</strong>
        <span className="news-summary-card__detail">Across {total} articles</span>
      </Card>
      {['POSITIVE', 'NEUTRAL', 'NEGATIVE'].map((sentiment) => (
        <Card className={`news-summary-card news-summary-card--${sentiment.toLowerCase()}`} key={sentiment}>
          <span className="eyebrow">{sentiment}</span>
          <strong>{counts[sentiment] || 0}</strong>
          <span className="news-summary-card__detail">Articles</span>
        </Card>
      ))}
      {(counts.UNSCORED || 0) > 0 && (
        <Card className="news-summary-card news-summary-card--neutral">
          <span className="eyebrow">Unscored</span>
          <strong>{counts.UNSCORED}</strong>
          <span className="news-summary-card__detail">Articles</span>
        </Card>
      )}
    </section>
  )
}
