import { Card } from '../common/Card'
import { getSentimentCounts } from '../../mocks/news'

export function NewsSummaryCards({ articles }) {
  const counts = getSentimentCounts(articles)
  const dominant = Object.entries(counts).sort(([, a], [, b]) => b - a)[0]?.[0] ?? 'NEUTRAL'
  return <section className="news-summary-grid" aria-label="News sentiment summary">
    <Card className="news-summary-card news-summary-card--overall"><span className="eyebrow">Market sentiment</span><strong className={`sentiment-text sentiment-text--${dominant.toLowerCase()}`}>{dominant}</strong><span className="news-summary-card__detail">Across {articles.length} demo articles</span></Card>
    {['POSITIVE', 'NEUTRAL', 'NEGATIVE'].map((sentiment) => <Card className={`news-summary-card news-summary-card--${sentiment.toLowerCase()}`} key={sentiment}><span className="eyebrow">{sentiment}</span><strong>{counts[sentiment]}</strong><span className="news-summary-card__detail">Articles</span></Card>)}
  </section>
}
