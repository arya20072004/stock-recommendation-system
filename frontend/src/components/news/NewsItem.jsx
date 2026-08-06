import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { SentimentBadge } from './SentimentBadge'

export function NewsItem({ article }) {
  // Format the ISO date safely
  let dateDisplay = 'Unknown date'
  if (article.published_at) {
    try {
      const d = new Date(article.published_at)
      dateDisplay = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric' }).format(d)
    } catch (e) {}
  }
  
  return (
    <Card className="news-feed-item" hoverable>
      <div className="news-feed-item__top">
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
          <SentimentBadge sentiment={article.sentiment} />
          <div className="news-feed-item__tickers" aria-label="Associated stocks">
            {article.tickers && article.tickers.map((symbol) => (
              <Link className="ticker-link" key={symbol} to={`/stocks/${encodeURIComponent(symbol)}`}>
                {symbol}
              </Link>
            ))}
          </div>
        </div>
        <time>{dateDisplay}</time>
      </div>
      
      <div className="news-feed-item__body">
        <Link className="news-feed-item__headline" to={`/news/${article.id}`}>
          <h2>{article.headline || article.title}</h2>
        </Link>
        {article.summary && (
          <p className="news-feed-item__summary">{article.summary}</p>
        )}
      </div>
      
      <div className="news-feed-item__bottom">
        <span className="news-feed-item__source">{article.source}</span>
        {article.url && (
          <a href={article.url} target="_blank" rel="noopener noreferrer" className="news-feed-item__link">
            Read Article ↗
          </a>
        )}
      </div>
    </Card>
  )
}
