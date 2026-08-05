import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { SentimentBadge } from './SentimentBadge'
import { toRouteTicker } from '../../mocks/stocks'

export function NewsItem({ article }) {
  return <Card className="news-feed-item" hoverable><div className="news-feed-item__top"><SentimentBadge sentiment={article.sentiment} /><time>{article.publishedAt}</time></div><Link className="news-feed-item__headline" to={`/news/${article.id}`}><h2>{article.headline}</h2></Link><p>{article.summary}</p><div className="news-feed-item__meta"><span>{article.source}</span><span>{article.category}</span><span className="news-feed-item__relevance">Relevance {article.relevance}%</span></div><div className="news-feed-item__tickers" aria-label="Associated stocks">{article.tickers.map((symbol) => <Link className="ticker-link" key={symbol} to={`/stocks/${toRouteTicker(symbol)}`}>{symbol}</Link>)}</div></Card>
}
