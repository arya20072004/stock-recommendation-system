import { ArrowLeft } from 'lucide-react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { PageHeader } from '../components/layout/PageHeader'
import { RelatedStockCard } from '../components/news/RelatedStockCard'
import { SentimentBadge } from '../components/news/SentimentBadge'
import { getNewsById } from '../mocks/news'
import { stockBySymbol } from '../mocks/stocks'
import '../components/news/news.css'
import './news-page.css'

export function NewsDetail() {
  const { newsId } = useParams()
  const navigate = useNavigate()
  const article = getNewsById(newsId)
  if (!article) return <div className="news-detail-page"><PageHeader title="News not found" description="The demo article you requested is not available." /><EmptyState title="News article not found" description={`No demo article exists for "${newsId}".`} action={{ label: 'Back to News Intelligence', onClick: () => navigate('/news') }} /></div>

  const relatedStocks = article.tickers.map((symbol) => stockBySymbol[symbol]).filter(Boolean)
  const impact = article.sentiment === 'POSITIVE' ? `Positive company-specific development with potentially supportive near-term sentiment for ${article.tickers.join(' and ')}.` : article.sentiment === 'NEGATIVE' ? `Negative or cautionary development that may keep near-term sentiment measured for ${article.tickers.join(' and ')}.` : `Mixed development that is useful context for monitoring ${article.tickers.join(' and ')} alongside the model signal.`

  return <div className="news-detail-page">
    <Link className="news-back-link" to="/news"><ArrowLeft size={16} aria-hidden="true" />Back to News Intelligence</Link>
    <PageHeader title="News Detail" actions={<Badge tone="accent">Demo data</Badge>} />
    <Card className="news-detail-article"><div className="news-detail-article__meta"><SentimentBadge sentiment={article.sentiment} /><span>{article.source}</span><span>•</span><time>{article.publishedAt}</time></div><h1>{article.headline}</h1><p className="news-detail-article__summary">{article.summary}</p></Card>
    <div className="news-detail-grid">
      <div className="news-detail-section"><h2>News summary</h2><Card><p>{article.summary} This is StockIntel demo intelligence, not a reproduction of a full source article.</p></Card><h2>Potential market impact</h2><Card><p>{impact}</p></Card></div>
      <div className="news-detail-section"><h2>Intelligence</h2><Card><dl className="intelligence-list"><div><dt>Sentiment</dt><dd><SentimentBadge sentiment={article.sentiment} /></dd></div><div><dt>Relevance</dt><dd>{article.relevance}%</dd></div><div><dt>Category</dt><dd>{article.category}</dd></div><div><dt>Stocks</dt><dd>{article.tickers.join(' · ')}</dd></div></dl></Card></div>
    </div>
    <section className="news-detail-section" aria-labelledby="related-stocks-heading"><div className="section-heading"><div><h2 id="related-stocks-heading">Related stocks</h2><p>Current demo metadata and model context.</p></div></div><div className="related-stocks">{relatedStocks.map((stock) => <RelatedStockCard key={stock.ticker} stock={stock} />)}</div></section>
  </div>
}
