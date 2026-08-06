import { useEffect, useState } from 'react'
import { ArrowLeft, ExternalLink } from 'lucide-react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { PageHeader } from '../components/layout/PageHeader'
import { SentimentBadge } from '../components/news/SentimentBadge'
import { fetchNewsById } from '../api/news'
import '../components/news/news.css'
import './news-page.css'
import { Button } from '../components/common/Button'

export function NewsDetail() {
  const { newsId } = useParams()
  const navigate = useNavigate()
  
  const [article, setArticle] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notFound, setNotFound] = useState(false)

  useEffect(() => {
    async function loadDetail() {
      try {
        setLoading(true)
        const res = await fetchNewsById(newsId)
        setArticle(res)
      } catch (err) {
        if (err.status === 404) setNotFound(true)
        else setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadDetail()
  }, [newsId])

  if (loading) return <div className="news-detail-page"><LoadingState text="Loading article..." /></div>
  
  if (notFound) {
    return (
      <div className="news-detail-page">
        <PageHeader title="News not found" description="The article you requested is not available." />
        <EmptyState title="News article not found" description={`No article exists for "${newsId}".`} action={{ label: 'Back to News Intelligence', onClick: () => navigate('/news') }} />
      </div>
    )
  }

  if (error) {
    return <div className="news-detail-page"><EmptyState title="Could not load article" description={error} /></div>
  }

  // Format the ISO date safely
  let dateDisplay = 'Unknown date'
  if (article.published_at) {
    try {
      const d = new Date(article.published_at)
      dateDisplay = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: 'numeric' }).format(d)
    } catch (e) {}
  }

  return (
    <div className="news-detail-page">
      <Link className="news-back-link" to="/news">
        <ArrowLeft size={16} aria-hidden="true" />
        Back to News Intelligence
      </Link>
      
      <PageHeader title="News Detail" />
      
      <Card className="news-detail-article">
        <div className="news-detail-article__meta">
          <SentimentBadge sentiment={article.sentiment} />
          <span>{article.source}</span>
          <span>•</span>
          <time>{dateDisplay}</time>
        </div>
        <h1>{article.headline}</h1>
        <p className="news-detail-article__summary">{article.summary}</p>
        
        {article.url && (
          <div style={{ marginTop: '2rem' }}>
            <Button variant="primary" as="a" href={article.url} target="_blank" rel="noopener noreferrer">
              Read Original Article <ExternalLink size={16} style={{ marginLeft: 8 }} />
            </Button>
          </div>
        )}
      </Card>
      
      <div className="news-detail-section">
        <h2>Intelligence</h2>
        <Card>
          <dl className="intelligence-list">
            <div>
              <dt>Sentiment</dt>
              <dd><SentimentBadge sentiment={article.sentiment} /></dd>
            </div>
            <div>
              <dt>Stocks</dt>
              <dd>
                {article.tickers && article.tickers.map((ticker, index) => (
                  <span key={ticker}>
                    <Link to={`/stocks/${encodeURIComponent(ticker)}`} className="ticker-link">
                      {ticker}
                    </Link>
                    {index < article.tickers.length - 1 ? ' · ' : ''}
                  </span>
                ))}
              </dd>
            </div>
          </dl>
        </Card>
      </div>
    </div>
  )
}
