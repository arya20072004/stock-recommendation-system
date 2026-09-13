import { useEffect, useState } from 'react'
import { ArrowLeft, ExternalLink } from 'lucide-react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { ErrorState } from '../components/common/ErrorState'
import { LoadingState } from '../components/common/LoadingState'
import { PageHeader } from '../components/layout/PageHeader'
import { SentimentIndicator } from '../components/common/SentimentIndicator'
import { fetchNewsById } from '../api/news'
import './news-page.css'

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
        if (err.status === 404 || err.message.includes('404')) {
          setNotFound(true)
        } else {
          setError(err.message)
        }
      } finally {
        setLoading(false)
      }
    }
    if (newsId) loadDetail()
  }, [newsId])

  if (loading) {
    return (
      <div className="news-detail-workspace">
        <LoadingState label="Loading news detail..." />
      </div>
    )
  }
  
  if (notFound) {
    return (
      <div className="news-detail-workspace">
        <PageHeader title="News not found" description="The requested article intelligence is not available." />
        <EmptyState 
          title="Article Not Found" 
          description={`No article exists with ID "${newsId}".`} 
          action={{ label: 'Back to News Intelligence', onClick: () => navigate('/news') }} 
        />
      </div>
    )
  }

  if (error) {
    return (
      <div className="news-detail-workspace">
        <ErrorState title="Could not load article" description={error} />
      </div>
    )
  }

  // Safe date formatting
  let dateDisplay = 'Unknown Date'
  if (article.published_at) {
    try {
      const d = new Date(article.published_at)
      dateDisplay = new Intl.DateTimeFormat('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: 'numeric'
      }).format(d)
    } catch (e) {}
  }

  const tickers = article.tickers || []

  return (
    <div className="news-detail-workspace">
      <div>
        <Link className="news-back-link" to="/news">
          <ArrowLeft size={16} aria-hidden="true" />
          Back to News Intelligence
        </Link>
      </div>
      
      <PageHeader title="News Detail" description="Institutional news analysis and metadata." />
      
      <Card>
        <div className="news-detail-content">
          
          <div className="news-detail-meta">
            <strong>{article.source || 'Unknown Source'}</strong>
            <span>•</span>
            <time>{dateDisplay}</time>
          </div>
          
          <h1 className="news-detail-headline">{article.headline}</h1>
          
          <p className="news-detail-summary">{article.summary}</p>
          
          <div className="news-detail-intelligence">
            <h3>Extracted Intelligence</h3>
            <div className="intelligence-grid">
              
              <div className="intelligence-item">
                <span className="intelligence-item-label">Sentiment</span>
                <div className="intelligence-item-value">
                  <SentimentIndicator sentiment={article.sentiment} />
                </div>
              </div>

              {tickers.length > 0 && (
                <div className="intelligence-item">
                  <span className="intelligence-item-label">Related Tickers</span>
                  <div className="intelligence-item-value">
                    {tickers.map(ticker => (
                      <Link key={ticker} to={`/stocks/${encodeURIComponent(ticker)}`} style={{ textDecoration: 'none' }}>
                        <Badge tone="neutral">{ticker}</Badge>
                      </Link>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
          
          {article.url && (
            <div className="news-detail-actions">
              <Button variant="primary" as="a" href={article.url} target="_blank" rel="noopener noreferrer">
                Read Original Article <ExternalLink size={16} style={{ marginLeft: 8 }} aria-hidden="true" />
              </Button>
            </div>
          )}
          
        </div>
      </Card>
    </div>
  )
}
