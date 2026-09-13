import { useEffect, useState, useMemo } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { RotateCcw, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { ErrorState } from '../components/common/ErrorState'
import { LoadingState } from '../components/common/LoadingState'
import { PageHeader } from '../components/layout/PageHeader'
import { Select } from '../components/common/Select'
import { SentimentIndicator } from '../components/common/SentimentIndicator'
import { fetchNews } from '../api/news'
import { fetchAllTickers } from '../api/stocks'
import './news-page.css'

const SENTIMENT_OPTIONS = [
  { value: 'ALL', label: 'All Sentiments' },
  { value: 'POSITIVE', label: 'Positive' },
  { value: 'NEUTRAL', label: 'Neutral' },
  { value: 'NEGATIVE', label: 'Negative' },
  { value: 'UNSCORED', label: 'Unscored' }
]

function NewsArticleCard({ article }) {
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
    } catch (e) {
      // fallback if invalid format
    }
  }

  // Tickers from array
  const tickers = article.tickers || []
  
  return (
    <Card className="news-article-card">
      <div className="news-article-header">
        <h2 className="news-article-title">
          <Link to={`/news/${article.id}`}>{article.headline}</Link>
        </h2>
        <div className="news-article-sentiment">
          <SentimentIndicator sentiment={article.sentiment} />
        </div>
      </div>
      
      <div className="news-article-meta">
        <strong>{article.source || 'Unknown Source'}</strong>
        <span className="news-article-meta-dot">•</span>
        <time>{dateDisplay}</time>
      </div>

      <p className="news-article-summary">
        {article.summary}
      </p>

      <div className="news-article-footer">
        <div className="news-article-tickers">
          {tickers.map(ticker => (
            <Link key={ticker} to={`/stocks/${encodeURIComponent(ticker)}`} style={{ textDecoration: 'none' }}>
              <Badge tone="neutral">{ticker}</Badge>
            </Link>
          ))}
        </div>
        
        <div className="news-article-actions">
          {article.url && (
            <Button variant="ghost" as="a" href={article.url} target="_blank" rel="noopener noreferrer">
              Read Source <ExternalLink size={14} style={{ marginLeft: 6 }} aria-hidden="true" />
            </Button>
          )}
          <Button variant="secondary" as={Link} to={`/news/${article.id}`}>
            View Details
          </Button>
        </div>
      </div>
    </Card>
  )
}

export function NewsIntelligence() {
  const [searchParams, setSearchParams] = useSearchParams()
  const urlStock = (searchParams.get('stock') ?? '').toUpperCase()
  const urlSentiment = (searchParams.get('sentiment') ?? 'ALL').toUpperCase()
  const urlPage = parseInt(searchParams.get('page'), 10) || 1

  const [stocks, setStocks] = useState([])
  const [articles, setArticles] = useState([])
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Fetch the authoritative ticker list
  useEffect(() => {
    fetchAllTickers().then(setStocks).catch(console.error)
  }, [])

  const stockOptions = useMemo(() => {
    return [
      { value: 'ALL', label: 'All Tracked Stocks' },
      ...stocks.map(s => ({ value: s, label: s }))
    ]
  }, [stocks])

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)
        setError(null)
        
        const params = { page: urlPage, limit: 25 }
        if (urlStock && urlStock !== 'ALL') params.ticker = urlStock
        if (urlSentiment && urlSentiment !== 'ALL') params.sentiment = urlSentiment
        
        const res = await fetchNews(params)
        setArticles(res.data || [])
        setMeta(res.meta)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [urlStock, urlSentiment, urlPage])

  const updateParam = (key, value) => {
    const newParams = new URLSearchParams(searchParams)
    if (value === 'ALL' || !value) {
      newParams.delete(key)
    } else {
      newParams.set(key, value)
    }
    // Any filter change resets to page 1
    newParams.delete('page')
    setSearchParams(newParams)
  }

  const setPage = (newPage) => {
    if (newPage < 1) return
    const newParams = new URLSearchParams(searchParams)
    if (newPage === 1) {
      newParams.delete('page')
    } else {
      newParams.set('page', newPage.toString())
    }
    setSearchParams(newParams)
  }

  const resetFilters = () => {
    setSearchParams({})
  }

  const isStale = meta?.stale || false
  const total = meta?.total || 0
  const currentPage = meta?.page || 1
  const totalPages = meta?.total_pages || 1
  
  let headerActions = null
  if (isStale && meta?.newest_article_at) {
    try {
      const staleDate = new Intl.DateTimeFormat('en-US', {
        month: 'short', day: 'numeric', hour: 'numeric', minute: 'numeric'
      }).format(new Date(meta.newest_article_at))
      headerActions = <Badge tone="warning">News data may be outdated. Latest article: {staleDate}</Badge>
    } catch(e) {}
  }

  return (
    <div className="news-workspace">
      <PageHeader 
        title="News Intelligence" 
        description="Market and company news connected to the tracked universe."
        actions={headerActions}
      />
      
      <div className="news-filters">
        <div className="news-filter-group">
          <Select 
            label="Filter by Ticker" 
            value={urlStock || 'ALL'} 
            onChange={(val) => updateParam('stock', val)} 
            options={stockOptions}
          />
        </div>
        <div className="news-filter-group">
          <Select 
            label="Filter by Sentiment" 
            value={urlSentiment} 
            onChange={(val) => updateParam('sentiment', val)} 
            options={SENTIMENT_OPTIONS}
          />
        </div>
        <div className="news-filter-actions">
          <Button variant="ghost" onClick={resetFilters}>
            <RotateCcw size={14} aria-hidden="true" />
            Reset Filters
          </Button>
        </div>
      </div>

      {error ? (
        <ErrorState title="Could not load news" description={error} />
      ) : loading ? (
        <LoadingState label="Loading news intelligence..." />
      ) : (
        <>
          <div className="news-summary-bar">
            <div className="news-summary-text">
              <strong>{total}</strong> Articles 
              {total > 0 && ` • Showing ${(currentPage - 1) * 25 + 1}–${Math.min(currentPage * 25, total)}`}
            </div>
            
            {totalPages > 1 && (
              <div className="news-pagination">
                <span className="news-pagination-info">
                  Page {currentPage} of {totalPages}
                </span>
                <div className="news-pagination-controls">
                  <Button 
                    variant="secondary" 
                    disabled={currentPage === 1} 
                    onClick={() => setPage(currentPage - 1)}
                    aria-label="Previous Page"
                  >
                    <ChevronLeft size={16} />
                  </Button>
                  <Button 
                    variant="secondary" 
                    disabled={currentPage >= totalPages} 
                    onClick={() => setPage(currentPage + 1)}
                    aria-label="Next Page"
                  >
                    <ChevronRight size={16} />
                  </Button>
                </div>
              </div>
            )}
          </div>

          <div className="news-feed">
            {articles.length === 0 ? (
              <EmptyState 
                title="No news matches your filters" 
                description="Try adjusting your ticker or sentiment criteria." 
                action={{ label: 'Reset filters', onClick: resetFilters }} 
                icon={RotateCcw} 
              />
            ) : (
              articles.map(article => (
                <NewsArticleCard key={article.id} article={article} />
              ))
            )}
          </div>
        </>
      )}
    </div>
  )
}
