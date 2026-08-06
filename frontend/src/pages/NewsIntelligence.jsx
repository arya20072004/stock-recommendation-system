import { useEffect, useState } from 'react'
import { RotateCcw } from 'lucide-react'
import { useSearchParams } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { PageHeader } from '../components/layout/PageHeader'
import { NewsFeed } from '../components/news/NewsFeed'
import { NewsSummaryCards } from '../components/news/NewsSummaryCards'
import { NewsToolbar } from '../components/news/NewsToolbar'
import { SentimentOverview } from '../components/news/SentimentOverview'
import { fetchNews } from '../api/news'
import { fetchAllTickers } from '../api/stocks'
import '../components/news/news.css'
import './news-page.css'
import { Button } from '../components/common/Button'

const defaultFilters = { sentiment: 'ALL', stock: 'ALL' }

export function NewsIntelligence() {
  const [searchParams, setSearchParams] = useSearchParams()
  const stockFromUrl = (searchParams.get('stock') ?? '').toUpperCase()
  
  const [filters, setFilters] = useState({ 
    ...defaultFilters, 
    stock: stockFromUrl || 'ALL' 
  })
  
  const [stocks, setStocks] = useState([])
  const [articles, setArticles] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  
  // Pagination
  const [page, setPage] = useState(1)
  const [hasMore, setHasMore] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)

  const [meta, setMeta] = useState(null)
  
  // Stale check uses backend meta
  const isStale = meta?.stale || false

  useEffect(() => {
    fetchAllTickers().then(setStocks).catch(console.error)
  }, [])

  useEffect(() => {
    if (stockFromUrl) {
      setFilters(current => ({ ...current, stock: stockFromUrl }))
    }
  }, [stockFromUrl])

  useEffect(() => {
    async function loadInitial() {
      try {
        setLoading(true)
        setError(null)
        setPage(1)
        
        const params = { page: 1, limit: 25 }
        if (filters.stock !== 'ALL') params.ticker = filters.stock
        if (filters.sentiment !== 'ALL') params.sentiment = filters.sentiment
        
        const res = await fetchNews(params)
        setArticles(res.data)
        setMeta(res.meta)
        setHasMore(res.meta.page < res.meta.total_pages)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadInitial()
  }, [filters])

  const loadMore = async () => {
    if (loadingMore || !hasMore) return
    try {
      setLoadingMore(true)
      const nextPage = page + 1
      const params = { page: nextPage, limit: 25 }
      if (filters.stock !== 'ALL') params.ticker = filters.stock
      if (filters.sentiment !== 'ALL') params.sentiment = filters.sentiment
      
      const res = await fetchNews(params)
      setArticles(prev => [...prev, ...res.data])
      setPage(nextPage)
      setHasMore(res.meta.page < res.meta.total_pages)
    } catch (err) {
      console.error(err)
    } finally {
      setLoadingMore(false)
    }
  }

  const updateFilter = (name, value) => {
    setFilters((current) => ({ ...current, [name]: value }))
    if (name === 'stock') {
      if (value === 'ALL') {
        const newParams = new URLSearchParams(searchParams)
        newParams.delete('stock')
        setSearchParams(newParams, { replace: true })
      } else {
        setSearchParams({ stock: value }, { replace: true })
      }
    }
  }
  
  const resetFilters = () => { 
    setFilters(defaultFilters)
    setSearchParams({}, { replace: true }) 
  }

  return (
    <div className="news-page">
      <PageHeader 
        title="News Intelligence" 
        description="Market-moving news connected to stocks and model signals."
        actions={
          isStale && meta?.newest_article_at ? (
            <Badge tone="warning">
              News data may be outdated. Latest article:{' '}
              {new Intl.DateTimeFormat('en-US', {
                month: 'short',
                day: 'numeric',
                year: 'numeric',
                hour: 'numeric',
                minute: 'numeric',
              }).format(new Date(meta.newest_article_at))}
            </Badge>
          ) : null
        }
      />
      
      {error ? (
        <EmptyState title="Could not load news" description={error} />
      ) : (
        <>
          <NewsToolbar filters={filters} stocks={stocks} onChange={updateFilter} />
          
          {loading ? (
            <LoadingState text="Loading news articles..." />
          ) : articles.length > 0 ? (
            <>
              <NewsSummaryCards meta={meta} />
              <SentimentOverview meta={meta} />
              <NewsFeed articles={articles} />
              {hasMore && (
                <div style={{ textAlign: 'center', margin: '2rem 0' }}>
                  <Button variant="secondary" onClick={loadMore} disabled={loadingMore}>
                    {loadingMore ? 'Loading...' : 'Load more articles'}
                  </Button>
                </div>
              )}
            </>
          ) : (
            <EmptyState 
              title="No news matches your filters" 
              description="Try adjusting your search or filters." 
              action={{ label: 'Reset filters', onClick: resetFilters }} 
              icon={RotateCcw} 
            />
          )}
        </>
      )}
    </div>
  )
}
