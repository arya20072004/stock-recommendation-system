import { useEffect, useMemo, useState } from 'react'
import { RotateCcw } from 'lucide-react'
import { useSearchParams } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { EmptyState } from '../components/common/EmptyState'
import { PageHeader } from '../components/layout/PageHeader'
import { NewsFeed } from '../components/news/NewsFeed'
import { NewsSummaryCards } from '../components/news/NewsSummaryCards'
import { NewsToolbar } from '../components/news/NewsToolbar'
import { SentimentOverview } from '../components/news/SentimentOverview'
import { demoNews } from '../mocks/news'
import { stockBySymbol, stockUniverse } from '../mocks/stocks'
import '../components/news/news.css'
import './news-page.css'

const defaultFilters = { search: '', sentiment: 'ALL', stock: 'ALL', category: 'ALL', sort: 'latest' }

export function NewsIntelligence() {
  const [searchParams, setSearchParams] = useSearchParams()
  const stockFromUrl = (searchParams.get('stock') ?? '').toUpperCase()
  const [filters, setFilters] = useState({ ...defaultFilters, stock: stockBySymbol[stockFromUrl] ? stockFromUrl : 'ALL' })
  const categories = useMemo(() => [...new Set(demoNews.map((article) => article.category))].sort(), [])

  useEffect(() => {
    setFilters((current) => ({ ...current, stock: stockBySymbol[stockFromUrl] ? stockFromUrl : 'ALL' }))
  }, [stockFromUrl])

  const filteredArticles = useMemo(() => {
    const query = filters.search.trim().toLowerCase()
    const result = demoNews.filter((article) => {
      const stockText = article.tickers.map((symbol) => `${symbol} ${stockBySymbol[symbol]?.companyName ?? ''}`).join(' ')
      const matchesSearch = !query || `${article.headline} ${article.summary} ${article.source} ${stockText}`.toLowerCase().includes(query)
      return matchesSearch && (filters.sentiment === 'ALL' || article.sentiment === filters.sentiment) && (filters.stock === 'ALL' || article.tickers.includes(filters.stock)) && (filters.category === 'ALL' || article.category === filters.category)
    })
    return result.sort((a, b) => filters.sort === 'oldest' ? b.publishedMinutesAgo - a.publishedMinutesAgo : filters.sort === 'relevant' ? b.relevance - a.relevance : a.publishedMinutesAgo - b.publishedMinutesAgo)
  }, [filters])

  const updateFilter = (name, value) => {
    setFilters((current) => ({ ...current, [name]: value }))
    if (name === 'stock') setSearchParams(value === 'ALL' ? {} : { stock: value }, { replace: true })
  }
  const resetFilters = () => { setFilters(defaultFilters); setSearchParams({}, { replace: true }) }

  return <div className="news-page">
    <PageHeader title="News Intelligence" description="Market-moving news connected to stocks and model signals." actions={<Badge tone="accent">Demo data</Badge>} />
    <NewsSummaryCards articles={demoNews} />
    <NewsToolbar filters={filters} stocks={stockUniverse} categories={categories} onChange={updateFilter} />
    <SentimentOverview articles={demoNews} />
    {filteredArticles.length > 0 ? <NewsFeed articles={filteredArticles} /> : <EmptyState title="No news matches your filters" description="Try adjusting your search or filters." action={{ label: 'Reset filters', onClick: resetFilters }} icon={RotateCcw} />}
  </div>
}
