import { useEffect, useState } from 'react'
import { Badge } from '../components/common/Badge'
import { LoadingState } from '../components/common/LoadingState'
import { ErrorState } from '../components/common/ErrorState'
import { ImportantNews } from '../components/dashboard/ImportantNews'
import { RecommendationSnapshot } from '../components/dashboard/RecommendationSnapshot'
import { WatchlistPreview } from '../components/dashboard/WatchlistPreview'
import { PageHeader } from '../components/layout/PageHeader'
import { MobileDataCard } from '../components/common/MobileDataCard'
import { SignalIndicator } from '../components/common/SignalIndicator'
import { ConfidenceIndicator } from '../components/common/ConfidenceIndicator'
import { StockIdentity } from '../components/common/StockIdentity'
import { PriceDisplay } from '../components/common/PriceDisplay'
import { ChangeDisplay } from '../components/common/ChangeDisplay'
import { fetchRecommendations } from '../api/recommendations'
import { fetchStocksSummary } from '../api/stocks'
import { fetchNews } from '../api/news'
import '../components/dashboard/dashboard.css'

export function Dashboard() {
  const [recommendations, setRecommendations] = useState(null)
  const [stocksSummary, setStocksSummary] = useState(null)
  const [newsResponse, setNewsResponse] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        setLoading(true)
        const [recsRes, stocksRes, newsRes] = await Promise.all([
          fetchRecommendations().catch(() => null),
          fetchStocksSummary().catch(() => null),
          fetchNews().catch(() => null)
        ])
        
        if (!recsRes) {
          throw new Error('Failed to load intelligence data.')
        }

        setRecommendations(recsRes)
        setStocksSummary(stocksRes?.data || [])
        setNewsResponse(newsRes || { data: [], meta: { stale: false } })
      } catch (err) {
        setError(err.message || 'An error occurred connecting to the backend.')
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  if (loading) return <div className="dashboard"><LoadingState label="Loading Dashboard Intelligence" /></div>
  if (error) return <div className="dashboard"><ErrorState title="Dashboard Unavailable" description={error} /></div>
  if (!recommendations) return null

  const { meta, data } = recommendations
  // Backend provides records already sorted by conviction
  const topSignals = data.slice(0, 4)

  const isStale = meta.mixed_date || !meta.complete

  return (
    <div className="dashboard">
      <PageHeader 
        title="Dashboard" 
        description="Market and recommendation intelligence." 
        actions={
          <>
            <Badge tone="neutral">{meta.market_date || 'Unknown Date'}</Badge>
            <Badge tone={isStale ? 'warning' : 'positive'}>{isStale ? 'Partial / Mixed Date' : 'Complete Snapshot'}</Badge>
          </>
        } 
      />

      <div className="dashboard-layout">
        <div className="dashboard-main">
          <section aria-labelledby="recommendation-snapshot-heading" className="dashboard-section">
            <h2 id="recommendation-snapshot-heading" className="section-label">Recommendation Snapshot</h2>
            <RecommendationSnapshot snapshotData={data} />
          </section>

          <section aria-labelledby="top-signals-heading" className="dashboard-section">
            <h2 id="top-signals-heading" className="section-label">Current Signals</h2>
            <div className="top-signals-grid">
              {topSignals.length > 0 ? topSignals.map(rec => (
                <MobileDataCard 
                  key={rec.ticker}
                  identity={<StockIdentity ticker={rec.ticker} link />}
                  primarySignal={<SignalIndicator signal={rec.recommendation} />}
                  primaryMetrics={
                    <>
                      <div><span className="metric-label">Price</span><PriceDisplay price={rec.last_close} /></div>
                      <div><span className="metric-label">Change</span><ChangeDisplay percentageChange={rec.day_change_pct} /></div>
                    </>
                  }
                  status={<ConfidenceIndicator confidence={rec.confidence} tier={rec.confidence_tier} />}
                />
              )) : (
                <div className="empty-signals">No active signals available.</div>
              )}
            </div>
          </section>

          <section aria-labelledby="watchlist-preview-heading" className="dashboard-section">
            <h2 id="watchlist-preview-heading" className="section-label">Watchlist</h2>
            <WatchlistPreview stocksSummary={stocksSummary} />
          </section>
        </div>

        <aside className="dashboard-sidebar">
          <section aria-labelledby="important-news-heading" className="dashboard-section">
            <h2 id="important-news-heading" className="section-label">Important News</h2>
            <ImportantNews 
              stories={newsResponse?.data ? newsResponse.data.slice(0, 4) : []} 
              isStale={newsResponse?.meta?.stale} 
            />
          </section>
        </aside>
      </div>
    </div>
  )
}
