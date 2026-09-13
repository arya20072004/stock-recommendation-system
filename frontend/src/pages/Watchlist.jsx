import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { ArrowDownRight, ArrowUpRight, Trash2 } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { ErrorState } from '../components/common/ErrorState'
import { LoadingState } from '../components/common/LoadingState'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { useWatchlist } from '../context/WatchlistContext'
import { fetchStocksSummary } from '../api/stocks'
import { directionForValue, formatCurrency, formatPercent } from '../utils/formatters'
import './watchlist-page.css'

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative' }

export function Watchlist() {
  const navigate = useNavigate()
  const { tickers, removeFromWatchlist } = useWatchlist()

  const [allStocks, setAllStocks] = useState([])
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let active = true
    async function loadData() {
      try {
        setLoading(true)
        setError(null)
        const res = await fetchStocksSummary()
        if (active) {
          setAllStocks(res.data || [])
          setMeta(res.meta || null)
        }
      } catch (err) {
        if (active) setError(err.message || 'Failed to load stock data.')
      } finally {
        if (active) setLoading(false)
      }
    }
    loadData()
    return () => { active = false }
  }, [])

  // Filter backend universe to only watchlisted tickers (selection, not computation)
  const stocks = allStocks.filter(s => tickers.includes(s.ticker))

  const isPartial = meta && !meta.complete

  if (loading) {
    return (
      <div className="watchlist-page">
        <PageHeader title="Watchlist" description="Monitor stocks you want to follow." />
        <LoadingState label="Loading watchlist data..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="watchlist-page">
        <PageHeader title="Watchlist" description="Monitor stocks you want to follow." />
        <ErrorState title="Watchlist Unavailable" description={error} />
      </div>
    )
  }

  const headerActions = meta ? (
    <>
      <Badge tone="neutral">{meta.market_date || 'Unknown Date'}</Badge>
      {isPartial && <Badge tone="warning">Partial Data</Badge>}
    </>
  ) : null

  return (
    <div className="watchlist-page">
      <PageHeader title="Watchlist" description="Monitor stocks you want to follow." actions={headerActions} />

      {tickers.length === 0 ? (
        <EmptyState
          title="Your watchlist is empty"
          description="Browse stocks or recommendations and add companies you want to monitor."
          action={{ label: 'Browse Stocks', onClick: () => navigate('/stocks') }}
        />
      ) : stocks.length === 0 ? (
        /* Tickers saved but none found in backend snapshot — do not fabricate data */
        <EmptyState
          title="No data available"
          description="Your saved stocks were not found in the current backend snapshot."
          action={{ label: 'Browse Stocks', onClick: () => navigate('/stocks') }}
        />
      ) : (
        <>
          {/* Desktop table */}
          <Card className="wl-table-card">
            <div className="wl-table" role="table" aria-label="Watchlist stocks">
              <div className="wl-table__header" role="row">
                <span role="columnheader">Stock</span>
                <span role="columnheader">Price</span>
                <span role="columnheader">Today</span>
                <span role="columnheader">Signal</span>
                <span role="columnheader">Confidence</span>
                <span role="columnheader">Risk</span>
                <span role="columnheader"><span className="sr-only">Actions</span></span>
              </div>
              {stocks.map(stock => {
                const direction = directionForValue(stock.day_change_pct)
                const Icon = direction === 'negative' ? ArrowDownRight : ArrowUpRight
                // Derive the display ticker symbol (strip .NS suffix for display)
                const displaySymbol = stock.ticker.replace(/\.NS$/, '')
                return (
                  <div className="wl-table__row" role="row" key={stock.ticker}>
                    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="wl-table__stock-link" role="cell" aria-label={`${displaySymbol} — ${stock.company_name}`}>
                      <strong>{displaySymbol}</strong><span>{stock.company_name}</span>
                    </Link>
                    <span role="cell" className="mono">{stock.last_close != null ? formatCurrency(stock.last_close) : '—'}</span>
                    <span role="cell" className={`wl-table__change wl-table__change--${direction} mono`}>
                      <Icon aria-hidden="true" size={14} />{stock.day_change_pct != null ? formatPercent(stock.day_change_pct) : '—'}
                    </span>
                    <span role="cell"><RecommendationBadge signal={stock.recommendation} /></span>
                    <span role="cell"><ConfidenceBar value={stock.confidence} tone={signalTones[stock.recommendation] ?? 'positive'} compact /></span>
                    <span role="cell"><RiskBadge risk={stock.confidence_tier} /></span>
                    <span role="cell" className="wl-table__action">
                      <Button variant="ghost" className="wl-remove-button" onClick={() => removeFromWatchlist(stock.ticker)} aria-label={`Remove ${displaySymbol} from watchlist`}>
                        <Trash2 size={15} aria-hidden="true" />
                      </Button>
                    </span>
                  </div>
                )
              })}
            </div>
          </Card>

          {/* Mobile cards */}
          <div className="wl-cards" aria-label="Watchlist stocks">
            {stocks.map(stock => {
              const direction = directionForValue(stock.day_change_pct)
              const displaySymbol = stock.ticker.replace(/\.NS$/, '')
              return (
                <Card className="wl-card" key={stock.ticker}>
                  <div className="wl-card__header">
                    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="wl-card__stock-link">
                      <strong>{displaySymbol}</strong>
                      <span>{stock.company_name}</span>
                    </Link>
                    <Button variant="ghost" className="wl-remove-button" onClick={() => removeFromWatchlist(stock.ticker)} aria-label={`Remove ${displaySymbol} from watchlist`}>
                      <Trash2 size={15} aria-hidden="true" />
                    </Button>
                  </div>
                  <div className="wl-card__price-row">
                    <span className="mono">{stock.last_close != null ? formatCurrency(stock.last_close) : '—'}</span>
                    <span className={`wl-table__change wl-table__change--${direction} mono`}>{stock.day_change_pct != null ? formatPercent(stock.day_change_pct) : '—'}</span>
                  </div>
                  <div className="wl-card__metrics">
                    <div><span>Signal</span><RecommendationBadge signal={stock.recommendation} /></div>
                    <div><span>Confidence</span><strong className="mono">{stock.confidence != null ? `${stock.confidence.toFixed(1)}%` : '—'}</strong></div>
                    <div><span>Risk</span><RiskBadge risk={stock.confidence_tier} /></div>
                  </div>
                </Card>
              )
            })}
          </div>
        </>
      )}
    </div>
  )
}
