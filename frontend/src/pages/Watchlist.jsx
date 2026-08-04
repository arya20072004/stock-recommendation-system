import { useMemo } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { ArrowDownRight, ArrowUpRight, Trash2 } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { useWatchlist } from '../context/WatchlistContext'
import { getStockDetail } from '../mocks/stocks'
import { directionForValue, formatCurrency, formatPercent } from '../utils/formatters'
import './watchlist-page.css'

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative' }

export function Watchlist() {
  const navigate = useNavigate()
  const { tickers, removeFromWatchlist } = useWatchlist()

  const stocks = useMemo(() => {
    return tickers.map(t => getStockDetail(t)).filter(Boolean)
  }, [tickers])

  return (
    <div className="watchlist-page">
      <PageHeader title="Watchlist" description="Monitor stocks you want to follow." actions={<Badge tone="accent">Demo data</Badge>} />

      {stocks.length === 0 ? (
        <EmptyState
          title="Your watchlist is empty"
          description="Browse stocks or recommendations and add companies you want to monitor."
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
                const direction = directionForValue(stock.priceChangePercent)
                const Icon = direction === 'negative' ? ArrowDownRight : ArrowUpRight
                return (
                  <div className="wl-table__row" role="row" key={stock.ticker}>
                    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="wl-table__stock-link" role="cell" aria-label={`${stock.symbol} — ${stock.companyName}`}>
                      <strong>{stock.symbol}</strong><span>{stock.companyName}</span>
                    </Link>
                    <span role="cell" className="mono">{formatCurrency(stock.currentPrice)}</span>
                    <span role="cell" className={`wl-table__change wl-table__change--${direction} mono`}>
                      <Icon aria-hidden="true" size={14} />{formatPercent(stock.priceChangePercent)}
                    </span>
                    <span role="cell"><RecommendationBadge signal={stock.signal} /></span>
                    <span role="cell"><ConfidenceBar value={stock.confidence} tone={signalTones[stock.signal] ?? 'positive'} compact /></span>
                    <span role="cell"><RiskBadge risk={stock.risk} /></span>
                    <span role="cell" className="wl-table__action">
                      <Button variant="ghost" className="wl-remove-button" onClick={() => removeFromWatchlist(stock.ticker)} aria-label={`Remove ${stock.symbol} from watchlist`}>
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
              const direction = directionForValue(stock.priceChangePercent)
              return (
                <Card className="wl-card" key={stock.ticker}>
                  <div className="wl-card__header">
                    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="wl-card__stock-link">
                      <strong>{stock.symbol}</strong>
                      <span>{stock.companyName}</span>
                    </Link>
                    <Button variant="ghost" className="wl-remove-button" onClick={() => removeFromWatchlist(stock.ticker)} aria-label={`Remove ${stock.symbol} from watchlist`}>
                      <Trash2 size={15} aria-hidden="true" />
                    </Button>
                  </div>
                  <div className="wl-card__price-row">
                    <span className="mono">{formatCurrency(stock.currentPrice)}</span>
                    <span className={`wl-table__change wl-table__change--${direction} mono`}>{formatPercent(stock.priceChangePercent)}</span>
                  </div>
                  <div className="wl-card__metrics">
                    <div><span>Signal</span><RecommendationBadge signal={stock.signal} /></div>
                    <div><span>Confidence</span><strong className="mono">{stock.confidence}%</strong></div>
                    <div><span>Risk</span><RiskBadge risk={stock.risk} /></div>
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
