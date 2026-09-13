import { useMemo } from 'react'
import { ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { SignalIndicator } from '../common/SignalIndicator'
import { PriceDisplay } from '../common/PriceDisplay'
import { ChangeDisplay } from '../common/ChangeDisplay'
import { useWatchlist } from '../../context/WatchlistContext'
import { EmptyState } from '../common/EmptyState'

const MAX_PREVIEW = 5

export function WatchlistPreview({ stocksSummary = [] }) {
  const { tickers } = useWatchlist()

  const stocks = useMemo(() => {
    if (!stocksSummary || stocksSummary.length === 0) return []
    return tickers.slice(0, MAX_PREVIEW)
      .map(t => stocksSummary.find(s => s.ticker === t))
      .filter(Boolean)
  }, [tickers, stocksSummary])

  return (
    <div className="dashboard-card-wrapper">
      <div className="section-heading">
        {stocks.length > 0 && <p className="watchlist-count">{tickers.length} tracked stock{tickers.length !== 1 ? 's' : ''}</p>}
        <Link className="section-link" to="/watchlist">View watchlist <ChevronRight aria-hidden="true" size={15} /></Link>
      </div>
      {stocks.length === 0 ? (
        <EmptyState title="Watchlist is empty" description="Browse stocks to add companies." />
      ) : (
        <Card className="watchlist-card">
          <div className="watchlist-table" role="table" aria-label="Watchlist preview">
            <div className="watchlist-table__header" role="row">
              <span role="columnheader">Stock</span>
              <span role="columnheader">Price</span>
              <span role="columnheader">Today</span>
              <span role="columnheader">Signal</span>
            </div>
            {stocks.map((stock) => (
              <Link className="watchlist-row" role="row" key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`}>
                <strong role="cell">{stock.ticker.replace('.NS', '')}</strong>
                <span className="mono" role="cell"><PriceDisplay price={stock.last_close} /></span>
                <span className="mono" role="cell"><ChangeDisplay percentageChange={stock.day_change_pct} /></span>
                <span role="cell"><SignalIndicator signal={stock.recommendation} /></span>
              </Link>
            ))}
          </div>
        </Card>
      )}
    </div>
  )
}
