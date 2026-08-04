import { useMemo } from 'react'
import { ArrowDownRight, ArrowUpRight, ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { useWatchlist } from '../../context/WatchlistContext'
import { getStockDetail } from '../../mocks/stocks'
import { directionForValue, formatCurrency, formatPercent } from '../../utils/formatters'

const MAX_PREVIEW = 5

export function WatchlistPreview() {
  const { tickers } = useWatchlist()

  const stocks = useMemo(() => {
    return tickers.slice(0, MAX_PREVIEW).map(t => getStockDetail(t)).filter(Boolean)
  }, [tickers])

  return (
    <section aria-labelledby="watchlist-heading">
      <div className="section-heading">
        <div>
          <h2 id="watchlist-heading">Watchlist</h2>
          {stocks.length > 0 && <p>{tickers.length} tracked stock{tickers.length !== 1 ? 's' : ''}</p>}
        </div>
        <Link className="section-link" to="/watchlist">View watchlist <ChevronRight aria-hidden="true" size={15} /></Link>
      </div>
      {stocks.length === 0 ? (
        <Card className="watchlist-card watchlist-card--empty">
          <p className="watchlist-empty">Your watchlist is empty. <Link to="/stocks" className="watchlist-empty__link">Browse stocks</Link> to add companies.</p>
        </Card>
      ) : (
        <Card className="watchlist-card">
          <div className="watchlist-table" role="table" aria-label="Watchlist preview">
            <div className="watchlist-table__header" role="row">
              <span>Stock</span>
              <span>Price</span>
              <span>Today</span>
              <span>Signal</span>
            </div>
            {stocks.map((stock) => {
              const direction = directionForValue(stock.priceChangePercent)
              const Icon = direction === 'negative' ? ArrowDownRight : ArrowUpRight
              return (
                <Link className="watchlist-row" role="row" key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`}>
                  <strong role="cell">{stock.symbol}</strong>
                  <span className="mono" role="cell">{formatCurrency(stock.currentPrice)}</span>
                  <span className={`watchlist-row__change watchlist-row__change--${direction} mono`} role="cell">
                    <Icon aria-hidden="true" size={14} />{formatPercent(stock.priceChangePercent)}
                  </span>
                  <span role="cell"><RecommendationBadge signal={stock.signal} /></span>
                </Link>
              )
            })}
          </div>
        </Card>
      )}
    </section>
  )
}

