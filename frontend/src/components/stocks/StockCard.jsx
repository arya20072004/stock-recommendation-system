import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { directionForValue, formatCurrency, formatPercent } from '../../utils/formatters'

export function StockCard({ stock }) {
  const direction = directionForValue(stock.priceChangePercent)

  return (
    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="stock-card-link" aria-label={`${stock.symbol} — ${stock.companyName}`}>
      <Card hoverable className="stock-card">
        <div className="stock-card__header">
          <strong className="stock-card__ticker">{stock.symbol}</strong>
          <span className="stock-card__sector">{stock.sector}</span>
        </div>
        <span className="stock-card__company">{stock.companyName}</span>
        <div className="stock-card__price-row">
          <span className="stock-card__price mono">{formatCurrency(stock.currentPrice)}</span>
          <span className={`stock-card__change stock-card__change--${direction} mono`}>{formatPercent(stock.priceChangePercent)}</span>
        </div>
        <div className="stock-card__signal">
          <RecommendationBadge signal={stock.signal} />
          <span className="stock-card__confidence mono">{stock.confidence}%</span>
        </div>
      </Card>
    </Link>
  )
}
