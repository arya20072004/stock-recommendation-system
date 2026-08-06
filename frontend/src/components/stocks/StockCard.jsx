import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { directionForValue, formatCurrency, formatPercent } from '../../utils/formatters'

export function StockCard({ stock }) {
  const direction = directionForValue(stock.day_change_pct)

  return (
    <Link to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="stock-card-link" aria-label={`${stock.ticker} — ${stock.company_name}`}>
      <Card hoverable className="stock-card">
        <div className="stock-card__header">
          <strong className="stock-card__ticker">{stock.ticker}</strong>
          <span className="stock-card__sector">{stock.sector || '—'}</span>
        </div>
        <span className="stock-card__company">{stock.company_name}</span>
        <div className="stock-card__price-row">
          <span className="stock-card__price mono">{stock.last_close != null ? formatCurrency(stock.last_close) : '—'}</span>
          <span className={`stock-card__change stock-card__change--${direction} mono`}>{stock.day_change_pct != null ? formatPercent(stock.day_change_pct) : '—'}</span>
        </div>
        <div className="stock-card__signal">
          <RecommendationBadge signal={stock.recommendation} />
          <span className="stock-card__confidence mono">{stock.confidence}%</span>
        </div>
      </Card>
    </Link>
  )
}
