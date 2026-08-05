import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { ConfidenceBar } from '../recommendations/ConfidenceBar'
import { formatCurrency } from '../../utils/formatters'

export function RelatedStockCard({ stock }) {
  return <Card className="related-stock-card" hoverable><Link to={`/stocks/${stock.ticker}`}><div><span className="related-stock-card__symbol">{stock.symbol}</span><strong>{stock.companyName}</strong></div><span className="mono related-stock-card__price">{formatCurrency(stock.currentPrice)}</span><div className="related-stock-card__signal"><RecommendationBadge signal={stock.signal} /><ConfidenceBar value={stock.confidence} compact /></div></Link></Card>
}
