import { ArrowDownRight, ArrowUpRight, ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { directionForValue, formatCurrency, formatPercent } from '../../utils/formatters'
import { Sparkline } from './Sparkline'

export function WatchlistPreview({ stocks }) {
  return <section aria-labelledby="watchlist-heading"><div className="section-heading"><div><h2 id="watchlist-heading">Watchlist</h2><p>Demo tracked stocks</p></div><Link className="section-link" to="/watchlist">View watchlist <ChevronRight aria-hidden="true" size={15} /></Link></div><Card className="watchlist-card"><div className="watchlist-table" role="table" aria-label="Demo watchlist"><div className="watchlist-table__header" role="row"><span>Stock</span><span>Price</span><span>Today</span><span>Signal</span></div>{stocks.map((stock) => { const direction = directionForValue(stock.change); const Icon = direction === 'negative' ? ArrowDownRight : ArrowUpRight; return <div className="watchlist-row" role="row" key={stock.ticker}><strong role="cell">{stock.ticker}</strong><span className="mono" role="cell">{formatCurrency(stock.price)}</span><span className={`watchlist-row__change watchlist-row__change--${direction} mono`} role="cell"><Icon aria-hidden="true" size={14} />{formatPercent(stock.change)}<Sparkline data={stock.sparkline} direction={direction} /></span><span role="cell"><RecommendationBadge signal={stock.signal} /></span></div> })}</div></Card></section>
}
