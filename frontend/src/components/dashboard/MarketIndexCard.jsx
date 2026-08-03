import { ArrowDownRight, ArrowUpRight, Circle } from 'lucide-react'
import { Card } from '../common/Card'
import { directionForValue, formatMarketNumber, formatPercent } from '../../utils/formatters'

export function MarketIndexCard({ item }) {
  if (item.type === 'status') return <Card className="market-index-card"><span className="eyebrow">{item.label}</span><div className="market-status"><Circle aria-hidden="true" size={9} fill="currentColor" /><strong>{item.value}</strong></div><span className="market-index-card__detail">{item.detail}</span></Card>
  const direction = directionForValue(item.change)
  const Icon = direction === 'negative' ? ArrowDownRight : ArrowUpRight
  return <Card className="market-index-card"><span className="eyebrow">{item.label}</span><strong className="market-index-card__value mono">{formatMarketNumber(item.value)}</strong><span className={`market-index-card__change market-index-card__change--${direction}`}><Icon aria-hidden="true" size={15} />{formatPercent(item.change)}</span></Card>
}
