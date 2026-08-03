import { ArrowDownRight, ArrowUpRight, ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { ConfidenceBar } from '../recommendations/ConfidenceBar'
import { RecommendationBadge } from '../recommendations/RecommendationBadge'
import { directionForValue, formatCurrency, formatPercent } from '../../utils/formatters'
import { Sparkline } from './Sparkline'

export function TopRecommendationCard({ recommendation }) {
  const direction = directionForValue(recommendation.dailyChange)
  const MovementIcon = direction === 'negative' ? ArrowDownRight : ArrowUpRight
  return <section aria-labelledby="top-recommendation-heading"><h2 id="top-recommendation-heading" className="section-label">Top model recommendation</h2><Link to={`/stocks/${recommendation.ticker}`} className="top-recommendation-link" aria-label={`View ${recommendation.companyName} stock details`}><Card hoverable className="top-recommendation"><div className="top-recommendation__heading"><div><span className="eyebrow">{recommendation.ticker.replace('.NS', '')}</span><h3>{recommendation.companyName}</h3></div><RecommendationBadge signal={recommendation.signal} /></div><div className="top-recommendation__price-row"><div><strong className="top-recommendation__price mono">{formatCurrency(recommendation.currentPrice)}</strong><span className={`top-recommendation__change top-recommendation__change--${direction}`}><MovementIcon aria-hidden="true" size={15} />{formatPercent(recommendation.dailyChange)}</span></div><Sparkline data={recommendation.sparkline} direction={direction} /></div><ConfidenceBar value={recommendation.confidence} /><div className="top-recommendation__metrics"><div><span>Target price</span><strong className="mono">{formatCurrency(recommendation.targetPrice)}</strong></div><div><span>Expected return</span><strong className="mono metric-positive">{formatPercent(recommendation.expectedReturn)}</strong></div></div><span className="top-recommendation__details">View stock details <ChevronRight aria-hidden="true" size={15} /></span></Card></Link></section>
}
