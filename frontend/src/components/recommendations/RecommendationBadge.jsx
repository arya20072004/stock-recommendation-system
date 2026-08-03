import { Badge } from '../common/Badge'
import './recommendations.css'

const signalTones = { 'STRONG BUY': 'positive', BUY: 'positive', HOLD: 'warning', SELL: 'negative', 'STRONG SELL': 'negative' }

export function RecommendationBadge({ signal }) {
  return <Badge tone={signalTones[signal] ?? 'neutral'} className="recommendation-badge">{signal}</Badge>
}
