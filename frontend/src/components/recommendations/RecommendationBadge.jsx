import { SignalIndicator } from '../common/SignalIndicator'
import './recommendations.css'

export function RecommendationBadge({ signal }) {
  return <SignalIndicator signal={signal} className="recommendation-badge" />
}
