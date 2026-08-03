import { Badge } from '../common/Badge'
import './recommendations.css'

const riskTones = { LOW: 'positive', MEDIUM: 'warning', HIGH: 'negative' }

export function RiskBadge({ risk }) {
  return <Badge tone={riskTones[risk] ?? 'neutral'} className="risk-badge">{risk}</Badge>
}
