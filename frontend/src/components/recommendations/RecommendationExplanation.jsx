import { Check } from 'lucide-react'
import { Card } from '../common/Card'
import './recommendations.css'

export function RecommendationExplanation({ explanations }) {
  if (!explanations?.length) return null
  return <Card className="explanation-card"><ul className="explanation-list">{explanations.map((text, i) => <li key={i}><Check size={15} aria-hidden="true" /><span>{text}</span></li>)}</ul></Card>
}
