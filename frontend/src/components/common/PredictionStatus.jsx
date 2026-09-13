import { Badge } from './Badge'

export function PredictionStatus({ status, className = '' }) {
  if (!status) return <Badge tone="neutral" className={className}>UNAVAILABLE</Badge>
  const normalized = status.toUpperCase()
  
  let tone = 'neutral'
  if (normalized === 'CORRECT') tone = 'positive'
  else if (normalized === 'INCORRECT') tone = 'negative'
  else if (normalized === 'PENDING') tone = 'pending'

  return <Badge tone={tone} className={className}>{status}</Badge>
}
