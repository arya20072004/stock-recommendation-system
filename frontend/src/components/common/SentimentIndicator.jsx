import { Badge } from './Badge'

const sentimentTones = { 
  POSITIVE: 'positive', 
  NEUTRAL: 'warning', 
  NEGATIVE: 'negative',
  UNSCORED: 'neutral'
}

export function SentimentIndicator({ sentiment, className = '' }) {
  if (!sentiment) return <Badge tone="neutral" className={className}>UNAVAILABLE</Badge>
  
  const normalized = sentiment.toUpperCase()
  return (
    <Badge tone={sentimentTones[normalized] ?? 'neutral'} className={className}>
      {normalized}
    </Badge>
  )
}
