import { Badge } from '../common/Badge'

const sentimentTones = { POSITIVE: 'positive', NEUTRAL: 'warning', NEGATIVE: 'negative' }

export function SentimentBadge({ sentiment }) {
  return <Badge tone={sentimentTones[sentiment] ?? 'neutral'}>{sentiment}</Badge>
}
