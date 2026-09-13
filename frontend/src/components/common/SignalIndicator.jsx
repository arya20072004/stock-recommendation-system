import { Badge } from './Badge'

const signalTones = { 
  'STRONG BUY': 'positive', 
  'BUY': 'positive', 
  'HOLD': 'warning', 
  'SELL': 'negative', 
  'STRONG SELL': 'negative' 
}

export function SignalIndicator({ signal, className = '' }) {
  if (!signal) return <Badge tone="neutral" className={className}>UNAVAILABLE</Badge>
  
  const normalized = signal.toUpperCase()
  return (
    <Badge tone={signalTones[normalized] ?? 'neutral'} className={className}>
      {normalized}
    </Badge>
  )
}
