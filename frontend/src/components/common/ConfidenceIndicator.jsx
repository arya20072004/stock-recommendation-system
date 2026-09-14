import './financial.css'

export function ConfidenceIndicator({ confidence, tier, className = '' }) {
  if (confidence == null && !tier) {
    return <span className={`confidence-indicator unavailable ${className}`}>Unavailable</span>
  }

  if (tier) {
    return <span className={`confidence-indicator tier ${className}`}>{tier}</span>
  }

  const formatted = typeof confidence === 'number' 
    ? `${(confidence > 1 ? confidence : confidence * 100).toFixed(0)}%` 
    : confidence

  return <span className={`confidence-indicator ${className}`}>{formatted}</span>
}
