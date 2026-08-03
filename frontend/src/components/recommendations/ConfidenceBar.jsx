import './recommendations.css'

export function ConfidenceBar({ value, tone = 'positive', compact = false }) {
  const percentage = Math.max(0, Math.min(100, value))
  const track = <div className="confidence-bar__track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow={percentage}><span className={`confidence-bar__fill confidence-bar__fill--${tone}`} style={{ width: `${percentage}%` }} /></div>
  if (compact) return <div className="confidence-bar confidence-bar--compact" aria-label={`Model confidence: ${percentage}%`}>{track}<span className="confidence-bar__value mono">{percentage}%</span></div>
  return <div className="confidence-bar" aria-label={`Model confidence: ${percentage}%`}><div className="confidence-bar__heading"><span>Confidence</span><strong className="mono">{percentage}%</strong></div>{track}</div>
}

