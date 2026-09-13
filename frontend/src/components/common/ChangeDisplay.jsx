import { ArrowUpRight, ArrowDownRight, Minus } from 'lucide-react'
import './financial.css'

export function ChangeDisplay({ absoluteChange, percentageChange, unavailable = false, className = '' }) {
  if (unavailable || (absoluteChange == null && percentageChange == null)) {
    return <span className={`change-display unavailable ${className}`.trim()}>—</span>
  }

  const val = percentageChange ?? absoluteChange
  const isPositive = val > 0
  const isNegative = val < 0

  let tone = 'neutral'
  let Icon = Minus
  if (isPositive) { tone = 'positive'; Icon = ArrowUpRight }
  if (isNegative) { tone = 'negative'; Icon = ArrowDownRight }

  const formattedAbs = absoluteChange != null ? Math.abs(absoluteChange).toFixed(2) : null
  const formattedPct = percentageChange != null ? `${Math.abs(percentageChange).toFixed(2)}%` : null

  return (
    <div className={`change-display change-display--${tone} ${className}`.trim()}>
      <Icon size={16} aria-hidden="true" />
      <span className="change-display__values">
        {formattedAbs && <span>{isPositive ? '+' : isNegative ? '-' : ''}{formattedAbs}</span>}
        {formattedPct && <span className="change-display__pct">({isPositive ? '+' : isNegative ? '-' : ''}{formattedPct})</span>}
      </span>
    </div>
  )
}
