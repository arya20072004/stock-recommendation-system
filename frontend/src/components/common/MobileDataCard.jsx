import { Card } from './Card'
import './data-presentation.css'

export function MobileDataCard({ 
  identity, 
  primarySignal, 
  primaryMetrics, 
  status, 
  metadata, 
  action, 
  className = '' 
}) {
  return (
    <Card className={`mobile-data-card ${className}`.trim()}>
      <div className="mobile-data-card__header">
        <div className="mobile-data-card__identity">{identity}</div>
        {primarySignal && <div className="mobile-data-card__signal">{primarySignal}</div>}
      </div>
      
      {primaryMetrics && (
        <div className="mobile-data-card__metrics">
          {primaryMetrics}
        </div>
      )}

      {(status || metadata) && (
        <div className="mobile-data-card__footer">
          {status && <div className="mobile-data-card__status">{status}</div>}
          {metadata && <div className="mobile-data-card__metadata">{metadata}</div>}
        </div>
      )}

      {action && (
        <div className="mobile-data-card__action">
          {action}
        </div>
      )}
    </Card>
  )
}
