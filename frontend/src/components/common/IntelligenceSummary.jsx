import { Card } from './Card'
import { UnavailableState } from './UnavailableState'
import './data-presentation.css'

export function IntelligenceSummary({ summary, className = '' }) {
  if (!summary) {
    return <UnavailableState title="No summary" description="No intelligence summary is available for this." />
  }

  return (
    <Card className={`intelligence-summary ${className}`.trim()}>
      <p className="intelligence-summary__text">{summary}</p>
    </Card>
  )
}
