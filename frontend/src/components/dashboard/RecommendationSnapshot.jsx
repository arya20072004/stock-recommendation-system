import { useMemo } from 'react'
import { Card } from '../common/Card'

export function RecommendationSnapshot({ snapshotData = [] }) {
  const metrics = useMemo(() => {
    let buy = 0
    let hold = 0
    let sell = 0
    
    snapshotData.forEach(item => {
      if (item.recommendation === 'BUY') buy++
      else if (item.recommendation === 'HOLD') hold++
      else if (item.recommendation === 'SELL') sell++
    })

    return [
      { label: 'BUY', value: buy, tone: 'positive' },
      { label: 'HOLD', value: hold, tone: 'warning' },
      { label: 'SELL', value: sell, tone: 'negative' }
    ]
  }, [snapshotData])

  return (
    <Card className="recommendation-snapshot">
      {metrics.map(({ label, value, tone }) => (
        <div key={label} className={`snapshot-metric snapshot-metric--${tone}`}>
          <strong className="mono">{value}</strong>
          <span>{label}</span>
        </div>
      ))}
    </Card>
  )
}
