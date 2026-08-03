export function Sparkline({ data, direction = 'positive' }) {
  const width = 64
  const height = 22
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const points = data.map((value, index) => `${(index / (data.length - 1)) * width},${height - ((value - min) / range) * (height - 3) - 1.5}`).join(' ')
  return <svg className={`sparkline sparkline--${direction}`} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Price trend"><polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.75" vectorEffect="non-scaling-stroke" /></svg>
}
