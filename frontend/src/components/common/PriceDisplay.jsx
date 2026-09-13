import './financial.css'

export function PriceDisplay({ price, currency = 'USD', unavailable = false, className = '' }) {
  if (unavailable || price == null) {
    return <span className={`price-display unavailable ${className}`.trim()}>—</span>
  }
  
  const formatted = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: currency,
  }).format(price)

  return <span className={`price-display ${className}`.trim()}>{formatted}</span>
}
