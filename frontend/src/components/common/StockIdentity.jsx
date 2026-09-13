import { Link } from 'react-router-dom'
import './financial.css'

export function StockIdentity({ ticker, companyName, disableLink = false, className = '' }) {
  const content = (
    <div className={`stock-identity ${className}`.trim()}>
      <span className="stock-identity__ticker">{ticker}</span>
      {companyName && <span className="stock-identity__name">{companyName}</span>}
    </div>
  )

  if (disableLink) return content
  return <Link to={`/stocks/${encodeURIComponent(ticker)}`} className="stock-identity__link">{content}</Link>
}
