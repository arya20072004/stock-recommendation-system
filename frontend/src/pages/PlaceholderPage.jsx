import { ArrowLeft, Construction } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../components/common/Card'
import { PageHeader } from '../components/layout/PageHeader'
import './pages.css'

export function PlaceholderPage({ title, description }) {
  return <><PageHeader title={title} description={description} /><Card className="placeholder-card"><Construction aria-hidden="true" size={28} /><div><h2>Coming in a later phase</h2><p>This area is intentionally kept free of sample market data until its corresponding product work begins.</p></div></Card></>
}

export function NotFoundPage() {
  return <><PageHeader title="Page not found" description="The address you entered does not match a StockIntel page." /><Card className="placeholder-card"><ArrowLeft aria-hidden="true" size={28} /><div><h2>Return to the workspace</h2><p>Use the navigation or return to your dashboard.</p></div><Link className="button button--secondary" to="/dashboard">Go to dashboard</Link></Card></>
}
