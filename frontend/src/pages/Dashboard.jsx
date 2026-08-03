import { lazy, Suspense } from 'react'
import { Badge } from '../components/common/Badge'
import { LoadingState } from '../components/common/LoadingState'
import { ImportantNews } from '../components/dashboard/ImportantNews'
import { MarketSummary } from '../components/dashboard/MarketSummary'
import { RecommendationSnapshot } from '../components/dashboard/RecommendationSnapshot'
import { TopRecommendationCard } from '../components/dashboard/TopRecommendationCard'
import { WatchlistPreview } from '../components/dashboard/WatchlistPreview'
import { PageHeader } from '../components/layout/PageHeader'
import { dashboardData } from '../mocks/dashboard'
import '../components/dashboard/dashboard.css'

const MarketOverviewChart = lazy(() => import('../components/dashboard/MarketOverviewChart').then((module) => ({ default: module.MarketOverviewChart })))

export function Dashboard() {
  return <div className="dashboard"><PageHeader title="Dashboard" description="Market and recommendation intelligence." actions={<Badge tone="accent">Demo data</Badge>} /><MarketSummary items={dashboardData.marketSummary} /><div className="dashboard-grid"><TopRecommendationCard recommendation={dashboardData.topRecommendation} /><Suspense fallback={<LoadingState label="Loading market overview chart" />}><MarketOverviewChart overview={dashboardData.marketOverview} /></Suspense></div><div className="dashboard-grid"><RecommendationSnapshot snapshot={dashboardData.recommendationSnapshot} /><WatchlistPreview stocks={dashboardData.watchlist} /></div><ImportantNews stories={dashboardData.importantNews} /></div>
}
