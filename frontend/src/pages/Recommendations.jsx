import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { ErrorState } from '../components/common/ErrorState'
import { LoadingState } from '../components/common/LoadingState'
import { SearchInput } from '../components/common/SearchInput'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { DataTable } from '../components/common/DataTable'
import { MobileDataCard } from '../components/common/MobileDataCard'
import { SignalIndicator } from '../components/common/SignalIndicator'
import { ConfidenceIndicator } from '../components/common/ConfidenceIndicator'
import { StockIdentity } from '../components/common/StockIdentity'
import { PriceDisplay } from '../components/common/PriceDisplay'
import { ChangeDisplay } from '../components/common/ChangeDisplay'
import { fetchRecommendations } from '../api/recommendations'
import './recommendations-page.css'

const signalFilters = ['ALL', 'BUY', 'HOLD', 'SELL']
// Map of backend tiers for filtering, based on common canonical values.
const tierOptions = [
  { value: 'ALL', label: 'All Tiers' }, 
  { value: 'VERY_HIGH', label: 'Very High' }, 
  { value: 'HIGH', label: 'High' }, 
  { value: 'MEDIUM', label: 'Medium' }, 
  { value: 'LOW', label: 'Low' },
  { value: 'VERY_LOW', label: 'Very Low' }
]

export function Recommendations() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [tierFilter, setTierFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  
  const [recs, setRecs] = useState(null)
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)
        const res = await fetchRecommendations()
        setRecs(res.data || [])
        setMeta(res.meta)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [])

  const filtered = useMemo(() => {
    if (!recs) return []
    let result = recs
    if (signalFilter !== 'ALL') result = result.filter(r => r.recommendation === signalFilter)
    if (tierFilter !== 'ALL') result = result.filter(r => r.confidence_tier === tierFilter)
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(r => r.ticker.toLowerCase().includes(q))
    }
    // We intentionally return the filtered subset WITHOUT re-sorting it.
    // This preserves the authoritative conviction ordering from the backend.
    return result
  }, [recs, signalFilter, tierFilter, search])

  if (loading) {
    return (
      <div className="recommendations-workspace">
        <PageHeader title="Recommendations" description="AI-generated market opportunities." />
        <LoadingState label="Loading recommendations..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="recommendations-workspace">
        <PageHeader title="Recommendations" description="AI-generated market opportunities." />
        <ErrorState title="Failed to load recommendations" description={error} />
      </div>
    )
  }

  if (!recs) return null

  const isPartial = meta && !meta.complete
  const totalCount = recs.length
  const buyCount = recs.filter(r => r.recommendation === 'BUY').length
  const holdCount = recs.filter(r => r.recommendation === 'HOLD').length
  const sellCount = recs.filter(r => r.recommendation === 'SELL').length

  const columns = [
    {
      key: 'ticker',
      header: 'Stock',
      render: (val, row) => (
        <Link to={`/stocks/${encodeURIComponent(row.ticker)}`} className="table-stock-link">
          <StockIdentity ticker={row.ticker} disableLink />
        </Link>
      )
    },
    {
      key: 'recommendation',
      header: 'Signal',
      render: (val) => <SignalIndicator signal={val} />
    },
    {
      key: 'last_close',
      header: 'Price',
      align: 'right',
      render: (val) => <PriceDisplay price={val} />
    },
    {
      key: 'day_change_pct',
      header: 'Day Change',
      align: 'right',
      render: (val) => <ChangeDisplay percentageChange={val} />
    },
    {
      key: 'confidence',
      header: 'Confidence',
      render: (val, row) => <ConfidenceIndicator confidence={row.confidence} tier={row.confidence_tier} />
    },
    {
      key: 'market_date',
      header: 'Date',
      align: 'right',
      render: (val) => <span className="mono text-sm text-secondary">{val}</span>
    }
  ]

  return (
    <div className="recommendations-workspace">
      <PageHeader 
        title="Recommendations" 
        description="AI-generated market opportunities." 
        actions={
          <>
            {meta?.market_date && <Badge tone="neutral">Target session {meta.market_date}</Badge>}
            {meta?.mixed_date ? (
              <Badge tone="warning">Mixed Market Dates</Badge>
            ) : (
              <Badge tone={isPartial ? 'warning' : 'positive'}>{isPartial ? 'Partial Snapshot' : 'Complete Snapshot'}</Badge>
            )}
          </>
        } 
      />

      <div className="universe-summary">
        <div className="universe-summary__stats">
          <span><strong>{totalCount}</strong> Universe</span>
          <span><strong>{buyCount}</strong> BUY</span>
          <span><strong>{holdCount}</strong> HOLD</span>
          <span><strong>{sellCount}</strong> SELL</span>
        </div>
        <div className="universe-summary__filtered">
          Showing <strong>{filtered.length}</strong> matching records
        </div>
      </div>

      <div className="filter-bar">
        <div className="signal-filters" role="group" aria-label="Filter by signal">
          {signalFilters.map(f => (
            <Button 
              key={f} 
              variant={signalFilter === f ? 'primary' : 'ghost'} 
              className={`signal-filter-button ${f !== 'ALL' ? `signal-filter-button--${f.toLowerCase()}` : ''}`} 
              onClick={() => setSignalFilter(f)} 
              aria-pressed={signalFilter === f}
            >
              {f === 'ALL' ? 'All Signals' : f}
            </Button>
          ))}
        </div>
        <div className="secondary-filters">
          <SearchInput className="rec-search" placeholder="Search ticker..." value={search} onChange={e => setSearch(e.target.value)} />
          <Select label="Confidence Tier" value={tierFilter} onChange={setTierFilter} options={tierOptions} hideLabel />
        </div>
      </div>

      {filtered.length === 0 ? (
        <EmptyState title="No records found" description="Try adjusting your filters or search criteria." />
      ) : (
        <div className="recommendations-content">
          <div className="recommendations-desktop">
            <Card className="recommendations-table-card">
              <DataTable columns={columns} data={filtered} keyField="ticker" />
            </Card>
          </div>

          <div className="recommendations-mobile">
            {filtered.map(rec => (
              <MobileDataCard 
                key={rec.ticker}
                identity={<StockIdentity ticker={rec.ticker} />}
                primarySignal={<SignalIndicator signal={rec.recommendation} />}
                primaryMetrics={
                  <>
                    <div><span className="metric-label">Price</span><PriceDisplay price={rec.last_close} /></div>
                    <div><span className="metric-label">Change</span><ChangeDisplay percentageChange={rec.day_change_pct} /></div>
                  </>
                }
                status={<ConfidenceIndicator confidence={rec.confidence} tier={rec.confidence_tier} />}
                metadata={<span className="mono text-xs text-tertiary">Model: {rec.model_version} • {rec.market_date}</span>}
                action={<Button as={Link} to={`/stocks/${encodeURIComponent(rec.ticker)}`} variant="outline" className="full-width">View Details</Button>}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
