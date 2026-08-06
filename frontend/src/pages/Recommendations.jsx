import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { SearchInput } from '../components/common/SearchInput'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { fetchRecommendations } from '../api/recommendations'
import { formatCurrency, formatPercent } from '../utils/formatters'
import './recommendations-page.css'

const signalFilters = ['ALL', 'BUY', 'HOLD', 'SELL']
const tierOptions = [{ value: 'ALL', label: 'All Tiers' }, { value: 'HIGH', label: 'High' }, { value: 'MEDIUM', label: 'Medium' }, { value: 'LOW', label: 'Low' }]
const sortOptions = [{ value: 'confidence', label: 'Confidence' }, { value: 'change', label: 'Day Change' }, { value: 'ticker', label: 'Ticker' }, { value: 'price', label: 'Price' }]
const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative', UNCERTAIN: 'neutral' }

export function Recommendations() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [tierFilter, setTierFilter] = useState('ALL')
  const [sortBy, setSortBy] = useState('confidence')
  const [search, setSearch] = useState('')
  
  const [recs, setRecs] = useState([])
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
    let result = recs
    if (signalFilter !== 'ALL') result = result.filter(r => r.recommendation === signalFilter)
    if (tierFilter !== 'ALL') result = result.filter(r => r.confidence_tier === tierFilter)
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(r => r.ticker.toLowerCase().includes(q))
    }
    const sorted = [...result]
    switch (sortBy) {
      case 'confidence': sorted.sort((a, b) => b.confidence - a.confidence); break
      case 'change': sorted.sort((a, b) => b.day_change_pct - a.day_change_pct); break
      case 'ticker': sorted.sort((a, b) => a.ticker.localeCompare(b.ticker)); break
      case 'price': sorted.sort((a, b) => b.last_close - a.last_close); break
    }
    return sorted
  }, [recs, signalFilter, tierFilter, sortBy, search])

  if (loading) {
    return (
      <div className="recommendations-page">
        <PageHeader title="Recommendations" description="AI-generated opportunities ranked by model confidence." />
        <LoadingState label="Loading recommendations..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="recommendations-page">
        <PageHeader title="Recommendations" description="AI-generated opportunities ranked by model confidence." />
        <EmptyState title="Failed to load recommendations" description={error} />
      </div>
    )
  }

  const isPartial = meta && !meta.complete

  return (
    <div className="recommendations-page">
      <PageHeader 
        title="Recommendations" 
        description="AI-generated opportunities ranked by model confidence." 
        actions={isPartial ? <Badge tone="warning">Partial Snapshot</Badge> : <Badge tone="positive">Live</Badge>} 
      />

      {isPartial && (
        <Card className="snapshot-warning-card" style={{ marginBottom: '1rem', borderColor: 'var(--warning)', background: 'var(--warning-light, rgba(255, 170, 0, 0.1))' }}>
          <p style={{ margin: 0, color: 'var(--warning-text, inherit)' }}>
            <strong>Mixed-date snapshot:</strong> This view contains predictions from multiple dates. 
            Missing tickers: {meta.missing_tickers?.join(', ') || 'None'}.
          </p>
        </Card>
      )}

      <div className="filter-bar">
        <div className="signal-filters" role="group" aria-label="Filter by signal">
          {signalFilters.map(f => (
            <Button 
              key={f} 
              variant="ghost" 
              className={`signal-filter-button ${signalFilter === f ? 'signal-filter-button--active' : ''} ${f !== 'ALL' ? `signal-filter-button--${signalTones[f]}` : ''}`} 
              onClick={() => setSignalFilter(f)} 
              aria-pressed={signalFilter === f}
            >
              {f === 'ALL' ? 'All' : f.charAt(0) + f.slice(1).toLowerCase()}
            </Button>
          ))}
        </div>
        <SearchInput className="rec-search" placeholder="Search recommendations..." value={search} onChange={e => setSearch(e.target.value)} />
        <div className="secondary-filters">
          <Select label="Confidence Tier" value={tierFilter} onChange={setTierFilter} options={tierOptions} />
          <Select label="Sort by" value={sortBy} onChange={setSortBy} options={sortOptions} />
        </div>
      </div>

      {filtered.length === 0 ? (
        <EmptyState title="No recommendations found" description="Try changing your filters or search criteria." />
      ) : (
        <>
          {/* Desktop table */}
          <Card className="rec-table-card">
            <div className="rec-table" role="table" aria-label="Stock recommendations">
              <div className="rec-table__header" role="row" style={{ gridTemplateColumns: 'minmax(120px, 1.5fr) 1fr 1fr 1fr 1.5fr 1fr' }}>
                <span role="columnheader">Ticker</span>
                <span role="columnheader">Signal</span>
                <span role="columnheader">Price</span>
                <span role="columnheader">Day Change</span>
                <span role="columnheader">Confidence</span>
                <span role="columnheader">Tier</span>
              </div>
              {filtered.map(rec => (
                <Link key={rec.ticker} to={`/stocks/${encodeURIComponent(rec.ticker)}`} className="rec-table__row" role="row" aria-label={`${rec.ticker} — ${rec.recommendation}`} style={{ gridTemplateColumns: 'minmax(120px, 1.5fr) 1fr 1fr 1fr 1.5fr 1fr' }}>
                  <span role="cell" className="rec-table__stock"><strong>{rec.ticker}</strong><span style={{ fontSize: '0.75rem', color: 'var(--text-tertiary)' }}>{rec.market_date}</span></span>
                  <span role="cell"><RecommendationBadge signal={rec.recommendation} /></span>
                  <span role="cell" className="mono">{formatCurrency(rec.last_close)}</span>
                  <span role="cell" className={`mono ${rec.day_change_pct >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(rec.day_change_pct)}</span>
                  <span role="cell"><ConfidenceBar value={rec.confidence} tone={signalTones[rec.recommendation] ?? 'positive'} compact /></span>
                  <span role="cell"><RiskBadge risk={rec.confidence_tier} /></span>
                </Link>
              ))}
            </div>
          </Card>

          {/* Mobile cards */}
          <div className="rec-cards" aria-label="Stock recommendations">
            {filtered.map(rec => (
              <Link key={rec.ticker} to={`/stocks/${encodeURIComponent(rec.ticker)}`} className="rec-card-link">
                <Card hoverable className="rec-card">
                  <div className="rec-card__header"><strong>{rec.ticker}</strong><RecommendationBadge signal={rec.recommendation} /></div>
                  <span className="rec-card__price mono">{formatCurrency(rec.last_close)}</span>
                  <div className="rec-card__metrics">
                    <div><span>Day Change</span><strong className={`mono ${rec.day_change_pct >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(rec.day_change_pct)}</strong></div>
                    <div><span>Confidence</span><strong className="mono">{rec.confidence}%</strong></div>
                    <div><span>Tier</span><RiskBadge risk={rec.confidence_tier} /></div>
                  </div>
                </Card>
              </Link>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
