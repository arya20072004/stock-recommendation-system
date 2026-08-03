import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { SearchInput } from '../components/common/SearchInput'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { getRecommendations, getSectors } from '../mocks/stocks'
import { formatCurrency, formatPercent } from '../utils/formatters'
import './recommendations-page.css'

const signalFilters = ['ALL', 'BUY', 'HOLD', 'SELL']
const riskOptions = [{ value: 'ALL', label: 'All Risk' }, { value: 'LOW', label: 'Low' }, { value: 'MEDIUM', label: 'Medium' }, { value: 'HIGH', label: 'High' }]
const sortOptions = [{ value: 'confidence', label: 'Confidence' }, { value: 'return', label: 'Expected Return' }, { value: 'ticker', label: 'Ticker' }, { value: 'price', label: 'Price' }]
const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative' }

export function Recommendations() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [riskFilter, setRiskFilter] = useState('ALL')
  const [sectorFilter, setSectorFilter] = useState('ALL')
  const [sortBy, setSortBy] = useState('confidence')
  const [search, setSearch] = useState('')

  const allRecs = useMemo(() => getRecommendations(), [])
  const sectors = useMemo(() => getSectors(), [])

  const sectorOptions = useMemo(() => [{ value: 'ALL', label: 'All Sectors' }, ...sectors.map(s => ({ value: s, label: s }))], [sectors])

  const filtered = useMemo(() => {
    let result = allRecs
    if (signalFilter !== 'ALL') result = result.filter(r => r.signal === signalFilter)
    if (riskFilter !== 'ALL') result = result.filter(r => r.risk === riskFilter)
    if (sectorFilter !== 'ALL') result = result.filter(r => r.sector === sectorFilter)
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(r => r.symbol.toLowerCase().includes(q) || r.companyName.toLowerCase().includes(q))
    }
    const sorted = [...result]
    switch (sortBy) {
      case 'confidence': sorted.sort((a, b) => b.confidence - a.confidence); break
      case 'return': sorted.sort((a, b) => b.expectedReturn - a.expectedReturn); break
      case 'ticker': sorted.sort((a, b) => a.symbol.localeCompare(b.symbol)); break
      case 'price': sorted.sort((a, b) => b.currentPrice - a.currentPrice); break
    }
    return sorted
  }, [allRecs, signalFilter, riskFilter, sectorFilter, sortBy, search])

  return (
    <div className="recommendations-page">
      <PageHeader title="Recommendations" description="AI-generated opportunities ranked by model confidence." actions={<Badge tone="accent">Demo data</Badge>} />

      <div className="filter-bar">
        <div className="signal-filters" role="group" aria-label="Filter by signal">
          {signalFilters.map(f => <Button key={f} variant="ghost" className={`signal-filter-button ${signalFilter === f ? 'signal-filter-button--active' : ''} ${f !== 'ALL' ? `signal-filter-button--${signalTones[f]}` : ''}`} onClick={() => setSignalFilter(f)} aria-pressed={signalFilter === f}>{f === 'ALL' ? 'All' : f.charAt(0) + f.slice(1).toLowerCase()}</Button>)}
        </div>
        <SearchInput className="rec-search" placeholder="Search recommendations..." value={search} onChange={e => setSearch(e.target.value)} />
        <div className="secondary-filters">
          <Select label="Risk filter" value={riskFilter} onChange={setRiskFilter} options={riskOptions} />
          <Select label="Sector filter" value={sectorFilter} onChange={setSectorFilter} options={sectorOptions} />
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
              <div className="rec-table__header" role="row">
                <span role="columnheader">Stock</span>
                <span role="columnheader">Signal</span>
                <span role="columnheader">Price</span>
                <span role="columnheader">Target</span>
                <span role="columnheader">Return</span>
                <span role="columnheader">Confidence</span>
                <span role="columnheader">Risk</span>
              </div>
              {filtered.map(rec => (
                <Link key={rec.ticker} to={`/stocks/${encodeURIComponent(rec.ticker)}`} className="rec-table__row" role="row" aria-label={`${rec.symbol} — ${rec.signal}`}>
                  <span role="cell" className="rec-table__stock"><strong>{rec.symbol}</strong><span>{rec.companyName}</span></span>
                  <span role="cell"><RecommendationBadge signal={rec.signal} /></span>
                  <span role="cell" className="mono">{formatCurrency(rec.currentPrice)}</span>
                  <span role="cell" className="mono">{formatCurrency(rec.targetPrice)}</span>
                  <span role="cell" className={`mono ${rec.expectedReturn >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(rec.expectedReturn)}</span>
                  <span role="cell"><ConfidenceBar value={rec.confidence} tone={signalTones[rec.signal] ?? 'positive'} compact /></span>
                  <span role="cell"><RiskBadge risk={rec.risk} /></span>
                </Link>
              ))}
            </div>
          </Card>

          {/* Mobile cards */}
          <div className="rec-cards" aria-label="Stock recommendations">
            {filtered.map(rec => (
              <Link key={rec.ticker} to={`/stocks/${encodeURIComponent(rec.ticker)}`} className="rec-card-link">
                <Card hoverable className="rec-card">
                  <div className="rec-card__header"><strong>{rec.symbol}</strong><RecommendationBadge signal={rec.signal} /></div>
                  <span className="rec-card__price mono">{formatCurrency(rec.currentPrice)}</span>
                  <div className="rec-card__metrics">
                    <div><span>Expected Return</span><strong className={`mono ${rec.expectedReturn >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(rec.expectedReturn)}</strong></div>
                    <div><span>Confidence</span><strong className="mono">{rec.confidence}%</strong></div>
                    <div><span>Risk</span><RiskBadge risk={rec.risk} /></div>
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
