import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Filter, RotateCcw } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { fetchStocksSummary } from '../api/stocks'
import { formatPercent, formatSectorName } from '../utils/formatters'
import './screener-page.css'

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative', UNCERTAIN: 'neutral' }
const SIGNALS = ['BUY', 'HOLD', 'SELL', 'UNCERTAIN']
const TIERS = ['VERY_HIGH', 'HIGH', 'MEDIUM', 'LOW', 'VERY_LOW']

const DEFAULTS = { signals: [], minConfidence: 0, minDayChange: -15, tiers: [], sector: 'ALL' }

export function Screener() {
  const [stocks, setStocks] = useState([])
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)
        const res = await fetchStocksSummary()
        setStocks(res.data || [])
        setMeta(res.meta)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadData()
  }, [])

  const sectors = useMemo(() => {
    const s = new Set()
    stocks.forEach(st => {
      if (st.sector) s.add(st.sector)
    })
    return Array.from(s).sort()
  }, [stocks])
  
  const sectorOptions = useMemo(() => [{ value: 'ALL', label: 'All Sectors' }, ...sectors.map(s => ({ value: s, label: formatSectorName(s) }))], [sectors])

  // Draft state (edited but not applied)
  const [draft, setDraft] = useState({ ...DEFAULTS })
  // Applied state (what results reflect)
  const [applied, setApplied] = useState({ ...DEFAULTS })
  // Mobile filter visibility
  const [filtersOpen, setFiltersOpen] = useState(false)

  const toggleDraftSignal = (sig) => setDraft(d => ({ ...d, signals: d.signals.includes(sig) ? d.signals.filter(s => s !== sig) : [...d.signals, sig] }))
  const toggleDraftTier = (tier) => setDraft(d => ({ ...d, tiers: d.tiers.includes(tier) ? d.tiers.filter(t => t !== tier) : [...d.tiers, tier] }))

  const applyFilters = () => { setApplied({ ...draft }); setFiltersOpen(false) }
  const resetFilters = () => { setDraft({ ...DEFAULTS }); setApplied({ ...DEFAULTS }) }

  const results = useMemo(() => {
    let result = stocks
    if (applied.signals.length > 0) result = result.filter(s => applied.signals.includes(s.recommendation))
    if (applied.minConfidence > 0) result = result.filter(s => s.confidence >= applied.minConfidence)
    if (applied.minDayChange > -15) result = result.filter(s => (s.day_change_pct || 0) >= applied.minDayChange)
    if (applied.tiers.length > 0) result = result.filter(s => applied.tiers.includes(s.confidence_tier))
    if (applied.sector !== 'ALL') result = result.filter(s => (s.sector || 'Unknown') === applied.sector)
    return result.sort((a, b) => b.confidence - a.confidence)
  }, [stocks, applied])

  if (loading) {
    return (
      <div className="screener-page">
        <PageHeader title="Screener" description="Find stocks matching your investment criteria." />
        <LoadingState label="Loading screener data..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="screener-page">
        <PageHeader title="Screener" description="Find stocks matching your investment criteria." />
        <EmptyState title="Failed to load screener data" description={error} />
      </div>
    )
  }

  const isPartial = meta && !meta.complete

  const filterPanel = (
    <div className="screener-filters">
      <h3 className="screener-filters__title">Filters</h3>

      {/* Signal */}
      <fieldset className="screener-fieldset">
        <legend>Signal</legend>
        {SIGNALS.map(sig => (
          <label className="screener-checkbox" key={sig}>
            <input type="checkbox" checked={draft.signals.includes(sig)} onChange={() => toggleDraftSignal(sig)} />
            <span>{sig.charAt(0) + sig.slice(1).toLowerCase().replace('_', ' ')}</span>
          </label>
        ))}
      </fieldset>

      {/* Min Confidence */}
      <fieldset className="screener-fieldset">
        <legend>Minimum Confidence</legend>
        <div className="screener-slider">
          <input type="range" min="0" max="100" step="5" value={draft.minConfidence} onChange={e => setDraft(d => ({ ...d, minConfidence: Number(e.target.value) }))} aria-label="Minimum confidence percentage" />
          <span className="screener-slider__value mono">{draft.minConfidence}%</span>
        </div>
      </fieldset>

      {/* Min Day Change */}
      <fieldset className="screener-fieldset">
        <legend>Minimum Day Change</legend>
        <div className="screener-slider">
          <input type="range" min="-15" max="15" step="1" value={draft.minDayChange} onChange={e => setDraft(d => ({ ...d, minDayChange: Number(e.target.value) }))} aria-label="Minimum day change percentage" />
          <span className="screener-slider__value mono">{draft.minDayChange > 0 ? '+' : ''}{draft.minDayChange}%</span>
        </div>
      </fieldset>

      {/* Confidence Tier */}
      <fieldset className="screener-fieldset">
        <legend>Confidence Tier</legend>
        {TIERS.map(tier => (
          <label className="screener-checkbox" key={tier}>
            <input type="checkbox" checked={draft.tiers.includes(tier)} onChange={() => toggleDraftTier(tier)} />
            <span>{tier.replace('_', ' ')}</span>
          </label>
        ))}
      </fieldset>

      {/* Sector */}
      <fieldset className="screener-fieldset">
        <legend>Sector</legend>
        <Select label="Sector" value={draft.sector} onChange={v => setDraft(d => ({ ...d, sector: v }))} options={sectorOptions} className="screener-sector-select" />
      </fieldset>

      <div className="screener-filter-actions">
        <Button variant="ghost" onClick={resetFilters}><RotateCcw size={14} aria-hidden="true" />Reset</Button>
        <Button variant="primary" onClick={applyFilters}>Apply Filters</Button>
      </div>
    </div>
  )

  return (
    <div className="screener-page">
      <PageHeader 
        title="Screener" 
        description="Find stocks matching your investment criteria." 
        actions={isPartial ? <Badge tone="warning">Partial Data</Badge> : <Badge tone="positive">Live</Badge>} 
      />

      {/* Mobile filter toggle */}
      <Button variant="secondary" className="screener-toggle-filters" onClick={() => setFiltersOpen(o => !o)}>
        <Filter size={16} aria-hidden="true" />{filtersOpen ? 'Hide Filters' : 'Show Filters'}
      </Button>

      <div className="screener-layout">
        {/* Filter panel — desktop always visible, mobile toggle */}
        <aside className={`screener-sidebar ${filtersOpen ? 'screener-sidebar--open' : ''}`} aria-label="Screener filters">
          <Card className="screener-sidebar__card">{filterPanel}</Card>
        </aside>

        {/* Results */}
        <div className="screener-results">
          <p className="screener-count">{results.length} stock{results.length !== 1 ? 's' : ''} match{results.length === 1 ? 'es' : ''} your criteria</p>

          {results.length === 0 ? (
            <EmptyState title="No stocks match these criteria" description="Try lowering minimum confidence or adjusting your filters." />
          ) : (
            <>
              {/* Desktop table */}
              <Card className="screener-table-card">
                <div className="screener-table" role="table" aria-label="Screener results">
                  <div className="screener-table__header" role="row">
                    <span role="columnheader">Stock</span>
                    <span role="columnheader">Sector</span>
                    <span role="columnheader">Signal</span>
                    <span role="columnheader">Day Change</span>
                    <span role="columnheader">Confidence</span>
                    <span role="columnheader">Tier</span>
                  </div>
                  {results.map(stock => (
                    <Link key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="screener-table__row" role="row" aria-label={`${stock.ticker} — ${stock.recommendation}`}>
                      <span role="cell" className="screener-table__stock"><strong>{stock.ticker}</strong><span>{stock.company_name}</span></span>
                      <span role="cell" className="screener-table__sector" title={stock.sector ? formatSectorName(stock.sector) : ''}>{stock.sector ? formatSectorName(stock.sector) : '—'}</span>
                      <span role="cell"><RecommendationBadge signal={stock.recommendation} /></span>
                      <span role="cell" className={`mono ${stock.day_change_pct >= 0 ? 'metric-positive' : 'metric-negative'}`}>{stock.day_change_pct != null ? formatPercent(stock.day_change_pct) : '—'}</span>
                      <span role="cell"><ConfidenceBar value={stock.confidence} tone={signalTones[stock.recommendation] ?? 'positive'} compact /></span>
                      <span role="cell"><RiskBadge risk={stock.confidence_tier} /></span>
                    </Link>
                  ))}
                </div>
              </Card>

              {/* Mobile cards */}
              <div className="screener-cards" aria-label="Screener results">
                {results.map(stock => (
                  <Link key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="screener-card-link">
                    <Card hoverable className="screener-card">
                      <div className="screener-card__header"><strong>{stock.ticker}</strong><RecommendationBadge signal={stock.recommendation} /></div>
                      <span className="screener-card__sector" title={stock.sector ? formatSectorName(stock.sector) : ''}>{stock.sector ? formatSectorName(stock.sector) : '—'}</span>
                      <div className="screener-card__metrics">
                        <div><span>Day Change</span><strong className={`mono ${stock.day_change_pct >= 0 ? 'metric-positive' : 'metric-negative'}`}>{stock.day_change_pct != null ? formatPercent(stock.day_change_pct) : '—'}</strong></div>
                        <div><span>Confidence</span><strong className="mono">{stock.confidence}%</strong></div>
                        <div><span>Tier</span><RiskBadge risk={stock.confidence_tier} /></div>
                      </div>
                    </Card>
                  </Link>
                ))}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
