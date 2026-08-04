import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Filter, RotateCcw } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { getAllStocks, getRiskLevels, getSectors, getSignals } from '../mocks/stocks'
import { formatPercent } from '../utils/formatters'
import './screener-page.css'

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative' }

const DEFAULTS = { signals: [], minConfidence: 0, minReturn: -15, risks: [], sector: 'ALL' }

export function Screener() {
  const allStocks = useMemo(() => getAllStocks(), [])
  const sectors = useMemo(() => getSectors(), [])
  const signals = useMemo(() => getSignals(), [])
  const riskLevels = useMemo(() => getRiskLevels(), [])

  const sectorOptions = useMemo(() => [{ value: 'ALL', label: 'All Sectors' }, ...sectors.map(s => ({ value: s, label: s }))], [sectors])

  // Draft state (edited but not applied)
  const [draft, setDraft] = useState({ ...DEFAULTS })
  // Applied state (what results reflect)
  const [applied, setApplied] = useState({ ...DEFAULTS })
  // Mobile filter visibility
  const [filtersOpen, setFiltersOpen] = useState(false)

  const toggleDraftSignal = (sig) => setDraft(d => ({ ...d, signals: d.signals.includes(sig) ? d.signals.filter(s => s !== sig) : [...d.signals, sig] }))
  const toggleDraftRisk = (risk) => setDraft(d => ({ ...d, risks: d.risks.includes(risk) ? d.risks.filter(r => r !== risk) : [...d.risks, risk] }))

  const applyFilters = () => { setApplied({ ...draft }); setFiltersOpen(false) }
  const resetFilters = () => { setDraft({ ...DEFAULTS }); setApplied({ ...DEFAULTS }) }

  const results = useMemo(() => {
    let result = allStocks
    if (applied.signals.length > 0) result = result.filter(s => applied.signals.includes(s.signal))
    if (applied.minConfidence > 0) result = result.filter(s => s.confidence >= applied.minConfidence)
    if (applied.minReturn > -15) result = result.filter(s => s.expectedReturn >= applied.minReturn)
    if (applied.risks.length > 0) result = result.filter(s => applied.risks.includes(s.risk))
    if (applied.sector !== 'ALL') result = result.filter(s => s.sector === applied.sector)
    return result.sort((a, b) => b.confidence - a.confidence)
  }, [allStocks, applied])

  const filterPanel = (
    <div className="screener-filters">
      <h3 className="screener-filters__title">Filters</h3>

      {/* Signal */}
      <fieldset className="screener-fieldset">
        <legend>Signal</legend>
        {signals.map(sig => (
          <label className="screener-checkbox" key={sig}>
            <input type="checkbox" checked={draft.signals.includes(sig)} onChange={() => toggleDraftSignal(sig)} />
            <span>{sig.charAt(0) + sig.slice(1).toLowerCase()}</span>
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

      {/* Min Expected Return */}
      <fieldset className="screener-fieldset">
        <legend>Minimum Expected Return</legend>
        <div className="screener-slider">
          <input type="range" min="-15" max="15" step="1" value={draft.minReturn} onChange={e => setDraft(d => ({ ...d, minReturn: Number(e.target.value) }))} aria-label="Minimum expected return percentage" />
          <span className="screener-slider__value mono">{draft.minReturn > 0 ? '+' : ''}{draft.minReturn}%</span>
        </div>
      </fieldset>

      {/* Risk */}
      <fieldset className="screener-fieldset">
        <legend>Risk</legend>
        {riskLevels.map(risk => (
          <label className="screener-checkbox" key={risk}>
            <input type="checkbox" checked={draft.risks.includes(risk)} onChange={() => toggleDraftRisk(risk)} />
            <span>{risk.charAt(0) + risk.slice(1).toLowerCase()}</span>
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
      <PageHeader title="Screener" description="Find stocks matching your investment criteria." actions={<Badge tone="accent">Demo data</Badge>} />

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
                    <span role="columnheader">Expected Return</span>
                    <span role="columnheader">Confidence</span>
                    <span role="columnheader">Risk</span>
                  </div>
                  {results.map(stock => (
                    <Link key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="screener-table__row" role="row" aria-label={`${stock.symbol} — ${stock.signal}`}>
                      <span role="cell" className="screener-table__stock"><strong>{stock.symbol}</strong><span>{stock.companyName}</span></span>
                      <span role="cell" className="screener-table__sector">{stock.sector}</span>
                      <span role="cell"><RecommendationBadge signal={stock.signal} /></span>
                      <span role="cell" className={`mono ${stock.expectedReturn >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(stock.expectedReturn)}</span>
                      <span role="cell"><ConfidenceBar value={stock.confidence} tone={signalTones[stock.signal] ?? 'positive'} compact /></span>
                      <span role="cell"><RiskBadge risk={stock.risk} /></span>
                    </Link>
                  ))}
                </div>
              </Card>

              {/* Mobile cards */}
              <div className="screener-cards" aria-label="Screener results">
                {results.map(stock => (
                  <Link key={stock.ticker} to={`/stocks/${encodeURIComponent(stock.ticker)}`} className="screener-card-link">
                    <Card hoverable className="screener-card">
                      <div className="screener-card__header"><strong>{stock.symbol}</strong><RecommendationBadge signal={stock.signal} /></div>
                      <span className="screener-card__sector">{stock.sector}</span>
                      <div className="screener-card__metrics">
                        <div><span>Expected Return</span><strong className={`mono ${stock.expectedReturn >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(stock.expectedReturn)}</strong></div>
                        <div><span>Confidence</span><strong className="mono">{stock.confidence}%</strong></div>
                        <div><span>Risk</span><RiskBadge risk={stock.risk} /></div>
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
