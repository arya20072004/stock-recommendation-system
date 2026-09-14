import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { Filter, RotateCcw } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { ErrorState } from '../components/common/ErrorState'
import { LoadingState } from '../components/common/LoadingState'
import { Select } from '../components/common/Select'
import { SearchInput } from '../components/common/SearchInput'
import { PageHeader } from '../components/layout/PageHeader'
import { DataTable } from '../components/common/DataTable'
import { MobileDataCard } from '../components/common/MobileDataCard'
import { SignalIndicator } from '../components/common/SignalIndicator'
import { ConfidenceIndicator } from '../components/common/ConfidenceIndicator'
import { StockIdentity } from '../components/common/StockIdentity'
import { PriceDisplay } from '../components/common/PriceDisplay'
import { ChangeDisplay } from '../components/common/ChangeDisplay'
import { fetchStocksSummary } from '../api/stocks'
import { formatSectorName } from '../utils/formatters'
import './screener-page.css'

const SIGNALS = ['BUY', 'HOLD', 'SELL']
const TIERS = ['VERY_HIGH', 'HIGH', 'MEDIUM', 'LOW', 'VERY_LOW']

const DEFAULTS = { signals: [], minConfidence: 0, minDayChange: -15, tiers: [], sector: 'ALL', search: '' }

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

  const [draft, setDraft] = useState({ ...DEFAULTS })
  const [applied, setApplied] = useState({ ...DEFAULTS })
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
    if (applied.search) {
      const q = applied.search.toLowerCase()
      result = result.filter(s => 
        s.ticker.toLowerCase().includes(q) || 
        (s.company_name && s.company_name.toLowerCase().includes(q))
      )
    }
    // We intentionally return the sorted result based on established Screener behavior.
    return result.sort((a, b) => b.confidence - a.confidence)
  }, [stocks, applied])

  if (loading) {
    return (
      <div className="screener-workspace">
        <PageHeader title="Screener" description="Find stocks matching your filtering criteria." />
        <LoadingState label="Loading screener data..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="screener-workspace">
        <PageHeader title="Screener" description="Find stocks matching your filtering criteria." />
        <ErrorState title="Failed to load screener data" description={error} />
      </div>
    )
  }

  const isPartial = meta && !meta.complete

  const filterPanel = (
    <div className="screener-filters">
      <div className="screener-filters__header">
        <h3 className="screener-filters__title">Filters</h3>
      </div>

      <div className="screener-filters__scroll">
        <fieldset className="screener-fieldset">
          <legend>Search this universe</legend>
          <div className="screener-search-input">
            <SearchInput value={draft.search} onChange={e => setDraft(d => ({ ...d, search: e.target.value }))} placeholder="Search ticker or company..." />
          </div>
        </fieldset>

        <fieldset className="screener-fieldset">
          <legend>Signal</legend>
          <div className="screener-checkbox-group">
            {SIGNALS.map(sig => {
              const isChecked = draft.signals.includes(sig);
              return (
                <label className={`screener-checkbox ${isChecked ? 'screener-checkbox--checked' : ''}`} key={sig}>
                  <input type="checkbox" checked={isChecked} onChange={() => toggleDraftSignal(sig)} />
                  <span>{sig.charAt(0) + sig.slice(1).toLowerCase().replace('_', ' ')}</span>
                </label>
              );
            })}
          </div>
        </fieldset>

        <fieldset className="screener-fieldset">
          <legend>Confidence Tier</legend>
          <div className="screener-checkbox-group">
            {TIERS.map(tier => {
              const isChecked = draft.tiers.includes(tier);
              return (
                <label className={`screener-checkbox ${isChecked ? 'screener-checkbox--checked' : ''}`} key={tier}>
                  <input type="checkbox" checked={isChecked} onChange={() => toggleDraftTier(tier)} />
                  <span>{tier.replace('_', ' ')}</span>
                </label>
              );
            })}
          </div>
        </fieldset>

        <fieldset className="screener-fieldset">
          <legend>Sector</legend>
          <Select label="Sector" value={draft.sector} onChange={v => setDraft(d => ({ ...d, sector: v }))} options={sectorOptions} className="screener-sector-select" hideLabel />
        </fieldset>

        <fieldset className="screener-fieldset">
          <legend>Minimum Confidence</legend>
          <div className="screener-slider">
            <input type="range" min="0" max="100" step="5" value={draft.minConfidence} onChange={e => setDraft(d => ({ ...d, minConfidence: Number(e.target.value) }))} aria-label="Minimum confidence percentage" />
            <span className="screener-slider__value mono">{draft.minConfidence}%</span>
          </div>
        </fieldset>

        <fieldset className="screener-fieldset">
          <legend>Minimum Day Change</legend>
          <div className="screener-slider">
            <input type="range" min="-15" max="15" step="1" value={draft.minDayChange} onChange={e => setDraft(d => ({ ...d, minDayChange: Number(e.target.value) }))} aria-label="Minimum day change percentage" />
            <span className="screener-slider__value mono">{draft.minDayChange > 0 ? '+' : ''}{draft.minDayChange}%</span>
          </div>
        </fieldset>
      </div>

      <div className="screener-filter-actions">
        <Button variant="ghost" onClick={resetFilters}><RotateCcw size={14} aria-hidden="true" />Reset</Button>
        <Button variant="primary" onClick={applyFilters}>Apply Filters</Button>
      </div>
    </div>
  )

  const columns = [
    {
      key: 'ticker',
      header: 'Stock',
      render: (val, row) => (
        <Link to={`/stocks/${encodeURIComponent(row.ticker)}`} className="table-stock-link">
          <StockIdentity ticker={row.ticker} companyName={row.company_name} disableLink />
        </Link>
      )
    },
    {
      key: 'sector',
      header: 'Sector',
      render: (val) => <span className="screener-sector-cell" title={val ? formatSectorName(val) : ''}>{val ? formatSectorName(val) : '—'}</span>
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
      key: 'recommendation',
      header: 'Signal',
      render: (val) => <SignalIndicator signal={val} />
    },
    {
      key: 'confidence',
      header: 'Confidence',
      render: (val, row) => <ConfidenceIndicator confidence={row.confidence} tier={row.confidence_tier} />
    }
  ]

  return (
    <div className="screener-workspace">
      <PageHeader 
        title="Screener" 
        description="Find stocks matching your filtering criteria." 
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

      <div className="screener-layout">
        <aside className={`screener-sidebar ${filtersOpen ? 'screener-sidebar--open' : ''}`} aria-label="Screener filters">
          <Card className="screener-sidebar__card">{filterPanel}</Card>
        </aside>

        <div className="screener-main">
          <div className="universe-summary">
            <div className="universe-summary__stats">
              <span><strong>{stocks.length}</strong> Total Universe</span>
            </div>
            <div className="universe-summary__filtered">
              Showing <strong>{results.length}</strong> matching stocks
            </div>
            
            <Button variant="secondary" className="screener-toggle-filters" onClick={() => setFiltersOpen(o => !o)}>
              <Filter size={16} aria-hidden="true" />{filtersOpen ? 'Hide Filters' : 'Filters'}
            </Button>
          </div>

          <div className="screener-results">
            {results.length === 0 ? (
              <EmptyState title="No matching stocks" description="Try lowering minimum confidence or adjusting your filters." />
            ) : (
              <>
                <div className="screener-desktop">
                  <Card className="screener-table-card">
                    <DataTable columns={columns} data={results} keyField="ticker" />
                  </Card>
                </div>

                <div className="screener-mobile">
                  {results.map(stock => (
                    <MobileDataCard 
                      key={stock.ticker}
                      identity={<StockIdentity ticker={stock.ticker} companyName={stock.company_name} />}
                      primarySignal={<SignalIndicator signal={stock.recommendation} />}
                      primaryMetrics={
                        <>
                          <div><span className="metric-label">Price</span><PriceDisplay price={stock.last_close} /></div>
                          <div><span className="metric-label">Change</span><ChangeDisplay percentageChange={stock.day_change_pct} /></div>
                        </>
                      }
                      status={<ConfidenceIndicator confidence={stock.confidence} tier={stock.confidence_tier} />}
                      metadata={<span className="screener-card-sector">{stock.sector ? formatSectorName(stock.sector) : ''}</span>}
                      action={<Button as={Link} to={`/stocks/${encodeURIComponent(stock.ticker)}`} variant="outline" className="full-width">View Details</Button>}
                    />
                  ))}
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
