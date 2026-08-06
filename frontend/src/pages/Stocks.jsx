import { useEffect, useMemo, useState } from 'react'
import { Badge } from '../components/common/Badge'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { SearchInput } from '../components/common/SearchInput'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { StockCard } from '../components/stocks/StockCard'
import { fetchStocksSummary } from '../api/stocks'
import './stocks-page.css'

const sortOptions = [
  { value: 'ticker', label: 'Ticker' },
  { value: 'company', label: 'Company Name' },
  { value: 'price', label: 'Price' },
  { value: 'change', label: 'Daily Change' },
]

export function Stocks() {
  const [search, setSearch] = useState('')
  const [sector, setSector] = useState('ALL')
  const [sortBy, setSortBy] = useState('ticker')

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
  
  const sectorOptions = useMemo(() => [{ value: 'ALL', label: 'All Sectors' }, ...sectors.map(s => ({ value: s, label: s }))], [sectors])

  const filtered = useMemo(() => {
    let result = stocks
    if (sector !== 'ALL') result = result.filter(s => (s.sector || 'Unknown') === sector)
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(s => s.ticker.toLowerCase().includes(q) || s.company_name.toLowerCase().includes(q))
    }
    const sorted = [...result]
    switch (sortBy) {
      case 'ticker': sorted.sort((a, b) => a.ticker.localeCompare(b.ticker)); break
      case 'company': sorted.sort((a, b) => a.company_name.localeCompare(b.company_name)); break
      case 'price': sorted.sort((a, b) => (b.last_close || 0) - (a.last_close || 0)); break
      case 'change': sorted.sort((a, b) => (b.day_change_pct || 0) - (a.day_change_pct || 0)); break
    }
    return sorted
  }, [stocks, sector, search, sortBy])

  if (loading) {
    return (
      <div className="stocks-page">
        <PageHeader title="Stocks" description="Explore companies tracked by the recommendation system." />
        <LoadingState label="Loading stocks..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="stocks-page">
        <PageHeader title="Stocks" description="Explore companies tracked by the recommendation system." />
        <EmptyState title="Failed to load stocks" description={error} />
      </div>
    )
  }

  const isPartial = meta && !meta.complete

  return (
    <div className="stocks-page">
      <PageHeader 
        title="Stocks" 
        description="Explore companies tracked by the recommendation system." 
        actions={isPartial ? <Badge tone="warning">Partial Data</Badge> : <Badge tone="positive">Live</Badge>} 
      />

      <div className="stocks-toolbar">
        <SearchInput className="stocks-search" label="Search stocks" placeholder="Search stocks by ticker or company..." value={search} onChange={e => setSearch(e.target.value)} />
        <div className="stocks-toolbar__controls">
          <Select label="Sector filter" value={sector} onChange={setSector} options={sectorOptions} />
          <Select label="Sort by" value={sortBy} onChange={setSortBy} options={sortOptions} />
        </div>
      </div>

      {filtered.length === 0 ? (
        <EmptyState title="No stocks found" description="Try changing your search or sector filter." />
      ) : (
        <div className="stock-grid" aria-label="Tracked stocks">
          {filtered.map(stock => <StockCard key={stock.ticker} stock={stock} />)}
        </div>
      )}
    </div>
  )
}
