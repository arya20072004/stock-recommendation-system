import { useMemo, useState } from 'react'
import { Badge } from '../components/common/Badge'
import { EmptyState } from '../components/common/EmptyState'
import { SearchInput } from '../components/common/SearchInput'
import { Select } from '../components/common/Select'
import { PageHeader } from '../components/layout/PageHeader'
import { StockCard } from '../components/stocks/StockCard'
import { getAllStocks, getSectors } from '../mocks/stocks'
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

  const allStocks = useMemo(() => getAllStocks(), [])
  const sectors = useMemo(() => getSectors(), [])
  const sectorOptions = useMemo(() => [{ value: 'ALL', label: 'All Sectors' }, ...sectors.map(s => ({ value: s, label: s }))], [sectors])

  const filtered = useMemo(() => {
    let result = allStocks
    if (sector !== 'ALL') result = result.filter(s => s.sector === sector)
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(s => s.symbol.toLowerCase().includes(q) || s.companyName.toLowerCase().includes(q))
    }
    const sorted = [...result]
    switch (sortBy) {
      case 'ticker': sorted.sort((a, b) => a.symbol.localeCompare(b.symbol)); break
      case 'company': sorted.sort((a, b) => a.companyName.localeCompare(b.companyName)); break
      case 'price': sorted.sort((a, b) => b.currentPrice - a.currentPrice); break
      case 'change': sorted.sort((a, b) => b.priceChangePercent - a.priceChangePercent); break
    }
    return sorted
  }, [allStocks, sector, search, sortBy])

  return (
    <div className="stocks-page">
      <PageHeader title="Stocks" description="Explore companies tracked by the recommendation system." actions={<Badge tone="accent">Demo data</Badge>} />

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
