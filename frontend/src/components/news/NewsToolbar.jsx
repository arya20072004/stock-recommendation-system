import { SearchInput } from '../common/SearchInput'

export function NewsToolbar({ filters, stocks, categories, onChange }) {
  return <div className="news-toolbar" aria-label="News filters">
    <SearchInput label="Search news, stocks or companies" placeholder="Search news, stocks or companies..." value={filters.search} onChange={(event) => onChange('search', event.target.value)} />
    <div className="news-toolbar__controls">
      <label className="filter-control"><select className="select" aria-label="Sentiment" value={filters.sentiment} onChange={(event) => onChange('sentiment', event.target.value)}><option value="ALL">All Sentiment</option><option value="POSITIVE">Positive</option><option value="NEUTRAL">Neutral</option><option value="NEGATIVE">Negative</option></select></label>
      <label className="filter-control"><select className="select" aria-label="Stock" value={filters.stock} onChange={(event) => onChange('stock', event.target.value)}><option value="ALL">All Stocks</option>{stocks.map((stock) => <option key={stock.symbol} value={stock.symbol}>{stock.symbol}</option>)}</select></label>
      <label className="filter-control"><select className="select" aria-label="Category" value={filters.category} onChange={(event) => onChange('category', event.target.value)}><option value="ALL">All Categories</option>{categories.map((category) => <option key={category} value={category}>{category}</option>)}</select></label>
      <label className="filter-control"><select className="select" aria-label="Sort" value={filters.sort} onChange={(event) => onChange('sort', event.target.value)}><option value="latest">Latest</option><option value="oldest">Oldest</option><option value="relevant">Most Relevant</option></select></label>
    </div>
  </div>
}
