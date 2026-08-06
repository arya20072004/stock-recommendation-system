export function NewsToolbar({ filters, stocks, onChange }) {
  return (
    <div className="news-toolbar" aria-label="News filters">
      <div className="news-toolbar__controls" style={{ marginTop: 0 }}>
        <label className="filter-control">
          <select className="select" aria-label="Sentiment" value={filters.sentiment} onChange={(event) => onChange('sentiment', event.target.value)}>
            <option value="ALL">All Sentiment</option>
            <option value="POSITIVE">Positive</option>
            <option value="NEUTRAL">Neutral</option>
            <option value="NEGATIVE">Negative</option>
          </select>
        </label>
        <label className="filter-control">
          <select className="select" aria-label="Stock" value={filters.stock} onChange={(event) => onChange('stock', event.target.value)}>
            <option value="ALL">All Stocks</option>
            {stocks.map((stock) => (
              <option key={stock} value={stock}>{stock}</option>
            ))}
          </select>
        </label>
      </div>
    </div>
  )
}
