import { MarketIndexCard } from './MarketIndexCard'

export function MarketSummary({ items }) {
  return <section aria-labelledby="market-summary-heading"><h2 id="market-summary-heading" className="section-label">Market summary</h2><div className="market-summary-grid">{items.map((item) => <MarketIndexCard key={item.label} item={item} />)}</div></section>
}
