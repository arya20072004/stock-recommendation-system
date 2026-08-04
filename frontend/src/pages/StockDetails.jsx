import { lazy, Suspense } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ArrowDownRight, ArrowLeft, ArrowUpRight, ChevronRight, Star } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RecommendationExplanation } from '../components/recommendations/RecommendationExplanation'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { SentimentBadge } from '../components/news/SentimentBadge'
import { TechnicalIndicators } from '../components/stocks/TechnicalIndicators'
import { useWatchlist } from '../context/WatchlistContext'
import { getStockDetail, getStockNews } from '../mocks/stocks'
import { directionForValue, formatCurrency, formatPercent } from '../utils/formatters'
import './stock-details.css'

const PriceChart = lazy(() => import('../components/charts/PriceChart').then(m => ({ default: m.PriceChart })))

const signalTones = { BUY: 'positive', 'STRONG BUY': 'positive', HOLD: 'warning', SELL: 'negative', 'STRONG SELL': 'negative' }

export function StockDetails() {
  const { ticker } = useParams()
  const navigate = useNavigate()
  const decodedTicker = decodeURIComponent(ticker)
  const stock = getStockDetail(decodedTicker)
  const news = stock ? getStockNews(decodedTicker) : []
  const { isInWatchlist, toggleWatchlist } = useWatchlist()
  const saved = stock ? isInWatchlist(stock.ticker) : false

  const goBack = () => {
    // Use browser history if available, otherwise fall back to /stocks
    if (window.history.length > 1) {
      navigate(-1)
    } else {
      navigate('/stocks')
    }
  }

  if (!stock) {
    return (
      <div className="stock-details">
        <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>
        <EmptyState title="Stock not found" description={`No demo stock data exists for "${decodedTicker}".`} action={{ label: 'Browse stocks', onClick: () => navigate('/stocks') }} />
      </div>
    )
  }

  const direction = directionForValue(stock.priceChangePercent)
  const MovementIcon = direction === 'negative' ? ArrowDownRight : ArrowUpRight

  return (
    <div className="stock-details">
      <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>

      {/* ── Stock Header ── */}
      <header className="stock-header">
        <div className="stock-header__identity">
          <div>
            <span className="eyebrow">{stock.symbol}</span>
            <h1>{stock.companyName} <span className="stock-header__exchange">{stock.exchange}</span></h1>
          </div>
          <Badge tone="accent">Demo data</Badge>
        </div>
        <div className="stock-header__price-row">
          <div>
            <strong className="stock-header__price mono">{formatCurrency(stock.currentPrice)}</strong>
            <span className={`stock-header__change stock-header__change--${direction}`}>
              <MovementIcon aria-hidden="true" size={15} />
              {stock.priceChange > 0 ? '+' : ''}{formatCurrency(stock.priceChange).replace('₹', '₹')} ({formatPercent(stock.priceChangePercent)}) Today
            </span>
          </div>
          <Button variant="secondary" className={`watchlist-button ${saved ? 'watchlist-button--saved' : ''}`} onClick={() => toggleWatchlist(stock.ticker)} aria-pressed={saved}>
            <Star size={16} aria-hidden="true" fill={saved ? 'currentColor' : 'none'} />{saved ? 'In Watchlist' : 'Add to Watchlist'}
          </Button>
        </div>
      </header>

      {/* ── Chart + Signal (row 1) ── */}
      <div className="details-grid">
        <Suspense fallback={<LoadingState label="Loading price chart" />}>
          <PriceChart seriesByRange={stock.priceHistory} direction={direction} />
        </Suspense>

        <section aria-labelledby="signal-heading">
          <h2 id="signal-heading" className="section-label">Model signal</h2>
          <Card className="signal-card">
            <RecommendationBadge signal={stock.signal} />
            <ConfidenceBar value={stock.confidence} tone={signalTones[stock.signal] ?? 'positive'} />
            <div className="signal-card__metrics">
              <div>
                <span>Target Price</span>
                <strong className="mono">{formatCurrency(stock.targetPrice)}</strong>
              </div>
              <div>
                <span>Expected Return</span>
                <strong className={`mono ${stock.expectedReturn >= 0 ? 'metric-positive' : 'metric-negative'}`}>{formatPercent(stock.expectedReturn)}</strong>
              </div>
              <div>
                <span>Risk</span>
                <RiskBadge risk={stock.risk} />
              </div>
            </div>
          </Card>
        </section>
      </div>

      {/* ── Explanations + Indicators (row 2) ── */}
      <div className="details-grid">
        <section aria-labelledby="explanation-heading">
          <h2 id="explanation-heading" className="section-label">Why this recommendation?</h2>
          <RecommendationExplanation explanations={stock.explanations} />
        </section>

        <section aria-labelledby="indicators-heading">
          <h2 id="indicators-heading" className="section-label">Technical indicators</h2>
          <TechnicalIndicators indicators={stock.technicalIndicators} />
        </section>
      </div>

      {/* ── Relevant News ── */}
      {news.length > 0 && (
        <section aria-labelledby="stock-news-heading">
          <div className="section-heading">
            <h2 id="stock-news-heading">Relevant news</h2>
            <Link className="section-link" to="/news">View all news <ChevronRight aria-hidden="true" size={15} /></Link>
          </div>
          <Card className="stock-news-card">
            {news.map(story => (
              <article className="stock-news-item" key={story.id}>
                <div className="stock-news-item__heading">
                  <SentimentBadge sentiment={story.sentiment} />
                  <time>{story.publishedAt}</time>
                </div>
                <h3>{story.headline}</h3>
                <span className="stock-news-item__source">{story.source}</span>
              </article>
            ))}
          </Card>
        </section>
      )}
    </div>
  )
}

