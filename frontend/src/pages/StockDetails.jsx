import { lazy, Suspense, useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ArrowDownRight, ArrowLeft, ArrowUpRight, Star } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { TradingViewAdvancedChart } from '../components/tradingview/TradingViewAdvancedChart'
import { useWatchlist } from '../context/WatchlistContext'
import { fetchStockDetails } from '../api/stocks'
import { directionForValue, formatCurrency, formatPercent, formatSectorName } from '../utils/formatters'
import '../components/tradingview/tradingview.css'
import './stock-details.css'

const PriceChart = lazy(() => import('../components/charts/PriceChart').then(m => ({ default: m.PriceChart })))

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative', UNCERTAIN: 'neutral' }

export function StockDetails() {
  const { ticker } = useParams()
  const navigate = useNavigate()
  const decodedTicker = decodeURIComponent(ticker)

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [range, setRange] = useState('1Y')
  const [chartLoading, setChartLoading] = useState(false)
  const [error, setError] = useState(null)
  const [notFound, setNotFound] = useState(false)
  const [chartMode, setChartMode] = useState('stockintel') // 'stockintel' | 'tradingview'

  const { isInWatchlist, toggleWatchlist } = useWatchlist()
  const saved = isInWatchlist(decodedTicker)

  useEffect(() => {
    async function loadInitial() {
      try {
        setLoading(true)
        setError(null)
        setNotFound(false)
        const res = await fetchStockDetails(decodedTicker, '1Y')
        setData(res)
        setRange('1Y')
      } catch (err) {
        if (err.status === 404) setNotFound(true)
        else setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    loadInitial()
  }, [decodedTicker]) // Load 1Y initially on mount/ticker change

  const handleRangeChange = async (newRange) => {
    if (newRange === range) return
    try {
      setChartLoading(true)
      const res = await fetchStockDetails(decodedTicker, newRange)
      setData(res)
      setRange(newRange)
    } catch (err) {
      console.error('Failed to change range', err)
      // On failure to switch range, just silently fail or show toast. We'll leave it as is.
    } finally {
      setChartLoading(false)
    }
  }

  const goBack = () => {
    if (window.history.length > 1) {
      navigate(-1)
    } else {
      navigate('/stocks')
    }
  }

  if (loading) {
    return (
      <div className="stock-details">
        <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>
        <LoadingState label={`Loading data for ${decodedTicker}...`} />
      </div>
    )
  }

  if (notFound) {
    return (
      <div className="stock-details">
        <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>
        <EmptyState title="Stock not found" description={`No data exists for "${decodedTicker}".`} action={{ label: 'Browse stocks', onClick: () => navigate('/stocks') }} />
      </div>
    )
  }

  if (error) {
    return (
      <div className="stock-details">
        <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>
        <EmptyState title="Failed to load stock" description={error} />
      </div>
    )
  }

  if (!data) return null

  const { company, market, prediction, chartData } = data
  const direction = directionForValue(market.day_change_pct)
  const MovementIcon = direction === 'negative' ? ArrowDownRight : ArrowUpRight

  const isPartial = !prediction

  return (
    <div className="stock-details">
      <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>

      {/* ── Stock Header ── */}
      <header className="stock-header">
        <div className="stock-header__identity">
          <div>
            <span className="eyebrow">{decodedTicker}</span>
            <h1>{company.name} <span className="stock-header__exchange" title={company.sector ? formatSectorName(company.sector) : ''}>{company.sector ? formatSectorName(company.sector) : 'Unknown Sector'}</span></h1>
          </div>
          {isPartial ? <Badge tone="warning">Partial Data</Badge> : <Badge tone="positive">Live</Badge>}
        </div>
        <div className="stock-header__price-row">
          <div>
            <strong className="stock-header__price mono">{market.last_close != null ? formatCurrency(market.last_close) : '—'}</strong>
            {market.day_change != null && (
              <span className={`stock-header__change stock-header__change--${direction}`}>
                <MovementIcon aria-hidden="true" size={15} />
                {market.day_change > 0 ? '+' : ''}{formatCurrency(market.day_change).replace('₹', '₹')} ({formatPercent(market.day_change_pct)}) Today
              </span>
            )}
          </div>
          <Button variant="secondary" className={`watchlist-button ${saved ? 'watchlist-button--saved' : ''}`} onClick={() => toggleWatchlist(decodedTicker)} aria-pressed={saved}>
            <Star size={16} aria-hidden="true" fill={saved ? 'currentColor' : 'none'} />{saved ? 'In Watchlist' : 'Add to Watchlist'}
          </Button>
        </div>
      </header>

      {/* ── Chart + Signal (row 1) ── */}
      <div className="details-grid">
        <section aria-labelledby="chart-heading">
          <div className="section-heading">
            <h2 id="chart-heading">Price chart</h2>
            <div className="chart-mode-toggle" role="radiogroup" aria-label="Chart data source">
              <button
                type="button"
                className={`chart-mode-toggle__btn ${chartMode === 'stockintel' ? 'chart-mode-toggle__btn--active' : ''}`}
                onClick={() => setChartMode('stockintel')}
                role="radio"
                aria-checked={chartMode === 'stockintel'}
              >StockIntel Data</button>
              <button
                type="button"
                className={`chart-mode-toggle__btn ${chartMode === 'tradingview' ? 'chart-mode-toggle__btn--active' : ''}`}
                onClick={() => setChartMode('tradingview')}
                role="radio"
                aria-checked={chartMode === 'tradingview'}
              >TradingView</button>
            </div>
          </div>

          {chartMode === 'stockintel' ? (
            <Suspense fallback={<LoadingState label="Loading price chart" />}>
              <PriceChart chartData={chartData} range={range} onRangeChange={handleRangeChange} loading={chartLoading} direction={direction} />
            </Suspense>
          ) : (
            <TradingViewAdvancedChart ticker={decodedTicker} />
          )}
        </section>

        <section aria-labelledby="signal-heading">
          <h2 id="signal-heading" className="section-label">Model signal</h2>
          {prediction ? (
            <Card className="signal-card">
              <RecommendationBadge signal={prediction.recommendation} />
              <ConfidenceBar value={prediction.confidence} tone={signalTones[prediction.recommendation] ?? 'positive'} />
              <div className="signal-card__metrics">
                <div>
                  <span>Raw Prediction</span>
                  <strong className="mono">{prediction.raw_prediction}</strong>
                </div>
                <div>
                  <span>Confidence Tier</span>
                  <RiskBadge risk={prediction.confidence_tier} />
                </div>
                <div>
                  <span>Model Version</span>
                  <strong className="mono" style={{ fontSize: '0.85em' }}>{prediction.model_version || 'unknown'}</strong>
                </div>
              </div>
            </Card>
          ) : (
            <Card className="signal-card">
              <EmptyState title="No prediction" description="No ML model prediction is available for this stock." />
            </Card>
          )}
        </section>
      </div>
    </div>
  )
}
