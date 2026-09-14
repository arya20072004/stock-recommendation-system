import { lazy, Suspense, useEffect, useState } from 'react'
import { useNavigate, useParams, Link } from 'react-router-dom'
import { ArrowDownRight, ArrowLeft, ArrowUpRight, Star, ExternalLink, Newspaper, History, BrainCircuit } from 'lucide-react'
import { Badge } from '../components/common/Badge'
import { Button } from '../components/common/Button'
import { Card } from '../components/common/Card'
import { EmptyState } from '../components/common/EmptyState'
import { LoadingState } from '../components/common/LoadingState'
import { ErrorState } from '../components/common/ErrorState'
import { UnavailableState } from '../components/common/UnavailableState'
import { ConfidenceBar } from '../components/recommendations/ConfidenceBar'
import { RecommendationBadge } from '../components/recommendations/RecommendationBadge'
import { RiskBadge } from '../components/recommendations/RiskBadge'
import { TradingViewAdvancedChart } from '../components/tradingview/TradingViewAdvancedChart'
import { SentimentIndicator } from '../components/common/SentimentIndicator'
import { DataTable } from '../components/common/DataTable'
import { MobileDataCard } from '../components/common/MobileDataCard'
import { SignalIndicator } from '../components/common/SignalIndicator'
import { ConfidenceIndicator } from '../components/common/ConfidenceIndicator'
import { PredictionStatus } from '../components/common/PredictionStatus'
import { useWatchlist } from '../context/WatchlistContext'
import { fetchStockDetails } from '../api/stocks'
import { fetchNews } from '../api/news'
import { directionForValue, formatCurrency, formatPercent, formatSectorName } from '../utils/formatters'
import '../components/tradingview/tradingview.css'
import './stock-details.css'

const PriceChart = lazy(() => import('../components/charts/PriceChart').then(m => ({ default: m.PriceChart })))

const signalTones = { BUY: 'positive', HOLD: 'warning', SELL: 'negative', UNCERTAIN: 'neutral' }

function formatDate(isoString) {
  if (!isoString) return '—'
  try {
    const d = new Date(isoString)
    return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric' }).format(d)
  } catch (e) {
    return '—'
  }
}

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

  // Contextual Data States
  const [news, setNews] = useState({ data: [], loading: true, error: false })
  const [history, setHistory] = useState({ data: [], loading: true, error: false })
  const [modelIntel, setModelIntel] = useState({ data: null, loading: true, error: false })

  useEffect(() => {
    let active = true

    async function loadInitial() {
      try {
        setLoading(true)
        setError(null)
        setNotFound(false)
        const res = await fetchStockDetails(decodedTicker, '1Y')
        if (active) {
          setData(res)
          setRange('1Y')
        }
      } catch (err) {
        if (active) {
          if (err.status === 404) setNotFound(true)
          else setError(err.message)
        }
      } finally {
        if (active) setLoading(false)
      }
    }

    loadInitial()

    // Fetch Contextual Data Parallelly
    setNews({ data: [], loading: true, error: false })
    fetchNews({ ticker: decodedTicker, limit: 3 })
      .then(res => { if (active) setNews({ data: res.data || [], loading: false, error: false }) })
      .catch(() => { if (active) setNews({ data: [], loading: false, error: true }) })

    setHistory({ data: [], loading: true, error: false })
    fetch(`/api/predictions/history?symbol=${encodeURIComponent(decodedTicker)}&limit=3`)
      .then(r => { if (!r.ok) throw new Error(); return r.json() })
      .then(res => { if (active) setHistory({ data: res.data || [], loading: false, error: false }) })
      .catch(() => { if (active) setHistory({ data: [], loading: false, error: true }) })

    setModelIntel({ data: null, loading: true, error: false })
    fetch(`/api/models/${encodeURIComponent(decodedTicker)}/intelligence`)
      .then(r => { if (!r.ok) throw new Error(); return r.json() })
      .then(res => { if (active) setModelIntel({ data: res, loading: false, error: false }) })
      .catch(() => { if (active) setModelIntel({ data: null, loading: false, error: true }) })

    return () => { active = false }
  }, [decodedTicker])

  const handleRangeChange = async (newRange) => {
    if (newRange === range) return
    try {
      setChartLoading(true)
      const res = await fetchStockDetails(decodedTicker, newRange)
      setData(res)
      setRange(newRange)
    } catch (err) {
      console.error('Failed to change range', err)
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
        <ErrorState title="Failed to load stock" description={error} />
      </div>
    )
  }

  if (!data) return null

  const { company, market, prediction, chartData } = data
  const direction = directionForValue(market.day_change_pct)
  const MovementIcon = direction === 'negative' ? ArrowDownRight : ArrowUpRight

  const isPartial = !prediction

  const historyColumns = [
    { key: 'market_date', header: 'Date', render: (val) => <span className="mono">{val}</span> },
    { key: 'recommendation', header: 'Signal', render: (val) => <SignalIndicator signal={val} /> },
    { key: 'confidence', header: 'Confidence', render: (val) => <ConfidenceIndicator confidence={val} /> },
    { key: 'outcome', header: 'Outcome', render: (val) => <PredictionStatus status={val} /> },
    { key: 'actual_return', header: 'Return', align: 'right', render: (val) => val != null ? <span className="mono" style={{ color: val > 0 ? 'var(--positive)' : val < 0 ? 'var(--negative)' : 'var(--text-secondary)' }}>{val > 0 ? '+' : ''}{(val * 100).toFixed(2)}%</span> : '—' }
  ]

  return (
    <div className="stock-details fade-in">
      <button className="back-link" onClick={goBack}><ArrowLeft size={16} aria-hidden="true" />Back</button>

      {/* ── Stock Header ── */}
      <header className="stock-header">
        <div className="stock-header__identity">
          <div>
            <span className="eyebrow">{decodedTicker}</span>
            <h1>{company.name} <span className="stock-header__exchange" title={company.sector ? formatSectorName(company.sector) : ''}>{company.sector ? formatSectorName(company.sector) : 'Unknown Sector'}</span></h1>
          </div>
          {isPartial && <Badge tone="warning">Partial Data</Badge>}
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

      {/* ── Analytical Region ── */}
      <div className="details-layout">
        <div className="details-main">
          {/* ── Model Signal (Mobile only order priority) ── */}
          <section aria-labelledby="signal-heading-mobile" className="mobile-only-signal">
            <h2 id="signal-heading-mobile" className="section-label">Model signal</h2>
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

          {/* ── Chart ── */}
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
        </div>

        <div className="details-sidebar">
          {/* ── Model Signal (Desktop) ── */}
          <section aria-labelledby="signal-heading" className="desktop-only">
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

          {/* ── Model Context / Why This Signal ── */}
          <section aria-labelledby="model-context-heading">
            <div className="section-heading">
              <h2 id="model-context-heading" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <BrainCircuit size={18} /> Why this signal
              </h2>
            </div>
            
            <Card className="model-context-card">
              {modelIntel.loading ? (
                <LoadingState label="Loading model context..." />
              ) : modelIntel.error ? (
                <UnavailableState title="Context Unavailable" description="Failed to load model intelligence data." />
              ) : modelIntel.data ? (
                <div className="model-context">
                  <p className="model-context-disclaimer">
                    The model identifies the following features as most influential for this prediction behavior. This does not indicate causality.
                  </p>
                  
                  {modelIntel.data.feature_importance && modelIntel.data.feature_importance.length > 0 ? (
                    <div className="feature-bars">
                      {modelIntel.data.feature_importance.slice(0, 5).map(f => {
                        const maxImp = modelIntel.data.feature_importance[0].importance
                        const pct = (f.importance / maxImp) * 100
                        return (
                          <div className="feature-bar-item" key={f.feature}>
                            <div className="feature-label" title={f.feature}>{f.feature}</div>
                            <div className="feature-bar-track">
                              <div className="feature-bar-fill" style={{ width: `${pct}%` }}></div>
                            </div>
                            <div className="feature-value">{(f.importance * 100).toFixed(1)}%</div>
                          </div>
                        )
                      })}
                    </div>
                  ) : (
                    <EmptyState title="No feature context" description="Feature importance is not available for this model." />
                  )}

                  <Link to={`/model?ticker=${decodedTicker}`} className="model-context-link">
                    View full model intelligence <ArrowUpRight size={14} />
                  </Link>
                </div>
              ) : (
                <UnavailableState title="Context Unavailable" description="Model intelligence is unavailable for this stock." />
              )}
            </Card>
          </section>
        </div>
      </div>
      
      {/* ── Prediction History (Full Width) ── */}
      <section aria-labelledby="history-heading">
        <div className="section-heading">
          <h2 id="history-heading" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <History size={18} /> Recent Predictions
          </h2>
          <Link to={`/predictions/history?ticker=${decodedTicker}`} className="view-all-link">View all</Link>
        </div>
        
        <Card className="history-card">
          {history.loading ? (
            <LoadingState label="Loading prediction history..." />
          ) : history.error ? (
            <ErrorState title="Unavailable" message="Failed to load historical predictions." />
          ) : history.data.length > 0 ? (
            <>
              <DataTable
                columns={historyColumns}
                data={history.data}
                keyField="market_date"
                className="desktop-only"
              />
              <div className="mobile-history-list mobile-only">
                {history.data.map(record => (
                  <MobileDataCard
                    key={record.market_date}
                    identity={record.market_date}
                    primaryMetrics={
                      <>
                        <div className="mi-mobile-card-row">
                          <span className="mi-mobile-card-label">Signal</span>
                          <span className="mi-mobile-card-value"><SignalIndicator signal={record.recommendation} /></span>
                        </div>
                        <div className="mi-mobile-card-row">
                          <span className="mi-mobile-card-label">Confidence</span>
                          <span className="mi-mobile-card-value"><ConfidenceIndicator confidence={record.confidence} /></span>
                        </div>
                        <div className="mi-mobile-card-row">
                          <span className="mi-mobile-card-label">Outcome</span>
                          <span className="mi-mobile-card-value"><PredictionStatus status={record.outcome} /></span>
                        </div>
                        <div className="mi-mobile-card-row">
                          <span className="mi-mobile-card-label">Return</span>
                          <span className="mi-mobile-card-value mono" style={{ color: record.actual_return > 0 ? 'var(--positive)' : record.actual_return < 0 ? 'var(--negative)' : 'var(--text-secondary)' }}>
                            {record.actual_return != null ? `${record.actual_return > 0 ? '+' : ''}${(record.actual_return * 100).toFixed(2)}%` : '—'}
                          </span>
                        </div>
                      </>
                    }
                  />
                ))}
              </div>
            </>
          ) : (
            <EmptyState title="No history" description="No previous predictions exist for this stock." />
          )}
        </Card>
      </section>

      {/* ── News (Full Width) ── */}
      <section aria-labelledby="news-heading">
        <div className="section-heading">
          <h2 id="news-heading" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <Newspaper size={18} /> Related News
          </h2>
          <Link to={`/news?ticker=${decodedTicker}`} className="view-all-link">View all</Link>
        </div>
        
        {news.loading ? (
          <Card><LoadingState label="Loading related news..." /></Card>
        ) : news.error ? (
          <Card><ErrorState title="Unavailable" message="Failed to load related news." /></Card>
        ) : news.data.length > 0 ? (
          <div className="stock-news-list">
            {news.data.map(item => (
              <Card key={item.id} className="stock-news-item">
                <div className="stock-news-item__heading">
                  <SentimentIndicator sentiment={item.sentiment} />
                  <time dateTime={item.published_at}>{formatDate(item.published_at)}</time>
                </div>
                <Link to={`/news/${item.id}`}><h3>{item.headline}</h3></Link>
                <div className="stock-news-item__source">Source: {item.source}</div>
              </Card>
            ))}
          </div>
        ) : (
          <Card><EmptyState title="No recent news" description="No related news items found." /></Card>
        )}
      </section>

    </div>
  )
}
