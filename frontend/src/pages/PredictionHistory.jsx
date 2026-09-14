import { useState, useEffect, useCallback, useMemo } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { PageHeader } from '../components/layout/PageHeader'
import { Card } from '../components/common/Card'
import { DataTable } from '../components/common/DataTable'
import { MobileDataCard } from '../components/common/MobileDataCard'
import { Button } from '../components/common/Button'
import { Select } from '../components/common/Select'
import { Pagination } from '../components/common/Pagination'
import { LoadingState } from '../components/common/LoadingState'
import { ErrorState } from '../components/common/ErrorState'
import { EmptyState } from '../components/common/EmptyState'
import { SignalIndicator } from '../components/common/SignalIndicator'
import { ConfidenceIndicator } from '../components/common/ConfidenceIndicator'
import { PredictionStatus } from '../components/common/PredictionStatus'
import { ChangeDisplay } from '../components/common/ChangeDisplay'
import { StockIdentity } from '../components/common/StockIdentity'
import { fetchAllTickers } from '../api/stocks'
import './prediction-history.css'

const RECOMMENDATION_OPTIONS = [
  { value: 'ALL', label: 'All Signals' },
  { value: 'BUY', label: 'BUY' },
  { value: 'HOLD', label: 'HOLD' },
  { value: 'SELL', label: 'SELL' }
]

const OUTCOME_OPTIONS = [
  { value: 'ALL', label: 'All Outcomes' },
  { value: 'PENDING', label: 'Pending' },
  { value: 'CORRECT', label: 'Correct' },
  { value: 'INCORRECT', label: 'Incorrect' }
]

function truncateHash(hash) {
  if (!hash || hash.length <= 8) return hash || 'unknown'
  return hash.substring(0, 8) + '...'
}

function formatDate(isoString) {
  if (!isoString) return '—'
  try {
    const d = new Date(isoString)
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    }).format(d)
  } catch (e) {
    return '—'
  }
}

export function PredictionHistory() {
  const [searchParams, setSearchParams] = useSearchParams()
  const urlTicker = searchParams.get('ticker') || 'ALL'
  const urlRecommendation = searchParams.get('recommendation') || 'ALL'
  const urlOutcome = searchParams.get('outcome') || 'ALL'
  const urlPage = parseInt(searchParams.get('page'), 10) || 1
  const limit = 50

  const [tickers, setTickers] = useState([])
  const [historyData, setHistoryData] = useState([])
  const [totalRecords, setTotalRecords] = useState(0)
  const [performance, setPerformance] = useState(null)
  
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)
  const [perfError, setPerfError] = useState(false)

  // 1. Fetch Tickers Universe
  useEffect(() => {
    let active = true
    fetchAllTickers()
      .then(res => {
        if (active) setTickers(res)
      })
      .catch(err => {
        console.error('Failed to load tickers:', err)
      })
    return () => { active = false }
  }, [])

  const tickerOptions = useMemo(() => {
    return [
      { value: 'ALL', label: 'All Stocks' },
      ...tickers.map(t => ({ value: t, label: t }))
    ]
  }, [tickers])

  // 2. Fetch History & Performance
  useEffect(() => {
    let active = true
    setLoading(true)
    setError(false)
    setPerfError(false)

    const offset = (urlPage - 1) * limit
    const historyParams = new URLSearchParams()
    historyParams.append('limit', limit)
    historyParams.append('offset', offset)
    
    if (urlTicker !== 'ALL') historyParams.append('symbol', urlTicker)
    if (urlRecommendation !== 'ALL') historyParams.append('recommendation', urlRecommendation)
    if (urlOutcome !== 'ALL') historyParams.append('outcome', urlOutcome)

    const perfParams = new URLSearchParams()
    if (urlTicker !== 'ALL') perfParams.append('ticker', urlTicker)
    // Note: performance endpoint does not support recommendation or outcome filtering,
    // so we strictly adhere to the backend contract and only pass ticker.

    Promise.all([
      fetch(`/api/predictions/history?${historyParams.toString()}`).then(r => {
        if (!r.ok) throw new Error('History API failed')
        return r.json()
      }),
      fetch(`/api/predictions/performance?${perfParams.toString()}`).then(r => {
        if (!r.ok) throw new Error('Performance API failed')
        return r.json()
      }).catch(err => {
        console.error('Performance fetch error:', err)
        if (active) setPerfError(true)
        return null
      })
    ]).then(([histData, perfData]) => {
      if (!active) return
      setHistoryData(histData.data || [])
      setTotalRecords(histData.total || 0)
      if (perfData) setPerformance(perfData)
      setLoading(false)
    }).catch(err => {
      if (!active) return
      console.error('History fetch error:', err)
      setError(true)
      setLoading(false)
    })

    return () => { active = false }
  }, [urlTicker, urlRecommendation, urlOutcome, urlPage])

  const updateFilters = useCallback((updates) => {
    const newParams = new URLSearchParams(searchParams)
    
    Object.entries(updates).forEach(([key, value]) => {
      if (value === 'ALL') {
        newParams.delete(key)
      } else {
        newParams.set(key, value)
      }
    })
    
    // Always reset to page 1 on filter change
    newParams.delete('page')
    setSearchParams(newParams)
  }, [searchParams, setSearchParams])

  const handlePageChange = useCallback((newPage) => {
    const newParams = new URLSearchParams(searchParams)
    if (newPage === 1) {
      newParams.delete('page')
    } else {
      newParams.set('page', newPage.toString())
    }
    setSearchParams(newParams)
  }, [searchParams, setSearchParams])

  const columns = useMemo(() => [
    { key: 'predictionDate', header: 'Prediction Date', render: (_, record) => <div><Link to={`/predictions/${record._id}`} className="ph-detail-link" aria-label={`View details for prediction on ${formatDate(record.prediction_timestamp || record.market_date)}`}>{formatDate(record.prediction_timestamp || record.market_date)}</Link></div> },
    { key: 'stock', header: 'Stock', render: (_, record) => <StockIdentity ticker={record.symbol} linkTo={`/stocks/${record.symbol}`} /> },
    { key: 'signal', header: 'Signal', render: (_, record) => <SignalIndicator signal={record.recommendation} /> },
    { key: 'confidence', header: 'Confidence', render: (_, record) => <ConfidenceIndicator confidence={record.confidence} tier={record.confidence_tier} size="sm" /> },
    { key: 'outcome', header: 'Outcome', render: (_, record) => <PredictionStatus status={record.outcome} /> },
    { key: 'settlementDate', header: 'Settlement Date', render: (_, record) => record.outcome === 'PENDING' ? '—' : formatDate(record.settlement_market_date) },
    { key: 'actualReturn', header: 'Actual Return', align: 'right', render: (_, record) => (record.outcome === 'PENDING' || record.actual_return === null || record.actual_return === undefined) ? '—' : <ChangeDisplay value={record.actual_return * 100} showPercent={true} /> },
    { key: 'model', header: 'Model', render: (_, record) => <span className="ph-model-hash" title={record.model_version}>{truncateHash(record.model_version)}</span> }
  ], [])

  return (
    <div className="prediction-history-workspace fade-in">
      <PageHeader 
        title="Prediction History" 
        description="Immutable record of system predictions and their evaluated outcomes." 
      />

      {/* Performance Summary */}
      {performance && !perfError && (
        <div className="ph-performance-summary">
          <Card className="ph-summary-card">
            <span className="ph-summary-card-title">Predictions</span>
            <span className="ph-summary-card-value">{performance.total_predictions ?? '—'}</span>
          </Card>
          <Card className="ph-summary-card">
            <span className="ph-summary-card-title">Evaluated</span>
            <span className="ph-summary-card-value">{performance.evaluated_predictions ?? '—'}</span>
          </Card>
          <Card className="ph-summary-card">
            <span className="ph-summary-card-title">Pending</span>
            <span className="ph-summary-card-value">{performance.pending_predictions ?? '—'}</span>
          </Card>
          <Card className="ph-summary-card">
            <span className="ph-summary-card-title">Accuracy</span>
            <span className="ph-summary-card-value">
              {performance.recommendation?.classification?.accuracy !== undefined && performance.recommendation?.classification?.accuracy !== null
                ? `${(performance.recommendation.classification.accuracy * 100).toFixed(1)}%` 
                : '—'}
            </span>
          </Card>
        </div>
      )}

      {/* Filters */}
      <div className="ph-filters">
        <div className="ph-filter-group">
          <label htmlFor="ticker-filter">Stock</label>
          <Select 
            id="ticker-filter"
            options={tickerOptions} 
            value={urlTicker} 
            onChange={(e) => updateFilters({ ticker: e.target.value })} 
          />
        </div>
        <div className="ph-filter-group">
          <label htmlFor="signal-filter">Signal</label>
          <Select 
            id="signal-filter"
            options={RECOMMENDATION_OPTIONS} 
            value={urlRecommendation} 
            onChange={(e) => updateFilters({ recommendation: e.target.value })} 
          />
        </div>
        <div className="ph-filter-group">
          <label htmlFor="outcome-filter">Outcome</label>
          <Select 
            id="outcome-filter"
            options={OUTCOME_OPTIONS} 
            value={urlOutcome} 
            onChange={(e) => updateFilters({ outcome: e.target.value })} 
          />
        </div>
      </div>

      {/* Toolbar */}
      {!loading && !error && (
        <div className="ph-toolbar">
          <div className="ph-summary-text">
            Showing <strong>{historyData.length > 0 ? (urlPage - 1) * limit + 1 : 0}</strong> to <strong>{Math.min(urlPage * limit, totalRecords)}</strong> of <strong>{totalRecords}</strong> predictions
          </div>
          <Pagination 
            currentPage={urlPage}
            totalPages={Math.ceil(totalRecords / limit)}
            onPageChange={handlePageChange}
          />
        </div>
      )}

      {/* Content State */}
      {loading ? (
        <LoadingState message="Loading prediction history..." />
      ) : error ? (
        <ErrorState 
          title="Could not load history" 
          message="The prediction history system is currently unavailable."
          onRetry={() => window.location.reload()}
        />
      ) : historyData.length === 0 ? (
        <EmptyState 
          title="No predictions found" 
          message="No historical records match the selected filters." 
        />
      ) : (
        <>
          {/* Desktop Table (hidden on small screens) */}
          <DataTable 
            columns={columns} 
            data={historyData} 
            keyField="_id" 
            className="desktop-only" 
          />

          {/* Mobile List (hidden on large screens) */}
          <div className="ph-mobile-list mobile-only">
            {historyData.map(record => (
              <MobileDataCard 
                key={record._id}
                identity={<StockIdentity ticker={record.symbol} linkTo={`/stocks/${record.symbol}`} />}
                primarySignal={<SignalIndicator signal={record.recommendation} />}
                primaryMetrics={
                  <>
                    <div className="ph-mobile-card-row" style={{ flex: 1 }}>
                      <span className="ph-mobile-card-label">Prediction Date</span>
                      <span className="ph-mobile-card-value">{formatDate(record.prediction_timestamp || record.market_date)}</span>
                    </div>
                    <div className="ph-mobile-card-row" style={{ flex: 1 }}>
                      <span className="ph-mobile-card-label">Confidence</span>
                      <ConfidenceIndicator confidence={record.confidence} tier={record.confidence_tier} size="sm" />
                    </div>
                    <div className="ph-mobile-card-row" style={{ flex: 1, marginTop: 'var(--space-2)' }}>
                      <span className="ph-mobile-card-label">Settlement</span>
                      <span className="ph-mobile-card-value">
                        {record.outcome === 'PENDING' ? '—' : formatDate(record.settlement_market_date)}
                      </span>
                    </div>
                    <div className="ph-mobile-card-row" style={{ flex: 1, marginTop: 'var(--space-2)' }}>
                      <span className="ph-mobile-card-label">Actual Return</span>
                      <span className="ph-mobile-card-value">
                        {record.outcome === 'PENDING' || record.actual_return === null || record.actual_return === undefined
                          ? '—'
                          : <ChangeDisplay value={record.actual_return * 100} showPercent={true} />}
                      </span>
                    </div>
                  </>
                }
                status={<PredictionStatus status={record.outcome} />}
                metadata={<span>Model: <span className="ph-model-hash" title={record.model_version}>{truncateHash(record.model_version)}</span></span>}
                action={<Button as={Link} to={`/predictions/${record._id}`} variant="outline" className="full-width">View Details</Button>}
              />
            ))}
          </div>

          <div className="ph-toolbar" style={{ marginTop: 'var(--space-4)' }}>
            <Pagination 
              currentPage={urlPage}
              totalPages={Math.ceil(totalRecords / limit)}
              onPageChange={handlePageChange}
            />
          </div>
        </>
      )}
    </div>
  )
}
