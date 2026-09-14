import { useState, useEffect, useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from 'recharts'
import { Settings, BarChart3, Binary, BrainCircuit } from 'lucide-react'
import { PageHeader } from '../components/layout/PageHeader'
import { Card } from '../components/common/Card'
import { Select } from '../components/common/Select'
import { LoadingState } from '../components/common/LoadingState'
import { ErrorState } from '../components/common/ErrorState'
import { EmptyState } from '../components/common/EmptyState'
import { UnavailableState } from '../components/common/UnavailableState'
import { DataTable } from '../components/common/DataTable'
import { MobileDataCard } from '../components/common/MobileDataCard'
import './model-intelligence.css'

export function ModelIntelligence() {
  const [models, setModels] = useState([])
  const [selectedTicker, setSelectedTicker] = useState('')
  const [intelligence, setIntelligence] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)

  // Fetch models list for selector
  useEffect(() => {
    let active = true
    const fetchModels = async () => {
      try {
        const res = await fetch('/api/models')
        if (res.ok && active) {
          const data = await res.json()
          setModels(data.data || [])
          if (data.data?.length > 0) {
            setSelectedTicker(data.data[0].ticker)
          }
        }
      } catch (err) {
        console.error('Failed to fetch models list:', err)
      }
    }
    fetchModels()
    return () => { active = false }
  }, [])

  // Fetch intelligence data
  useEffect(() => {
    let active = true
    const fetchIntelligence = async () => {
      if (!selectedTicker) return
      setLoading(true)
      setError(false)
      try {
        const res = await fetch(`/api/models/${selectedTicker}/intelligence`)
        if (active) {
          if (res.ok) {
            const data = await res.json()
            setIntelligence(data)
          } else {
            setError(true)
          }
        }
      } catch (err) {
        console.error('Failed to fetch intelligence:', err)
        if (active) setError(true)
      } finally {
        if (active) setLoading(false)
      }
    }
    fetchIntelligence()
    return () => { active = false }
  }, [selectedTicker])

  const tickerOptions = useMemo(() => {
    return models.map(m => ({ value: m.ticker, label: m.ticker }))
  }, [models])

  const distributionData = useMemo(() => {
    if (!intelligence?.distributions?.test_predictions) return null
    const testDist = intelligence.distributions.test_predictions
    
    // We map to CSS variable strings. Recharts fill attribute supports CSS variables in modern browsers.
    return [
      { name: 'SELL', Test: testDist['SELL'] || 0, color: 'var(--color-signal-sell)' },
      { name: 'HOLD', Test: testDist['HOLD'] || 0, color: 'var(--color-signal-hold)' },
      { name: 'BUY', Test: testDist['BUY'] || 0, color: 'var(--color-signal-buy)' }
    ]
  }, [intelligence])

  const featureImportance = useMemo(() => {
    if (!intelligence?.feature_importance?.length) return null
    return intelligence.feature_importance.slice(0, 15) // Show top 15
  }, [intelligence])

  const perClassColumns = useMemo(() => [
    { key: 'class', header: 'Class', render: (val) => <span style={{ 
      color: val === 'BUY' ? 'var(--color-signal-buy)' : val === 'SELL' ? 'var(--color-signal-sell)' : 'var(--text-secondary)',
      fontWeight: 'var(--weight-medium)'
    }}>{val}</span> },
    { key: 'precision', header: 'Precision', align: 'right', render: (val) => `${(val * 100).toFixed(1)}%` },
    { key: 'recall', header: 'Recall', align: 'right', render: (val) => `${(val * 100).toFixed(1)}%` },
    { key: 'f1', header: 'F1 Score', align: 'right', render: (val) => `${(val * 100).toFixed(1)}%` },
    { key: 'support', header: 'Support', align: 'right' }
  ], [])

  const perClassData = useMemo(() => {
    if (!intelligence?.metrics?.per_class) return []
    return ['BUY', 'HOLD', 'SELL'].map(cls => {
      const metrics = intelligence.metrics.per_class[cls]
      if (!metrics) return null
      return {
        class: cls,
        precision: metrics.precision,
        recall: metrics.recall,
        f1: metrics.f1,
        support: metrics.support
      }
    }).filter(Boolean)
  }, [intelligence])

  return (
    <div className="model-intelligence-page fade-in">
      <PageHeader 
        title="Model Intelligence" 
        description="Transparency and performance metrics for trained ML artifacts." 
      />

      <div className="model-intelligence-header">
        <div className="ticker-selector-container">
          <label htmlFor="ticker-selector" className="metric-label" style={{ marginBottom: 0 }}>Model:</label>
          <Select 
            id="ticker-selector"
            options={tickerOptions} 
            value={selectedTicker} 
            onChange={(e) => setSelectedTicker(e.target.value)} 
          />
        </div>
      </div>

      {loading ? (
        <LoadingState message="Loading model intelligence..." />
      ) : error ? (
        <ErrorState 
          title="Model Unavailable" 
          message="Failed to load intelligence metrics for this ticker."
          onRetry={() => window.location.reload()}
        />
      ) : !intelligence ? (
        <EmptyState 
          title="No Model Selected" 
          message="Please select an available model from the list." 
        />
      ) : (
        <div className="model-dashboard-grid">
          {/* Metrics Card */}
          <Card className="model-card">
            <h2 className="model-card-title"><BarChart3 size={18} /> Model Overview</h2>
            
            <div className="metrics-grid">
              <div className="metric-item">
                <span className="metric-label">Macro F1 Score</span>
                <span className="metric-value">
                  {intelligence.metrics.f1_macro !== undefined && intelligence.metrics.f1_macro !== null ? `${(intelligence.metrics.f1_macro * 100).toFixed(2)}%` : '—'}
                </span>
              </div>
              <div className="metric-item">
                <span className="metric-label" title="Observed maximum predicted probability">Mean Max Probability</span>
                <span className="metric-value">
                  {intelligence.metrics.mean_max_probability !== undefined && intelligence.metrics.mean_max_probability !== null ? `${(intelligence.metrics.mean_max_probability * 100).toFixed(1)}%` : '—'}
                </span>
              </div>
              <div className="metric-item">
                <span className="metric-label">Evaluation Sample Size</span>
                <span className="metric-value">
                  {intelligence.metrics.test_size !== undefined && intelligence.metrics.test_size !== null ? `${intelligence.metrics.test_size} rows` : '—'}
                </span>
              </div>
              <div className="metric-item">
                <span className="metric-label">Model Version</span>
                <span className="metric-value font-mono">
                  {intelligence.model_metadata?.model_version ? `${intelligence.model_metadata.model_version.substring(0,8)}...` : '—'}
                </span>
              </div>
            </div>

            <h3 className="metadata-label" style={{ marginTop: 'var(--space-6)', marginBottom: 'var(--space-2)' }}>Per-Class Performance</h3>
            {perClassData.length > 0 ? (
              <>
                <DataTable 
                  columns={perClassColumns} 
                  data={perClassData} 
                  keyField="class"
                  className="desktop-only" 
                />
                <div className="mi-mobile-list mobile-only">
                  {perClassData.map(record => (
                    <MobileDataCard 
                      key={record.class}
                      identity={record.class}
                      primaryMetrics={
                        <>
                          <div className="mi-mobile-card-row">
                            <span className="mi-mobile-card-label">Precision</span>
                            <span className="mi-mobile-card-value">{(record.precision * 100).toFixed(1)}%</span>
                          </div>
                          <div className="mi-mobile-card-row">
                            <span className="mi-mobile-card-label">Recall</span>
                            <span className="mi-mobile-card-value">{(record.recall * 100).toFixed(1)}%</span>
                          </div>
                          <div className="mi-mobile-card-row">
                            <span className="mi-mobile-card-label">F1 Score</span>
                            <span className="mi-mobile-card-value">{(record.f1 * 100).toFixed(1)}%</span>
                          </div>
                          <div className="mi-mobile-card-row">
                            <span className="mi-mobile-card-label">Support</span>
                            <span className="mi-mobile-card-value">{record.support}</span>
                          </div>
                        </>
                      }
                    />
                  ))}
                </div>
              </>
            ) : (
              <UnavailableState title="Metrics Unavailable" description="Per-class performance metrics are missing." />
            )}
          </Card>

          {/* Feature Importance Card */}
          <Card className="model-card">
            <h2 className="model-card-title"><Binary size={18} /> Feature Importance</h2>
            {featureImportance ? (
              <div className="feature-bars">
                {featureImportance.map((f) => {
                  const maxImportance = featureImportance[0].importance
                  const pct = (f.importance / maxImportance) * 100
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
              <UnavailableState title="Features Unavailable" description="Feature importance is unavailable for this artifact." />
            )}
          </Card>

          {/* Prediction Distribution Card */}
          <Card className="model-card">
            <h2 className="model-card-title"><BrainCircuit size={18} /> Test Prediction Distribution</h2>
            {distributionData ? (
              <div style={{ height: 250, width: '100%', marginTop: 'var(--space-4)' }}>
                <ResponsiveContainer>
                  <BarChart data={distributionData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border-default)" vertical={false} />
                    <XAxis dataKey="name" stroke="var(--text-secondary)" fontSize={12} tickLine={false} axisLine={false} />
                    <YAxis stroke="var(--text-secondary)" fontSize={12} tickLine={false} axisLine={false} />
                    <Tooltip 
                      contentStyle={{ backgroundColor: 'var(--surface-secondary)', borderColor: 'var(--border-default)', borderRadius: 'var(--radius-md)' }}
                      itemStyle={{ color: 'var(--text-primary)' }}
                      formatter={(value, name) => [value, `Prediction Count`]}
                    />
                    <Bar dataKey="Test" radius={[4, 4, 0, 0]}>
                      {distributionData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <UnavailableState title="Distribution Unavailable" description="Test prediction distribution is unavailable." />
            )}
          </Card>

          {/* Training Metadata Card */}
          <Card className="model-card">
            <h2 className="model-card-title"><Settings size={18} /> Training Metadata</h2>
            <div className="metadata-grid">
              <div className="metadata-item">
                <span className="metadata-label">Model Type</span>
                <span className="metadata-value">{intelligence.model_metadata?.model_type || 'XGBClassifier'}</span>
              </div>
              <div className="metadata-item">
                <span className="metadata-label">Trained At</span>
                <span className="metadata-value">
                  {intelligence.model_metadata?.trained_at 
                    ? new Date(intelligence.model_metadata.trained_at).toLocaleString() 
                    : '—'}
                </span>
              </div>
              <div className="metadata-item">
                <span className="metadata-label">Prediction Horizon</span>
                <span className="metadata-value">{intelligence.model_metadata?.prediction_horizon ? `${intelligence.model_metadata.prediction_horizon} Days` : '—'}</span>
              </div>
              <div className="metadata-item">
                <span className="metadata-label">Data Range (Train/Test)</span>
                <span className="metadata-value">
                  {intelligence.training?.data_start?.split(' ')[0] || '—'} to {intelligence.training?.data_end?.split(' ')[0] || '—'}
                </span>
              </div>
              <div className="metadata-item">
                <span className="metadata-label">Feature Count</span>
                <span className="metadata-value">{intelligence.model_metadata?.feature_count || '—'}</span>
              </div>
              <div className="metadata-item">
                <span className="metadata-label">Full Provenance</span>
                <span className="metadata-value font-mono" title={intelligence.model_metadata?.model_version || ''}>
                  {intelligence.model_metadata?.model_version ? `${intelligence.model_metadata.model_version.substring(0,8)}...` : '—'}
                </span>
              </div>
            </div>
            
            {intelligence.training?.optuna?.best_params && (
              <div className="optuna-params">
                <h3 className="metadata-label">Optuna Best Parameters</h3>
                <div className="params-grid">
                  {Object.entries(intelligence.training.optuna.best_params).map(([k, v]) => (
                    <div key={k} className="param-item">
                      <span className="param-key">{k}:</span>{' '}
                      <span className="param-value">{typeof v === 'number' && !Number.isInteger(v) ? v.toFixed(4) : v}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </Card>

        </div>
      )}
    </div>
  )
}
