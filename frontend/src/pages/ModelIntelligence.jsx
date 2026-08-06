import React, { useState, useEffect, useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from 'recharts';
import { Settings, BarChart3, Binary, BrainCircuit } from 'lucide-react';
import './model-intelligence.css';

export function ModelIntelligence() {
  const [models, setModels] = useState([]);
  const [selectedTicker, setSelectedTicker] = useState('');
  const [intelligence, setIntelligence] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    const fetchModels = async () => {
      try {
        const res = await fetch('/api/models');
        if (res.ok) {
          const data = await res.json();
          setModels(data.data || []);
          if (data.data?.length > 0) {
            setSelectedTicker(data.data[0].ticker);
          }
        }
      } catch (err) {
        console.error('Failed to fetch models list:', err);
      }
    };
    fetchModels();
  }, []);

  useEffect(() => {
    const fetchIntelligence = async () => {
      if (!selectedTicker) return;
      setLoading(true);
      setError(false);
      try {
        const res = await fetch(`/api/models/${selectedTicker}/intelligence`);
        if (res.ok) {
          const data = await res.json();
          setIntelligence(data);
        } else {
          setError(true);
        }
      } catch (err) {
        console.error('Failed to fetch intelligence:', err);
        setError(true);
      } finally {
        setLoading(false);
      }
    };
    fetchIntelligence();
  }, [selectedTicker]);

  const handleTickerChange = (e) => {
    setSelectedTicker(e.target.value);
  };

  const distributionData = useMemo(() => {
    if (!intelligence?.distributions?.test_predictions) return null;
    const testDist = intelligence.distributions.test_predictions;
    const trainDist = intelligence.distributions.training_labels || {};
    
    return [
      { name: 'SELL', Test: testDist['SELL'] || 0, Train: trainDist['SELL'] || 0, color: '#ef4444' },
      { name: 'HOLD', Test: testDist['HOLD'] || 0, Train: trainDist['HOLD'] || 0, color: '#a1a1aa' },
      { name: 'BUY', Test: testDist['BUY'] || 0, Train: trainDist['BUY'] || 0, color: '#22c55e' }
    ];
  }, [intelligence]);

  const featureImportance = useMemo(() => {
    if (!intelligence?.feature_importance?.length) return null;
    return intelligence.feature_importance.slice(0, 15); // Show top 15
  }, [intelligence]);

  return (
    <div className="model-intelligence-page fade-in">
      <div className="model-intelligence-header">
        <div>
          <h1>Model Intelligence</h1>
          <p>Transparency and performance metrics for trained ML artifacts.</p>
        </div>
        <div className="ticker-selector-container">
          <label className="filter-control">
            <select className="select" value={selectedTicker} onChange={handleTickerChange}>
              {models.map(m => (
                <option key={m.ticker} value={m.ticker}>{m.ticker}</option>
              ))}
            </select>
          </label>
        </div>
      </div>

      {loading && <div className="loading-state">Loading model intelligence...</div>}
      {error && <div className="error-state">Failed to load model data.</div>}

      {!loading && !error && intelligence && (
        <>
          <div className="model-dashboard-grid">
            {/* Metrics Card */}
            <div className="model-card">
              <h2 className="model-card-title"><BarChart3 size={18} /> Model Metrics</h2>
              
              <div className="metrics-grid">
                <div className="metric-item">
                  <span className="metric-label">Macro F1 Score</span>
                  <span className="metric-value">{(intelligence.metrics.f1_macro * 100).toFixed(2)}%</span>
                </div>
                <div className="metric-item">
                  <span className="metric-label">Mean Max Probability</span>
                  <span className="metric-value">{(intelligence.metrics.mean_max_probability * 100).toFixed(1)}%</span>
                </div>
                <div className="metric-item">
                  <span className="metric-label">Test Size</span>
                  <span className="metric-value">{intelligence.metrics.test_size} rows</span>
                </div>
                <div className="metric-item">
                  <span className="metric-label">Very Low Confidence</span>
                  <span className="metric-value" style={{ color: intelligence.metrics.very_low_confidence ? '#ef4444' : '#22c55e' }}>
                    {intelligence.metrics.very_low_confidence ? 'Yes' : 'No'}
                  </span>
                </div>
              </div>

              <h3 className="metadata-label" style={{ marginTop: '24px', marginBottom: '8px' }}>Per-Class Performance</h3>
              <table className="class-metrics-table">
                <thead>
                  <tr>
                    <th>Class</th>
                    <th>Precision</th>
                    <th>Recall</th>
                    <th>F1 Score</th>
                    <th>Support</th>
                  </tr>
                </thead>
                <tbody>
                  {['BUY', 'HOLD', 'SELL'].map(cls => {
                    const metrics = intelligence.metrics.per_class?.[cls];
                    if (!metrics) return null;
                    return (
                      <tr key={cls}>
                        <td style={{ fontWeight: 500, color: cls === 'BUY' ? '#22c55e' : cls === 'SELL' ? '#ef4444' : '#a1a1aa' }}>{cls}</td>
                        <td>{(metrics.precision * 100).toFixed(1)}%</td>
                        <td>{(metrics.recall * 100).toFixed(1)}%</td>
                        <td>{(metrics.f1 * 100).toFixed(1)}%</td>
                        <td>{metrics.support}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Feature Importance Card */}
            <div className="model-card">
              <h2 className="model-card-title"><Binary size={18} /> Top Feature Importance</h2>
              {featureImportance ? (
                <div className="feature-bars">
                  {featureImportance.map((f) => {
                    const maxImportance = featureImportance[0].importance;
                    const pct = (f.importance / maxImportance) * 100;
                    return (
                      <div className="feature-bar-item" key={f.feature}>
                        <div className="feature-label" title={f.feature}>{f.feature}</div>
                        <div className="feature-bar-track">
                          <div className="feature-bar-fill" style={{ width: `${pct}%` }}></div>
                        </div>
                        <div className="feature-value">{(f.importance * 100).toFixed(1)}%</div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="unavailable-state">
                  Feature importance is unavailable for this training run.
                </div>
              )}
            </div>

            {/* Prediction Distribution Card */}
            <div className="model-card">
              <h2 className="model-card-title"><BrainCircuit size={18} /> Test Prediction Distribution</h2>
              {distributionData ? (
                <div style={{ height: 250, width: '100%', marginTop: '16px' }}>
                  <ResponsiveContainer>
                    <BarChart data={distributionData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
                      <XAxis dataKey="name" stroke="#a1a1aa" fontSize={12} tickLine={false} axisLine={false} />
                      <YAxis stroke="#a1a1aa" fontSize={12} tickLine={false} axisLine={false} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#18181b', borderColor: '#27272a', borderRadius: '8px' }}
                        itemStyle={{ color: '#fff' }}
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
                <div className="unavailable-state">
                  Test prediction distribution is unavailable for this training run.
                </div>
              )}
            </div>

            {/* Training Metadata Card */}
            <div className="model-card">
              <h2 className="model-card-title"><Settings size={18} /> Training Metadata</h2>
              <div className="metadata-grid">
                <div className="metadata-item">
                  <span className="metadata-label">Model Version</span>
                  <span className="metadata-value font-mono">{intelligence.model_metadata?.model_version || 'Unavailable'}</span>
                </div>
                <div className="metadata-item">
                  <span className="metadata-label">Trained At</span>
                  <span className="metadata-value">
                    {intelligence.model_metadata?.trained_at 
                      ? new Date(intelligence.model_metadata.trained_at).toLocaleString() 
                      : 'Unavailable'}
                  </span>
                </div>
                <div className="metadata-item">
                  <span className="metadata-label">Prediction Horizon</span>
                  <span className="metadata-value">{intelligence.model_metadata?.prediction_horizon || '10'} Days</span>
                </div>
                <div className="metadata-item">
                  <span className="metadata-label">Data Range (Train/Test)</span>
                  <span className="metadata-value">{intelligence.training.data_start?.split(' ')[0]} to {intelligence.training.data_end?.split(' ')[0]}</span>
                </div>
                <div className="metadata-item">
                  <span className="metadata-label">Feature Count</span>
                  <span className="metadata-value">{intelligence.model_metadata?.feature_count || 'Unavailable'}</span>
                </div>
                <div className="metadata-item">
                  <span className="metadata-label">Model Type</span>
                  <span className="metadata-value">{intelligence.model_metadata?.model_type || 'XGBClassifier'}</span>
                </div>
              </div>
              
              <div className="optuna-params">
                <h3 className="metadata-label">Optuna Best Parameters</h3>
                <div className="params-grid font-mono">
                  {intelligence.training.optuna?.best_params && Object.entries(intelligence.training.optuna.best_params).map(([k, v]) => (
                    <div key={k} style={{ fontSize: '12px' }}>
                      <span style={{ color: '#a1a1aa' }}>{k}:</span>{' '}
                      <span style={{ color: '#60a5fa' }}>{typeof v === 'number' && !Number.isInteger(v) ? v.toFixed(4) : v}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

          </div>
        </>
      )}
    </div>
  );
}
