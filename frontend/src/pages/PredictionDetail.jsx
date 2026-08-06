import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import './prediction-history.css';

export function PredictionDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [showAllFeatures, setShowAllFeatures] = useState(false);

  useEffect(() => {
    const fetchDetail = async () => {
      setLoading(true);
      setError(false);
      try {
        const res = await fetch(`/api/predictions/history/${id}`);
        if (res.ok) {
          const json = await res.json();
          setData(json);
        } else {
          throw new Error('API failed');
        }
      } catch (err) {
        console.error("Failed to fetch prediction detail:", err);
        setError(true);
      } finally {
        setLoading(false);
      }
    };
    
    fetchDetail();
  }, [id]);

  if (loading) {
    return <div className="prediction-history-page" style={{textAlign: 'center', padding: '48px'}}>Loading...</div>;
  }

  if (error) {
    return (
      <div className="prediction-history-page">
        <div className="back-link" onClick={() => navigate('/predictions/history')}>
          <ArrowLeft size={16} /> Back
        </div>
        <div style={{textAlign: 'center', padding: '48px', color: 'var(--negative, #ef4444)'}}>
          Unable to load prediction detail.
        </div>
      </div>
    );
  }

  if (!data) {
    return <div className="prediction-history-page" style={{textAlign: 'center', padding: '48px', color: 'var(--text-muted, #a1a1aa)'}}>Prediction not found.</div>;
  }

  const getBadgeClass = (value) => {
    if (!value) return '';
    return `badge ${value.toLowerCase()}`;
  };

  const formatReturn = (val) => {
    if (val === null || val === undefined) return '—';
    const num = (val * 100).toFixed(2);
    return num > 0 ? `+${num}%` : `${num}%`;
  };

  const getReturnClass = (val) => {
    if (val === null || val === undefined) return 'text-muted';
    return val > 0 ? 'text-green' : (val < 0 ? 'text-red' : 'text-muted');
  };

  const formatCurrency = (val) => {
    if (val === null || val === undefined) return '—';
    return `₹${val.toFixed(2)}`;
  };

  // Convert feature snapshot object to array for easier rendering
  const featureEntries = data.feature_snapshot 
    ? Object.entries(data.feature_snapshot) 
    : [];
  
  const displayedFeatures = showAllFeatures ? featureEntries : featureEntries.slice(0, 8);

  const displayDate = data.prediction_timestamp || data.market_date;

  return (
    <div className="prediction-history-page fade-in">
      <div className="back-link" onClick={() => navigate('/predictions/history')}>
        <ArrowLeft size={16} />
        Back to Prediction History
      </div>

      <div className="detail-header">
        <div>
          <h1 className="detail-title">{data.symbol}</h1>
          <div className="detail-meta">
            Generated: {new Date(displayDate).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })} · {new Date(displayDate).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
            <br />
            Model: StockIntel Classifier (v{data.model_version})
          </div>
        </div>
        <div className="detail-prediction-info">
          <div className={`${getBadgeClass(data.recommendation)} detail-prediction-badge`}>
            {data.recommendation}
          </div>
          <div className="detail-confidence">
            {data.confidence}% Confidence
          </div>
        </div>
      </div>

      <div className="detail-section">
        <h2>PRICE OUTCOME</h2>
        <div className="price-outcome-grid">
          <div className="price-item">
            <span className="price-label">Price at prediction</span>
            <span className="price-value">{formatCurrency(data.price_at_prediction)}</span>
          </div>
          
          <div className="price-item">
            <span className="price-label">Threshold (Return)</span>
            <span className="price-value">{data.threshold_pct ? formatReturn(data.threshold_pct) : '—'}</span>
          </div>

          <div className="price-item">
            <span className="price-label">Actual price</span>
            <span className="price-value">{formatCurrency(data.actual_price)}</span>
          </div>

          <div className="price-item">
            <span className="price-label">Actual return</span>
            <span className={`price-value ${getReturnClass(data.actual_return)}`}>
              {formatReturn(data.actual_return)}
            </span>
          </div>

          <div className="price-item">
            <span className="price-label">Outcome</span>
            <span className={getBadgeClass(data.outcome)}>
              {data.outcome}
            </span>
          </div>
        </div>

        {data.outcome === 'PENDING' && (
          <div className="pending-message">
            Waiting for the {data.prediction_horizon}-day prediction horizon to complete before evaluation.
          </div>
        )}
      </div>

      {featureEntries.length > 0 && (
        <div className="detail-section">
          <h2>FEATURE SNAPSHOT</h2>
          <div className="features-grid">
            {displayedFeatures.map(([key, value]) => (
              <div key={key} className="feature-item">
                <span className="feature-name">{key}</span>
                <span className="feature-value">{typeof value === 'number' ? Number.isInteger(value) ? value : value.toFixed(4) : value}</span>
              </div>
            ))}
          </div>
          {featureEntries.length > 8 && (
            <div style={{marginTop: '16px', textAlign: 'center'}}>
              <button 
                onClick={() => setShowAllFeatures(!showAllFeatures)}
                style={{
                  background: 'none', 
                  border: '1px solid var(--border-color)', 
                  color: 'var(--text-secondary)',
                  padding: '8px 16px',
                  borderRadius: '4px',
                  cursor: 'pointer'
                }}
              >
                {showAllFeatures ? 'Show Less' : `View all features (${featureEntries.length})`}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
