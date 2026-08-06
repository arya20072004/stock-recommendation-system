import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { SearchInput } from '../components/common/SearchInput';
import '../components/news/news.css';
import './prediction-history.css';

export function PredictionHistory() {
  const navigate = useNavigate();
  const [historyData, setHistoryData] = useState([]);
  const [performance, setPerformance] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const [filters, setFilters] = useState({
    symbol: '',
    recommendation: '',
    outcome: '',
    model_version: ''
  });

  const [debouncedSymbol, setDebouncedSymbol] = useState(filters.symbol);
  const [offset, setOffset] = useState(0);
  const [totalRecords, setTotalRecords] = useState(0);
  const limit = 50;

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSymbol(filters.symbol);
    }, 400);
    return () => clearTimeout(handler);
  }, [filters.symbol]);

  // When filters change, reset offset to 0
  useEffect(() => {
    setOffset(0);
  }, [debouncedSymbol, filters.recommendation, filters.outcome, filters.model_version]);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(false);
      try {
        const queryParams = new URLSearchParams();
        if (debouncedSymbol) queryParams.append('symbol', debouncedSymbol.toUpperCase());
        if (filters.recommendation) queryParams.append('recommendation', filters.recommendation);
        if (filters.outcome) queryParams.append('outcome', filters.outcome);
        if (filters.model_version) queryParams.append('model_version', filters.model_version);
        queryParams.append('limit', limit);
        queryParams.append('offset', offset);

        const [histRes, perfRes] = await Promise.all([
          fetch(`/api/predictions/history?${queryParams.toString()}`),
          fetch('/api/predictions/performance')
        ]);

        if (histRes.ok && perfRes.ok) {
          const histData = await histRes.json();
          const perfData = await perfRes.json();
          
          setHistoryData(histData.data || []);
          setTotalRecords(histData.total || 0);
          setPerformance(perfData);
        } else {
          throw new Error('API failed');
        }
      } catch (err) {
        console.error("Failed to fetch prediction history:", err);
        setError(true);
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [debouncedSymbol, filters.recommendation, filters.outcome, filters.model_version, offset]);

  const handleFilterChange = (e) => {
    const { name, value } = e.target;
    setFilters(prev => ({ ...prev, [name]: value }));
  };

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

  return (
    <div className="prediction-history-page fade-in">
      <div className="page-header">
        <h1>Prediction History</h1>
        <p>Track historical model predictions and evaluate how they performed.</p>
      </div>

      {performance && (
        <div className="performance-summary">
          <div className="summary-card">
            <span className="summary-card-title">Total Predictions</span>
            <span className="summary-card-value">{performance.total_predictions}</span>
          </div>
          <div className="summary-card">
            <span className="summary-card-title">Model Accuracy</span>
            <span className="summary-card-value">
              {performance.evaluated_predictions > 0 
                ? `${(performance.accuracy * 100).toFixed(1)}%` 
                : 'Not enough data'}
            </span>
          </div>
          <div className="summary-card">
            <span className="summary-card-title">Avg Confidence</span>
            <span className="summary-card-value">{performance.average_confidence}%</span>
          </div>
          <div className="summary-card">
            <span className="summary-card-title">Pending</span>
            <span className="summary-card-value">{performance.pending_predictions}</span>
          </div>
        </div>
      )}

      <div className="news-toolbar" aria-label="Prediction filters" style={{marginBottom: '24px'}}>
        <SearchInput 
          name="symbol" 
          label="Search stock"
          placeholder="Search stock..." 
          value={filters.symbol}
          onChange={handleFilterChange}
        />
        <div className="news-toolbar__controls">
          <label className="filter-control">
            <select name="recommendation" aria-label="Recommendation" className="select" value={filters.recommendation} onChange={handleFilterChange}>
              <option value="">All Recommendations</option>
              <option value="BUY">BUY</option>
              <option value="HOLD">HOLD</option>
              <option value="SELL">SELL</option>
            </select>
          </label>
          <label className="filter-control">
            <select name="outcome" aria-label="Outcome" className="select" value={filters.outcome} onChange={handleFilterChange}>
              <option value="">All Outcomes</option>
              <option value="CORRECT">Correct</option>
              <option value="INCORRECT">Incorrect</option>
              <option value="PENDING">Pending</option>
            </select>
          </label>
        </div>
      </div>

      <div className="history-table-container">
        <table className="history-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Stock</th>
              <th>Price</th>
              <th>Prediction</th>
              <th>Confidence</th>
              <th>Actual Return</th>
              <th>Outcome</th>
              <th>Model</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan="8" style={{textAlign: 'center', padding: '24px'}}>Loading predictions...</td></tr>
            ) : error ? (
              <tr><td colSpan="8" style={{textAlign: 'center', padding: '24px', color: 'var(--negative, #ef4444)'}}>Unable to load prediction history.</td></tr>
            ) : historyData.length === 0 ? (
              <tr><td colSpan="8" style={{textAlign: 'center', padding: '24px', color: 'var(--text-muted, #a1a1aa)'}}>No prediction history available.</td></tr>
            ) : (
              historyData.map((row) => (
                <tr key={row._id} onClick={() => navigate(`/predictions/${row._id}`)}>
                  <td>
                    <div>{new Date(row.prediction_timestamp || row.market_date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })}</div>
                    <div className="text-muted" style={{fontSize: '12px'}}>
                      {new Date(row.prediction_timestamp || row.market_date).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}
                    </div>
                  </td>
                  <td style={{fontWeight: 500}}>{row.symbol}</td>
                  <td>₹{row.price_at_prediction?.toFixed(2)}</td>
                  <td>
                    <span className={getBadgeClass(row.recommendation)}>{row.recommendation}</span>
                  </td>
                  <td>{row.confidence}%</td>
                  <td className={getReturnClass(row.actual_return)}>
                    {formatReturn(row.actual_return)}
                  </td>
                  <td>
                    <span className={getBadgeClass(row.outcome)}>{row.outcome}</span>
                  </td>
                  <td className="text-muted">{row.model_version}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {!loading && !error && totalRecords > 0 && (
        <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '16px'}}>
          <div className="text-muted" style={{fontSize: '14px'}}>
            Showing {offset + 1} to {Math.min(offset + limit, totalRecords)} of {totalRecords} predictions
          </div>
          <div style={{display: 'flex', gap: '8px'}}>
            <button 
              className="select" 
              style={{padding: '6px 12px', cursor: offset === 0 ? 'not-allowed' : 'pointer', opacity: offset === 0 ? 0.5 : 1}}
              disabled={offset === 0} 
              onClick={() => setOffset(Math.max(0, offset - limit))}
            >
              Previous
            </button>
            <button 
              className="select" 
              style={{padding: '6px 12px', cursor: offset + limit >= totalRecords ? 'not-allowed' : 'pointer', opacity: offset + limit >= totalRecords ? 0.5 : 1}}
              disabled={offset + limit >= totalRecords} 
              onClick={() => setOffset(offset + limit)}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
