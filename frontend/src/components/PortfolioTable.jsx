import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { RecommendationBadge } from './RecommendationBadge';
import { ConfidenceBar } from './ConfidenceBar';
import { ArrowDown, ArrowUp, ArrowUpDown, Filter } from 'lucide-react';

export const PortfolioTable = ({ data }) => {
  const navigate = useNavigate();
  const [filter, setFilter] = useState('ALL');
  const [sortConfig, setSortConfig] = useState({ key: 'confidence', direction: 'desc' });

  const handleSort = (key) => {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') {
      direction = 'asc';
    }
    setSortConfig({ key, direction });
  };

  const sortedAndFilteredData = useMemo(() => {
    if (!data) return [];
    
    // Filter
    let result = data;
    if (filter !== 'ALL') {
      result = result.filter(item => item.recommendation === filter);
    }
    
    // Sort
    return [...result].sort((a, b) => {
      const { key, direction } = sortConfig;
      let aValue = a[key];
      let bValue = b[key];
      
      if (key === 'recommendation') {
        const order = { 'BUY': 0, 'HOLD': 1, 'UNCERTAIN': 2, 'SELL': 3 };
        aValue = order[aValue] || 4;
        bValue = order[bValue] || 4;
      }
      
      if (aValue < bValue) {
        return direction === 'asc' ? -1 : 1;
      }
      if (aValue > bValue) {
        return direction === 'asc' ? 1 : -1;
      }
      return 0;
    });
  }, [data, filter, sortConfig]);

  const SortIcon = ({ columnKey }) => {
    if (sortConfig.key !== columnKey) return <ArrowUpDown size={14} style={{ color: 'var(--text-muted)' }} />;
    return sortConfig.direction === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />;
  };

  const getDayChangeColor = (pct) => {
    if (pct > 0) return 'var(--signal-buy-text)';
    if (pct < 0) return 'var(--signal-sell-text)';
    return 'var(--text-secondary)';
  };

  return (
    <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
      <div className="flex items-center gap-4" style={{ padding: '1rem 1.5rem', borderBottom: '1px solid var(--border-color)' }}>
        <Filter size={18} style={{ color: 'var(--text-muted)' }} />
        {['ALL', 'BUY', 'HOLD', 'SELL'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            style={{
              padding: '0.25rem 0.75rem',
              borderRadius: '9999px',
              fontSize: '0.875rem',
              fontWeight: filter === f ? 600 : 400,
              backgroundColor: filter === f ? 'var(--bg-hover)' : 'transparent',
              color: filter === f ? 'var(--text-primary)' : 'var(--text-secondary)',
            }}
          >
            {f}
          </button>
        ))}
      </div>
      
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-color)', backgroundColor: 'var(--bg-page)' }}>
              <th onClick={() => handleSort('ticker')} style={{ padding: '1rem 1.5rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.875rem', color: 'var(--text-secondary)' }}>
                <div className="flex items-center gap-2">Ticker <SortIcon columnKey="ticker" /></div>
              </th>
              <th onClick={() => handleSort('last_close')} style={{ padding: '1rem 1.5rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.875rem', color: 'var(--text-secondary)', textAlign: 'right' }}>
                <div className="flex items-center justify-end gap-2">Price <SortIcon columnKey="last_close" /></div>
              </th>
              <th onClick={() => handleSort('day_change_pct')} style={{ padding: '1rem 1.5rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.875rem', color: 'var(--text-secondary)', textAlign: 'right' }}>
                <div className="flex items-center justify-end gap-2">Change % <SortIcon columnKey="day_change_pct" /></div>
              </th>
              <th onClick={() => handleSort('recommendation')} style={{ padding: '1rem 1.5rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.875rem', color: 'var(--text-secondary)', textAlign: 'center' }}>
                <div className="flex items-center justify-center gap-2">Signal <SortIcon columnKey="recommendation" /></div>
              </th>
              <th onClick={() => handleSort('confidence')} style={{ padding: '1rem 1.5rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.875rem', color: 'var(--text-secondary)' }}>
                <div className="flex items-center gap-2">Confidence <SortIcon columnKey="confidence" /></div>
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedAndFilteredData.map((row) => (
              <tr 
                key={row.ticker} 
                onClick={() => navigate(`/?ticker=${row.ticker}`)}
                style={{ borderBottom: '1px solid var(--border-color)', cursor: 'pointer' }}
                onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--bg-hover)'}
                onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
              >
                <td style={{ padding: '1rem 1.5rem', fontWeight: 600 }}>
                  {row.ticker}
                  {row.threshold_calibration_applied && <sup style={{ marginLeft: '2px', opacity: 0.8 }} title="Threshold-calibrated model">†</sup>}
                </td>
                <td className="mono" style={{ padding: '1rem 1.5rem', textAlign: 'right' }}>{row.last_close.toFixed(2)}</td>
                <td className="mono" style={{ padding: '1rem 1.5rem', textAlign: 'right', color: getDayChangeColor(row.day_change_pct) }}>
                  {row.day_change_pct > 0 ? '+' : ''}{row.day_change_pct.toFixed(2)}%
                </td>
                <td style={{ padding: '1rem 1.5rem', textAlign: 'center' }}>
                  <RecommendationBadge recommendation={row.recommendation} />
                </td>
                <td style={{ padding: '1rem 1.5rem', minWidth: '200px' }}>
                  <ConfidenceBar confidence={row.confidence} recommendation={row.recommendation} />
                </td>
              </tr>
            ))}
            {sortedAndFilteredData.length === 0 && (
              <tr>
                <td colSpan="5" style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                  No stocks match the selected filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        <div style={{ padding: '0.75rem 1.5rem', fontSize: '0.75rem', color: 'var(--text-muted)', borderTop: '1px solid var(--border-color)' }}>
          † Threshold-calibrated model
        </div>
      </div>
    </div>
  );
};
