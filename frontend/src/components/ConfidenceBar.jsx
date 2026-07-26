import React from 'react';

export const ConfidenceBar = ({ confidence, recommendation }) => {
  // Determine color based on recommendation
  const getColor = (rec) => {
    switch (rec) {
      case 'BUY':
        return 'var(--signal-buy-text)';
      case 'SELL':
        return 'var(--signal-sell-text)';
      case 'HOLD':
        return 'var(--signal-hold-text)';
      default:
        return 'var(--text-muted)';
    }
  };

  const color = getColor(recommendation?.toUpperCase());

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', width: '100%' }}>
      <div 
        style={{ 
          flex: 1, 
          height: '6px', 
          backgroundColor: 'var(--bg-hover)', 
          borderRadius: '3px',
          overflow: 'hidden'
        }}
      >
        <div 
          style={{ 
            height: '100%', 
            width: `${Math.min(Math.max(confidence || 0, 0), 100)}%`, 
            backgroundColor: color,
            transition: 'width 0.5s ease-out'
          }} 
        />
      </div>
      <span className="mono" style={{ fontSize: '0.875rem', color: 'var(--text-secondary)', minWidth: '40px', textAlign: 'right' }}>
        {Number(confidence || 0).toFixed(1)}%
      </span>
    </div>
  );
};
