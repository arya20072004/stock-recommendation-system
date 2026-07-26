import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Search } from 'lucide-react';

const fetchPortfolio = async () => {
  const response = await fetch('/api/portfolio');
  if (!response.ok) throw new Error('Failed to fetch portfolio');
  const data = await response.json();
  return data.portfolio;
};

export const TickerRail = ({ selectedTicker, onSelectTicker }) => {
  const [search, setSearch] = useState('');
  
  const { data: stocks, isLoading, error } = useQuery({
    queryKey: ['portfolio'],
    queryFn: fetchPortfolio,
  });

  const filteredStocks = stocks?.filter(s => 
    s.ticker.toLowerCase().includes(search.toLowerCase())
  ) || [];

  return (
    <div className="terminal-pane ticker-rail" style={{ width: '250px', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
      <div style={{ padding: '1rem', borderBottom: '1px solid var(--border-color)' }}>
        <div style={{ position: 'relative' }}>
          <Search size={16} style={{ position: 'absolute', left: '0.75rem', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
          <input 
            type="text" 
            placeholder="Search tickers..." 
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ 
              width: '100%', 
              padding: '0.5rem 0.5rem 0.5rem 2.25rem',
              backgroundColor: 'var(--bg-hover)',
              border: '1px solid var(--border-color)',
              borderRadius: '0.375rem',
              color: 'var(--text-primary)',
              fontSize: '0.875rem',
              outline: 'none'
            }}
            onFocus={(e) => e.target.style.borderColor = 'var(--accent-blue)'}
            onBlur={(e) => e.target.style.borderColor = 'var(--border-color)'}
          />
        </div>
      </div>
      
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {isLoading ? (
          <div style={{ padding: '1rem', color: 'var(--text-muted)', fontSize: '0.875rem' }}>Loading tickers...</div>
        ) : error ? (
          <div style={{ padding: '1rem', color: 'var(--signal-sell-text)', fontSize: '0.875rem' }}>Failed to load</div>
        ) : (
          filteredStocks.map(stock => (
            <div 
              key={stock.ticker}
              onClick={() => onSelectTicker(stock.ticker)}
              style={{
                padding: '0.75rem 1rem',
                borderBottom: '1px solid var(--border-color)',
                cursor: 'pointer',
                backgroundColor: selectedTicker === stock.ticker ? 'var(--bg-hover)' : 'transparent',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                borderLeft: selectedTicker === stock.ticker ? '3px solid var(--accent-blue)' : '3px solid transparent'
              }}
              onMouseEnter={(e) => {
                if (selectedTicker !== stock.ticker) e.currentTarget.style.backgroundColor = 'var(--bg-hover)';
              }}
              onMouseLeave={(e) => {
                if (selectedTicker !== stock.ticker) e.currentTarget.style.backgroundColor = 'transparent';
              }}
            >
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.875rem' }}>{stock.ticker}</div>
                <div style={{ 
                  fontSize: '0.75rem', 
                  fontWeight: 600,
                  color: stock.recommendation === 'BUY' ? 'var(--signal-buy-text)' : 
                         stock.recommendation === 'SELL' ? 'var(--signal-sell-text)' : 
                         stock.recommendation === 'HOLD' ? 'var(--signal-hold-text)' : 'var(--text-secondary)'
                }}>{stock.recommendation}</div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <div className="mono" style={{ fontSize: '0.875rem' }}>{stock.last_close.toFixed(2)}</div>
                <div className="mono" style={{ 
                  fontSize: '0.75rem', 
                  color: stock.day_change_pct > 0 ? 'var(--signal-buy-text)' : stock.day_change_pct < 0 ? 'var(--signal-sell-text)' : 'var(--text-secondary)' 
                }}>
                  {stock.day_change_pct > 0 ? '+' : ''}{stock.day_change_pct.toFixed(2)}%
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
