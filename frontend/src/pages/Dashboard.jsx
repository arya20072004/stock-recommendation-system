import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { fetchStocks, fetchStockData } from '../api/client';
import { Chart } from '../components/Chart';
import { SignalPanel } from '../components/SignalPanel';

export const Dashboard = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialTicker = searchParams.get('ticker');
  const [selectedTicker, setSelectedTicker] = useState(initialTicker || null);

  // Fetch list of available stocks
  const { data: stocks, isLoading: stocksLoading, error: stocksError } = useQuery({
    queryKey: ['stocks'],
    queryFn: fetchStocks,
  });

  // Set initial ticker when stocks are loaded if not set
  useEffect(() => {
    if (stocks && stocks.length > 0 && !selectedTicker) {
      setSelectedTicker(stocks[0]);
    }
  }, [stocks, selectedTicker]);

  // Update URL when ticker changes
  useEffect(() => {
    if (selectedTicker) {
      setSearchParams({ ticker: selectedTicker });
    }
  }, [selectedTicker, setSearchParams]);

  // Fetch data for the selected stock
  const { data: stockData, isLoading: stockDataLoading, error: stockDataError } = useQuery({
    queryKey: ['stockData', selectedTicker],
    queryFn: () => fetchStockData(selectedTicker),
    enabled: !!selectedTicker,
  });

  const handleTickerChange = (e) => {
    setSelectedTicker(e.target.value);
  };

  return (
    <div className="container flex flex-col gap-6">
      <div className="flex justify-between items-center" style={{ marginBottom: '1rem' }}>
        <div>
          <h1 style={{ margin: 0 }}>Terminal</h1>
          <p className="caption" style={{ marginTop: '0.25rem' }}>Select an asset to view its predictive signal</p>
        </div>
        <div>
          {stocksLoading ? (
            <div style={{ padding: '0.5rem 1rem', background: 'var(--bg-card)', borderRadius: '0.5rem' }}>Loading assets...</div>
          ) : stocksError ? (
            <div style={{ color: 'var(--signal-sell-text)' }}>Failed to load assets</div>
          ) : (
            <select 
              value={selectedTicker || ''} 
              onChange={handleTickerChange}
              style={{
                padding: '0.5rem 2rem 0.5rem 1rem',
                backgroundColor: 'var(--bg-card)',
                color: 'var(--text-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '0.5rem',
                fontSize: '1rem',
                fontWeight: 500,
                cursor: 'pointer',
                appearance: 'none',
                outline: 'none',
                minWidth: '200px'
              }}
            >
              {stocks?.map(ticker => (
                <option key={ticker} value={ticker}>{ticker}</option>
              ))}
            </select>
          )}
        </div>
      </div>

      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 350px), 1fr))', 
        gap: '1.5rem',
        alignItems: 'start'
      }}>
        <div style={{ 
          gridColumn: '1 / -1', 
          '@media (min-width: 1024px)': { gridColumn: 'span 2 / span 2' } 
        }} className="chart-container-wrapper">
          <div className="card flex flex-col items-center justify-center" style={{ height: '600px', padding: '1rem' }}>
            {stockDataLoading ? (
              <div style={{ color: 'var(--text-muted)' }}>Loading chart data...</div>
            ) : stockDataError ? (
              <div style={{ color: 'var(--signal-sell-text)' }}>{stockDataError.message || 'Failed to load chart data'}</div>
            ) : stockData && stockData.chartData ? (
              <Chart data={stockData.chartData} />
            ) : (
              <div style={{ color: 'var(--text-muted)' }}>No chart data available</div>
            )}
          </div>
        </div>

        <div className="signal-panel-wrapper">
          {stockDataLoading ? (
            <div className="card flex items-center justify-center" style={{ height: '300px' }}>
              <div style={{ color: 'var(--text-muted)' }}>Loading signal...</div>
            </div>
          ) : stockDataError ? (
            <div className="card flex items-center justify-center" style={{ height: '300px' }}>
              <div style={{ color: 'var(--signal-sell-text)' }}>Failed to load signal</div>
            </div>
          ) : stockData ? (
            <SignalPanel ticker={selectedTicker} data={stockData} />
          ) : null}
        </div>
        
        <style>{`
          .chart-container-wrapper { grid-column: 1 / -1; }
          .signal-panel-wrapper { grid-column: 1 / -1; }
          
          @media (min-width: 1024px) {
            .chart-container-wrapper { grid-column: span 2 / span 2; }
            .signal-panel-wrapper { grid-column: span 1 / span 1; }
            .container > div:nth-child(2) {
              grid-template-columns: 2fr 1fr;
            }
          }
        `}</style>
      </div>
    </div>
  );
};
