import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchPortfolio } from '../api/client';
import { PortfolioTable } from '../components/PortfolioTable';

export const Portfolio = () => {
  const { data, isLoading, error } = useQuery({
    queryKey: ['portfolio'],
    queryFn: fetchPortfolio,
  });

  return (
    <div className="container flex flex-col gap-6">
      <div style={{ marginBottom: '1rem' }}>
        <h1 style={{ margin: 0 }}>Portfolio Overview</h1>
        <p className="caption" style={{ marginTop: '0.25rem' }}>
          Quantitative signals across all tracked assets
        </p>
      </div>
      
      {isLoading ? (
        <div className="card flex items-center justify-center" style={{ height: '400px' }}>
          <div style={{ color: 'var(--text-muted)' }}>Loading portfolio data...</div>
        </div>
      ) : error ? (
        <div className="card flex items-center justify-center" style={{ height: '400px' }}>
          <div style={{ color: 'var(--signal-sell-text)' }}>
            {error.message || 'Failed to load portfolio data'}
          </div>
        </div>
      ) : data?.portfolio ? (
        <PortfolioTable data={data.portfolio} />
      ) : (
        <div className="card flex items-center justify-center" style={{ height: '400px' }}>
          <div style={{ color: 'var(--text-muted)' }}>No portfolio data available</div>
        </div>
      )}
    </div>
  );
};
