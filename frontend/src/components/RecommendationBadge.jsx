import React from 'react';

export const RecommendationBadge = ({ recommendation }) => {
  const normalized = recommendation ? recommendation.toUpperCase() : 'UNCERTAIN';
  
  // Base classes for the badge
  const baseStyle = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '0.25rem 0.75rem',
    borderRadius: '9999px',
    fontSize: '0.75rem',
    fontWeight: '600',
    letterSpacing: '0.05em',
    textTransform: 'uppercase',
  };

  // Determine colors based on theme tokens
  const getColors = (rec) => {
    switch (rec) {
      case 'BUY':
        return { color: 'var(--signal-buy-text)', backgroundColor: 'var(--signal-buy-bg)' };
      case 'SELL':
        return { color: 'var(--signal-sell-text)', backgroundColor: 'var(--signal-sell-bg)' };
      case 'HOLD':
        return { color: 'var(--signal-hold-text)', backgroundColor: 'var(--signal-hold-bg)' };
      default:
        return { color: 'var(--signal-uncertain-text)', backgroundColor: 'var(--signal-uncertain-bg)' };
    }
  };

  const colors = getColors(normalized);

  return (
    <span style={{ ...baseStyle, ...colors }}>
      {normalized}
    </span>
  );
};
