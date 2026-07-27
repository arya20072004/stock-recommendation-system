import React from 'react';

export const RecommendationBadge = ({ recommendation, isCalibrated, calibrationChanged, rawPrediction }) => {
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

  const badge = (
    <span style={{ ...baseStyle, ...colors }} title={isCalibrated ? "Threshold-calibrated" : ""}>
      {normalized}
      {isCalibrated && <sup style={{ marginLeft: '3px', fontSize: '0.75em' }}>†</sup>}
    </span>
  );

  if (calibrationChanged) {
    return (
      <div style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}>
        {badge}
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          (Raw: {rawPrediction} → Calibrated: {normalized})
        </span>
      </div>
    );
  }

  return badge;
};
