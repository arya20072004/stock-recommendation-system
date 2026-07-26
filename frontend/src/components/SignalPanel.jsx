import React, { useState } from 'react';
import { RecommendationBadge } from './RecommendationBadge';
import { ConfidenceBar } from './ConfidenceBar';
import { ChevronDown, ChevronUp, Clock, Zap } from 'lucide-react';

export const SignalPanel = ({ ticker, data }) => {
  const [showAdvanced, setShowAdvanced] = useState(false);

  if (!data) return null;

  return (
    <div className="card flex flex-col gap-6" style={{ height: '100%' }}>
      <div className="flex justify-between items-center">
        <div>
          <h2 style={{ margin: 0 }}>{ticker}</h2>
          <div className="caption flex items-center gap-2 mt-4" style={{ marginTop: '0.25rem' }}>
            <Clock size={12} />
            <span>Predicted: {data.predicted_at}</span>
          </div>
        </div>
        <RecommendationBadge recommendation={data.recommendation} />
      </div>

      <div>
        <div className="flex justify-between mb-4" style={{ marginBottom: '0.5rem' }}>
          <span style={{ fontSize: '0.875rem', fontWeight: 500 }}>Model Confidence</span>
        </div>
        <ConfidenceBar confidence={data.confidence} recommendation={data.recommendation} />
      </div>

      <div style={{ marginTop: 'auto' }}>
        <button 
          onClick={() => setShowAdvanced(!showAdvanced)}
          className="flex justify-between items-center"
          style={{ 
            width: '100%', 
            padding: '0.75rem 0', 
            borderTop: '1px solid var(--border-color)',
            fontSize: '0.875rem',
            fontWeight: 500,
            color: 'var(--text-secondary)'
          }}
        >
          <div className="flex items-center gap-2">
            <Zap size={16} />
            <span>Why this signal?</span>
          </div>
          {showAdvanced ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>
        
        {showAdvanced && data.top_features && (
          <div style={{ paddingTop: '0.75rem' }}>
            <div className="caption mb-4" style={{ marginBottom: '0.75rem' }}>
              Top signal drivers and feature contributions:
            </div>
            <div className="flex flex-col gap-2">
              {data.top_features.map((feature, idx) => (
                <div key={idx} className="flex justify-between items-center" style={{ fontSize: '0.875rem' }}>
                  <span className="mono" style={{ color: 'var(--text-primary)' }}>{feature.feature}</span>
                  <span className="mono" style={{ color: 'var(--text-secondary)' }}>
                    {(feature.importance * 100).toFixed(2)}%
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
