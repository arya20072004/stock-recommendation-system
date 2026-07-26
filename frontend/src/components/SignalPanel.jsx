import React, { useState } from 'react';
import { RecommendationBadge } from './RecommendationBadge';
import { ConfidenceBar } from './ConfidenceBar';
import { ChevronDown, ChevronUp, Clock, Zap } from 'lucide-react';

export const SignalPanel = ({ ticker, data }) => {
  const [showAdvanced, setShowAdvanced] = useState(false);

  if (!data) return null;

  return (
    <div className="terminal-pane signal-dock" style={{ display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
      <div style={{ padding: '1.5rem' }}>
        <h2 style={{ fontSize: '1.25rem', fontWeight: 600, marginBottom: '1.5rem', color: 'var(--text-primary)' }}>Signal</h2>
        
        <div style={{ marginBottom: '2rem' }}>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Recommendation
          </div>
          <div style={{ display: 'inline-block' }}>
            <RecommendationBadge recommendation={data.recommendation} />
          </div>
        </div>

        <div style={{ marginBottom: '2rem' }}>
          <div className="flex justify-between items-center mb-2">
            <span style={{ fontSize: '0.875rem', fontWeight: 500, color: 'var(--text-primary)' }}>Model Confidence</span>
          </div>
          <ConfidenceBar confidence={data.confidence} recommendation={data.recommendation} />
        </div>

        {data.predicted_at && (
          <div className="flex items-center gap-2 mb-6" style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>
            <Clock size={14} />
            <span>Predicted: {data.predicted_at}</span>
          </div>
        )}

        <div style={{ borderTop: '1px solid var(--border-color)', paddingTop: '1.5rem', marginTop: '1.5rem' }}>
          <button 
            onClick={() => setShowAdvanced(!showAdvanced)}
            style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'space-between', 
              width: '100%', 
              color: 'var(--text-primary)',
              padding: '0.5rem 0',
              fontWeight: 500,
              fontSize: '0.875rem'
            }}
          >
            <div className="flex items-center gap-2">
              <Zap size={16} style={{ color: 'var(--accent-blue)' }} />
              Why this signal?
            </div>
            {showAdvanced ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </button>

          {showAdvanced && data.top_features && data.top_features.length > 0 && (
            <div style={{ marginTop: '1rem', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginBottom: '0.25rem' }}>
                Key drivers for this recommendation based on the ML model:
              </div>
              {data.top_features.map((feature, idx) => (
                <div key={idx} className="flex justify-between items-center" style={{ fontSize: '0.875rem' }}>
                  <span style={{ color: 'var(--text-secondary)' }}>{feature.feature}</span>
                  <span className="mono" style={{ color: 'var(--text-primary)' }}>
                    {(feature.importance * 100).toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
