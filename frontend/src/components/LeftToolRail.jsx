import React from 'react';
import { Crosshair, Edit3, Type, Maximize, Search } from 'lucide-react';

export const LeftToolRail = ({ onToggleTickerRail }) => {
  return (
    <div className="terminal-pane hidden md:flex flex-col items-center py-4 gap-6" style={{ width: '48px', flexShrink: 0 }}>
      <button 
        onClick={onToggleTickerRail}
        className="tool-btn lg:hidden" 
        title="Search Tickers"
      >
        <Search size={20} />
      </button>
      
      <div className="flex flex-col gap-4 mt-2">
        <button className="tool-btn" title="Crosshair"><Crosshair size={20} /></button>
        <button className="tool-btn" title="Draw Line"><Edit3 size={20} /></button>
        <button className="tool-btn" title="Text"><Type size={20} /></button>
      </div>
      
      <div className="mt-auto mb-4">
        <button className="tool-btn" title="Fullscreen"><Maximize size={20} /></button>
      </div>
      
      <style>{`
        .tool-btn {
          width: 32px;
          height: 32px;
          display: flex;
          align-items: center;
          justify-content: center;
          border-radius: 4px;
          color: var(--text-muted);
          transition: all 0.2s;
        }
        .tool-btn:hover {
          color: var(--text-primary);
          background-color: var(--bg-hover);
        }
      `}</style>
    </div>
  );
};
