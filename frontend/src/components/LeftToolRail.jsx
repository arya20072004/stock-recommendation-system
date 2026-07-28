import React from 'react';
import { Crosshair, Edit3, Type, Maximize, Search } from 'lucide-react';

export const LeftToolRail = ({ onToggleTickerRail, activeTool, setActiveTool, onFullscreen }) => {
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
        <button 
          className={`tool-btn ${activeTool === 'crosshair' ? 'active' : ''}`} 
          title="Crosshair"
          onClick={() => setActiveTool('crosshair')}
        >
          <Crosshair size={20} />
        </button>
        <button 
          className={`tool-btn ${activeTool === 'line' ? 'active' : ''}`} 
          title="Horizontal Line"
          onClick={() => setActiveTool('line')}
        >
          <Edit3 size={20} />
        </button>
        <button 
          className={`tool-btn ${activeTool === 'text' ? 'active' : ''}`} 
          title="Text"
          onClick={() => setActiveTool('text')}
        >
          <Type size={20} />
        </button>
      </div>
      
      <div className="mt-auto mb-4">
        <button className="tool-btn" title="Fullscreen" onClick={onFullscreen}>
          <Maximize size={20} />
        </button>
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
        .tool-btn.active {
          color: var(--text-primary);
          background-color: rgba(59, 130, 246, 0.2); /* theme blue slightly transparent */
          border: 1px solid rgba(59, 130, 246, 0.5);
        }
      `}</style>
    </div>
  );
};
