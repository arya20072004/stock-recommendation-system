import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Chart } from '../components/Chart';
import { SignalPanel } from '../components/SignalPanel';
import { TickerRail } from '../components/TickerRail';
import { LeftToolRail } from '../components/LeftToolRail';

export const Dashboard = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialTicker = searchParams.get('ticker') || 'ADANIENT.NS'; // default to something if empty
  const [selectedTicker, setSelectedTicker] = useState(initialTicker);
  const [showMobileRail, setShowMobileRail] = useState(false);
  const [activeTool, setActiveTool] = useState('crosshair'); // 'crosshair', 'line', 'text'
  const chartAreaRef = React.useRef(null);

  const handleFullscreen = () => {
    if (!document.fullscreenElement) {
      if (chartAreaRef.current) {
        chartAreaRef.current.requestFullscreen().catch(err => console.warn(err));
      }
    } else {
      document.exitFullscreen();
    }
  };

  // Update URL when ticker changes
  useEffect(() => {
    if (selectedTicker) {
      setSearchParams({ ticker: selectedTicker }, { replace: true });
    }
  }, [selectedTicker, setSearchParams]);

  // Fetch stock detailed data
  const fetchStockData = async (ticker) => {
    if (!ticker) return null;
    const response = await fetch(`/api/stocks/${ticker}`);
    if (!response.ok) throw new Error('Failed to fetch stock data');
    return response.json();
  };

  const { data: stockData, isLoading: stockDataLoading } = useQuery({
    queryKey: ['stock', selectedTicker],
    queryFn: () => fetchStockData(selectedTicker),
    enabled: !!selectedTicker,
  });

  const handleSelectTicker = (ticker) => {
    setSelectedTicker(ticker);
    setShowMobileRail(false); // hide overlay on mobile when selected
  };

  return (
    <div className="terminal-workspace" style={{ position: 'relative' }}>
      
      {/* Tool Rail (Desktop only) */}
      <LeftToolRail 
        onToggleTickerRail={() => setShowMobileRail(!showMobileRail)}
        activeTool={activeTool}
        setActiveTool={setActiveTool}
        onFullscreen={handleFullscreen}
      />

      {/* Ticker Rail (Desktop/Tablet) or Mobile Overlay */}
      <div 
        className={`terminal-pane ${showMobileRail ? 'flex' : 'hidden lg:flex'}`}
        style={{ 
          position: showMobileRail ? 'absolute' : 'relative',
          zIndex: showMobileRail ? 50 : 1,
          height: '100%',
          boxShadow: showMobileRail ? '4px 0 24px rgba(0,0,0,0.5)' : 'none'
        }}
      >
        <TickerRail 
          selectedTicker={selectedTicker} 
          onSelectTicker={handleSelectTicker} 
        />
      </div>

      {/* Mobile overlay backdrop */}
      {showMobileRail && (
        <div 
          className="lg:hidden"
          style={{ position: 'absolute', inset: 0, backgroundColor: 'rgba(0,0,0,0.5)', zIndex: 40 }}
          onClick={() => setShowMobileRail(false)}
        />
      )}

      {/* Main Content Area (Chart + Signal Panel) */}
      <div className="flex flex-col md:flex-row flex-1" style={{ overflow: 'hidden' }}>
        
        {/* Chart Area */}
        <div ref={chartAreaRef} className="terminal-pane flex-1" style={{ position: 'relative', display: 'flex', flexDirection: 'column', backgroundColor: 'var(--bg-primary)' }}>
          {stockDataLoading ? (
            <div className="flex items-center justify-center flex-1" style={{ color: 'var(--text-muted)' }}>
              Loading chart data...
            </div>
          ) : stockData && stockData.chartData ? (
            <Chart data={stockData.chartData} ticker={selectedTicker} activeTool={activeTool} />
          ) : (
            <div className="flex items-center justify-center flex-1" style={{ color: 'var(--text-muted)' }}>
              Select a stock to view its chart
            </div>
          )}
        </div>

        {/* Signal Panel (Docked right on desktop, bottom on mobile/tablet) */}
        <div className="terminal-pane" style={{ overflowY: 'auto', borderRight: 'none' }}>
          {stockDataLoading ? (
            <div style={{ padding: '1.5rem', color: 'var(--text-muted)' }}>Loading signals...</div>
          ) : stockData ? (
            <SignalPanel ticker={selectedTicker} data={stockData} />
          ) : null}
        </div>

      </div>
    </div>
  );
};
