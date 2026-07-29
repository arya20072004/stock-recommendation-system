import React, { useEffect, useRef, useState } from 'react';
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts';
import { useTheme } from '../theme/ThemeContext';

export const Chart = ({ data, ticker }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const seriesRef = useRef();
  const volumeSeriesRef = useRef();
  const { theme } = useTheme();
  
  const [crosshairData, setCrosshairData] = useState(null);
  
  // Theme colors
  const colors = {
    dark: {
      bg: 'transparent',
      text: '#8b93a3',
      grid: '#1f2937',
      up: '#4ade80',
      down: '#f87171',
    },
    light: {
      bg: 'transparent',
      text: '#6b7280',
      grid: '#e5e7eb',
      up: '#15803d',
      down: '#b91c1c',
    }
  };

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const currentColors = colors[theme];
    
    // Initialize chart if not exists
    if (!chartRef.current) {
      const chart = createChart(chartContainerRef.current, {
        layout: {
          background: { type: 'solid', color: currentColors.bg },
          textColor: currentColors.text,
        },
        grid: {
          vertLines: { color: currentColors.grid },
          horzLines: { color: currentColors.grid },
        },
        rightPriceScale: {
          borderColor: currentColors.grid,
        },
        timeScale: {
          borderColor: currentColors.grid,
        },
        crosshair: {
          mode: 0,
        },
      });

      const candlestickSeries = chart.addSeries(CandlestickSeries, {
        upColor: currentColors.up,
        downColor: currentColors.down,
        borderVisible: false,
        wickUpColor: currentColors.up,
        wickDownColor: currentColors.down,
      });

      const volumeSeries = chart.addSeries(HistogramSeries, {
        priceFormat: {
          type: 'volume',
        },
        priceScaleId: '', // set as an overlay by setting a blank priceScaleId
      });
      
      // Scale volume differently
      volumeSeries.priceScale().applyOptions({
        scaleMargins: {
          top: 0.8, // highest point of the series will be at 80% of the chart
          bottom: 0,
        },
      });

      chartRef.current = chart;
      seriesRef.current = candlestickSeries;
      volumeSeriesRef.current = volumeSeries;

      chart.subscribeCrosshairMove((param) => {
        if (
          param.point === undefined ||
          !param.time ||
          param.point.x < 0 ||
          param.point.x > chartContainerRef.current.clientWidth ||
          param.point.y < 0 ||
          param.point.y > chartContainerRef.current.clientHeight
        ) {
          setCrosshairData(null);
        } else {
          const priceData = param.seriesData.get(seriesRef.current);
          if (priceData) {
            setCrosshairData(priceData);
          }
        }
      });

      const resizeObserver = new ResizeObserver((entries) => {
        if (entries.length === 0 || entries[0].target !== chartContainerRef.current) {
          return;
        }
        const newRect = entries[0].contentRect;
        chart.applyOptions({ width: newRect.width, height: newRect.height });
      });

      resizeObserver.observe(chartContainerRef.current);

      // Clean up on unmount
      return () => {
        resizeObserver.disconnect();
        chart.remove();
        chartRef.current = null;
      };
    }
  }, []); // Run once to create chart

  // Update theme dynamically
  useEffect(() => {
    if (!chartRef.current) return;
    const currentColors = colors[theme];
    
    chartRef.current.applyOptions({
      layout: {
        background: { type: 'solid', color: currentColors.bg },
        textColor: currentColors.text,
      },
      grid: {
        vertLines: { color: currentColors.grid },
        horzLines: { color: currentColors.grid },
      },
      rightPriceScale: {
        borderColor: currentColors.grid,
      },
      timeScale: {
        borderColor: currentColors.grid,
      },
    });

    seriesRef.current.applyOptions({
      upColor: currentColors.up,
      downColor: currentColors.down,
      wickUpColor: currentColors.up,
      wickDownColor: currentColors.down,
    });
  }, [theme]);

  // Update data when it changes
  useEffect(() => {
    if (!seriesRef.current || !volumeSeriesRef.current || !data || data.length === 0) return;

    // Transform data to lightweight-charts format if needed
    const upColorRGBA = theme === 'dark' ? 'rgba(74, 222, 128, 0.4)' : 'rgba(21, 128, 61, 0.4)';
    const downColorRGBA = theme === 'dark' ? 'rgba(248, 113, 113, 0.4)' : 'rgba(185, 28, 28, 0.4)';
    
    const volumeData = data.map(d => ({
      time: d.time,
      value: d.volume,
      color: d.close >= d.open ? upColorRGBA : downColorRGBA
    }));

    seriesRef.current.setData(data);
    volumeSeriesRef.current.setData(volumeData);
    
    // Fit content
    chartRef.current.timeScale().fitContent();
  }, [data, theme]);

  const displayData = crosshairData || (data && data.length > 0 ? data[data.length - 1] : null);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {/* OHLC Strip */}
      <div style={{ 
        position: 'absolute', 
        top: '12px', 
        left: '16px', 
        zIndex: 10,
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        fontSize: '0.875rem',
        whiteSpace: 'nowrap'
      }}>
        <div style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{ticker}</div>
        {displayData && (
          <div className="mono flex items-center gap-4" style={{ color: 'var(--text-secondary)' }}>
            <span><span style={{ color: 'var(--text-muted)' }}>O</span> {displayData.open.toFixed(2)}</span>
            <span><span style={{ color: 'var(--text-muted)' }}>H</span> {displayData.high.toFixed(2)}</span>
            <span><span style={{ color: 'var(--text-muted)' }}>L</span> {displayData.low.toFixed(2)}</span>
            <span>
              <span style={{ color: 'var(--text-muted)' }}>C</span>{' '}
              <span style={{ color: displayData.close >= displayData.open ? 'var(--chart-up)' : 'var(--chart-down)' }}>
                {displayData.close.toFixed(2)}
              </span>
            </span>
          </div>
        )}
      </div>
      
      {/* Chart Container */}
      <div 
        ref={chartContainerRef} 
        style={{ width: '100%', height: '100%', minHeight: '400px' }} 
      />
    </div>
  );
};
