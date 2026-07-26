import React, { useEffect, useRef, useState } from 'react';
import { createChart } from 'lightweight-charts';
import { useTheme } from '../theme/ThemeContext';

export const Chart = ({ data }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const seriesRef = useRef();
  const volumeSeriesRef = useRef();
  const { theme } = useTheme();
  
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

      const candlestickSeries = chart.addCandlestickSeries({
        upColor: currentColors.up,
        downColor: currentColors.down,
        borderVisible: false,
        wickUpColor: currentColors.up,
        wickDownColor: currentColors.down,
      });

      const volumeSeries = chart.addHistogramSeries({
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

      const handleResize = () => {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      };

      window.addEventListener('resize', handleResize);

      // Clean up on unmount
      return () => {
        window.removeEventListener('resize', handleResize);
        chart.remove();
        chartRef.current = null;
      };
    }
  }, []); // Run once to create chart

  // Update data when it changes
  useEffect(() => {
    if (!chartRef.current || !data || data.length === 0) return;

    const candleData = data.map(d => ({
      time: d.time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));
    
    const volumeData = data.map(d => ({
      time: d.time,
      value: d.volume,
      color: d.close >= d.open ? 'rgba(74, 222, 128, 0.3)' : 'rgba(248, 113, 113, 0.3)', 
    }));

    // Update colors for volume based on theme
    const currentColors = colors[theme];
    const upColorRGBA = theme === 'dark' ? 'rgba(74, 222, 128, 0.3)' : 'rgba(21, 128, 61, 0.3)';
    const downColorRGBA = theme === 'dark' ? 'rgba(248, 113, 113, 0.3)' : 'rgba(185, 28, 28, 0.3)';
    
    volumeData.forEach((d, i) => {
      d.color = data[i].close >= data[i].open ? upColorRGBA : downColorRGBA;
    });

    seriesRef.current.setData(candleData);
    volumeSeriesRef.current.setData(volumeData);
    
    chartRef.current.timeScale().fitContent();
  }, [data, theme]);

  // Apply theme changes
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

  return (
    <div 
      ref={chartContainerRef} 
      style={{ width: '100%', height: '100%', minHeight: '400px' }} 
    />
  );
};
