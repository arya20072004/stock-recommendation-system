import { useEffect, useRef, useState, memo } from 'react'
import { resolveTradingViewWidgetSymbol, toTradingViewUrl } from '../../utils/tradingViewSymbols'

const WIDGET_SCRIPT_SRC = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js'

/**
 * TradingView Advanced Chart — dynamically renders for any StockIntel ticker.
 *
 * @param {{ ticker: string }} props
 */
function TradingViewAdvancedChartInner({ ticker }) {
  const containerRef = useRef(null)
  const [status, setStatus] = useState('loading')

  const resolution = resolveTradingViewWidgetSymbol(ticker)
  const symbol = resolution?.widget
  
  // Format the label for the UI (e.g., RELIANCE)
  const baseTicker = resolution ? resolution.canonical.replace('NSE:', '').replace('BSE:', '') : ''
  const symbolLabel = resolution?.isFallback 
    ? `${baseTicker} (${resolution.fallbackExchange} fallback)` 
    : baseTicker

  useEffect(() => {
    const node = containerRef.current
    if (!node || !symbol) {
      setStatus('unavailable')
      return
    }

    setStatus('loading')

    node.replaceChildren()

    const widgetHost = document.createElement('div')
    widgetHost.className = 'tradingview-widget-container__widget'
    widgetHost.style.height = 'calc(100% - 32px)'
    widgetHost.style.width = '100%'
    node.appendChild(widgetHost)

    const attribution = document.createElement('div')
    attribution.className = 'tradingview-widget-copyright'
    attribution.style.textAlign = 'center'
    
    const attrLink = document.createElement('a')
    attrLink.href = toTradingViewUrl(ticker)
    attrLink.rel = 'noopener nofollow noreferrer'
    attrLink.target = '_blank'
    attrLink.innerHTML = `<span class="blue-text">${symbolLabel} chart</span> by TradingView`
    
    attribution.appendChild(attrLink)
    node.appendChild(attribution)

    const config = {
      autosize: true,
      symbol: symbol,
      interval: 'D',
      timezone: 'Asia/Kolkata',
      theme: 'dark',
      style: '1',
      locale: 'en',
      backgroundColor: 'rgba(14, 18, 27, 1)',
      gridColor: 'rgba(37, 45, 58, 0.5)',
      allow_symbol_change: false,
      calendar: false,
      details: false,
      hide_side_toolbar: true,
      hide_top_toolbar: false,
      hide_legend: false,
      hide_volume: false,
      save_image: true,
      support_host: 'https://www.tradingview.com'
    }

    if (process.env.NODE_ENV === 'development') {
      console.debug('[TradingView]', {
        ticker,
        resolvedSymbol: symbol,
        configSymbol: config.symbol
      })
    }

    const script = document.createElement('script')
    script.src = WIDGET_SCRIPT_SRC
    script.type = 'text/javascript'
    script.async = true
    script.innerHTML = JSON.stringify(config)

    script.onload = () => setStatus('ready')
    script.onerror = () => setStatus('unavailable')

    node.appendChild(script)

    return () => {
      node.replaceChildren()
    }
  }, [symbol, ticker, symbolLabel])

  if (!ticker || !symbol) {
    return (
      <div className="tv-chart-unavailable">
        <p>TradingView chart unavailable for this stock.</p>
      </div>
    )
  }

  return (
    <div className="tv-chart-wrapper">
      {status === 'loading' && (
        <div className="tv-chart-loading">
          <span>Loading TradingView chart…</span>
        </div>
      )}
      {status === 'unavailable' && (
        <div className="tv-chart-unavailable">
          <p>TradingView chart unavailable for this stock.</p>
        </div>
      )}
      <div
        ref={containerRef}
        className="tradingview-widget-container"
        style={{
          height: '100%',
          width: '100%',
          display: status === 'unavailable' ? 'none' : 'block'
        }}
      />
    </div>
  )
}

export const TradingViewAdvancedChart = memo(TradingViewAdvancedChartInner)
