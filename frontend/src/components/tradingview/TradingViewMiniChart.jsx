import { useEffect, useRef, useState, memo } from 'react'
import { toTradingViewSymbol } from '../../utils/tradingViewSymbols'

const WIDGET_SCRIPT_SRC = 'https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js'

/**
 * TradingView Mini Chart — compact chart for watchlists and dashboard panels.
 *
 * WARNING: Do NOT render 51 of these simultaneously. Use for selected stocks only.
 *
 * @param {{ ticker: string, width?: string|number, height?: string|number }} props
 */
function TradingViewMiniChartInner({ ticker, width = '100%', height = 180 }) {
  const containerRef = useRef(null)
  const [status, setStatus] = useState('loading')

  const symbol = toTradingViewSymbol(ticker)

  useEffect(() => {
    if (!symbol || !containerRef.current) {
      setStatus('unavailable')
      return
    }

    setStatus('loading')
    const container = containerRef.current
    container.innerHTML = ''

    const widgetDiv = document.createElement('div')
    widgetDiv.className = 'tradingview-widget-container__widget'
    container.appendChild(widgetDiv)

    const script = document.createElement('script')
    script.src = WIDGET_SCRIPT_SRC
    script.type = 'text/javascript'
    script.async = true

    script.innerHTML = JSON.stringify({
      "symbol": symbol,
      "width": typeof width === 'number' ? width : '100%',
      "height": typeof height === 'number' ? height : '100%',
      "locale": "en",
      "dateRange": "1M",
      "colorTheme": "dark",
      "isTransparent": true,
      "autosize": false,
      "largeChartUrl": ""
    })

    script.onload = () => setStatus('ready')
    script.onerror = () => setStatus('unavailable')

    container.appendChild(script)

    return () => {
      if (container) container.innerHTML = ''
    }
  }, [symbol, width, height])

  if (!ticker || status === 'unavailable') {
    return null // Mini charts should degrade silently
  }

  return (
    <div
      ref={containerRef}
      className="tradingview-widget-container"
      style={{ width: typeof width === 'number' ? `${width}px` : width }}
    />
  )
}

export const TradingViewMiniChart = memo(TradingViewMiniChartInner)
