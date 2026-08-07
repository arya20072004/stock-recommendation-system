import { useEffect, useMemo, useRef, memo } from 'react'
import { toTradingViewSymbol } from '../../utils/tradingViewSymbols'

const WIDGET_SCRIPT_SRC = 'https://s3.tradingview.com/external-embedding/embed-widget-symbol-overview.js'

/**
 * TradingView Symbol Overview — multi-symbol comparison widget.
 *
 * Suitable for dashboards, watchlists, and comparison panels.
 *
 * @param {{ tickers: string[], height?: number }} props
 */
function TradingViewSymbolOverviewInner({ tickers = [], height = 400 }) {
  const containerRef = useRef(null)

  const symbolsKey = useMemo(() => {
    return tickers
      .map(t => toTradingViewSymbol(t))
      .filter(Boolean)
      .join(',')
  }, [tickers])

  const symbols = useMemo(() => {
    return tickers
      .map(t => toTradingViewSymbol(t))
      .filter(Boolean)
      .map(s => [s])
  }, [tickers])

  useEffect(() => {
    if (symbols.length === 0 || !containerRef.current) {
      return
    }
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
      "symbols": symbols,
      "chartOnly": false,
      "width": "100%",
      "height": height,
      "locale": "en",
      "colorTheme": "dark",
      "autosize": false,
      "showVolume": false,
      "showMA": false,
      "hideDateRanges": false,
      "hideMarketStatus": false,
      "hideSymbolLogo": false,
      "scalePosition": "right",
      "scaleMode": "Normal",
      "fontFamily": "-apple-system, BlinkMacSystemFont, Trebuchet MS, Roboto, Ubuntu, sans-serif",
      "fontSize": "10",
      "noTimeScale": false,
      "valuesTracking": "1",
      "changeMode": "price-and-percent",
      "chartType": "area",
      "lineWidth": 2,
      "lineType": 0,
      "dateRanges": ["1d|1", "1m|30", "3m|60", "12m|1D", "60m|1W", "all|1M"]
    })

    script.onload = () => {}
    script.onerror = () => {}

    container.appendChild(script)

    return () => {
      if (container) container.innerHTML = ''
    }
  }, [symbolsKey, symbols, height])

  if (symbols.length === 0) {
    return null
  }

  return (
    <div
      ref={containerRef}
      className="tradingview-widget-container"
      style={{ width: '100%' }}
    />
  )
}

export const TradingViewSymbolOverview = memo(TradingViewSymbolOverviewInner)
