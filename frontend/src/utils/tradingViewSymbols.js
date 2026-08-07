/**
 * TradingView Symbol Resolution Utility
 * 
 * Converts StockIntel internal tickers (Yahoo/NSE format) to TradingView symbols.
 * 
 * General rule: *.NS → NSE:<symbol>
 * 
 * Override map exists only for genuinely exceptional symbols where the
 * TradingView symbol differs from the simple NSE ticker.
 */

// Only genuinely exceptional mappings where TradingView symbol differs
// from the canonical NSE ticker prefix.
const NSE_SYMBOL_OVERRIDES = {
  // M&M.NS contains an ampersand which TradingView handles as M_M
  'M&M.NS': 'NSE:M_M',
  // BAJAJ-AUTO.NS contains a hyphen
  'BAJAJ-AUTO.NS': 'NSE:BAJAJ_AUTO',
};

const BSE_SYMBOL_OVERRIDES = {
  'M&M.NS': 'BSE:M_M',
  'BAJAJ-AUTO.NS': 'BSE:BAJAJ_AUTO',
};

/**
 * Convert a StockIntel ticker to a TradingView-compatible canonical NSE symbol.
 */
export function toTradingViewSymbol(ticker) {
  if (!ticker || typeof ticker !== 'string') return ''
  const trimmed = ticker.trim()
  if (!trimmed) return ''
  if (trimmed.includes(':')) return trimmed
  if (NSE_SYMBOL_OVERRIDES[trimmed]) return NSE_SYMBOL_OVERRIDES[trimmed]
  
  if (trimmed.endsWith('.NS')) {
    return `NSE:${trimmed.slice(0, -3)}`
  }
  return `NSE:${trimmed}`
}

/**
 * Resolves the symbol specifically for the TradingView Advanced Chart widget.
 * Since TradingView restricts NSE symbols from being embedded in widgets on free tiers,
 * this explicitly falls back to the BSE equivalent while preserving the canonical NSE symbol.
 */
export function resolveTradingViewWidgetSymbol(ticker) {
  const canonical = toTradingViewSymbol(ticker)
  if (!canonical) return null
  
  let widgetSymbol = canonical
  let isFallback = false
  
  const trimmed = ticker.trim()
  
  // Since we verified NSE is restricted in the embed widget, we must fallback to BSE
  if (canonical.startsWith('NSE:')) {
    isFallback = true
    if (BSE_SYMBOL_OVERRIDES[trimmed]) {
      widgetSymbol = BSE_SYMBOL_OVERRIDES[trimmed]
    } else if (trimmed.endsWith('.NS')) {
      widgetSymbol = `BSE:${trimmed.slice(0, -3)}`
    } else {
      widgetSymbol = `BSE:${trimmed}`
    }
  }

  return {
    canonical,
    widget: widgetSymbol,
    isFallback,
    fallbackExchange: isFallback ? 'BSE' : null
  }
}

/**
 * Generate a TradingView symbol page URL for attribution.
 * We can link to the canonical NSE page since viewing on their main site is not restricted.
 */
export function toTradingViewUrl(ticker) {
  const symbol = toTradingViewSymbol(ticker)
  if (!symbol) return 'https://www.tradingview.com/'
  const urlPath = symbol.replace(':', '-')
  return `https://www.tradingview.com/symbols/${urlPath}/`
}
