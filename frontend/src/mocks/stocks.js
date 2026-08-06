// Phase 3 stock universe and detail metadata. This remains the single stock source of truth.
function createRng(seed) {
  let h = 0
  for (let i = 0; i < seed.length; i += 1) h = ((h << 5) - h + seed.charCodeAt(i)) | 0
  let s = Math.abs(h) || 1
  return () => { s = (s * 16807) % 2147483647; return s / 2147483647 }
}

function generateSeries(seed, currentPrice, count, labelFn) {
  const rng = createRng(seed)
  const data = []
  let price = currentPrice * (1 - (rng() * 0.08 + 0.02))
  for (let i = 0; i < count; i += 1) {
    const drift = (currentPrice - price) / (count - i) * 0.35
    const noise = (rng() - 0.48) * 0.012 * price * 2
    price = Math.max(price + drift + noise, currentPrice * 0.88)
    data.push({ time: labelFn(i), value: Math.round(price * 100) / 100 })
  }
  data[data.length - 1].value = currentPrice
  return data
}

function buildPriceHistory(ticker, currentPrice) {
  return {
    '1D': generateSeries(`${ticker}1D`, currentPrice, 13, (i) => { const minutes = 9 * 60 + 15 + i * 30; return `${Math.floor(minutes / 60)}:${String(minutes % 60).padStart(2, '0')}` }),
    '1W': generateSeries(`${ticker}1W`, currentPrice, 5, (i) => ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'][i]),
    '1M': generateSeries(`${ticker}1M`, currentPrice, 22, (i) => `${i + 1} Jul`),
    '3M': generateSeries(`${ticker}3M`, currentPrice, 13, (i) => `W${i + 1}`),
    '6M': generateSeries(`${ticker}6M`, currentPrice, 26, (i) => `W${i + 1}`),
    '1Y': generateSeries(`${ticker}1Y`, currentPrice, 12, (i) => ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug'][i]),
  }
}

function formatInr(value) { return `₹${Math.round(value).toLocaleString('en-IN')}` }

function buildIndicators(currentPrice, signal) {
  if (signal === 'BUY') return [{ name: 'RSI', value: '61.4', interpretation: 'Bullish' }, { name: 'MACD', value: '+8.42', interpretation: 'Bullish' }, { name: 'SMA 20', value: formatInr(currentPrice * 0.984), interpretation: 'Above' }, { name: 'SMA 50', value: formatInr(currentPrice * 0.952), interpretation: 'Above' }, { name: 'Volatility', value: '18.3%', interpretation: 'Moderate' }]
  if (signal === 'SELL') return [{ name: 'RSI', value: '38.2', interpretation: 'Bearish' }, { name: 'MACD', value: '-5.63', interpretation: 'Bearish' }, { name: 'SMA 20', value: formatInr(currentPrice * 1.018), interpretation: 'Below' }, { name: 'SMA 50', value: formatInr(currentPrice * 1.052), interpretation: 'Below' }, { name: 'Volatility', value: '32.4%', interpretation: 'High' }]
  return [{ name: 'RSI', value: '52.1', interpretation: 'Neutral' }, { name: 'MACD', value: '+1.84', interpretation: 'Neutral' }, { name: 'SMA 20', value: formatInr(currentPrice * 1.002), interpretation: 'Near' }, { name: 'SMA 50', value: formatInr(currentPrice * 0.988), interpretation: 'Above' }, { name: 'Volatility', value: '22.1%', interpretation: 'Moderate' }]
}

const explanationsBySignal = {
  BUY: ['Strong 20-day momentum', 'RSI remains below overbought territory', 'Price above 50-day moving average', 'Positive volume trend', 'Model probability exceeds BUY threshold'],
  HOLD: ['Mixed short-term momentum signals', 'RSI in neutral territory', 'Price near key moving averages', 'Volume shows no clear directional bias', 'Model probability does not exceed action threshold'],
  SELL: ['Negative 20-day momentum', 'RSI approaching oversold conditions', 'Price below 50-day moving average', 'Declining volume trend', 'Model probability exceeds SELL threshold'],
}

const stockDefinitions = [
  { ticker: 'RELIANCE.NS', symbol: 'RELIANCE', companyName: 'Reliance Industries Ltd.', exchange: 'NSE', sector: 'Energy', currentPrice: 1412.30, priceChange: 19.80, priceChangePercent: 1.42, signal: 'BUY', confidence: 87, targetPrice: 1530, expectedReturn: 8.35, risk: 'MEDIUM' },
  { ticker: 'TCS.NS', symbol: 'TCS', companyName: 'Tata Consultancy Services Ltd.', exchange: 'NSE', sector: 'IT', currentPrice: 3087.40, priceChange: 21.80, priceChangePercent: 0.71, signal: 'BUY', confidence: 82, targetPrice: 3320, expectedReturn: 7.53, risk: 'LOW' },
  { ticker: 'INFY.NS', symbol: 'INFY', companyName: 'Infosys Ltd.', exchange: 'NSE', sector: 'IT', currentPrice: 1598.75, priceChange: -4.48, priceChangePercent: -0.28, signal: 'HOLD', confidence: 62, targetPrice: 1650, expectedReturn: 3.21, risk: 'MEDIUM' },
  { ticker: 'SBIN.NS', symbol: 'SBIN', companyName: 'State Bank of India', exchange: 'NSE', sector: 'Banking', currentPrice: 812.10, priceChange: -3.41, priceChangePercent: -0.42, signal: 'HOLD', confidence: 58, targetPrice: 830, expectedReturn: 2.20, risk: 'HIGH' },
  { ticker: 'ITC.NS', symbol: 'ITC', companyName: 'ITC Ltd.', exchange: 'NSE', sector: 'FMCG', currentPrice: 456.85, priceChange: 0.73, priceChangePercent: 0.16, signal: 'BUY', confidence: 74, targetPrice: 495, expectedReturn: 8.35, risk: 'LOW' },
  { ticker: 'HDFCBANK.NS', symbol: 'HDFCBANK', companyName: 'HDFC Bank Ltd.', exchange: 'NSE', sector: 'Banking', currentPrice: 2011.50, priceChange: 7.60, priceChangePercent: 0.38, signal: 'HOLD', confidence: 65, targetPrice: 2060, expectedReturn: 2.41, risk: 'MEDIUM' },
  { ticker: 'AXISBANK.NS', symbol: 'AXISBANK', companyName: 'Axis Bank Ltd.', exchange: 'NSE', sector: 'Banking', currentPrice: 1186.40, priceChange: 10.90, priceChangePercent: 0.92, signal: 'BUY', confidence: 78, targetPrice: 1280, expectedReturn: 7.89, risk: 'MEDIUM' },
  { ticker: 'BAJFINANCE.NS', symbol: 'BAJFINANCE', companyName: 'Bajaj Finance Ltd.', exchange: 'NSE', sector: 'Financial Services', currentPrice: 7245.60, priceChange: -13.04, priceChangePercent: -0.18, signal: 'HOLD', confidence: 61, targetPrice: 7400, expectedReturn: 2.13, risk: 'MEDIUM' },
  { ticker: 'SUNPHARMA.NS', symbol: 'SUNPHARMA', companyName: 'Sun Pharmaceutical Industries Ltd.', exchange: 'NSE', sector: 'Pharma', currentPrice: 1824.15, priceChange: 11.49, priceChangePercent: 0.63, signal: 'BUY', confidence: 76, targetPrice: 1960, expectedReturn: 7.45, risk: 'LOW' },
  { ticker: 'BHARTIARTL.NS', symbol: 'BHARTIARTL', companyName: 'Bharti Airtel Ltd.', exchange: 'NSE', sector: 'Telecom', currentPrice: 1652.40, priceChange: 7.93, priceChangePercent: 0.48, signal: 'BUY', confidence: 80, targetPrice: 1780, expectedReturn: 7.72, risk: 'LOW' },
  { ticker: 'MARUTI.NS', symbol: 'MARUTI', companyName: 'Maruti Suzuki India Ltd.', exchange: 'NSE', sector: 'Auto', currentPrice: 12456.80, priceChange: 29.90, priceChangePercent: 0.24, signal: 'HOLD', confidence: 59, targetPrice: 12700, expectedReturn: 1.95, risk: 'LOW' },
  { ticker: 'KOTAKBANK.NS', symbol: 'KOTAKBANK', companyName: 'Kotak Mahindra Bank Ltd.', exchange: 'NSE', sector: 'Banking', currentPrice: 1842.90, priceChange: 2.76, priceChangePercent: 0.15, signal: 'HOLD', confidence: 56, targetPrice: 1880, expectedReturn: 2.01, risk: 'MEDIUM' },
  { ticker: 'WIPRO.NS', symbol: 'WIPRO', companyName: 'Wipro Ltd.', exchange: 'NSE', sector: 'IT', currentPrice: 462.30, priceChange: -5.73, priceChangePercent: -1.24, signal: 'SELL', confidence: 72, targetPrice: 420, expectedReturn: -9.14, risk: 'HIGH' },
  { ticker: 'TATASTEEL.NS', symbol: 'TATASTEEL', companyName: 'Tata Steel Ltd.', exchange: 'NSE', sector: 'Metals', currentPrice: 148.90, priceChange: -1.30, priceChangePercent: -0.87, signal: 'SELL', confidence: 69, targetPrice: 135, expectedReturn: -9.33, risk: 'HIGH' },
  { ticker: 'ADANIENT.NS', symbol: 'ADANIENT', companyName: 'Adani Enterprises Ltd.', exchange: 'NSE', sector: 'Energy', currentPrice: 3142.55, priceChange: -49.65, priceChangePercent: -1.58, signal: 'SELL', confidence: 71, targetPrice: 2850, expectedReturn: -9.31, risk: 'HIGH' },
]

const stocks = stockDefinitions.map((stock) => ({ ...stock, explanations: explanationsBySignal[stock.signal], technicalIndicators: buildIndicators(stock.currentPrice, stock.signal), priceHistory: buildPriceHistory(stock.ticker, stock.currentPrice) }))

export const stockUniverse = stocks
export const stockBySymbol = Object.fromEntries(stocks.map((stock) => [stock.symbol, stock]))
export const stockByTicker = Object.fromEntries(stocks.map((stock) => [stock.ticker, stock]))
export function getStockByRouteTicker(routeTicker) { const normalized = decodeURIComponent(routeTicker ?? '').toUpperCase(); return stockByTicker[normalized] ?? stockBySymbol[normalized.replace(/\.NS$/, '')] }
export function toRouteTicker(symbol) { return stockBySymbol[symbol]?.ticker ?? `${symbol}.NS` }
export function getRecommendations() { return stocks.map((stock) => ({ ticker: stock.ticker, symbol: stock.symbol, companyName: stock.companyName, exchange: stock.exchange, sector: stock.sector, currentPrice: stock.currentPrice, priceChange: stock.priceChange, priceChangePercent: stock.priceChangePercent, signal: stock.signal, confidence: stock.confidence, targetPrice: stock.targetPrice, expectedReturn: stock.expectedReturn, risk: stock.risk })) }
export function getStockDetail(ticker) { return stocks.find((stock) => stock.ticker === ticker) ?? null }
export function getStockNews(ticker) { 
  // News logic temporarily disabled until dashboard integration
  return { 
    totalNews: 0, 
    sentiment: { POSITIVE: 0, NEUTRAL: 0, NEGATIVE: 0 } 
  }
}
export function getSectors() { return [...new Set(stocks.map((stock) => stock.sector))].sort() }
export function getAllStocks() { return getRecommendations() }
export function getSignals() { return [...new Set(stocks.map((stock) => stock.signal))] }
export function getRiskLevels() { return [...new Set(stocks.map((stock) => stock.risk))] }
