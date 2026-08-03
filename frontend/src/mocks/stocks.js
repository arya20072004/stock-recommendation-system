// ── Demo Stock Universe for Phase 3 ──
// All mock data for Recommendations Explorer and Stock Details.
// Values for RELIANCE, TCS, SBIN, ITC, INFY match src/mocks/dashboard.js exactly.
// This file is the single source of truth for Phase 3 mock data.

// ── Deterministic chart data generation ──

function createRng(seed) {
  let h = 0
  for (let i = 0; i < seed.length; i++) h = ((h << 5) - h + seed.charCodeAt(i)) | 0
  let s = Math.abs(h) || 1
  return () => { s = (s * 16807) % 2147483647; return s / 2147483647 }
}

function generateSeries(seed, currentPrice, count, labelFn) {
  const rng = createRng(seed)
  const data = []
  let price = currentPrice * (1 - (rng() * 0.08 + 0.02))
  for (let i = 0; i < count; i++) {
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
    '1D': generateSeries(ticker + '1D', currentPrice, 13, i => {
      const m = 9 * 60 + 15 + i * 30
      return `${Math.floor(m / 60)}:${(m % 60).toString().padStart(2, '0')}`
    }),
    '1W': generateSeries(ticker + '1W', currentPrice, 5, i => ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'][i]),
    '1M': generateSeries(ticker + '1M', currentPrice, 22, i => `${i + 1} Jul`),
    '3M': generateSeries(ticker + '3M', currentPrice, 13, i => `W${i + 1}`),
    '6M': generateSeries(ticker + '6M', currentPrice, 26, i => `W${i + 1}`),
    '1Y': generateSeries(ticker + '1Y', currentPrice, 12, i => ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug'][i]),
  }
}

// ── Technical indicator templates by signal ──

function formatInr(value) {
  return '₹' + Math.round(value).toLocaleString('en-IN')
}

function buildIndicators(currentPrice, signal) {
  if (signal === 'BUY') return [
    { name: 'RSI', value: '61.4', interpretation: 'Bullish' },
    { name: 'MACD', value: '+8.42', interpretation: 'Bullish' },
    { name: 'SMA 20', value: formatInr(currentPrice * 0.984), interpretation: 'Above' },
    { name: 'SMA 50', value: formatInr(currentPrice * 0.952), interpretation: 'Above' },
    { name: 'Volatility', value: '18.3%', interpretation: 'Moderate' },
  ]
  if (signal === 'SELL') return [
    { name: 'RSI', value: '38.2', interpretation: 'Bearish' },
    { name: 'MACD', value: '-5.63', interpretation: 'Bearish' },
    { name: 'SMA 20', value: formatInr(currentPrice * 1.018), interpretation: 'Below' },
    { name: 'SMA 50', value: formatInr(currentPrice * 1.052), interpretation: 'Below' },
    { name: 'Volatility', value: '32.4%', interpretation: 'High' },
  ]
  return [
    { name: 'RSI', value: '52.1', interpretation: 'Neutral' },
    { name: 'MACD', value: '+1.84', interpretation: 'Neutral' },
    { name: 'SMA 20', value: formatInr(currentPrice * 1.002), interpretation: 'Near' },
    { name: 'SMA 50', value: formatInr(currentPrice * 0.988), interpretation: 'Above' },
    { name: 'Volatility', value: '22.1%', interpretation: 'Moderate' },
  ]
}

// ── Explanation templates by signal ──

const explanationsBySignal = {
  BUY: [
    'Strong 20-day momentum',
    'RSI remains below overbought territory',
    'Price above 50-day moving average',
    'Positive volume trend',
    'Model probability exceeds BUY threshold',
  ],
  HOLD: [
    'Mixed short-term momentum signals',
    'RSI in neutral territory',
    'Price near key moving averages',
    'Volume shows no clear directional bias',
    'Model probability does not exceed action threshold',
  ],
  SELL: [
    'Negative 20-day momentum',
    'RSI approaching oversold conditions',
    'Price below 50-day moving average',
    'Declining volume trend',
    'Model probability exceeds SELL threshold',
  ],
}

// ── Stock definitions ──
// Fields: ticker, symbol, companyName, exchange, sector,
//         currentPrice, priceChange, priceChangePercent,
//         signal, confidence, targetPrice, expectedReturn, risk

const stockDefinitions = [
  // ── Dashboard-consistent stocks (match dashboard.js exactly) ──
  { ticker: 'RELIANCE.NS', symbol: 'RELIANCE', companyName: 'Reliance Industries Ltd.', exchange: 'NSE', sector: 'Energy', currentPrice: 1412.30, priceChange: 19.80, priceChangePercent: 1.42, signal: 'BUY', confidence: 87, targetPrice: 1530, expectedReturn: 8.35, risk: 'MEDIUM' },
  { ticker: 'TCS.NS', symbol: 'TCS', companyName: 'Tata Consultancy Services Ltd.', exchange: 'NSE', sector: 'IT', currentPrice: 3087.40, priceChange: 21.80, priceChangePercent: 0.71, signal: 'BUY', confidence: 82, targetPrice: 3320, expectedReturn: 7.53, risk: 'LOW' },
  { ticker: 'INFY.NS', symbol: 'INFY', companyName: 'Infosys Ltd.', exchange: 'NSE', sector: 'IT', currentPrice: 1598.75, priceChange: -4.48, priceChangePercent: -0.28, signal: 'HOLD', confidence: 62, targetPrice: 1650, expectedReturn: 3.21, risk: 'MEDIUM' },
  { ticker: 'SBIN.NS', symbol: 'SBIN', companyName: 'State Bank of India', exchange: 'NSE', sector: 'Banking', currentPrice: 812.10, priceChange: -3.41, priceChangePercent: -0.42, signal: 'HOLD', confidence: 58, targetPrice: 830, expectedReturn: 2.20, risk: 'HIGH' },
  { ticker: 'ITC.NS', symbol: 'ITC', companyName: 'ITC Ltd.', exchange: 'NSE', sector: 'FMCG', currentPrice: 456.85, priceChange: 0.73, priceChangePercent: 0.16, signal: 'BUY', confidence: 74, targetPrice: 495, expectedReturn: 8.35, risk: 'LOW' },

  // ── Additional Phase 3 stocks ──
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

// ── News items ──
// First 3 match dashboard.js importantNews identically.

const allNews = [
  { id: 'demo-news-1', headline: 'Renewable energy capacity expansion plans enter the next planning stage.', source: 'Demo Financial Brief', publishedAt: '42m ago', tickers: ['RELIANCE'], sentiment: 'POSITIVE' },
  { id: 'demo-news-2', headline: 'Banking sector activity remains in focus ahead of the next market session.', source: 'Demo Market Wire', publishedAt: '1h ago', tickers: ['SBIN'], sentiment: 'NEUTRAL' },
  { id: 'demo-news-3', headline: 'Technology services outlook remains mixed across global demand signals.', source: 'Demo Research Desk', publishedAt: '2h ago', tickers: ['TCS', 'INFY'], sentiment: 'NEGATIVE' },
  { id: 'demo-news-4', headline: 'Quarterly market expectations remain stable ahead of results season.', source: 'Demo Market Wire', publishedAt: '3h ago', tickers: ['RELIANCE', 'HDFCBANK'], sentiment: 'NEUTRAL' },
  { id: 'demo-news-5', headline: 'Pharmaceutical sector sees renewed institutional interest.', source: 'Demo Financial Brief', publishedAt: '4h ago', tickers: ['SUNPHARMA'], sentiment: 'POSITIVE' },
  { id: 'demo-news-6', headline: 'Telecom infrastructure spending expected to accelerate in coming quarters.', source: 'Demo Research Desk', publishedAt: '5h ago', tickers: ['BHARTIARTL'], sentiment: 'POSITIVE' },
  { id: 'demo-news-7', headline: 'Auto sector faces mixed demand signals in domestic market.', source: 'Demo Market Wire', publishedAt: '6h ago', tickers: ['MARUTI'], sentiment: 'NEUTRAL' },
  { id: 'demo-news-8', headline: 'Metal prices decline as global demand outlook softens.', source: 'Demo Financial Brief', publishedAt: '7h ago', tickers: ['TATASTEEL'], sentiment: 'NEGATIVE' },
  { id: 'demo-news-9', headline: 'IT services margins under pressure from wage inflation and attrition.', source: 'Demo Research Desk', publishedAt: '8h ago', tickers: ['WIPRO', 'TCS', 'INFY'], sentiment: 'NEGATIVE' },
  { id: 'demo-news-10', headline: 'Infrastructure development projects gain momentum in current quarter.', source: 'Demo Financial Brief', publishedAt: '10h ago', tickers: ['ADANIENT'], sentiment: 'POSITIVE' },
  { id: 'demo-news-11', headline: 'FMCG volumes recover as rural demand improves.', source: 'Demo Market Wire', publishedAt: '12h ago', tickers: ['ITC'], sentiment: 'POSITIVE' },
  { id: 'demo-news-12', headline: 'Banking sector NPAs continue downward trend across major lenders.', source: 'Demo Research Desk', publishedAt: '14h ago', tickers: ['HDFCBANK', 'AXISBANK', 'KOTAKBANK', 'SBIN'], sentiment: 'POSITIVE' },
]

// ── Build full stock objects with generated detail data ──

const stocks = stockDefinitions.map(def => ({
  ...def,
  explanations: explanationsBySignal[def.signal] ?? explanationsBySignal.HOLD,
  technicalIndicators: buildIndicators(def.currentPrice, def.signal),
  priceHistory: buildPriceHistory(def.ticker, def.currentPrice),
}))

// ── Public API ──

/** Recommendation list for the Recommendations Explorer table */
export function getRecommendations() {
  return stocks.map(({ ticker, symbol, companyName, exchange, sector, currentPrice, priceChange, priceChangePercent, signal, confidence, targetPrice, expectedReturn, risk }) => ({
    ticker, symbol, companyName, exchange, sector, currentPrice, priceChange, priceChangePercent, signal, confidence, targetPrice, expectedReturn, risk,
  }))
}

/** Full stock detail for the Stock Details page. Returns null for unknown tickers. */
export function getStockDetail(ticker) {
  return stocks.find(s => s.ticker === ticker) ?? null
}

/** News items filtered by ticker symbol (e.g. 'RELIANCE') */
export function getStockNews(ticker) {
  const symbol = ticker.replace('.NS', '')
  return allNews.filter(n => n.tickers.includes(symbol))
}

/** Unique sector list for filter dropdowns */
export function getSectors() {
  return [...new Set(stocks.map(s => s.sector))].sort()
}
