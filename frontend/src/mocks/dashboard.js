import { demoNews } from './news'
import { stockBySymbol } from './stocks'

const reliance = stockBySymbol.RELIANCE
const tcs = stockBySymbol.TCS
const sbin = stockBySymbol.SBIN
const itc = stockBySymbol.ITC
const infy = stockBySymbol.INFY

export const dashboardData = {
  marketSummary: [
    { label: 'NIFTY 50', value: 24842.65, change: 0.72 },
    { label: 'SENSEX', value: 81903.24, change: 0.58 },
    { label: 'BANK NIFTY', value: 54312.1, change: -0.14 },
    { label: 'MARKET STATUS', type: 'status', value: 'OPEN', detail: 'Demo session' },
  ],
  topRecommendation: {
    ticker: reliance.ticker,
    companyName: reliance.companyName,
    signal: reliance.signal,
    currentPrice: reliance.currentPrice,
    dailyChange: reliance.priceChangePercent,
    confidence: reliance.confidence,
    targetPrice: reliance.targetPrice,
    expectedReturn: reliance.expectedReturn,
    sparkline: [1378, 1383, 1376, 1391, 1388, 1403, 1398, 1412],
  },
  marketOverview: {
    label: 'NIFTY 50',
    change: 0.72,
    seriesByRange: {
      '1D': [{ time: '09:15', value: 24664 }, { time: '10:00', value: 24706 }, { time: '11:00', value: 24691 }, { time: '12:00', value: 24752 }, { time: '13:00', value: 24731 }, { time: '14:00', value: 24794 }, { time: '15:00', value: 24843 }],
      '1W': [{ time: 'Mon', value: 24512 }, { time: 'Tue', value: 24598 }, { time: 'Wed', value: 24541 }, { time: 'Thu', value: 24708 }, { time: 'Fri', value: 24843 }],
      '1M': [{ time: 'Week 1', value: 24190 }, { time: 'Week 2', value: 24386 }, { time: 'Week 3', value: 24298 }, { time: 'Week 4', value: 24843 }],
    },
  },
  recommendationSnapshot: { buy: 21, hold: 17, sell: 13 },
  watchlist: [
    { ticker: reliance.symbol, price: reliance.currentPrice, change: reliance.priceChangePercent, signal: reliance.signal, sparkline: [12, 13, 12, 15, 14, 16] },
    { ticker: tcs.symbol, price: tcs.currentPrice, change: tcs.priceChangePercent, signal: tcs.signal, sparkline: [11, 12, 13, 12, 13, 14] },
    { ticker: sbin.symbol, price: sbin.currentPrice, change: sbin.priceChangePercent, signal: sbin.signal, sparkline: [15, 14, 14, 13, 14, 13] },
    { ticker: itc.symbol, price: itc.currentPrice, change: itc.priceChangePercent, signal: itc.signal, sparkline: [10, 10, 11, 10, 11, 11] },
    { ticker: infy.symbol, price: infy.currentPrice, change: infy.priceChangePercent, signal: infy.signal, sparkline: [15, 14, 14, 15, 14, 14] },
  ],
  importantNews: demoNews.slice().sort((a, b) => b.relevance - a.relevance).slice(0, 3),
}
