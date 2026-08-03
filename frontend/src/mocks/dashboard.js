export const dashboardData = {
  marketSummary: [
    { label: 'NIFTY 50', value: 24842.65, change: 0.72 },
    { label: 'SENSEX', value: 81903.24, change: 0.58 },
    { label: 'BANK NIFTY', value: 54312.1, change: -0.14 },
    { label: 'MARKET STATUS', type: 'status', value: 'OPEN', detail: 'Demo session' },
  ],
  topRecommendation: {
    ticker: 'RELIANCE.NS',
    companyName: 'Reliance Industries Ltd.',
    signal: 'BUY',
    currentPrice: 1412.3,
    dailyChange: 1.42,
    confidence: 87,
    targetPrice: 1530,
    expectedReturn: 8.35,
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
    { ticker: 'RELIANCE', price: 1412.3, change: 1.42, signal: 'BUY', sparkline: [12, 13, 12, 15, 14, 16] },
    { ticker: 'TCS', price: 3087.4, change: 0.71, signal: 'BUY', sparkline: [11, 12, 13, 12, 13, 14] },
    { ticker: 'SBIN', price: 812.1, change: -0.42, signal: 'HOLD', sparkline: [15, 14, 14, 13, 14, 13] },
    { ticker: 'ITC', price: 456.85, change: 0.16, signal: 'BUY', sparkline: [10, 10, 11, 10, 11, 11] },
    { ticker: 'INFY', price: 1598.75, change: -0.28, signal: 'HOLD', sparkline: [15, 14, 14, 15, 14, 14] },
  ],
  importantNews: [
    { id: 'demo-news-1', headline: 'Renewable energy capacity expansion plans enter the next planning stage.', source: 'Demo Financial Brief', publishedAt: '42m ago', tickers: ['RELIANCE'], sentiment: 'POSITIVE' },
    { id: 'demo-news-2', headline: 'Banking sector activity remains in focus ahead of the next market session.', source: 'Demo Market Wire', publishedAt: '1h ago', tickers: ['SBIN'], sentiment: 'NEUTRAL' },
    { id: 'demo-news-3', headline: 'Technology services outlook remains mixed across global demand signals.', source: 'Demo Research Desk', publishedAt: '2h ago', tickers: ['TCS', 'INFY'], sentiment: 'NEGATIVE' },
  ],
}
