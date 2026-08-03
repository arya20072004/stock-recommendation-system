const currencyFormatter = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', minimumFractionDigits: 2, maximumFractionDigits: 2 })
const marketNumberFormatter = new Intl.NumberFormat('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export const formatCurrency = (value) => currencyFormatter.format(value)
export const formatMarketNumber = (value) => marketNumberFormatter.format(value)
export const formatPercent = (value) => `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
export const directionForValue = (value) => (value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral')
