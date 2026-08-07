const currencyFormatter = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', minimumFractionDigits: 2, maximumFractionDigits: 2 })
const marketNumberFormatter = new Intl.NumberFormat('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export const formatCurrency = (value) => {
  if (value === null || value === undefined || isNaN(value) || value === '') return '—'
  return currencyFormatter.format(value)
}

export const formatMarketNumber = (value) => {
  if (value === null || value === undefined || isNaN(value) || value === '') return '—'
  return marketNumberFormatter.format(value)
}

export const formatPercentage = (value) => {
  if (value === null || value === undefined || isNaN(value) || value === '') return '—'
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

// Keep formatPercent for backwards compatibility if used elsewhere
export const formatPercent = formatPercentage;

export const directionForValue = (value) => {
  if (value === null || value === undefined || isNaN(value) || value === '') return 'neutral'
  return value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral'
}

const sectorMapping = {
  'AutomobileAndAutoComponents': 'Automobile & Auto Components',
  'CapitalGoods': 'Capital Goods',
  'Chemicals': 'Chemicals',
  'Construction': 'Construction',
  'ConstructionMaterials': 'Construction Materials',
  'ConsumerDurables': 'Consumer Durables',
  'ConsumerServices': 'Consumer Services',
  'Diversified': 'Diversified',
  'FastMovingConsumerGoods': 'Fast Moving Consumer Goods',
  'FinancialServices': 'Financial Services',
  'Healthcare': 'Healthcare',
  'InformationTechnology': 'Information Technology',
  'MediaEntertainmentAndPublication': 'Media, Entertainment & Publication',
  'MetalsAndMining': 'Metals & Mining',
  'OilGasAndConsumableFuels': 'Oil, Gas & Consumable Fuels',
  'PowerUtilities': 'Power Utilities',
  'Realty': 'Realty',
  'Services': 'Services',
  'Telecommunication': 'Telecommunication',
  'Textiles': 'Textiles'
};

export const formatSectorName = (sector) => {
  if (!sector) return '—'
  if (sectorMapping[sector]) return sectorMapping[sector]
  
  // Fallback for unknown sectors: simple CamelCase split
  return sector.replace(/([A-Z])/g, ' $1').trim()
}
