export async function fetchAllTickers() {
  const response = await fetch('/api/stocks')
  if (!response.ok) throw new Error('Failed to fetch stock list')
  return await response.json()
}

export async function fetchStocksSummary() {
  const response = await fetch('/api/stocks/summary')
  
  if (!response.ok) {
    throw new Error('Failed to fetch stocks summary')
  }
  
  const result = await response.json()
  return result
}

export async function fetchStockDetails(ticker, range = '1Y') {
  const response = await fetch(`/api/stocks/${encodeURIComponent(ticker)}/details?range=${encodeURIComponent(range)}`)
  
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}))
    const error = new Error(errorData.error || 'Failed to fetch stock details')
    error.status = response.status
    throw error
  }
  
  return await response.json()
}
