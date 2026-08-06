export async function fetchRecommendations() {
  const response = await fetch('/api/recommendations')
  
  if (!response.ok) {
    throw new Error('Failed to fetch recommendations')
  }
  
  const result = await response.json()
  return result
}
