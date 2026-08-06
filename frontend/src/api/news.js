export async function fetchNews(params = {}) {
  const query = new URLSearchParams()
  if (params.ticker && params.ticker !== 'ALL') query.append('ticker', params.ticker)
  if (params.sentiment && params.sentiment !== 'ALL') query.append('sentiment', params.sentiment)
  if (params.page) query.append('page', params.page)
  if (params.limit) query.append('limit', params.limit)

  const response = await fetch(`/api/news?${query.toString()}`)
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}))
    throw { status: response.status, message: errorData.error || 'Failed to fetch news' }
  }
  return response.json()
}

export async function fetchNewsById(id) {
  const response = await fetch(`/api/news/${id}`)
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}))
    throw { status: response.status, message: errorData.error || 'Failed to fetch news article' }
  }
  return response.json()
}
