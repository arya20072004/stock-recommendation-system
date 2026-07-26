export const fetchStocks = async () => {
  const response = await fetch('/api/stocks');
  if (!response.ok) {
    throw new Error('Failed to fetch stocks');
  }
  return response.json();
};

export const fetchStockData = async (ticker) => {
  const response = await fetch(`/api/stocks/${ticker}`);
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.error || 'Failed to fetch stock data');
  }
  return response.json();
};

export const fetchPortfolio = async () => {
  const response = await fetch('/api/portfolio');
  if (!response.ok) {
    throw new Error('Failed to fetch portfolio');
  }
  return response.json();
};
