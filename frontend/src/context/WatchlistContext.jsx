import { createContext, useCallback, useContext, useEffect, useState } from 'react'

const STORAGE_KEY = 'stockintel_watchlist'
const WatchlistContext = createContext(null)

function loadWatchlist() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(t => typeof t === 'string')
  } catch {
    return []
  }
}

function saveWatchlist(tickers) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(tickers))
  } catch { /* localStorage may be full or unavailable */ }
}

export function WatchlistProvider({ children }) {
  const [tickers, setTickers] = useState(() => loadWatchlist())

  useEffect(() => { saveWatchlist(tickers) }, [tickers])

  const addToWatchlist = useCallback((ticker) => {
    setTickers(prev => prev.includes(ticker) ? prev : [...prev, ticker])
  }, [])

  const removeFromWatchlist = useCallback((ticker) => {
    setTickers(prev => prev.filter(t => t !== ticker))
  }, [])

  const isInWatchlist = useCallback((ticker) => tickers.includes(ticker), [tickers])

  const toggleWatchlist = useCallback((ticker) => {
    setTickers(prev => prev.includes(ticker) ? prev.filter(t => t !== ticker) : [...prev, ticker])
  }, [])

  return (
    <WatchlistContext value={{ tickers, addToWatchlist, removeFromWatchlist, isInWatchlist, toggleWatchlist }}>
      {children}
    </WatchlistContext>
  )
}

export function useWatchlist() {
  const ctx = useContext(WatchlistContext)
  if (!ctx) throw new Error('useWatchlist must be used within WatchlistProvider')
  return ctx
}
