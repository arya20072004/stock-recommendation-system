import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { AppLayout } from './components/layout/AppLayout'
import { Dashboard } from './pages/Dashboard'
import { Recommendations } from './pages/Recommendations'
import { NewsDetail } from './pages/NewsDetail'
import { NewsIntelligence } from './pages/NewsIntelligence'
import { Screener } from './pages/Screener'
import { StockDetails } from './pages/StockDetails'
import { Stocks } from './pages/Stocks'
import { Watchlist } from './pages/Watchlist'
import { NotFoundPage, PlaceholderPage } from './pages/PlaceholderPage'
import { PredictionHistory } from './pages/PredictionHistory'
import { PredictionDetail } from './pages/PredictionDetail'

const pages = {
  news: {
    title: 'News Intelligence',
    description: 'News and sentiment intelligence will be implemented in a later phase.',
  },
  history: {
    title: 'Prediction History',
    description: 'Historical model performance will be implemented in a later phase.',
  },
  model: {
    title: 'Model Intelligence',
    description: 'Model transparency will be implemented in a later phase.',
  },
  portfolio: {
    title: 'Portfolio',
    description: 'Portfolio functionality will be implemented in a later phase.',
  },
  settings: {
    title: 'Settings',
    description: 'Application settings will be implemented in a later phase.',
  },
}

const placeholder = (page) => <PlaceholderPage {...page} />

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/stocks" element={<Stocks />} />
          <Route path="/stocks/:ticker" element={<StockDetails />} />
          <Route path="/screener" element={<Screener />} />
          <Route path="/watchlist" element={<Watchlist />} />
          <Route path="/recommendations" element={<Recommendations />} />
          <Route path="/news" element={<NewsIntelligence />} />
          <Route path="/news/:newsId" element={<NewsDetail />} />
          <Route path="/predictions/history" element={<PredictionHistory />} />
          <Route path="/predictions/:id" element={<PredictionDetail />} />
          <Route path="/model" element={placeholder(pages.model)} />
          <Route path="/portfolio" element={placeholder(pages.portfolio)} />
          <Route path="/settings" element={placeholder(pages.settings)} />
          <Route path="/not-found" element={<NotFoundPage />} />
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="*" element={<Navigate to="/not-found" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
