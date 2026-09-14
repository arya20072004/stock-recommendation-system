import { lazy, Suspense } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { AppLayout } from './components/layout/AppLayout'
import { Dashboard } from './pages/Dashboard'
import { Recommendations } from './pages/Recommendations'
import { Screener } from './pages/Screener'
import { Stocks } from './pages/Stocks'
import { Watchlist } from './pages/Watchlist'
import { NotFoundPage, PlaceholderPage } from './pages/PlaceholderPage'
import { LoadingState } from './components/common/LoadingState'

const StockDetails = lazy(() => import('./pages/StockDetails').then(m => ({ default: m.StockDetails })))
const NewsIntelligence = lazy(() => import('./pages/NewsIntelligence').then(m => ({ default: m.NewsIntelligence })))
const NewsDetail = lazy(() => import('./pages/NewsDetail').then(m => ({ default: m.NewsDetail })))
const PredictionHistory = lazy(() => import('./pages/PredictionHistory').then(m => ({ default: m.PredictionHistory })))
const PredictionDetail = lazy(() => import('./pages/PredictionDetail').then(m => ({ default: m.PredictionDetail })))
const ModelIntelligence = lazy(() => import('./pages/ModelIntelligence').then(m => ({ default: m.ModelIntelligence })))

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

const SuspenseWrapper = ({ children }) => (
  <Suspense fallback={<LoadingState label="Loading..." />}>
    {children}
  </Suspense>
)

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/stocks" element={<Stocks />} />
          <Route path="/stocks/:ticker" element={<SuspenseWrapper><StockDetails /></SuspenseWrapper>} />
          <Route path="/screener" element={<Screener />} />
          <Route path="/watchlist" element={<Watchlist />} />
          <Route path="/recommendations" element={<Recommendations />} />
          <Route path="/news" element={<SuspenseWrapper><NewsIntelligence /></SuspenseWrapper>} />
          <Route path="/news/:newsId" element={<SuspenseWrapper><NewsDetail /></SuspenseWrapper>} />
          <Route path="/predictions/history" element={<SuspenseWrapper><PredictionHistory /></SuspenseWrapper>} />
          <Route path="/predictions/:id" element={<SuspenseWrapper><PredictionDetail /></SuspenseWrapper>} />
          <Route path="/model" element={<SuspenseWrapper><ModelIntelligence /></SuspenseWrapper>} />
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
