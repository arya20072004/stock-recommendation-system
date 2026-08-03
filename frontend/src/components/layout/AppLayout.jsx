import { useState } from 'react'
import { Outlet } from 'react-router-dom'
import { Header } from './Header'
import { Sidebar } from './Sidebar'
import './layout.css'

export function AppLayout() {
  const [isSidebarOpen, setSidebarOpen] = useState(false)
  const closeSidebar = () => setSidebarOpen(false)
  return <div className="app-shell"><Sidebar isOpen={isSidebarOpen} onClose={closeSidebar} /><div className="app-shell__main"><Header onMenuToggle={() => setSidebarOpen(true)} />{isSidebarOpen && <button className="sidebar-backdrop" onClick={closeSidebar} aria-label="Close navigation" />}<main className="app-content"><Outlet /></main></div></div>
}
