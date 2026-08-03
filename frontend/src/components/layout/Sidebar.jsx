import { BarChart3, BookOpenCheck, BrainCircuit, BriefcaseBusiness, ChartNoAxesCombined, LayoutDashboard, ListFilter, Newspaper, Settings, Star, X } from 'lucide-react'
import { NavLink } from 'react-router-dom'
import { Button } from '../common/Button'
import './layout.css'

const groups = [
  { label: 'Overview', items: [{ to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard }] },
  { label: 'Discover', items: [{ to: '/stocks', label: 'Stocks', icon: ChartNoAxesCombined }, { to: '/screener', label: 'Screener', icon: ListFilter }, { to: '/watchlist', label: 'Watchlist', icon: Star }] },
  { label: 'Intelligence', items: [{ to: '/recommendations', label: 'Recommendations', icon: BookOpenCheck }, { to: '/news', label: 'News Intelligence', icon: Newspaper }, { to: '/predictions/history', label: 'Prediction History', icon: BarChart3 }, { to: '/model', label: 'Model Intelligence', icon: BrainCircuit }] },
  { label: 'Portfolio', items: [{ to: '/portfolio', label: 'Portfolio', icon: BriefcaseBusiness }] },
  { label: 'System', items: [{ to: '/settings', label: 'Settings', icon: Settings }] },
]

export function Sidebar({ isOpen, onClose }) {
  return <aside className={`sidebar ${isOpen ? 'sidebar--open' : ''}`} aria-label="Primary navigation"><div className="sidebar__brand"><span className="brand-mark" aria-hidden="true"><ChartNoAxesCombined size={19} /></span><span>StockIntel</span><Button variant="ghost" className="sidebar__close" onClick={onClose} aria-label="Close navigation"><X size={20} /></Button></div><nav className="sidebar__nav">{groups.map((group) => <section className="nav-group" key={group.label}><h2>{group.label}</h2>{group.items.map(({ to, label, icon: Icon }) => <NavLink key={to} to={to} end={to === '/dashboard'} onClick={onClose} className={({ isActive }) => `nav-item ${isActive ? 'nav-item--active' : ''}`}><Icon size={18} aria-hidden="true" /><span>{label}</span></NavLink>)}</section>)}</nav></aside>
}
