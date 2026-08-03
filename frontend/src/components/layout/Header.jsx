import { Bell, Menu, UserRound } from 'lucide-react'
import { Button } from '../common/Button'
import { SearchInput } from '../common/SearchInput'
import './layout.css'

export function Header({ onMenuToggle }) {
  return <header className="app-header"><div className="app-header__content"><Button className="menu-button" variant="ghost" onClick={onMenuToggle} aria-label="Open navigation" aria-expanded={undefined}><Menu size={20} aria-hidden="true" /></Button><SearchInput className="header-search" label="Search stocks" placeholder="Search stocks..." /><div className="header-actions"><Button variant="ghost" className="icon-button" aria-label="Notifications"><Bell size={19} aria-hidden="true" /></Button><Button variant="ghost" className="profile-button" aria-label="Profile"><UserRound size={18} aria-hidden="true" /><span>Profile</span></Button></div></div></header>
}
