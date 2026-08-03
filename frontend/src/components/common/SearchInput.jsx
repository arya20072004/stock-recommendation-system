import { Search } from 'lucide-react'
import './common.css'

export function SearchInput({ label = 'Search', className = '', ...props }) {
  return (
    <label className={`search-input ${className}`.trim()}>
      <span className="sr-only">{label}</span>
      <Search aria-hidden="true" size={18} strokeWidth={2} />
      <input type="search" aria-label={label} {...props} />
    </label>
  )
}
