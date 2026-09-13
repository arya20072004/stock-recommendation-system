import { ChevronLeft, ChevronRight } from 'lucide-react'
import { Button } from './Button'
import './data-presentation.css'

export function Pagination({ currentPage, totalPages, onPageChange, className = '' }) {
  if (!totalPages || totalPages <= 1) return null

  return (
    <nav className={`pagination ${className}`.trim()} aria-label="Pagination">
      <Button 
        variant="ghost" 
        disabled={currentPage <= 1} 
        onClick={() => onPageChange(currentPage - 1)}
        aria-label="Previous page"
      >
        <ChevronLeft size={16} />
      </Button>
      
      <span className="pagination__info">
        Page {currentPage} of {totalPages}
      </span>
      
      <Button 
        variant="ghost" 
        disabled={currentPage >= totalPages} 
        onClick={() => onPageChange(currentPage + 1)}
        aria-label="Next page"
      >
        <ChevronRight size={16} />
      </Button>
    </nav>
  )
}
