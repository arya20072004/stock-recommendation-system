import './data-presentation.css'

export function FilterGroup({ title, children, onClear, clearLabel = 'Clear', className = '' }) {
  return (
    <div className={`filter-group ${className}`.trim()}>
      <div className="filter-group__header">
        {title && <h3 className="filter-group__title">{title}</h3>}
        {onClear && (
          <button type="button" onClick={onClear} className="filter-group__clear">
            {clearLabel}
          </button>
        )}
      </div>
      <div className="filter-group__controls">
        {children}
      </div>
    </div>
  )
}
