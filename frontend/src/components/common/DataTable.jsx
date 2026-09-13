import './data-presentation.css'

export function DataTable({ columns, data, keyField = 'id', className = '' }) {
  if (!data || data.length === 0) {
    return null
  }

  return (
    <div className={`data-table-container ${className}`.trim()}>
      <table className="data-table">
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col.key} className={col.align ? `align-${col.align}` : ''}>
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={row[keyField] ?? i}>
              {columns.map(col => (
                <td key={col.key} className={col.align ? `align-${col.align}` : ''}>
                  {col.render ? col.render(row[col.key], row) : row[col.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
