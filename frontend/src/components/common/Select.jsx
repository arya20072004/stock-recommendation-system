import './common.css'

export function Select({ label, value, onChange, options, className = '' }) {
  return (
    <label className={`select-wrapper ${className}`.trim()}>
      <span className="sr-only">{label}</span>
      <select value={value} onChange={e => onChange(e.target.value)} className="select">
        {options.map(opt => <option key={opt.value} value={opt.value}>{opt.label}</option>)}
      </select>
    </label>
  )
}
