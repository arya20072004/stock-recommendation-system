import './common.css'

export function Badge({ children, tone = 'neutral', className = '', ...props }) {
  return <span className={`badge badge--${tone} ${className}`.trim()} {...props}>{children}</span>
}
