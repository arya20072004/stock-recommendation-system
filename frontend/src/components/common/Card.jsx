import './common.css'

export function Card({ children, hoverable = false, className = '', ...props }) {
  return <section className={`card ${hoverable ? 'card--hoverable' : ''} ${className}`.trim()} {...props}>{children}</section>
}
