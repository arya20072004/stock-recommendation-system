import './common.css'

export function Skeleton({ className = '', ...props }) {
  return <span aria-hidden="true" className={`skeleton ${className}`.trim()} {...props} />
}
