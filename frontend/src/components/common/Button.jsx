import './common.css'

export function Button({ variant = 'primary', className = '', type = 'button', ...props }) {
  return <button type={type} className={`button button--${variant} ${className}`.trim()} {...props} />
}
