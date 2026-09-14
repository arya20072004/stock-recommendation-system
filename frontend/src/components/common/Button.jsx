import './common.css'

export function Button({ as: Component = 'button', variant = 'primary', className = '', type = 'button', ...props }) {
  const isButton = Component === 'button'
  
  return (
    <Component 
      type={isButton ? type : undefined} 
      className={`button button--${variant} ${className}`.trim()} 
      {...props} 
    />
  )
}
