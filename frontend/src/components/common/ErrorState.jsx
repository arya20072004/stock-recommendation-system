import { AlertCircle } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function ErrorState({ title = 'An error occurred', description = 'We encountered an error while loading this data.', action, icon: Icon = AlertCircle }) {
  return <section className="state state--error" aria-labelledby="error-state-title"><Icon aria-hidden="true" size={26} /><h2 id="error-state-title">{title}</h2><p>{description}</p>{action && <Button variant="secondary" onClick={action.onClick}>{action.label}</Button>}</section>
}
