import { Loader2 } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function PendingState({ title = 'Pending evaluation', description = 'This prediction is not yet legitimately evaluable.', action, icon: Icon = Loader2 }) {
  return <section className="state state--pending" aria-labelledby="pending-state-title"><Icon aria-hidden="true" size={26} className="spinner" /><h2 id="pending-state-title">{title}</h2><p>{description}</p>{action && <Button variant="secondary" onClick={action.onClick}>{action.label}</Button>}</section>
}
