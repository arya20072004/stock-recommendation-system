import { Inbox } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function EmptyState({ title, description, action, icon: Icon = Inbox }) {
  return <section className="state" aria-labelledby="empty-state-title"><Icon aria-hidden="true" size={26} /><h2 id="empty-state-title">{title}</h2><p>{description}</p>{action && <Button variant="secondary" onClick={action.onClick}>{action.label}</Button>}</section>
}
