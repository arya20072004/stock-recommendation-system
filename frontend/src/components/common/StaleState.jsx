import { Clock } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function StaleState({ title = 'Data may be stale', description = 'This information has not been updated recently.', action, icon: Icon = Clock }) {
  return <section className="state state--stale" aria-labelledby="stale-state-title"><Icon aria-hidden="true" size={26} /><h2 id="stale-state-title">{title}</h2><p>{description}</p>{action && <Button variant="secondary" onClick={action.onClick}>{action.label}</Button>}</section>
}
