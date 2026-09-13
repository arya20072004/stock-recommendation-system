import { CloudOff } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function UnavailableState({ title = 'Data unavailable', description = 'This information is currently not available.', action, icon: Icon = CloudOff }) {
  return <section className="state state--unavailable" aria-labelledby="unavailable-state-title"><Icon aria-hidden="true" size={26} /><h2 id="unavailable-state-title">{title}</h2><p>{description}</p>{action && <Button variant="secondary" onClick={action.onClick}>{action.label}</Button>}</section>
}
