import { CircleAlert } from 'lucide-react'
import { Button } from './Button'
import './common.css'

export function ErrorState({ title = 'Unable to load content', description, onRetry }) {
  return <section className="state state--error" role="alert"><CircleAlert aria-hidden="true" size={26} /><h2>{title}</h2>{description && <p>{description}</p>}{onRetry && <Button variant="secondary" onClick={onRetry}>Try again</Button>}</section>
}
