import { Skeleton } from './Skeleton'
import './common.css'

export function LoadingState({ label = 'Loading content' }) {
  return <div className="state state--loading" aria-label={label} role="status"><Skeleton className="state__skeleton state__skeleton--title" /><Skeleton className="state__skeleton" /><Skeleton className="state__skeleton state__skeleton--short" /></div>
}
