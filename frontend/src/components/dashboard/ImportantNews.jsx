import { ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { SentimentBadge } from '../news/SentimentBadge'

export function ImportantNews({ stories }) {
  return <section aria-labelledby="important-news-heading"><div className="section-heading"><div><h2 id="important-news-heading">Important news</h2><p>Supplementary demo intelligence</p></div><Link className="section-link" to="/news">View all news <ChevronRight aria-hidden="true" size={15} /></Link></div><Card className="important-news">{stories.map((story) => <article className="news-item" key={story.id}><div className="news-item__heading"><SentimentBadge sentiment={story.sentiment} /><time>{story.publishedAt}</time></div><h3>{story.headline}</h3><div className="news-item__meta"><span>{story.source}</span><span>{story.tickers.join(' · ')}</span></div></article>)}</Card></section>
}
