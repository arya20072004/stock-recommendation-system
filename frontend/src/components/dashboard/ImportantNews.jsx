import { ChevronRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card } from '../common/Card'
import { SentimentIndicator } from '../common/SentimentIndicator'

export function ImportantNews({ stories = [], isStale = false }) {
  return (
    <div className="dashboard-card-wrapper">
      <div className="section-heading">
        <p className="news-context-label">{isStale ? 'Historical market context' : 'Latest market context'}</p>
        <Link className="section-link" to="/news">View all news <ChevronRight aria-hidden="true" size={15} /></Link>
      </div>
      {stories.length === 0 ? (
        <Card className="important-news important-news--empty">
          <p>No news available.</p>
        </Card>
      ) : (
        <Card className="important-news">
          {stories.map((story) => (
            <article className="news-item" key={story.id}>
              <div className="news-item__heading">
                <SentimentIndicator sentiment={story.sentiment} />
                <time>{new Date(story.published_at).toLocaleDateString()}</time>
              </div>
              <Link to={`/news/${story.id}`} className="news-item__headline">
                <h3>{story.headline}</h3>
              </Link>
              <div className="news-item__meta">
                <span>{story.source}</span>
                {story.tickers && story.tickers.length > 0 && <span>{story.tickers.join(' · ')}</span>}
              </div>
            </article>
          ))}
        </Card>
      )}
    </div>
  )
}
