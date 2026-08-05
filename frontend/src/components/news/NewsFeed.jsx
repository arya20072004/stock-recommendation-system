import { NewsItem } from './NewsItem'

export function NewsFeed({ articles }) {
  return <div className="news-feed">{articles.map((article) => <NewsItem article={article} key={article.id} />)}</div>
}
