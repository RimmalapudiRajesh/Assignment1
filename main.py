# main.py
from flask import Flask, request, jsonify
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base, NewsArticle
from config import DATABASE_URI
from celery_tasks.tasks import classify_category
import feedparser



app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://127.0.0.1:6379'
app.config['CELERY_RESULT_BACKEND'] = 'redis://127.0.0.1:6379'

engine = create_engine(DATABASE_URI)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

@app.route('/parse_and_store', methods=['POST'])
def parse_and_store():
    feed_url = request.json.get('feed_url')
    feed = parse_rss(feed_url)
    articles = []

    for entry in feed.entries:
        article = {
            'title': entry.title,
            'content': entry.summary,
            'pub_date': entry.published,
            'source_url': entry.link,
        }
        articles.append(article)

    store_articles(articles)
    return jsonify({"status": "success"})

def fetch_rss_data(feed_url):
    feed = feedparser.parse(feed_url)
    print("Feed Title:", feed.feed.title)
    for entry in feed.entries:
        print("Entry Title:", entry.title)
        print("Entry Link:", entry.link)
        print("Entry Published Date:", entry.published)
        print("Entry Summary:", entry.summary)
        print("\n")

# Define a list of RSS feed URLs
rss_feed_urls = [
    "http://rss.cnn.com/rss/cnn_topstories.rss",
    "http://qz.com/feed",
    "http://feeds.foxnews.com/foxnews/politics",
    "http://feeds.reuters.com/reuters/businessNews",
    "http://feeds.feedburner.com/NewshourWorld",
    "https://feeds.bbci.co.uk/news/world/asia/india/rs.xml"
]

# Fetch and print RSS data for each URL
for feed_url in rss_feed_urls:
    fetch_rss_data(feed_url)

# Search for feeds containing specific keywords
f1 = feedparser.search('Terrorism OR protest OR political unrest OR riot')
for entry in f1["entries"]:
    print(entry["title"])
    print(entry["link"])
    print(entry["summary"])

f2 = feedparser.search('positive OR uplifting')
for entry in f2["entries"]:
    print(entry["title"])
    print(entry["link"])
    print(entry["summary"])

f3 = feedparser.search('Natural Disaster')
for entry in f3["entries"]:
    print(entry["title"])
    print(entry["link"])
    print(entry["summary"])

f4 = feedparser.search('others')
for entry in f4["entries"]:
    print(entry["title"])
    print(entry["link"])
    print(entry["summary"])

def store_articles(articles):
    for article in articles:
        existing_article = session.query(NewsArticle).filter_by(title=article['title']).first()
        if not existing_article:
            db_article = NewsArticle(**article)
            session.add(db_article)
            classify_category.apply_async(args=[db_article.id])  # Asynchronous category classification

    session.commit()

if __name__ == "__main__":
    app.run(debug=True)
