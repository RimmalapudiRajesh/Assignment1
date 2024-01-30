# celery_tasks/tasks.py
from celery import Celery
from nltk.sentiment import SentimentIntensityAnalyzer
from sqlalchemy.orm import sessionmaker
from db.models import NewsArticle
from config import DATABASE_URI

app = Celery('tasks', broker='redis://127.0.0.1:6379')
app.config_from_object('config')

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

@app.task
def classify_category(article_id):
    article = session.query(NewsArticle).get(article_id)
    sia = SentimentIntensityAnalyzer()
    sentiment_score = sia.polarity_scores(article.content)['compound']

    if sentiment_score > 0.2:
        article.category = 'Positive/Uplifting'
    elif sentiment_score < -0.2:
        article.category = 'Terrorism/Protest/Political Unrest/Riot'
    else:
        article.category = 'Others'

    session.commit()
