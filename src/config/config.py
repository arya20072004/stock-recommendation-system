# config.py
import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
