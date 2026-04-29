# ML-Powered Stock Recommendation System

An advanced machine learning-based stock analysis and recommendation system that provides intelligent BUY/HOLD/SELL recommendations for Nifty 50 stocks using technical indicators, market data, and news sentiment analysis.

## 🚀 Features

- **Real-time Stock Analysis**: Live price data and technical indicators for Nifty 50 stocks
- **Machine Learning Models**: XGBoost-based prediction models trained on historical data
- **Sentiment Analysis**: News article sentiment integration for enhanced predictions
- **Interactive Web Dashboard**: Modern web interface with real-time charts
- **Backtesting Engine**: Historical performance analysis and strategy validation
- **Comprehensive Data Pipeline**: Automated data collection, processing, and model training
- **MongoDB Integration**: Scalable data storage for historical prices and news

## 📊 Supported Stocks

The system analyzes all Nifty 50 stocks including:
- Adani Enterprises, Adani Ports, Apollo Hospitals
- Asian Paints, Axis Bank, Bajaj Auto, Bajaj Finance, Bajaj Finserv
- Bharat Petroleum, Bharti Airtel, and 40+ more major Indian companies

## 🛠️ Technology Stack

- **Backend**: Python Flask, MongoDB
- **Machine Learning**: XGBoost, scikit-learn, imbalanced-learn
- **Data Processing**: pandas, pandas-ta, yfinance
- **Sentiment Analysis**: NLTK
- **Frontend**: HTML, Tailwind CSS, Lightweight Charts
- **APIs**: Alpha Vantage, NewsAPI

## 📋 Prerequisites

- Python 3.8+
- MongoDB (local or cloud instance)
- API Keys for Alpha Vantage and NewsAPI

## 🚀 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/stock-recommendations.git
   cd stock-recommendations
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**

   Create a `.env` file in the root directory:
   ```env
   MONGO_URI=mongodb://localhost:27017/
   ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
   NEWS_API_KEY=your_news_api_key
   ```

5. **Start MongoDB**
   Make sure MongoDB is running on your system.

## 📊 Data Pipeline Setup

Run the complete data collection and model training pipeline:

```bash
python run_pipeline.py
```

This will:
1. Collect historical stock data for Nifty 50 stocks
2. Gather recent news articles
3. Perform sentiment analysis on news
4. Train ML models for each stock
5. Save models and feature lists

**Note**: The pipeline processes 10 stocks by default to avoid API rate limits. Modify `STOCKS_TO_PROCESS` in `run_pipeline.py` to process all 50 stocks.

## 🌐 Running the Web Application

1. **Start the Flask server**
   ```bash
   flask run
   ```

2. **Open your browser**
   Navigate to `http://localhost:5000`

3. **Select a stock** from the dropdown to view:
   - Interactive price charts
   - Technical indicators (RSI, MACD, Bollinger Bands)
   - ML-based BUY/HOLD/SELL recommendation

## 📁 Project Structure

```
stock-recommendations/
├── app.py                 # Main Flask application
├── run_pipeline.py        # Data collection and training pipeline
├── data_collector.py      # Historical stock data collection
├── news_collector.py      # News article collection
├── sentiment_analyzer.py  # News sentiment analysis
├── ml_trainer.py         # Machine learning model training
├── backtester.py         # Strategy backtesting
├── analysis.py           # Data analysis utilities
├── config.py             # API keys and configuration
├── nifty50.py            # Nifty 50 stock list
├── db.py                 # Database utilities
├── requirements.txt      # Python dependencies
├── models/               # Trained ML models (.joblib files)
├── features/             # Feature lists for each stock (.json files)
├── templates/
│   └── index.html        # Web dashboard template
└── .env                  # Environment variables (create this)
```

## 🔧 Configuration

### API Keys
- **Alpha Vantage**: Get your free API key from [alphavantage.co](https://www.alphavantage.co/support/#api-key)
- **NewsAPI**: Get your API key from [newsapi.org](https://newsapi.org/)

### Database
The application uses MongoDB with two main collections:
- `historical_data`: Stock price data
- `news_articles`: News articles with sentiment scores

### Model Configuration
Models are trained using:
- Technical indicators (RSI, MACD, Bollinger Bands, ATR)
- Market correlation metrics
- News sentiment scores
- Price momentum features

## 📈 Backtesting

Test your strategies with historical data:

```python
from backtester import Backtester

# Initialize backtester
bt = Backtester()

# Run backtest for a specific stock
results = bt.run_backtest('RELIANCE.NS', initial_capital=100000)

# View performance metrics
print(results.summary())
```

## 🤖 ML Model Details

- **Algorithm**: XGBoost Classifier
- **Target Classes**: BUY (2), HOLD (1), SELL (0)
- **Features**: 15+ technical and sentiment indicators
- **Training Data**: Historical price data with labels based on future returns
- **Handling Imbalance**: SMOTE oversampling for minority classes

## ⚠️ Important Disclaimers

- **Not Financial Advice**: This system is for educational and research purposes only. All recommendations are generated by algorithms and should not be considered as financial advice.
- **Past Performance**: Historical performance does not guarantee future results.
- **Risk Warning**: Stock trading involves substantial risk of loss. Always do your own research and consult with financial professionals.
- **Data Accuracy**: While we strive for accuracy, data sources may have limitations or delays.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Nifty 50 data provided by Yahoo Finance
- News data from NewsAPI
- Technical analysis indicators from pandas-ta
- ML framework by XGBoost and scikit-learn

## 📞 Support

If you encounter any issues or have questions:
1. Check the existing issues on GitHub
2. Create a new issue with detailed information
3. Include error messages, Python version, and steps to reproduce

---

**Remember**: Always invest responsibly and never risk more than you can afford to lose.</content>
<<<<<<< HEAD
<parameter name="filePath">c:\Users\aryab\Coding\stock_recommendations\README.md
=======
<parameter name="filePath">c:\Users\aryab\Coding\stock_recommendations\README.md
>>>>>>> 91d31e8afbfd2eb4397f1c46851a2c203b84ff68
