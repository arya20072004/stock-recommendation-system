# ML-Powered Stock Recommendation System

An advanced machine learning-based stock analysis and recommendation system that provides intelligent BUY/HOLD/SELL recommendations for Nifty 50 stocks using technical indicators, market data, sector indices, options data, and news sentiment analysis.

## 🚀 Features

- **Real-time Stock Analysis**: Live price data and technical indicators for Nifty 50 stocks.
- **Machine Learning Models**: XGBoost-based prediction models trained on historical data, supporting native `.ubj` and `.joblib` formats.
- **Confidence Tiering**: Advanced confidence tier assignment (VERY_HIGH, HIGH, MEDIUM, LOW) based on F1-macro scores, prediction probabilities, and class margins to filter noisy signals.
- **Advanced Feature Engineering**: 
  - **Sector Indices**: Equal-weighted daily sector return indices built from the Nifty 500 universe for robust sector momentum features.
  - **Options Data**: Integration of NIFTY and BANKNIFTY Put-Call Ratios (PCR) based on Open Interest from NSE F&O Bhavcopy archives.
  - **Institutional Activity**: Tracks FII (Foreign Institutional Investors) and DII (Domestic Institutional Investors) data.
- **Sentiment Analysis**: News article sentiment integration for enhanced predictions.
- **Interactive Web Dashboard**: Modern web interface with real-time charts and a dedicated Portfolio Overview page sorted by conviction.
- **Backtesting Engine**: Historical performance analysis and strategy validation.
- **Comprehensive Data Pipeline**: Automated data collection, processing, and model training.
- **MongoDB Integration**: Scalable data storage for historical prices, sector indices, options data, and news.

## 📊 Supported Stocks

The system analyzes all Nifty 50 stocks including:
- Adani Enterprises, Adani Ports, Apollo Hospitals
- Asian Paints, Axis Bank, Bajaj Auto, Bajaj Finance, Bajaj Finserv
- Bharat Petroleum, Bharti Airtel, and 40+ more major Indian companies

## 🛠️ Technology Stack

- **Backend**: Python Flask, MongoDB
- **Machine Learning**: XGBoost, scikit-learn, imbalanced-learn
- **Data Processing**: pandas, pandas-ta, yfinance, numpy
- **Sentiment Analysis**: NLTK
- **Frontend**: HTML, Tailwind CSS, Lightweight Charts
- **APIs**: Alpha Vantage, NewsAPI, NSE F&O Archives

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

Run the complete data collection and model training pipeline. Before training, you need to build the foundational data collections:

1. **Collect historical stock data for Nifty 50 stocks**
   ```bash
   python -m src.data.collector
   ```

2. **Build Sector Indices (Nifty 500)**
   ```bash
   python -m src.data.sector_index_builder
   ```

3. **Build Options Put-Call Ratio (PCR) History**
   ```bash
   python -m src.data.pcr_builder
   ```

4. **Run the Main Pipeline (News, Sentiment, Training)**
   ```bash
   python scripts/run_pipeline.py
   ```

This will perform sentiment analysis on news, train ML models for each stock, and save models (`.ubj`/`.joblib`) and feature lists.

**Note**: The pipeline processes 10 stocks by default to avoid API rate limits. Modify `STOCKS_TO_PROCESS` in `scripts/run_pipeline.py` to process all 50 stocks.

## 🌐 Running the Web Application

1. **Start the Flask server**
   ```bash
   flask run
   ```

2. **Open your browser**
   Navigate to `http://localhost:5000`

3. **Dashboard Views**:
   - **Individual Stock View**: Select a stock to view interactive price charts, technical indicators, ML-based BUY/HOLD/SELL recommendation, and confidence metrics.
   - **Portfolio Overview** (`/portfolio`): View ML signals for all loaded stocks, intelligently sorted by algorithm conviction.

## 📁 Project Structure

```text
stock-recommendations/
├── app.py                     # Main Flask application with portfolio and stock routes
├── src/                       # Core python packages
│   ├── config/                # DB and config utilities
│   ├── data/                  # Data collection, indexing, and Nifty50 list
│   ├── features/              # Feature engineering and importance
│   ├── ml/                    # ML training, backtesting, confidence, and sentiment
│   └── analysis/              # Data analysis utilities
├── scripts/                   # Executable scripts
│   ├── run_pipeline.py        # Main data collection and training pipeline
│   └── migrate_models.py      # Model migration utilities
├── tests/                     # Unit and integration tests
├── data/raw/                  # Raw CSV data files
├── saved_models/              # Trained ML models (.ubj or .joblib)
├── saved_features/            # Feature lists for each stock (.json files)
├── reports/                   # Generated reports (e.g., MLStrategy.html)
├── templates/                 # HTML templates
│   ├── index.html             # Web dashboard template
│   └── portfolio.html         # Portfolio overview template
├── requirements.txt           # Python dependencies
└── .env                       # Environment variables
```

## 🔧 Configuration

### API Keys
- **Alpha Vantage**: Get your free API key from [alphavantage.co](https://www.alphavantage.co/support/#api-key)
- **NewsAPI**: Get your API key from [newsapi.org](https://newsapi.org/)

### Database
The application uses MongoDB with several main collections:
- `historical_data`: Stock price data
- `news_articles`: News articles with sentiment scores
- `sector_indices`: Sector performance data built from the Nifty 500
- `pcr_data`: Options Put-Call Ratio data

### Model Configuration
Models are trained using an extensive feature set:
- Technical indicators (RSI, MACD, Bollinger Bands, ATR, VWAP, OBV)
- Market correlation metrics (Nifty 50 SMA, etc.)
- Options Put-Call Ratio (PCR)
- Sector performance indices (Equal-weighted)
- FII/DII data tracking
- News sentiment scores
- Price momentum features

Models output a base prediction which is then filtered through `src.ml.confidence` to ensure only actionable predictions (MEDIUM confidence and above) are shown as BUY/SELL, otherwise defaulting to HOLD.

## 📈 Backtesting

Test your strategies with historical data:

```python
from src.ml.backtester import Backtester

# Initialize backtester
bt = Backtester()

# Run backtest for a specific stock
results = bt.run_backtest('RELIANCE.NS', initial_capital=100000)

# View performance metrics
print(results.summary())
```

## 🤖 ML Model Details

- **Algorithm**: XGBoost Classifier (saving in native `.ubj` format)
- **Target Classes**: BUY (2), HOLD (1), SELL (0)
- **Features**: Highly engineered dataset with technicals, options, sector momentum, and sentiment indicators
- **Confidence Gates**: Uses F1-macro, prediction certainty, and class margins to assign confidence tiers
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
- Options data from NSE India Archives
- News data from NewsAPI
- Technical analysis indicators from pandas-ta
- ML framework by XGBoost and scikit-learn

## 📞 Support

If you encounter any issues or have questions:
1. Check the existing issues on GitHub
2. Create a new issue with detailed information
3. Include error messages, Python version, and steps to reproduce

---

**Remember**: Always invest responsibly and never risk more than you can afford to lose.

---

## 🖥️ React Frontend

The default frontend for this application has been migrated to a React/Vite Single Page Application. It uses a modern trading terminal aesthetic with dark and light mode themes.

### Running the React app locally

1. Ensure your Flask backend is running on port 5000:
   ```bash
   python app.py
   ```

2. Open a new terminal and start the Vite dev server:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

3. The React app will run on `http://localhost:5173` and automatically proxy `/api` requests to the Flask backend.

### Assumptions

- The Flask app runs on `http://localhost:5000` (the Vite proxy targets this).
- The API responses conform strictly to the specified contract.
- The React application is intended to run as a separate dev server during development. (For production deployment, the Vite app should be built with `npm run build` and either hosted independently or integrated into Flask's static folders).
