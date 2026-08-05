export const demoPerformance = {
    total_predictions: 248,
    evaluated_predictions: 192,
    pending_predictions: 56,
    accuracy: 0.682,
    buy_accuracy: 0.71,
    hold_accuracy: 0.63,
    sell_accuracy: 0.67,
    average_confidence: 74.5
  };
  
  export const demoHistory = {
    total: 248,
    limit: 50,
    offset: 0,
    data: [
      {
        _id: "demo_1",
        symbol: "RELIANCE",
        market_date: "2026-08-05",
        prediction_timestamp: "2026-08-05T14:32:00Z",
        recommendation: "BUY",
        confidence: 78.4,
        confidence_tier: "HIGH",
        price_at_prediction: 1412.80,
        prediction_horizon: 10,
        actual_price: 1470.72,
        actual_return: 0.041,
        outcome: "CORRECT",
        model_version: "v2.3",
      },
      {
        _id: "demo_2",
        symbol: "TCS",
        market_date: "2026-08-04",
        prediction_timestamp: "2026-08-04T15:10:00Z",
        recommendation: "SELL",
        confidence: 62.1,
        confidence_tier: "MEDIUM",
        price_at_prediction: 3450.20,
        prediction_horizon: 10,
        actual_price: 3420.00,
        actual_return: -0.0087,
        outcome: "PENDING",
        model_version: "v2.3",
      },
      {
        _id: "demo_3",
        symbol: "HDFCBANK",
        market_date: "2026-07-25",
        prediction_timestamp: "2026-07-25T11:20:00Z",
        recommendation: "HOLD",
        confidence: 55.0,
        confidence_tier: "LOW",
        price_at_prediction: 1600.00,
        prediction_horizon: 10,
        actual_price: 1580.00,
        actual_return: -0.0125,
        outcome: "INCORRECT",
        model_version: "v2.2",
      },
      {
        _id: "demo_4",
        symbol: "INFY",
        market_date: "2026-07-20",
        prediction_timestamp: "2026-07-20T09:45:00Z",
        recommendation: "BUY",
        confidence: 82.5,
        confidence_tier: "HIGH",
        price_at_prediction: 1420.50,
        prediction_horizon: 10,
        actual_price: 1485.00,
        actual_return: 0.0454,
        outcome: "CORRECT",
        model_version: "v2.2",
      },
      {
        _id: "demo_5",
        symbol: "ICICIBANK",
        market_date: "2026-07-15",
        prediction_timestamp: "2026-07-15T10:30:00Z",
        recommendation: "SELL",
        confidence: 68.9,
        confidence_tier: "MEDIUM",
        price_at_prediction: 980.00,
        prediction_horizon: 10,
        actual_price: 940.00,
        actual_return: -0.0408,
        outcome: "CORRECT",
        model_version: "v2.1",
      }
    ]
  };
  
  export const demoPredictionDetail = {
    ...demoHistory.data[0],
    feature_snapshot: {
      "close_sma_20_ratio": 1.05,
      "rsi_14": 62.4,
      "macd": 12.5,
      "atr_pct": 0.021,
      "volume_sma_20_ratio": 1.4,
      "nifty_return_5d": 0.015,
      "sentiment": 0.45
    }
  };
  
