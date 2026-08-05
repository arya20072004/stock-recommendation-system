import logging
import os
from pymongo import MongoClient
import pymongo
from datetime import datetime

logger = logging.getLogger(__name__)

def evaluate_pending_predictions(client):
    """
    Evaluates PENDING predictions by checking if their prediction horizon has elapsed.
    """
    db = client['stock_market_db']
    
    pending_predictions = list(db.prediction_history.find({"status": "PENDING"}))
    if not pending_predictions:
        logger.info("No PENDING predictions to evaluate.")
        return
        
    logger.info(f"Evaluating {len(pending_predictions)} PENDING predictions...")
    
    evaluated_count = 0
    for p in pending_predictions:
        symbol = p['symbol']
        market_date_str = p['market_date']
        horizon = p.get('prediction_horizon', 10)
        price_at_prediction = p['price_at_prediction']
        recommendation = p['recommendation'] # BUY, SELL, HOLD
        threshold = p.get('threshold_pct', 0.01) # Default 1% if missing
        
        # Need to parse market_date back to datetime for querying
        try:
            market_date = datetime.strptime(market_date_str, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid market_date format: {market_date_str} for prediction {p['_id']}")
            continue
            
        # Fetch subsequent market days
        # We need exactly 'horizon' number of trading days after the prediction
        subsequent_days = list(db.historical_data.find(
            {"ticker": symbol, "date": {"$gt": market_date}}
        ).sort("date", pymongo.ASCENDING).limit(horizon))
        
        if len(subsequent_days) < horizon:
            # Not enough days have passed yet
            continue
            
        # Evaluation is ready
        target_day = subsequent_days[-1]
        actual_price = float(target_day['close'])
        actual_return = (actual_price / price_at_prediction) - 1
        
        is_correct = False
        outcome = "NEUTRAL"
        
        if recommendation == 'BUY':
            if actual_return > threshold:
                is_correct = True
                outcome = "CORRECT"
            else:
                is_correct = False
                outcome = "INCORRECT"
        elif recommendation == 'SELL':
            if actual_return < -threshold:
                is_correct = True
                outcome = "CORRECT"
            else:
                is_correct = False
                outcome = "INCORRECT"
        elif recommendation == 'HOLD':
            if -threshold <= actual_return <= threshold:
                is_correct = True
                outcome = "CORRECT"
            else:
                is_correct = False
                outcome = "INCORRECT"
                
        # Update record
        db.prediction_history.update_one(
            {"_id": p["_id"]},
            {
                "$set": {
                    "actual_price": actual_price,
                    "actual_return": actual_return,
                    "outcome": outcome,
                    "prediction_correct": is_correct,
                    "status": "EVALUATED",
                    "evaluation_timestamp": datetime.utcnow()
                }
            }
        )
        evaluated_count += 1
        
    logger.info(f"Evaluation complete. Evaluated {evaluated_count} predictions.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    from dotenv import load_dotenv
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    evaluate_pending_predictions(client)
