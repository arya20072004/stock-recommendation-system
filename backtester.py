# backtester.py

from backtesting import Backtest, Strategy
import pandas as pd
import joblib
import json # <-- Add this import

# We need the create_dataset function from your trainer to prepare the data
from ml_trainer import create_dataset

class MLStrategy(Strategy):
    def init(self):
        # Load the pre-trained model
        self.model = joblib.load(f"model_{self.data.index.name}.joblib")
        
        # --- KEY CHANGE: Load the exact feature list ---
        # Load the feature names from the JSON file created during training
        with open(f"features_{self.data.index.name}.json", 'r') as f:
            self.feature_names = json.load(f)
        
        # Prepare the features using the loaded list to ensure a perfect match
        self.features = self.data.df[self.feature_names]
        # --- END OF KEY CHANGE ---

    def next(self):
        # Get the features for the current day
        current_features = self.features.iloc[len(self.data)-1].values.reshape(1, -1)
        
        # Predict the signal for today
        signal = self.model.predict(current_features)[0]

        # If signal is 1 (Buy) and we are not in a position, then buy.
        if signal == 1 and not self.position:
            self.buy()
        # If signal is 0 (Don't Buy/Sell) and we are in a position, then sell.
        elif signal == 0 and self.position:
            self.position.close()

if __name__ == "__main__":
    TICKER = 'RELIANCE.NS' # Let's backtest the more "interesting" model
    
    print(f"--- Running Backtest for {TICKER} ---")
    
    # 1. Create the dataset with all features and the correct index
    full_dataset = create_dataset(TICKER)
    full_dataset.index.name = TICKER # Name the index for the strategy
    
    # The backtesting library needs columns named 'Open', 'High', 'Low', 'Close'
    # Our original data is named 'open', 'high', etc., so we rename them.
    bt_data = full_dataset.rename(columns={
        'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
    })

    # 2. Run the backtest
    bt = Backtest(bt_data, MLStrategy, cash=100000, commission=.002)
    stats = bt.run()
    
    # 3. Print the results and plot the performance
    print("\n--- Backtest Results ---")
    print(stats)
    print("\nAttempting to open plot in browser...")
    bt.plot()