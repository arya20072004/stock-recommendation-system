import os, pymongo, pandas as pd
from datetime import datetime, timezone, date
from dotenv import load_dotenv
load_dotenv()
from src.features.v1.engineering import _prepare_macro_data

client = pymongo.MongoClient(os.getenv("MONGO_URI"))
start_date = pd.Timestamp("2025-09-04")
end_date = pd.Timestamp("2026-09-07")
target_date = date(2026, 9, 7)

macro_df = _prepare_macro_data(start_date, end_date, client, prediction_target_date=target_date)

print(macro_df.tail(3)[["usdinr_ret_1d", "vix_level", "vix_ret_1d"]])
print("NaNs in tail:")
print(macro_df.tail(3).isna().sum())
