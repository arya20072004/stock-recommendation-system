import yfinance as yf
import pandas as pd

ndx = yf.download("^NDX", start="2026-09-01", end="2026-09-10")
print("NASDAQ (^NDX):")
print(ndx)

nifty = yf.download("^NSEI", start="2026-09-01", end="2026-09-10")
print("\nNIFTY (^NSEI):")
print(nifty)
