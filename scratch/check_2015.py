import yfinance as yf
ndx = yf.download("^NDX", start="2015-01-01", end="2015-01-10", progress=False)
print(ndx)
