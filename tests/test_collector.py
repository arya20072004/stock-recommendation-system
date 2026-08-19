import pytest
import mongomock
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import pandas as pd
from pymongo import UpdateOne
from src.data.collector import run

# Patch mongomock Collection bulk_write
original_bulk_write = mongomock.Collection.bulk_write
def patched_bulk_write(self, requests, ordered=True, bypass_document_validation=False):
    for req in requests:
        if isinstance(req, UpdateOne):
            self.update_one(req._filter, req._doc, upsert=req._upsert)
    from pymongo.results import BulkWriteResult
    return BulkWriteResult({}, True)
mongomock.Collection.bulk_write = patched_bulk_write

@pytest.fixture
def mock_db():
    client = mongomock.MongoClient()
    db = client['stock_market_db']
    yield client, db

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
@patch("src.data.collector.fetch_equity_ohlcv_for_date")
def test_explicit_target_date_boundary(mock_fetch, mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        
    records = list(db.historical_data.find())
    assert len(records) == 1
    assert records[0]["date"] == pd.Timestamp("2026-08-10")
    assert records[0]["source"] == "YAHOO"
    mock_calendar.assert_called_with(db, mock_df.index.min(), pd.Timestamp("2026-08-10"))

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
def test_backward_compatible_no_target_date(mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run() # No target date
        
    mock_calendar.assert_called_with(db, pd.Timestamp("2026-08-10"), pd.Timestamp("2026-08-10"))

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
def test_yahoo_insertion(mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        
    assert db.historical_data.count_documents({}) == 1

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
def test_yahoo_updating_yahoo(mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    db.historical_data.insert_one({
        "ticker": "RELIANCE.NS", "date": pd.Timestamp("2026-08-10"), "source": "YAHOO", "close": 100
    })
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        
    record = db.historical_data.find_one({"ticker": "RELIANCE.NS"})
    assert record["close"] == 102
    assert record["source"] == "YAHOO"

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
def test_yahoo_blocked_by_bhavcopy(mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    db.historical_data.insert_one({
        "ticker": "RELIANCE.NS", "date": pd.Timestamp("2026-08-10"), "source": "NSE_BHAVCOPY", "close": 105
    })
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        
    record = db.historical_data.find_one({"ticker": "RELIANCE.NS"})
    assert record["close"] == 105
    assert record["source"] == "NSE_BHAVCOPY"

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
@patch("src.data.collector.fetch_equity_ohlcv_for_date")
def test_bhavcopy_replacing_yahoo(mock_fetch, mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    db.historical_data.insert_one({
        "ticker": "RELIANCE.NS", "date": pd.Timestamp("2026-08-10"), "source": "YAHOO", "close": 100
    })
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-09")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    mock_fetch.return_value = [{
        "ticker": "RELIANCE.NS", "date": pd.Timestamp("2026-08-10"), "open": 100, "high": 110, "low": 95, "close": 108, "volume": 1000
    }]
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        
    record = db.historical_data.find_one({"ticker": "RELIANCE.NS"})
    assert record["close"] == 108
    assert record["source"] == "NSE_BHAVCOPY"

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
def test_invalid_yahoo_rows(mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [110], "Close": [102], "Volume": [1000] # Low > Open (invalid)
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        with patch("src.data.collector._get_trading_day_calendar", return_value=set()):
            run(target_date=date(2026, 8, 10))
        
    assert db.historical_data.count_documents({}) == 0

@patch("src.data.collector.MongoClient")
@patch("src.data.collector.yf.download")
@patch("src.data.collector._get_trading_day_calendar")
def test_repeated_idempotent_collection(mock_calendar, mock_yf, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    mock_df = pd.DataFrame({
        "Open": [100], "High": [105], "Low": [95], "Close": [102], "Volume": [1000]
    }, index=[pd.Timestamp("2026-08-10")])
    mock_yf.return_value = mock_df
    mock_calendar.return_value = {pd.Timestamp("2026-08-10")}
    
    with patch("src.data.collector.TICKERS", ["RELIANCE.NS"]):
        run(target_date=date(2026, 8, 10))
        run(target_date=date(2026, 8, 10))
        
    assert db.historical_data.count_documents({}) == 1
