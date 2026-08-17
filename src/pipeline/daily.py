import argparse
import sys
import logging
import uuid
import warnings
import threading
import time
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

MIN_PREDICTION_COVERAGE_RATIO = 0.90

import pandas as pd
from pymongo import MongoClient, ReturnDocument
import pymongo.errors

from src.pipeline.notifications import notify, Severity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("daily_pipeline")

IST = ZoneInfo("Asia/Kolkata")
warnings.filterwarnings("ignore")

class DailyPipeline:
    def __init__(self, mongo_uri, dry_run=False, skip_collection=False, force=False, 
                 lock_ttl=7200, heartbeat_interval=30, lease_safety_margin=60):
        self.client = MongoClient(mongo_uri)
        self.db = self.client["stock_market_db"]
        self.dry_run = dry_run
        self.skip_collection = skip_collection
        self.force = force
        self.run_id = str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc)
        self.last_completed_session = None
        self.prediction_target_date = None
        self.stages = {}
        self.degraded_stages = []
        self.errors = []
        self.status = "RUNNING"
        self.blocked_reason = None
        
        self.lock_ttl = lock_ttl
        self.heartbeat_interval = heartbeat_interval
        self.lease_safety_margin = lease_safety_margin
        self._stop_heartbeat = threading.Event()
        self._heartbeat_thread = None
        self.lock_state_lock = threading.Lock()
        self.lock_lost = False
        self.lock_lease_unconfirmed = False
        self.confirmed_lease_expires_at = None
        
        # Ensure collection exists and has index
        self.db.pipeline_runs.create_index("run_id", unique=True)
        self.db.pipeline_runs.create_index("status")
        self.db.pipeline_locks.create_index("lock_id", unique=True)
        
    def _log_stage(self, stage_name, status, error=None, metrics=None):
        logger.info(f"Stage {stage_name}: {status}")
        now = datetime.now(timezone.utc)
        
        # Calculate duration if it's completing
        duration_ms = None
        if stage_name in self.stages and "started_at" in self.stages[stage_name]:
            started = self.stages[stage_name]["started_at"]
            duration_ms = int((now - started).total_seconds() * 1000)
            
        stage_record = {
            "status": status,
            "timestamp": now,
        }
        if stage_name not in self.stages:
            stage_record["started_at"] = now
        else:
            stage_record["started_at"] = self.stages[stage_name]["started_at"]
            if duration_ms is not None:
                stage_record["duration_ms"] = duration_ms
                
        if metrics:
            stage_record["metrics"] = metrics
        elif stage_name in self.stages and "metrics" in self.stages[stage_name]:
            stage_record["metrics"] = self.stages[stage_name]["metrics"]
            
        self.stages[stage_name] = stage_record
        
        if error:
            logger.error(f"Error in {stage_name}: {error}")
            self.errors.append(f"{stage_name}: {error}")
            if status == "FAILED":
                self.status = "FAILED"
            elif status == "DEGRADED":
                self.degraded_stages.append(stage_name)
                
        self._update_run_record()

    def _update_run_record(self):
        if self.dry_run:
            return
            
        now = datetime.now(timezone.utc)
        duration_ms = int((now - self.start_time).total_seconds() * 1000)
        
        record = {
            "run_id": self.run_id,
            "owner": self.run_id,
            "market_date": self.prediction_target_date.strftime("%Y-%m-%d") if self.prediction_target_date else None,
            "status": self.status,
            "started_at": self.start_time,
            "stages": self.stages,
            "degraded_stages": self.degraded_stages,
            "errors": self.errors,
            "duration_ms": duration_ms
        }
        
        if self.blocked_reason:
            record["blocked_reason"] = self.blocked_reason
            
        if self.status in ["SUCCESS", "FAILED", "DEGRADED", "BLOCKED"]:
            record["completed_at"] = now
            
        self.db.pipeline_runs.update_one(
            {"run_id": self.run_id},
            {"$set": record},
            upsert=True
        )

    def _reconcile_stale_runs(self):
        # Find all RUNNING runs that are not this one
        stale_runs = list(self.db.pipeline_runs.find({
            "status": "RUNNING", 
            "run_id": {"$ne": self.run_id}
        }))
        
        now = datetime.now(timezone.utc)
        for r in stale_runs:
            r_id = r["run_id"]
            # Check if this run_id currently owns the lock
            lock = self.db.pipeline_locks.find_one({"lock_id": "daily_production_lock"})
            
            # If the lock is missing, expired, or owned by someone else, this run is stale
            if not lock or lock.get("owner") != r_id or lock.get("expires_at", now) < now:
                logger.warning(f"Reconciling stale run: {r_id}")
                self.db.pipeline_runs.update_one(
                    {"run_id": r_id},
                    {
                        "$set": {
                            "status": "FAILED",
                            "completed_at": now,
                        },
                        "$push": {
                            "errors": "STALE_EXECUTION"
                        }
                    }
                )

    def _heartbeat_worker(self):
        while not self._stop_heartbeat.is_set():
            now = datetime.now(timezone.utc)
            new_expiry = now + timedelta(seconds=self.lock_ttl)
            try:
                result = self.db.pipeline_locks.update_one(
                    {
                        "lock_id": "daily_production_lock",
                        "owner": self.run_id,
                        "run_id": self.run_id,
                        "status": "RUNNING"
                    },
                    {
                        "$set": {
                            "heartbeat_at": now,
                            "expires_at": new_expiry
                        }
                    }
                )
                with self.lock_state_lock:
                    if result.matched_count == 0:
                        logger.error("LOCK_OWNERSHIP_LOST: Heartbeat failed to match our active lock.")
                        self.lock_lost = True
                        break
                    else:
                        self.confirmed_lease_expires_at = new_expiry
            except Exception as e:
                logger.warning(f"Transient error updating lock heartbeat: {e}")
                
            with self.lock_state_lock:
                if self.confirmed_lease_expires_at:
                    if datetime.now(timezone.utc) >= self.confirmed_lease_expires_at - timedelta(seconds=self.lease_safety_margin):
                        logger.error("LOCK_LEASE_UNCONFIRMED: Lease safety margin breached due to repeated heartbeat failures.")
                        self.lock_lease_unconfirmed = True
                        break
            
            self._stop_heartbeat.wait(self.heartbeat_interval)

    def _verify_ownership(self):
        with self.lock_state_lock:
            if self.lock_lost:
                raise RuntimeError("LOCK_OWNERSHIP_LOST")
            
            if self.lock_lease_unconfirmed:
                raise RuntimeError("LOCK_LEASE_UNCONFIRMED")
                
            if self.confirmed_lease_expires_at:
                if datetime.now(timezone.utc) >= self.confirmed_lease_expires_at - timedelta(seconds=self.lease_safety_margin):
                    raise RuntimeError("LOCK_LEASE_UNCONFIRMED")

    def acquire_lock(self):
        logger.info("Acquiring pipeline lock...")
        self._reconcile_stale_runs()
        
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=self.lock_ttl)
        
        try:
            self.db.pipeline_locks.insert_one({
                "lock_id": "daily_production_lock",
                "run_id": self.run_id,
                "status": "RUNNING",
                "started_at": now,
                "heartbeat_at": now,
                "expires_at": expires_at,
                "owner": self.run_id
            })
            acquired = True
        except pymongo.errors.DuplicateKeyError:
            result = self.db.pipeline_locks.find_one_and_update(
                {
                    "lock_id": "daily_production_lock",
                    "$or": [
                        {"status": {"$ne": "RUNNING"}},
                        {"expires_at": {"$lt": now}}
                    ]
                },
                {
                    "$set": {
                        "run_id": self.run_id,
                        "status": "RUNNING",
                        "started_at": now,
                        "heartbeat_at": now,
                        "expires_at": expires_at,
                        "owner": self.run_id
                    }
                },
                return_document=ReturnDocument.AFTER
            )
            acquired = result is not None
            
        if acquired:
            logger.info("Lock acquired successfully.")
            self._log_stage("ACQUIRE_LOCK", "SUCCESS")
            
            with self.lock_state_lock:
                self.confirmed_lease_expires_at = expires_at
                
            # Start heartbeat
            self._stop_heartbeat.clear()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
            self._heartbeat_thread.start()
        else:
            self.status = "BLOCKED"
            self.blocked_reason = "LOCK_ALREADY_HELD"
            raise RuntimeError("Concurrent active pipeline run detected or failed to acquire lock.")

    def _release_lock(self):
        self._stop_heartbeat.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)
            
        with self.lock_state_lock:
            safe_to_release = not self.lock_lost and not self.lock_lease_unconfirmed
            
        if safe_to_release:
            self.db.pipeline_locks.update_one(
                {"lock_id": "daily_production_lock", "owner": self.run_id, "run_id": self.run_id},
                {"$set": {"status": self.status, "completed_at": datetime.now(timezone.utc)}}
            )

    def _detect_missed_sessions(self):
        # Proactively identify missed sessions anchored to production history.
        logger.info("Checking for missed production sessions...")
        
        last_run = self.db.pipeline_runs.find_one(
            {"status": {"$in": ["SUCCESS", "DEGRADED"]}, "market_date": {"$ne": None}},
            sort=[("market_date", -1)]
        )
        
        if not last_run:
            logger.info("No prior production history found. Establishing operational baseline.")
            return
            
        last_run_date_str = last_run["market_date"]
        try:
            last_run_date = datetime.strptime(last_run_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Invalid market_date in last_run: {last_run_date_str}")
            return
            
        now_ist = datetime.now(IST)
        candidate = now_ist.date() - timedelta(days=1)
        
        MAX_MISSED_SESSION_SCAN_DAYS = 60
        from src.data.pcr_builder import _fetch_bhavcopy
        
        missed = []
        scan_days = 0
        
        while candidate > last_run_date:
            if scan_days >= MAX_MISSED_SESSION_SCAN_DAYS:
                notify({
                    "severity": Severity.ACTION_REQUIRED,
                    "run_id": self.run_id,
                    "status": "MISSED_RUN",
                    "message": f"Missed session scan reached maximum safety cap of {MAX_MISSED_SESSION_SCAN_DAYS} days.",
                    "reason": "MISSED_SESSION_SCAN_LIMIT_EXCEEDED"
                })
                break
                
            if candidate.weekday() < 5:
                dt_candidate = datetime.combine(candidate, datetime.min.time())
                bhav = _fetch_bhavcopy(dt_candidate)
                if bhav is not None and not bhav.empty:
                    # It was a trading session. Did we run?
                    run = self.db.pipeline_runs.find_one({
                        "market_date": candidate.strftime("%Y-%m-%d"),
                        "status": {"$in": ["SUCCESS", "DEGRADED"]}
                    })
                    if not run:
                        missed.append(str(candidate))
                        
            candidate -= timedelta(days=1)
            scan_days += 1
            
        if missed:
            logger.warning(f"MISSED_SESSIONS detected: {missed}")
            notify({
                "severity": Severity.ACTION_REQUIRED,
                "run_id": self.run_id,
                "status": "MISSED_RUN",
                "message": f"Detected {len(missed)} missed production sessions without auto-replay.",
                "reason": str(missed)
            })

    def resolve_trading_session(self):
        self._verify_ownership()
        logger.info("Resolving target trading session...")
        now_ist = datetime.now(IST)
        
        if not self.force and now_ist.hour < 20:
            self.status = "BLOCKED"
            self.blocked_reason = "BEFORE_CUTOFF"
            raise RuntimeError("Blocked: It's before 20:00 IST.")

        candidate = now_ist.date()
        if now_ist.hour < 20:
            candidate -= timedelta(days=1)
            
        from src.data.pcr_builder import _fetch_bhavcopy
        
        rejected = []
        max_lookback = 10
        lookback = 0
        
        while lookback < max_lookback:
            if candidate.weekday() >= 5:
                rejected.append(str(candidate))
                candidate -= timedelta(days=1)
                lookback += 1
                continue
                
            dt_candidate = datetime.combine(candidate, datetime.min.time())
            bhav = _fetch_bhavcopy(dt_candidate)
            
            if bhav is not None and not bhav.empty:
                self.last_completed_session = candidate
                import src.data.session_calendar as session_calendar
                self.prediction_target_date = session_calendar.next_session(self.last_completed_session)
                logger.info(f"Rejected non-trading dates: {rejected}")
                logger.info(f"Resolved last completed session: {self.last_completed_session}")
                logger.info(f"Resolved prediction target date: {self.prediction_target_date}")
                self._log_stage("TRADING_SESSION", "SUCCESS")
                self._detect_missed_sessions()
                return
            else:
                rejected.append(str(candidate))
                candidate -= timedelta(days=1)
                lookback += 1
                
        msg = f"Failed to resolve trading session within {max_lookback} days."
        self.status = "BLOCKED"
        self.blocked_reason = "NO_CONFIRMED_COMPLETED_SESSION"
        self._log_stage("TRADING_SESSION", "FAILED", msg)
        raise RuntimeError(msg)

    def collect_market_data(self):
        self._verify_ownership()
        if self.skip_collection:
            logger.info("Skipping data collection (--skip-collection)")
            self._log_stage("COLLECT_OHLCV", "SKIPPED")
            return
            
        logger.info("Collecting OHLCV data...")
        if self.dry_run:
            self._log_stage("COLLECT_OHLCV", "SUCCESS")
            return
            
        from src.data.collector import run as run_collector
        results = run_collector()
        
        metrics = {"successful": results.get("successful", 0), "failed": results.get("failed", 0)}
        
        if results.get("failed", 0) > 0:
            logger.error(f"OHLCV Collection had failures: {results.get('failed_tickers', [])}")
            self._log_stage("COLLECT_OHLCV", "DEGRADED", f"{results['failed']} tickers failed", metrics=metrics)
        else:
            self._log_stage("COLLECT_OHLCV", "SUCCESS", metrics=metrics)

    def collect_auxiliary_data(self):
        self._verify_ownership()
        if self.skip_collection:
            self._log_stage("COLLECT_AUX", "SKIPPED")
            return
            
        logger.info("Collecting auxiliary data...")
        if self.dry_run:
            self._log_stage("COLLECT_AUX", "SUCCESS")
            return
            
        from src.data.pcr_builder import build_pcr_history
        from src.data.sector_index_builder import build_sector_indices
        from src.data.fii_dii_builder import fetch_and_store_latest
        from src.data.news_collector import run as run_news
        from src.ml.sentiment import run as run_sentiment

        stages = {
            "COLLECT_PCR": lambda: build_pcr_history(self.client),
            "COLLECT_SECTOR": lambda: build_sector_indices(self.client),
            "COLLECT_FII_DII": lambda: fetch_and_store_latest(self.client),
            "COLLECT_NEWS": lambda: run_news(),
            "SENTIMENT": lambda: run_sentiment()
        }
        
        for name, func in stages.items():
            self._verify_ownership()
            try:
                func()
                self._log_stage(name, "SUCCESS")
            except Exception as e:
                self._log_stage(name, "DEGRADED", str(e))

    def validate_ohlcv_integrity(self):
        self._verify_ownership()
        logger.info("Validating OHLCV integrity (GATE)...")
        dt_start = datetime.combine(self.last_completed_session, datetime.min.time())
        
        from src.data.nifty50 import TICKERS
        expected_count = len(TICKERS)
        
        rows = list(self.db.historical_data.find({"date": dt_start}))
        
        found_tickers = [r["ticker"] for r in rows]
        missing_tickers = set(TICKERS) - set(found_tickers)
        
        if missing_tickers:
            msg = f"Missing tickers for {self.last_completed_session}: {missing_tickers}"
            self._log_stage("OHLCV_INTEGRITY", "FAILED", msg)
            raise RuntimeError(msg)
            
        if len(rows) > expected_count:
            import collections
            dups = [item for item, count in collections.Counter(found_tickers).items() if count > 1]
            if dups:
                msg = f"Duplicate rows for tickers: {dups}"
                self._log_stage("OHLCV_INTEGRITY", "FAILED", msg)
                raise RuntimeError(msg)
                
        for r in rows:
            ticker = r["ticker"]
            for f in ["open", "high", "low", "close"]:
                if r.get(f) is None or r.get(f) <= 0:
                    msg = f"Invalid {f} for {ticker}"
                    self._log_stage("OHLCV_INTEGRITY", "FAILED", msg)
                    raise RuntimeError(msg)
            
            if r.get("volume") is None or r.get("volume") < 0:
                msg = f"Invalid volume for {ticker}"
                self._log_stage("OHLCV_INTEGRITY", "FAILED", msg)
                raise RuntimeError(msg)
                
            h, l, o, c = r["high"], r["low"], r["open"], r["close"]
            if h < l or h < o or h < c or l > o or l > c:
                msg = f"OHLC rules violated for {ticker}: H={h}, L={l}, O={o}, C={c}"
                self._log_stage("OHLCV_INTEGRITY", "FAILED", msg)
                raise RuntimeError(msg)

        metrics = {"processed": len(rows), "missing": len(missing_tickers)}
        self._log_stage("OHLCV_INTEGRITY", "SUCCESS", metrics=metrics)

    def run_settlement(self):
        self._verify_ownership()
        logger.info("Running settlement...")
        try:
            from src.ml.settlement import evaluate_predictions
            stats = evaluate_predictions(self.client, apply=not self.dry_run)
            self._log_stage("SETTLEMENT", "SUCCESS", metrics=stats)
        except Exception as e:
            self._log_stage("SETTLEMENT", "FAILED", str(e))
            raise

    def generate_predictions(self):
        self._verify_ownership()
        logger.info("Generating predictions...")
        if self.dry_run:
            self._log_stage("PREDICTION_GENERATION", "SUCCESS")
            return
            
        try:
            from src.ml.history import generate_and_persist_predictions
            result = generate_and_persist_predictions(
                self.client, 
                last_completed_session=self.last_completed_session,
                prediction_target_date=self.prediction_target_date
            )
            
            stale_list = result.get("stale", [])
            failed_list = result.get("failed", [])
            if failed_list:
                self._log_stage("PREDICTION_GENERATION", "DEGRADED", f"{len(failed_list)} tickers failed local generation", metrics=result)
            elif stale_list:
                self._log_stage("PREDICTION_GENERATION", "DEGRADED", f"{len(stale_list)} tickers skipped due to stale data", metrics=result)
            else:
                self._log_stage("PREDICTION_GENERATION", "SUCCESS", metrics=result)
        except Exception as e:
            self._log_stage("PREDICTION_GENERATION", "FAILED", str(e))
            raise

    def validate_prediction_batch(self):
        self._verify_ownership()
        logger.info("Validating prediction batch...")
        if self.dry_run:
            self._log_stage("PREDICTION_VALIDATION", "SUCCESS")
            return
            
        dt_str = self.prediction_target_date.strftime("%Y-%m-%d")
        preds = list(self.db.prediction_history.find({
            "market_date": dt_str,
            "prediction_horizon": 10
        }))
        
        from src.data.nifty50 import TICKERS
        found_tickers = [p["symbol"] for p in preds]
        missing = set(TICKERS) - set(found_tickers)
        
        if missing:
            generation_metrics = self.stages.get("PREDICTION_GENERATION", {}).get("metrics", {})
            stale_tickers = set(generation_metrics.get("stale", []))
            failed_tickers = set(generation_metrics.get("failed", []))
            
            explained_missing = stale_tickers | failed_tickers
            unexplained = missing - explained_missing
            
            if unexplained:
                msg = f"Unexplained missing predictions: {unexplained}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)
                
            required_predictions = math.ceil(len(TICKERS) * MIN_PREDICTION_COVERAGE_RATIO)
            generated_count = len(found_tickers)
            
            if generated_count < required_predictions:
                msg = f"Prediction coverage failure. Generated {generated_count} < required {required_predictions}. Stale: {stale_tickers}, Failed: {failed_tickers}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)
                
            if failed_tickers:
                msg = f"Missing predictions safely explained by recognized localized failures: {failed_tickers}"
                logger.warning(msg)
                self._log_stage("PREDICTION_VALIDATION", "DEGRADED", msg)
            elif stale_tickers:
                msg = f"Missing predictions safely explained by explicitly skipped stale tickers: {stale_tickers}"
                logger.warning(msg)
                self._log_stage("PREDICTION_VALIDATION", "DEGRADED", msg)
            
        import collections
        dups = [item for item, count in collections.Counter(found_tickers).items() if count > 1]
        if dups:
            msg = f"Duplicate predictions for {dups}"
            self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
            raise RuntimeError(msg)
            
        for p in preds:
            ticker = p["symbol"]
            if p.get("confidence") is None or p.get("confidence") < 0 or p.get("confidence") > 100:
                msg = f"Invalid confidence for {ticker}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)
            
            if p.get("price_at_prediction") is None or p.get("price_at_prediction") <= 0:
                msg = f"Invalid price_at_prediction for {ticker}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)
                
            model_ver = p.get("model_version")
            from src.ml.model_utils import get_model_version
            expected_ver = get_model_version(ticker)
            if not model_ver or model_ver != expected_ver:
                msg = f"Model version mismatch for {ticker}: expected {expected_ver}, got {model_ver}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)
                
            if p.get("recommendation") not in ["BUY", "HOLD", "SELL"]:
                msg = f"Invalid recommendation for {ticker}: {p.get('recommendation')}"
                self._log_stage("PREDICTION_VALIDATION", "FAILED", msg)
                raise RuntimeError(msg)

        current_status = self.stages.get("PREDICTION_VALIDATION", {}).get("status")
        if current_status != "DEGRADED":
            self._log_stage("PREDICTION_VALIDATION", "SUCCESS")

    def run_api_health_check(self):
        self._verify_ownership()
        logger.info("Running API health check...")
        try:
            import app
            from src.data.nifty50 import TICKERS
            app.app.testing = True
            with app.app.test_client() as client:
                resp = client.get('/api/stocks/summary')
                if resp.status_code == 200:
                    data = resp.get_json()
                    if isinstance(data, dict) and "data" in data:
                        expected = set(TICKERS)
                        actual = {row["ticker"] for row in data["data"]}
                        missing = expected - actual
                        unexpected = actual - expected
                        
                        import collections
                        all_tickers = [row["ticker"] for row in data["data"]]
                        duplicates = [t for t, c in collections.Counter(all_tickers).items() if c > 1]
                        
                        if missing or unexpected or duplicates:
                            msg = f"API Ticker mismatch. Expected: {len(expected)}, Returned: {len(all_tickers)}, Missing: {missing}, Unexpected: {unexpected}, Duplicates: {duplicates}"
                            self._log_stage("API_HEALTH", "FAILED", msg)
                            raise RuntimeError(msg)
                        else:
                            metrics = {"expected_count": len(expected), "returned_count": len(all_tickers)}
                            self._log_stage("API_HEALTH", "SUCCESS", metrics=metrics)
                            return
                
            msg = f"API returned non-200 or unexpected structure: {resp.status_code}"
            self._log_stage("API_HEALTH", "FAILED", msg)
            raise RuntimeError(msg)
        except Exception as e:
            self._log_stage("API_HEALTH", "FAILED", str(e))
            raise RuntimeError(str(e))

    def execute(self):
        try:
            self._log_stage("START", "SUCCESS")
            self.acquire_lock()
            self.resolve_trading_session()
            self.collect_market_data()
            self.collect_auxiliary_data()
            self.validate_ohlcv_integrity()
            self.run_settlement()
            self.generate_predictions()
            self.validate_prediction_batch()
            self.run_api_health_check()
            
            if len(self.degraded_stages) > 0:
                self.status = "DEGRADED"
                exit_code = 2
            else:
                self.status = "SUCCESS"
                exit_code = 0
                
            self._update_run_record()
            logger.info(f"Pipeline completed with status {self.status}.")
            
            notify({
                "severity": Severity.WARNING if exit_code == 2 else Severity.INFO,
                "run_id": self.run_id,
                "market_date": self.prediction_target_date.strftime("%Y-%m-%d") if self.prediction_target_date else None,
                "status": self.status,
                "message": f"Pipeline finished gracefully.",
                "reason": f"Degraded stages: {self.degraded_stages}" if exit_code == 2 else "Nominal"
            })
            
            return exit_code
            
        except RuntimeError as e:
            if self.status == "BLOCKED":
                logger.warning(f"Pipeline BLOCKED: {e}")
                self._update_run_record()
                notify({
                    "severity": Severity.INFO,
                    "run_id": self.run_id,
                    "status": "BLOCKED",
                    "reason": self.blocked_reason,
                    "message": str(e)
                })
                return 3
            else:
                logger.error(f"Pipeline FAILED: {e}")
                self.status = "FAILED"
                self._update_run_record()
                notify({
                    "severity": Severity.CRITICAL,
                    "run_id": self.run_id,
                    "market_date": self.prediction_target_date.strftime("%Y-%m-%d") if self.prediction_target_date else None,
                    "status": "FAILED",
                    "reason": "RUNTIME_ERROR",
                    "message": str(e)
                })
                return 1
        except Exception as e:
            logger.error(f"Pipeline FAILED: {e}")
            self.status = "FAILED"
            self._update_run_record()
            notify({
                "severity": Severity.CRITICAL,
                "run_id": self.run_id,
                "market_date": self.prediction_target_date.strftime("%Y-%m-%d") if self.prediction_target_date else None,
                "status": "FAILED",
                "reason": "UNHANDLED_EXCEPTION",
                "message": str(e)
            })
            return 1
        finally:
            self._release_lock()
            self.client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily Production Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Run without database mutations")
    parser.add_argument("--skip-collection", action="store_true", help="Skip data collection (validation still runs)")
    parser.add_argument("--force", action="store_true", help="Bypass time-of-day protections")
    
    args = parser.parse_args()
    
    from dotenv import load_dotenv
    import os
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    
    pipeline = DailyPipeline(
        mongo_uri=mongo_uri,
        dry_run=args.dry_run,
        skip_collection=args.skip_collection,
        force=args.force
    )
    
    sys.exit(pipeline.execute())
