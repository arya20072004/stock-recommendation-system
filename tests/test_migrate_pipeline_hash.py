import pytest
import sys
import os
import copy
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.migrate_pipeline_hash import run_migration

EXPECTED_OLD_HASH = "426253a3d8a9dc6a8d6e4210d825d926c393e717f2f334df4a3de1267912328d"
PIPELINE_VERSION = "v1"

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {"MONGO_URI": "mongodb://mock:27017"}):
        yield

@pytest.fixture
def base_active_models():
    return [
        {
            "_id": "id1",
            "ticker": "AAPL",
            "status": "ACTIVE",
            "feature_pipeline_hash": EXPECTED_OLD_HASH,
            "feature_pipeline_version": PIPELINE_VERSION,
            "version": "v1.0",
            "model_hash": "mhash1"
        },
        {
            "_id": "id2",
            "ticker": "MSFT",
            "status": "ACTIVE",
            "feature_pipeline_hash": EXPECTED_OLD_HASH,
            "feature_pipeline_version": PIPELINE_VERSION,
            "version": "v1.0",
            "model_hash": "mhash2"
        }
    ]

@pytest.fixture
def mock_mongo():
    with patch("scripts.migrate_pipeline_hash.MongoClient") as mock_client:
        client_instance = MagicMock()
        mock_client.return_value = client_instance
        db = MagicMock()
        client_instance.__getitem__.return_value = db
        yield mock_client, client_instance, db

@pytest.fixture
def mock_manifest():
    with patch("scripts.migrate_pipeline_hash.read_active_manifest") as mock_read:
        def side_effect(ticker):
            return {
                "model_version": "v1.0",
                "model_hash": "mhash1" if ticker == "AAPL" else "mhash2"
            }
        mock_read.side_effect = side_effect
        yield mock_read

@pytest.fixture
def mock_hash():
    with patch("scripts.migrate_pipeline_hash.get_feature_pipeline_hash") as mock_get:
        mock_get.return_value = "NEW_HASH"
        yield mock_get

def test_hash_calculation_failure():
    with patch("scripts.migrate_pipeline_hash.get_feature_pipeline_hash", side_effect=Exception("Failed")):
        with pytest.raises(SystemExit) as e:
            run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
        assert e.value.code == 1

def test_no_db_connection():
    with patch("scripts.migrate_pipeline_hash.get_feature_pipeline_hash", return_value="NEW_HASH"):
        with patch.dict(os.environ, clear=True):
            with pytest.raises(SystemExit) as e:
                run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
            assert e.value.code == 1

def test_zero_active_models(mock_env, mock_mongo, mock_hash):
    _, _, db = mock_mongo
    db.model_registry.find.return_value = []
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_unexpected_active_hash(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    _, _, db = mock_mongo
    models = copy.deepcopy(base_active_models)
    models[0]["feature_pipeline_hash"] = "UNEXPECTED_HASH"
    db.model_registry.find.return_value = models
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_missing_hash(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    _, _, db = mock_mongo
    models = copy.deepcopy(base_active_models)
    del models[0]["feature_pipeline_hash"]
    db.model_registry.find.return_value = models
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_missing_version(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    _, _, db = mock_mongo
    models = copy.deepcopy(base_active_models)
    
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict) and "$exists" in query["feature_pipeline_version"]:
            return [models[0]] # Return a model missing the field
        return models
    db.model_registry.find.side_effect = mock_find
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_mixed_hashes(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    _, _, db = mock_mongo
    models = copy.deepcopy(base_active_models)
    models[0]["feature_pipeline_hash"] = "NEW_HASH"
    
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        return models
    db.model_registry.find.side_effect = mock_find
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_already_migrated(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    _, _, db = mock_mongo
    models = copy.deepcopy(base_active_models)
    models[0]["feature_pipeline_hash"] = "NEW_HASH"
    models[1]["feature_pipeline_hash"] = "NEW_HASH"
    
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        return models
    db.model_registry.find.side_effect = mock_find
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 0

def test_valid_migration(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    client_class, client_instance, db = mock_mongo
    
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        if db.model_registry.find.call_count == 1:
            return base_active_models
        else:
            m = copy.deepcopy(base_active_models)
            for x in m: x["feature_pipeline_hash"] = "NEW_HASH"
            return m
            
    db.model_registry.find.side_effect = mock_find
    
    update_result = MagicMock()
    update_result.modified_count = 2
    db.model_registry.update_many.return_value = update_result
    db.model_registry.count_documents.return_value = 0
    
    try:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    except SystemExit:
        pytest.fail("Migration failed unexpectedly")
        
    assert db.model_registry.update_many.called
    args, kwargs = db.model_registry.update_many.call_args
    assert args[0]["feature_pipeline_version"] == PIPELINE_VERSION

def test_modified_count_mismatch(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    client_class, client_instance, db = mock_mongo
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        return base_active_models
    db.model_registry.find.side_effect = mock_find
    
    update_result = MagicMock()
    update_result.modified_count = 1 # Expected 2
    db.model_registry.update_many.return_value = update_result
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_post_migration_verification_failure(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    client_class, client_instance, db = mock_mongo
    
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        if db.model_registry.find.call_count <= 2:
            return base_active_models
        else:
            return base_active_models # DB still has old hash!
            
    db.model_registry.find.side_effect = mock_find
    
    update_result = MagicMock()
    update_result.modified_count = 2
    db.model_registry.update_many.return_value = update_result
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH)
    assert e.value.code == 1

def test_dry_run_no_update(mock_env, mock_mongo, mock_hash, mock_manifest, base_active_models):
    client_class, client_instance, db = mock_mongo
    def mock_find(query):
        if "feature_pipeline_version" in query and isinstance(query["feature_pipeline_version"], dict):
            return []
        return base_active_models
    db.model_registry.find.side_effect = mock_find
    
    with pytest.raises(SystemExit) as e:
        run_migration(PIPELINE_VERSION, EXPECTED_OLD_HASH, dry_run=True)
    assert e.value.code == 0
    assert not db.model_registry.update_many.called
