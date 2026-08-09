import importlib
import hashlib
import os

def resolve_feature_pipeline(version: str):
    """
    Resolves the feature pipeline module for the given version.
    Returns the module, or raises RuntimeError if it does not exist.
    """
    # For Phase 14, we only allow known versions to prevent arbitrary loading.
    if version not in ["v1"]:
        raise RuntimeError(f"Feature pipeline version '{version}' is unavailable.")
    try:
        module_path = f"src.features.{version}.engineering"
        module = importlib.import_module(module_path)
        return module
    except ImportError as e:
        raise RuntimeError(f"Feature pipeline version '{version}' could not be loaded: {e}")

def get_feature_pipeline_hash(version: str) -> str:
    """
    Calculates the deterministic SHA-256 hash of the specified version's engineering module.
    It reads the source file, strips trailing whitespace and standardizes line endings.
    """
    if version not in ["v1"]:
        raise RuntimeError(f"Feature pipeline version '{version}' is unavailable.")    
    filepath = os.path.join(os.path.dirname(__file__), version, "engineering.py")
    if not os.path.exists(filepath):
        raise RuntimeError(f"Feature pipeline file not found for version '{version}'.")    
    with open(filepath, 'rb') as f:
        content = f.read().decode('utf-8')    
    # Standardize line endings and remove trailing whitespace per line
    normalized_lines = [line.rstrip() for line in content.splitlines()]
    # Ensure it ends with exactly one newline
    normalized_content = '\n'.join(normalized_lines).strip() + '\n'
    return hashlib.sha256(normalized_content.encode('utf-8')).hexdigest()
