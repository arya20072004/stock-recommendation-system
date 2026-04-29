"""
One-time migration script to convert XGBoost models from joblib to native .ubj format.
Loads each .joblib file, extracts the booster, and saves it as .ubj.
Does NOT delete the original .joblib files (kept as backup).
"""

import os
import joblib
from xgboost import XGBClassifier

MODELS_DIR = "models"

def migrate_models():
    """Loop through all .joblib files and migrate to .ubj format."""
    if not os.path.exists(MODELS_DIR):
        print(f"Models directory '{MODELS_DIR}' not found.")
        return

    joblib_files = [f for f in os.listdir(MODELS_DIR) if f.endswith('.joblib')]
    
    if not joblib_files:
        print(f"No .joblib files found in '{MODELS_DIR}'.")
        return

    print(f"Found {len(joblib_files)} .joblib files to migrate.\n")

    for joblib_file in joblib_files:
        joblib_path = os.path.join(MODELS_DIR, joblib_file)
        # Replace .joblib with .ubj for the output filename
        ubj_file = joblib_file.replace('.joblib', '.ubj')
        ubj_path = os.path.join(MODELS_DIR, ubj_file)

        try:
            print(f"Migrating {joblib_file}...", end=" ")
            
            # Load the model from joblib
            model = joblib.load(joblib_path)
            
            # Extract the underlying XGBoost booster
            booster = model.get_booster()
            
            # Save it in native XGBoost binary format (.ubj)
            booster.save_model(ubj_path)
            
            print(f"✓ Success (saved to {ubj_file})")
            
        except Exception as e:
            print(f"✗ Failed - {type(e).__name__}: {e}")

    print("\nMigration complete. Original .joblib files kept as backup.")

if __name__ == '__main__':
    migrate_models()
