import pandas as pd
import numpy as np
import os
import threading

FEATURE_ORDER = [
    'merch_te','cat_id_te','amount','gender_te','street_te','one_city_te','us_state_te', 'post_code_te',
    'population_city','jobs_te','minutes','hour','day','month','year','day_of_week',
    'is_weekend','is_night','week_of_year','distance','is_far','is_night_and_far',
    'amount_x_distance','amount_per_citizen'
]
MODEL_THRESHOLD = float(os.getenv("MODEL_THRESHOLD", 0.5))
_lock = threading.Lock()

def get_preds(df, model_updater):
    with _lock:
        model, version = model_updater._current_model()
    preds = model.predict_proba(df[FEATURE_ORDER].to_numpy(dtype=np.float32))
    result = pd.DataFrame({
        'score': preds[:, 1], 
        'fraud_flag': (preds[:, 1] > MODEL_THRESHOLD).astype(int),
        'model_version': version
    })
    return result