import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import TargetEncoder

CATEGORICAL_COLS = ['merch', 'cat_id', 'gender', 'one_city', 'us_state', 'post_code', 'jobs', 'street']
def preprocess_time_features(df):
    df['transaction_time'] = pd.to_datetime(df.transaction_time, format="%Y-%m-%d %H:%M")
    df['minutes'] = df.transaction_time.dt.minute
    df['hour'] = df.transaction_time.dt.hour
    df['day'] = df.transaction_time.dt.day
    df['month'] = df.transaction_time.dt.month
    df['year'] = df.transaction_time.dt.year
    df['day_of_week'] = df.transaction_time.dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_night'] = df['hour'].isin([0, 1, 2, 3, 4, 5]).astype(int)
    df['week_of_year'] = df['transaction_time'].dt.isocalendar().week.astype(int)
    df = df.drop(columns=['transaction_time'])
    return df

def distance(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def cat_encoding(df, encoder):
    new_cols = [col + '_te' for col in CATEGORICAL_COLS]
    df.loc[:, new_cols] = encoder.transform(df[CATEGORICAL_COLS])
    df = df.drop(columns=CATEGORICAL_COLS)
    return df

def preprocess_data(df, encoder_path='./models/target_encoder_initial.pkl', mode='inference', encoder_updater=None):
    if mode == 'retrain_inference':
        encoder = joblib.load(encoder_path)
    elif mode == 'retrain_train':
        encoder = TargetEncoder()
        encoder.fit(df[CATEGORICAL_COLS], df['target'])
        joblib.dump(encoder, encoder_path)
    else:
        encoder, version = encoder_updater._current_model()
    df = preprocess_time_features(df)

    df['distance'] = distance(
        df['lat'],
        df['lon'],
        df['merchant_lat'],
        df['merchant_lon']
    )
    df['is_far'] = (df.distance > 500).astype(int)
    df = df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])

    df['is_night_and_far'] = ((df['is_night'] == 1) & (df['is_far'] == 1)).astype(int)
    df['amount_x_distance'] = df['amount'] * df['distance']
    df['amount_per_citizen'] = df['amount'] / (df['population_city'] + 1e-7)

    df = cat_encoding(df, encoder)

    df = df.drop(columns=['name_1', 'name_2'])
    return df
