"""
Score script cho Rossmann Sales Prediction.
Features phải KHỚP CHÍNH XÁC với training pipeline trong train.ipynb.
"""
import os
import pandas as pd
import numpy as np
import pickle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Load model
model_path = os.path.join(BASE_DIR, "model_output", "model.pkl") if os.path.exists(
    os.path.join(BASE_DIR, "model_output", "model.pkl")) else os.path.join(BASE_DIR, "model.pkl")
with open(model_path, "rb") as f:
    model = pickle.load(f)

# Load test + store
test = pd.read_csv(os.path.join(DATA_DIR, "test.csv"), parse_dates=['Date'],
                    dtype={'StateHoliday': str, 'SchoolHoliday': str})
store = pd.read_csv(os.path.join(DATA_DIR, "store.csv"))


def create_features(df, store_df):
    """Feature engineering KHỚP với training pipeline."""
    df = df.copy()
    df = df.merge(store_df, how='left', on='Store')

    # Fill missing — giống training
    df['CompetitionDistance'] = df['CompetitionDistance'].fillna(1e5)
    df['CompetitionOpenSinceMonth'] = df['CompetitionOpenSinceMonth'].fillna(1)
    df['CompetitionOpenSinceYear'] = df['CompetitionOpenSinceYear'].fillna(df['Date'].dt.year)
    df['Promo2SinceWeek'] = df['Promo2SinceWeek'].fillna(0)
    df['Promo2SinceYear'] = df['Promo2SinceYear'].fillna(0)
    df['PromoInterval'] = df['PromoInterval'].fillna('')

    # Time features
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['DayOfWeek'] = df['Date'].dt.dayofweek
    df['WeekOfYear'] = df['Date'].dt.isocalendar().week.astype(int)
    df['IsWeekend'] = (df['DayOfWeek'] >= 5).astype(int)

    # Competition open duration
    df['CompetitionOpenMonths'] = (
        (df['Year'] - df['CompetitionOpenSinceYear']) * 12 +
        (df['Month'] - df['CompetitionOpenSinceMonth'])
    ).clip(lower=0)

    # IsPromoMonth
    month_map = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,
                 'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    promo_list = df['PromoInterval'].str.split(',').apply(
        lambda x: [month_map[m] for m in x if m in month_map] if isinstance(x, list) else [])
    df['IsPromoMonth'] = [d.month in p for d, p in zip(df['Date'], promo_list)]
    df['IsPromoMonth'] = df['IsPromoMonth'].astype(int)

    # Lag features = 0 cho test (không có lịch sử bán hàng)
    for lag in [1, 3, 7]:
        df[f'Sales_lag_{lag}'] = 0

    # Encode categorical — giống training
    for col in ['StateHoliday', 'StoreType', 'Assortment', 'SchoolHoliday']:
        if col in df.columns:
            df[col] = df[col].astype('category').cat.codes

    return df


# Features KHỚP CHÍNH XÁC với training
features = [
    'Store', 'DayOfWeek', 'Promo', 'SchoolHoliday', 'IsWeekend',
    'StoreType', 'Assortment',
    'CompetitionDistance', 'CompetitionOpenMonths',
    'Promo2', 'IsPromoMonth',
    'Year', 'Month', 'Day', 'WeekOfYear',
    'Sales_lag_1', 'Sales_lag_3', 'Sales_lag_7',
]

test_prepared = create_features(test, store)

# Đảm bảo đúng features
for f in features:
    if f not in test_prepared.columns:
        test_prepared[f] = 0

# Predict
test['PredictedSales'] = model.predict(test_prepared[features])

# Save submission
output_path = os.path.join(BASE_DIR, "submission.csv")
test[['Id', 'PredictedSales']].to_csv(output_path, index=False)
print(f"Submission saved: {output_path}")
print(f"Predictions: {test.shape[0]:,} rows")
print(f"Sales range: {test['PredictedSales'].min():.0f} -> {test['PredictedSales'].max():.0f}")
