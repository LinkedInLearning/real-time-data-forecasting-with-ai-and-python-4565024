# feature_processing.py

import pandas as pd

def feature_pipeline(energy_data):
    df_daily = energy_data.resample('D').sum("value")

    batch_df = pd.DataFrame()

    # Lagging features
    batch_df['lag_1'] = df_daily['value'].shift(1) # Energy demand -1 day

    batch_df['lag_4'] = df_daily['value'].shift(4) # Energy demand +3 days - 7 days
    batch_df['lag_5'] = df_daily['value'].shift(5) # Energy demand +2 days - 7 days
    batch_df['lag_6'] = df_daily['value'].shift(6) # Energy demand +1 days - 7 days

    batch_df['lag_11'] = df_daily['value'].shift(11) # Energy demand +3 days - 14 days
    batch_df['lag_12'] = df_daily['value'].shift(12) # Energy demand +2 days - 14 days
    batch_df['lag_13'] = df_daily['value'].shift(13) # Energy demand +1 days - 14 days

    # Rolling statistics
    batch_df['rolling_mean_7'] = df_daily['value'].rolling(window=7).mean().round(2)
    batch_df['rolling_std_7'] = df_daily['value'].rolling(window=7).std().round(2)
    
    batch_df = batch_df.dropna()

    return batch_df

def get_targets(energy_data):
    df_daily = energy_data.resample('D').sum("value")
    
    targets_df = pd.DataFrame()
    # Lagging target variable
    targets_df['target_1d'] = df_daily['value'].shift(-1) # Next day
    targets_df['target_2d'] = df_daily['value'].shift(-2) # Second-next day
    targets_df['target_3d'] = df_daily['value'].shift(-3) # Third-next day
    targets_df = targets_df.dropna()

    return targets_df