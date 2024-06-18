# feature_store.py

import yaml
import pandas as pd
import os

def update_feature_store(batch_df, yaml_file_path, targets = False):

    # Load YAML file
    with open(yaml_file_path, 'r') as file:
        config = yaml.safe_load(file)

    if targets:
        # Write feature set to blob
        targets_path = config['feature_store']['targets_path']
        batch_df.to_csv(targets_path, index=True)    

        # Update the 'latest_target' field in the 'feature_store' dictionary
        new_last_update = batch_df.index.max()
        config['feature_store']['latest_target'] = str(new_last_update)
        
        # Write the updated configuration back to the YAML file
        with open(yaml_file_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        
        return("Targets updated in feature store with last date " + str(new_last_update))

    # Write feature set to blob
    features_path = config['feature_store']['features_path']
    batch_df.to_csv(features_path, index=True)    

    # Update the 'last_update' field in the 'feature_store' dictionary
    new_last_update = batch_df.index.max()
    config['feature_store']['latest_feature'] = str(new_last_update)
    
    # Write the updated configuration back to the YAML file
    with open(yaml_file_path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)
    
    print("Feature store updated with last date " + str(new_last_update))


def fetch_data_from_store(period=None, yaml_file_path = None, targets = False):
    
    # Load YAML file
    with open(yaml_file_path, 'r') as file:
        config = yaml.safe_load(file)

    if targets:
        data_path = config['feature_store']['targets_path']
        data_df = pd.read_csv(data_path, index_col='period', parse_dates=True)
        if period is not None:        
            data_df = data_df.loc[period:]
        return data_df

    data_path = config['feature_store']['features_path']
    
    # Load data from CSV
    data_df = pd.read_csv(data_path, index_col='period', parse_dates=True)
    
    # Filter data by period if provided
    if period is not None:        
        # Filter to get data from the period onward
        data_df = data_df.loc[period:]
    
    return data_df