from quixstreams import Application
from datetime import datetime
import pandas as pd
import json


def parse_period(period_str):
    # Streamlined date formats for known input formats
    date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H', '%Y-%m-%d %H:%M']
    for date_format in date_formats:
        try:
            return datetime.strptime(period_str, date_format)
        except ValueError:
            continue
    print(f"Failed to parse date: {period_str}")
    return None

def calculate_lags(window):
    # Convert window to a DataFrame
    df = pd.DataFrame({'value': window})
    
    features = {
        'lag_1': float(df['value'].shift(1).iloc[-1]) if len(df) > 1 else None,
        'lag_2': float(df['value'].shift(2).iloc[-1]) if len(df) > 2 else None,
        'lag_6': float(df['value'].shift(6).iloc[-1]) if len(df) > 6 else None,
        'lag_12': float(df['value'].shift(12).iloc[-1]) if len(df) > 12 else None,
        'lag_24': float(df['value'].shift(24).iloc[-1]) if len(df) > 24 else None,
        'rolling_mean_7': float(df['value'].rolling(window=7).mean().iloc[-1]) if len(df) >= 7 else None,
        'rolling_std_7': float(df['value'].rolling(window=7).std().iloc[-1]) if len(df) >= 7 else None,
    }
    
    return features

def feature_pipeline_online(value, state, producer, feature_store_topic):
    period_dt = parse_period(value['Period'])
    
    if period_dt is None:
        print("Error parsing the period date.")
        return
    
    feature_record_id = period_dt.strftime('%Y-%m-%d %H')
    
    if value['Type'] == 'energy':
        window = state.get('energy_window', [])
        window.append(value['Value'])        
        if len(window) > 25:
            window.pop(0)
        
        state.set('energy_window', window)

        features = calculate_lags(window)
        features.update({'hour': period_dt.hour,
            'day_of_week': period_dt.weekday(),
            'month': period_dt.month})
        
        message = {
            "id": feature_record_id,
            **features
        }

        producer.produce(topic=feature_store_topic.name, key=feature_record_id, value=json.dumps(message))
    
    elif value['Type'] == 'temperature':
        message = {
            'id': feature_record_id,
            'temperature_forecast': value['Value']
        }
        producer.produce(topic=feature_store_topic.name, key=feature_record_id, value=json.dumps(message))

app = Application(broker_address='localhost:9092', consumer_group='example')

# Define a topic for real-time feature storage with JSON serialization
feature_store_topic = app.topic(name='feature_store', value_serializer='json')

# Combined data topic setup
incoming_data_topic = app.topic(name='incoming_data', value_deserializer='json')

# Producer for sending feature data
with app.get_producer() as producer:
    # Simulate or integrate data handling and processing
    data_df = app.dataframe(topic=incoming_data_topic).update(
        lambda value, state: feature_pipeline_online(value, state, producer, feature_store_topic), 
        stateful=True
    )
    app.run(data_df)