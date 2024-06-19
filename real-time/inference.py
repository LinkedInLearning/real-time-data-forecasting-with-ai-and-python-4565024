from confluent_kafka import Consumer
import pandas as pd
import json
import joblib
import warnings

warnings.filterwarnings("ignore")

def load_model(model_path):
  model = joblib.load(model_path)
  return model

def fetch_all_feature_records():
    # Kafka Consumer configuration for reading from the beginning of the topic
    conf = {
        'bootstrap.servers': "localhost:9092",
        'group.id': "feature_store_reader",
        'auto.offset.reset': 'latest'
        }

    # Initialize Kafka Consumer and subscribe to the topic
    consumer = Consumer(conf)
    consumer.subscribe(['feature_store'])

    feature_records = []  # List to store feature data

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a 1-second timeout
            if msg is None:
                break  # Exit loop if no more messages
            if not msg.error():
                # Convert message from JSON and add to list
                feature_records.append(json.loads(msg.value().decode('utf-8')))
            else:
                break  # Exit loop on error
    finally:
        consumer.close()  # Clean up: close consumer

    return feature_records  # Return the collected feature records


latest_feature_record = pd.DataFrame(fetch_all_feature_records()).groupby('id').first().dropna().sort_index(ascending=False)[:1]

feature_names = ['lag_1', 'lag_2', 'lag_6', 'lag_12', 'lag_24', 'rolling_mean_7', 'rolling_std_7', 'hour', 'day_of_week', 'month', 'temperature_forecast']
latest_feature_record = latest_feature_record[feature_names]

model = load_model("models/energy_demand_model_v4.pkl")
prediction = str(model.predict(latest_feature_record))
print(f"Next 24 hours energy prediction = {prediction} Last updated:{str(latest_feature_record.index.to_list())}")