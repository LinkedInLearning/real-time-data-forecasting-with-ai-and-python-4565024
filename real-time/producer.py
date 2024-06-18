from quixstreams import Application
import csv
import json
import time

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="demand_forecasting")

# Define a single topic for both types of data
combined_data_topic = app.topic(name="combined_data", value_serializer="json")

# Paths to the CSV files with data
ENERGY_DATA_FILE = "data/energy_data.csv"
TEMPERATURE_DATA_FILE = "data/weather_data.csv"

def load_csv(path: str):
    rows = []
    with open(path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert all string fields that should be numeric
            for key, value in row.items():
                try:
                    row[key] = float(value)
                except ValueError:
                    pass
            rows.append(row)
    return rows

# Load data from the CSV files
energy_data = load_csv(ENERGY_DATA_FILE)
temperature_data = load_csv(TEMPERATURE_DATA_FILE)

print(f'Writing CSV data to the "{combined_data_topic.name}" topic ...')
with app.get_producer() as producer:
    # Assuming both CSV files are aligned by rows (same timestamps or matching periods)
    for energy, temp in zip(energy_data, temperature_data):
        # Prepare a message for energy data
        energy_message = {
            "Period": energy['period'],
            "Type": "energy",
            "Value": energy['value']
        }
        # Prepare a message for temperature data
        temperature_message = {
            "Period": temp['period'],
            "Type": "temperature",
            "Value": temp['temperature']
        }
        
        # Send energy data message to the combined topic
        producer.produce(
            topic=combined_data_topic.name,
            key="energy-csv",
            value=json.dumps(energy_message),
        )
        
        time.sleep(2) # Adjust the sleep time if necessary to simulate real-time flow
        
        # Send temperature data message to the same combined topic
        producer.produce(
            topic=combined_data_topic.name,
            key="temperature-csv",
            value=json.dumps(temperature_message),
        )
        
        time.sleep(3)  # Adjust the sleep time if necessary to simulate real-time flow

# Ensure all messages are sent before closing
producer.flush()
