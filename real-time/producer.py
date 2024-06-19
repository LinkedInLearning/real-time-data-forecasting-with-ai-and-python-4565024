# Part 1: Application Setup --
from quixstreams import Application
import csv
import json
import time
import random

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="demand_forecasting")

# Define a single topic for both types of data
combined_data_topic = app.topic(name="incoming_data", value_serializer="json")


# Part 2: Simulating Streaming Data --

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

# Function to introduce anomalies
def introduce_anomaly(value):
    return value * random.uniform(2.0, 3.0)  # Increase the value by 200% to 300%

print(f'Writing CSV data to the "{combined_data_topic.name}" topic ...')
with app.get_producer() as producer:
    # Determine the length of the shorter dataset to avoid index errors
    min_length = min(len(energy_data), len(temperature_data))
    
    # Initial step for the next anomaly
    next_anomaly_step = random.randint(40, 60)
    
    for i in range(min_length):
        # Access the current row from energy data
        energy = energy_data[i]
        
        # Introduce anomaly if the step matches the anomaly step
        if i == next_anomaly_step:
            energy['value'] = introduce_anomaly(energy['value'])
            next_anomaly_step = i + random.randint(40, 60)  # Schedule next anomaly
        
        # Prepare a message for energy data
        energy_message = {
            "Period": energy['period'],
            "Type": "energy",
            "Value": energy['value']
        }
        
        # Send the energy data message to the combined topic
        producer.produce(
            topic=combined_data_topic.name,
            key="energy-csv",
            value=json.dumps(energy_message),
        )
        
        time.sleep(0.2) # Simulate real-time flow with a sleep for energy data
        
        # Access the current row from temperature data
        temp = temperature_data[i]
        # Prepare a message for temperature data
        temperature_message = {
            "Period": temp['period'],
            "Type": "temperature",
            "Value": temp['temperature']
        }
        
        # Send the temperature data message to the same combined topic
        producer.produce(
            topic=combined_data_topic.name,
            key="temperature-csv",
            value=json.dumps(temperature_message),
        )
        
        time.sleep(0.3)  # Simulate real-time flow with a sleep for temperature data

# Part 3: Shutting Down ---
# Ensure all messages are sent before closing
producer.flush()