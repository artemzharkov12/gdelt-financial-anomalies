import time
import requests
import json
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData


BINANCE_URL = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000"
EVENTHUB_NAME = "artemzharkov10_evh"
CONNECTION_STR = "deleted for github"

def fetch_1000_candles():
    try:
        response = requests.get(BINANCE_URL)
        response.raise_for_status() 
        
        data_list = response.json()
        parsed_data = []
        
        for i, data in enumerate(data_list):
            timestamp_ms = data[0]
            date_str = datetime.fromtimestamp(timestamp_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
            
            event_payload = {
                "Date": date_str,
                "Open": float(data[1]),
                "High": float(data[2]),
                "Low": float(data[3]),
                "Close": float(data[4]),
                "Volume": float(data[5]),
            }
            if i >= 500:
                event_payload["Market_Cap"] = float(data[4]) * float(data[5])
            
            parsed_data.append(event_payload)
            
        return parsed_data
        
    except Exception as e:
        print(f"API Error: {e}")
        return []

def run_producer():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )
        
    print("Downloading 1000 candles from Binance...")
    candles = fetch_1000_candles()
    
    if not candles:
        print("No data to send.")
        return

    BATCH_SIZE = 100
    total_sent = 0
        
    with producer:
        # Splitting our list of 1000 items into chunks
        for i in range(0, len(candles), BATCH_SIZE):
            chunk = candles[i : i + BATCH_SIZE]
            
            try:
                # Creating an empty batch
                event_batch = producer.create_batch()
                
                # Adding 100 messages to the batch
                for item in chunk:
                    message_str = json.dumps(item)
                    event_batch.add(EventData(message_str))
                    
                # Sending the entire batch at once
                producer.send_batch(event_batch)
                
                total_sent += len(chunk)
                current_batch_num = (i // BATCH_SIZE) + 1
                
                print(f"Sent batch {current_batch_num}/10. Total messages in Event Hub: {total_sent}. Last Date: {chunk[-1]['Date']}")
                
                # Waiting 2 seconds before sending the next 100 packs
                time.sleep(2)
                
            except Exception as e:
                print(f"Process stopped due to an error: {e}")
                break
                

if __name__ == "__main__":
    run_producer()