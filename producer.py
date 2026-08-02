import time
import json
import yfinance as yf
import pandas as pd

from azure.eventhub import EventHubProducerClient, EventData

EVENTHUB_NAME = "artemzharkov10_evh"
CONNECTION_STR = ""

def fetch_historical_data():
    df = yf.download("BTC-USD", start="2015-01-01")
    
    if df.empty:
        return []
        
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
        
    df = df.reset_index()
    parsed_data = []
    
    iteration_counter = 0 
    
    for _, row in df.iterrows():
        date_str = row['Date'].strftime('%Y-%m-%d %H:%M:%S')
        
        event_payload = {
            "Date": date_str,
            "Open": float(row['Open']),
            "High": float(row['High']),
            "Low": float(row['Low']),
            "Close": float(row['Close']),
            "Volume": float(row['Volume']),
        }
        
        # Эволюция схемы: добавляем поле после 1000 записей
        if iteration_counter >= 1000:
            event_payload["Market_Cap"] = float(row['Close']) * float(row['Volume'])
            
        parsed_data.append(event_payload)
        iteration_counter += 1
        
    return parsed_data

def run_producer():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )
        
    candles = fetch_historical_data()
    if not candles:
        print("No data to send.")
        return

    BATCH_SIZE = 200
    total_sent = 0
        
    with producer:
        for i in range(0, len(candles), BATCH_SIZE):
            chunk = candles[i : i + BATCH_SIZE]
            
            try:
                event_batch = producer.create_batch()
                
                for item in chunk:
                    event_batch.add(EventData(json.dumps(item)))
                    
                producer.send_batch(event_batch)
                
                total_sent += len(chunk)
                current_batch = (i // BATCH_SIZE) + 1
                
                print(f"Sent batch {current_batch}. Total messages: {total_sent}. Last Date: {chunk[-1]['Date']}")
                time.sleep(2)
                
            except Exception as e:
                print(f"Process stopped: {e}")
                break
                
if __name__ == "__main__":
    run_producer()