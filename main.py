import boto3
import pandas as pd
import json
import time

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Kinesis Stream Name
kinesis_stream_name = 'olist_data_sales'

# Read CSV file
df = pd.read_csv('merged_olist_dataset.csv')

# Variable to track the current date
current_date = None

# Iterate through rows and send to Kinesis
for i, (_, row) in enumerate(df.iterrows()):
    # Skip rows with missing dates
    if pd.isna(row['order_approved_at']):
        continue

    # Extract the date part of order_approved_at (YYYY-MM-DD)
    approved_date = pd.to_datetime(row['order_approved_at']).date()

    # Check if the date has changed
    if current_date is None:
        current_date = approved_date
    elif approved_date != current_date:
        print(f"Date changed from {current_date} to {approved_date}. Pausing for 1 hour...")
        time.sleep(3)  # Pause for 1 hour
        current_date = approved_date

    # Convert row to JSON
    record = row.to_dict()

    # Send record to Kinesis
    kinesis_client.put_record(
        StreamName=kinesis_stream_name,
        Data=json.dumps(record),
        PartitionKey=str(row['order_id'])  # Use order_id for partitioning
    )

    print(f"Data streamed: {i} (Order ID: {row['order_id']}) at {current_date}")
    time.sleep(1)  # Simulate real-time streaming

print("All records sent to Kinesis!")
