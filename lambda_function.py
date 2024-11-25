import boto3
import base64
import json

# Initialize S3 client
s3_client = boto3.client('s3')

# S3 bucket name (replace with your bucket name)
S3_BUCKET_NAME = 'olist-stock-sales'

def clean_spaces(data):
    """Recursively clean extra spaces from all string values in the JSON object."""
    if isinstance(data, dict):
        return {key: clean_spaces(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_spaces(item) for item in data]
    elif isinstance(data, str):
        return data.strip()
    else:
        return data

def lambda_handler(event, context):
    print("Lambda function triggered by Kinesis...")

    # Iterate through records received from Kinesis
    for record in event['Records']:
        try:
            # Decode the Base64 Kinesis data
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            print("Decoded payload:", payload)

            # Parse the JSON payload
            data = json.loads(payload)
            print("Parsed JSON data:", data)

            # Clean extra spaces
            cleaned_data = clean_spaces(data)
            print("Cleaned JSON data:", cleaned_data)

            # Transform the data into subcategories
            transformed_data = {
                "order_details": {
                    "order_id": cleaned_data.get("order_id"),
                    "status": cleaned_data.get("order_status"),
                    "purchase_date": cleaned_data.get("order_purchase_timestamp"),
                    "approved_at": cleaned_data.get("order_approved_at"),
                    "delivered_carrier_date": cleaned_data.get("order_delivered_carrier_date"),
                    "delivered_customer_date": cleaned_data.get("order_delivered_customer_date"),
                    "estimated_delivery_date": cleaned_data.get("order_estimated_delivery_date"),
                },
                "customer": {
                    "customer_id": cleaned_data.get("customer_id"),
                    "unique_id": cleaned_data.get("customer_unique_id"),
                    "zip_code_prefix": cleaned_data.get("customer_zip_code_prefix"),
                    "city": cleaned_data.get("customer_city"),
                    "state": cleaned_data.get("customer_state"),
                },
                "product": {
                    "product_id": cleaned_data.get("product_id"),
                    "category": cleaned_data.get("product_category_name"),
                    "category_english": cleaned_data.get("product_category_name_english"),
                    "name_length": cleaned_data.get("product_name_lenght"),
                    "description_length": cleaned_data.get("product_description_lenght"),
                    "photos_qty": cleaned_data.get("product_photos_qty"),
                    "weight_g": cleaned_data.get("product_weight_g"),
                    "dimensions_cm": {
                        "length": cleaned_data.get("product_length_cm"),
                        "height": cleaned_data.get("product_height_cm"),
                        "width": cleaned_data.get("product_width_cm"),
                    },
                },
                "seller": {
                    "seller_id": cleaned_data.get("seller_id"),
                    "zip_code_prefix": cleaned_data.get("seller_zip_code_prefix"),
                    "city": cleaned_data.get("seller_city"),
                    "state": cleaned_data.get("seller_state"),
                },
                "payment": {
                    "sequential": cleaned_data.get("payment_sequential"),
                    "type": cleaned_data.get("payment_type"),
                    "installments": cleaned_data.get("payment_installments"),
                    "value": cleaned_data.get("payment_value"),
                },
                "shipping": {
                    "shipping_limit_date": cleaned_data.get("shipping_limit_date"),
                    "freight_value": cleaned_data.get("freight_value"),
                },
                "order_item": {
                    "order_item_id": cleaned_data.get("order_item_id"),
                },
            }

            # Save to S3 as JSON
            file_name = f"{transformed_data['order_details']['order_id']}.json"
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"streamed_data/{file_name}",
                Body=json.dumps(transformed_data)
            )
            print(f"Saved to S3: {file_name}")

        except Exception as e:
            print(f"Error processing record: {e}")
            continue

    return {"statusCode": 200, "body": "Processing complete"}
