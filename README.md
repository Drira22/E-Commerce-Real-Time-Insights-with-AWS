# **Olist Sales Analysis and Visualization**

## **Project Overview**
This project focuses on processing, cleaning, and analyzing sales data from the Olist e-commerce platform. The data is ingested via AWS Kinesis, transformed using AWS Lambda, stored in Amazon S3, and queried using Amazon Athena. The project aims to extract valuable insights and create visualizations using Grafana.

---

## **Features Implemented**

### **1. Data Ingestion**
- **Source:** Sales data streams through AWS Kinesis.
- **Processing:** AWS Lambda cleans and transforms the JSON data.
- **Storage:** The transformed data is stored in Amazon S3 as JSON files.

### **2. Data Transformation**
- JSON cleaning:
  - Removed leading/trailing spaces from values.
  - Ensured records are single-line JSON objects for compatibility with Athena.
- Categorized and structured data into key subcategories:
  - `order_details`
  - `customer`
  - `product`
  - `seller`
  - `payment`
  - `shipping`
  - `order_item`

### **3. Querying and Analysis**
- Used **Amazon Athena** to query the transformed data stored in S3.
- Key queries:
  - **Total Sales by Date:** Analyze daily sales trends.
  - **Top 5 Product Categories:** Identify best-selling product categories.
  - **Orders by State:** Understand regional demand distribution.
  - **Orders with Multiple Products:** Identify large orders containing multiple products.

### **4. Visualization**
- Created interactive dashboards using **Grafana** to visualize:
  - Sales trends by date.
  - Top-performing product categories.
  - State-wise order distribution.

---

## **Technology Stack**
- **AWS Services:**
  - Kinesis
  - Lambda
  - S3
  - Athena
- **Data Visualization:** Grafana
- **Development Tools:** Python, Boto3, Pandas, and JSON
- **Version Control:** Git and GitHub

---

## **Setup Instructions**

### **1. AWS Setup**
1. Configure Kinesis data streams.
2. Deploy the Lambda function with the provided code for JSON transformation.
3. Create an S3 bucket to store transformed JSON files.

### **2. Querying**
1. Use Amazon Athena to connect to the S3 bucket.
2. Run the predefined SQL queries to analyze data.

### **3. Visualization**
1. Set up Grafana and connect it to Athena or other data sources.
2. Import or create dashboards using the prepared queries.

---

