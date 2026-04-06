"""Export Snowflake tables to CSV files for the blinkit-dashboard project."""
import snowflake.connector
import csv
import os
import tomllib

# Read connection params from secrets
secrets_path = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
with open(secrets_path, "rb") as f:
    secrets = tomllib.load(f)
sf = secrets["connections"]["snowflake"]

conn = snowflake.connector.connect(
    account=sf["account"],
    user=sf["user"],
    password=sf["password"],
    warehouse=sf["warehouse"],
    database=sf["database"],
    schema=sf["schema"],
    role=sf["role"],
)

data_dir = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(data_dir, exist_ok=True)

tables = {
    "blinkit_orders.csv": "SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, PROMISED_DELIVERY_TIME, ACTUAL_DELIVERY_TIME, DELIVERY_STATUS, ORDER_TOTAL, PAYMENT_METHOD, DELIVERY_PARTNER_ID, STORE_ID FROM BLINKIT_ORDERS ORDER BY ORDER_ID",
    "blinkit_order_items.csv": "SELECT ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, TOTAL_PRICE FROM BLINKIT_ORDER_ITEMS ORDER BY ORDER_ID, PRODUCT_ID",
    "blinkit_delivery_performance.csv": "SELECT ORDER_ID, DELIVERY_PARTNER_ID, PROMISED_TIME, ACTUAL_TIME, DELIVERY_TIME_MINUTES, DISTANCE_KM, DELIVERY_STATUS, REASONS_IF_DELAYED FROM BLINKIT_DELIVERY_PERFORMANCE ORDER BY ORDER_ID",
    "blinkit_marketing_performance.csv": "SELECT CAMPAIGN_ID, CAMPAIGN_NAME, DATE, TARGET_AUDIENCE, CHANNEL, IMPRESSIONS, CLICKS, CONVERSIONS, SPEND, REVENUE_GENERATED, ROAS FROM BLINKIT_MARKETING_PERFORMANCE ORDER BY DATE, CAMPAIGN_ID",
}

cur = conn.cursor()
for filename, query in tables.items():
    filepath = os.path.join(data_dir, filename)
    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"Exported {len(rows)} rows to {filename}")

cur.close()
conn.close()
print("Done!")
