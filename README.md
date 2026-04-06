# Blinkit Dashboard

A Streamlit dashboard for analyzing Blinkit's operational data including orders, deliveries, and marketing performance.

## Data Warehouse

- **Database**: `BLINKIT_DW`
- **Schema**: `RAW`
- **Tables**:
  | Table | Rows | Description |
  |-------|------|-------------|
  | `BLINKIT_ORDERS` | 5,000 | Customer orders with delivery and payment details |
  | `BLINKIT_ORDER_ITEMS` | 1,000 | Line items per order with quantity and pricing |
  | `BLINKIT_DELIVERY_PERFORMANCE` | 1,000 | Delivery tracking with time and distance metrics |
  | `BLINKIT_MARKETING_PERFORMANCE` | 5,400 | Campaign metrics across channels |

## Project Structure

```
blinkit-dashboard/
├── streamlit_app.py          # Main Streamlit dashboard application
├── requirements.txt          # Python dependencies
├── pyproject.toml            # Project metadata
├── data/                     # Exported CSV data files
│   ├── blinkit_orders.csv
│   ├── blinkit_order_items.csv
│   ├── blinkit_delivery_performance.csv
│   └── blinkit_marketing_performance.csv
├── ddl/
│   ├── create_tables.sql     # Table DDL statements
│   └── copy_into.sql         # COPY INTO statements for data loading
└── export_data.py            # Script to export tables from Snowflake
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Create Snowflake Tables

Run the DDL statements in `ddl/create_tables.sql` against your Snowflake account to create the tables.

### 3. Load Data

Upload the CSV files from `data/` to a Snowflake stage and run the COPY INTO statements in `ddl/copy_into.sql`:

```sql
-- Upload files to stage (from SnowSQL)
PUT file://data/blinkit_orders.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
PUT file://data/blinkit_order_items.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
PUT file://data/blinkit_delivery_performance.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
PUT file://data/blinkit_marketing_performance.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;

-- Then run ddl/copy_into.sql to load data into tables
```

### 4. Configure Streamlit Secrets

Create `.streamlit/secrets.toml` with your Snowflake connection details:

```toml
[connections.snowflake]
account = "your_account"
user = "your_user"
password = "your_password"
warehouse = "your_warehouse"
database = "BLINKIT_DW"
schema = "RAW"
role = "your_role"
```

### 5. Run the Dashboard

```bash
streamlit run streamlit_app.py
```

## Notes

- `BLINKIT_ORDER_ITEMS.TOTAL_PRICE` is a computed column (`QUANTITY * UNIT_PRICE`) — excluded from COPY INTO.
- `BLINKIT_DELIVERY_PERFORMANCE.DELIVERY_TIME_MINUTES` is a computed column (`TIMESTAMPDIFF(MINUTE, PROMISED_TIME, ACTUAL_TIME)`) — excluded from COPY INTO.
