-- ============================================================================
-- Blinkit Data Warehouse - COPY INTO Statements
-- Database: BLINKIT_DW | Schema: RAW
-- 
-- Prerequisites:
--   1. Run create_tables.sql first to create the tables
--   2. Create a named stage or use a temporary stage to upload CSV files
--   3. Upload CSV files from the data/ directory to the stage
-- ============================================================================

-- Create a named stage for loading data
CREATE OR REPLACE STAGE BLINKIT_DW.RAW.BLINKIT_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL')
        EMPTY_FIELD_AS_NULL = TRUE
    );

-- Upload CSV files to stage (run from SnowSQL or Snowflake CLI):
-- PUT file://data/blinkit_orders.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
-- PUT file://data/blinkit_order_items.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
-- PUT file://data/blinkit_delivery_performance.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;
-- PUT file://data/blinkit_marketing_performance.csv @BLINKIT_DW.RAW.BLINKIT_STAGE;

-- ============================================================================
-- 1. Load Orders
-- ============================================================================
COPY INTO BLINKIT_DW.RAW.BLINKIT_ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_DATE, PROMISED_DELIVERY_TIME,
    ACTUAL_DELIVERY_TIME, DELIVERY_STATUS, ORDER_TOTAL,
    PAYMENT_METHOD, DELIVERY_PARTNER_ID, STORE_ID
)
FROM @BLINKIT_DW.RAW.BLINKIT_STAGE/blinkit_orders.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- 2. Load Order Items (TOTAL_PRICE is a computed column - excluded)
-- ============================================================================
COPY INTO BLINKIT_DW.RAW.BLINKIT_ORDER_ITEMS (
    ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE
)
FROM (
    SELECT $1, $2, $3, $4
    FROM @BLINKIT_DW.RAW.BLINKIT_STAGE/blinkit_order_items.csv
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- 3. Load Delivery Performance (DELIVERY_TIME_MINUTES is a computed column - excluded)
-- ============================================================================
COPY INTO BLINKIT_DW.RAW.BLINKIT_DELIVERY_PERFORMANCE (
    ORDER_ID, DELIVERY_PARTNER_ID, PROMISED_TIME, ACTUAL_TIME,
    DISTANCE_KM, DELIVERY_STATUS, REASONS_IF_DELAYED
)
FROM (
    SELECT $1, $2, $3, $4, $6, $7, $8
    FROM @BLINKIT_DW.RAW.BLINKIT_STAGE/blinkit_delivery_performance.csv
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- 4. Load Marketing Performance
-- ============================================================================
COPY INTO BLINKIT_DW.RAW.BLINKIT_MARKETING_PERFORMANCE (
    CAMPAIGN_ID, CAMPAIGN_NAME, DATE, TARGET_AUDIENCE, CHANNEL,
    IMPRESSIONS, CLICKS, CONVERSIONS, SPEND, REVENUE_GENERATED, ROAS
)
FROM @BLINKIT_DW.RAW.BLINKIT_STAGE/blinkit_marketing_performance.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';
