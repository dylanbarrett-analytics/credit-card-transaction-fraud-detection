/* ==================================================================
   Step 4: Load CSV into staging table
   PURPOSE: Copy raw data from stage into TRANSACTIONS_RAW
   DETAILS:
     Validate parsing with RETURN_10_ROWs and RETURN_ERRORS
     COPY INTO loads data from @TRANSACTIONS_STAGE
     Verification checks confirm row counts and load history
   ================================================================== */

USE WAREHOUSE FRAUD_WH;         -- warehouse required for data load
USE DATABASE FRAUD_DETECTION;
USE SCHEMA STAGING;

-- Validate parse (sample rows, no data loaded)
COPY INTO TRANSACTIONS_RAW
FROM @TRANSACTIONS_STAGE
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
VALIDATION_MODE = 'RETURN_10_ROWS';  -- first 10 rows

-- Validate errors (no data loaded)
COPY INTO TRANSACTIONS_RAW
FROM @TRANSACTIONS_STAGE
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
VALIDATION_MODE = 'RETURN_ERRORS';

-- Load data into staging table
COPY INTO TRANSACTIONS_RAW
    (id, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10,
     V11, V12, V13, V14, V15, V16, V17, V18, V19, V20,
     V21, V22, V23, V24, V25, V26, V27, V28, Amount, Class)
FROM @TRANSACTIONS_STAGE
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'ABORT_STATEMENT';

-- Verify row count
SELECT COUNT(*) AS row_count
FROM FRAUD_DETECTION.STAGING.TRANSACTIONS_RAW;

-- Spot-check sample rows
SELECT * FROM TRANSACTIONS_RAW LIMIT 20;

-- Review copy history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FRAUD_DETECTION.STAGING.TRANSACTIONS_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())  -- last 24 hours
))
ORDER BY LAST_LOAD_TIME DESC;
