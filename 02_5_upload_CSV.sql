/* ==================================================================
   Step 2.5: Upload CSV into the stage
   PURPOSE: Place raw transaction file into internal stage
   DETAILS:
     Upload via Snowsight UI into @TRANSACTIONS_STAGE
     Supports compression (e.g., .gz files) with automatic handling
     Listing confirms files are available before COPY INTO
   ================================================================== */

USE DATABASE FRAUD_DETECTION;
USE SCHEMA STAGING;

-- List files in stage after upload
LIST @TRANSACTIONS_STAGE;

-- Optional: inspect stage directory
-- SELECT * FROM DIRECTORY(@TRANSACTIONS_STAGE);