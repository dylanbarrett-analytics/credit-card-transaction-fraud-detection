/* ==================================================================
   Step 2: Create file format and stage
   PURPOSE: Define CSV parsing rules and internal staging area
   DETAILS:
     Stage stores uploaded raw files temporarily
     File format specifies how to parse CSV structure
     Both are required for COPY INTO to function correctly
   ================================================================== */

USE DATABASE FRAUD_DETECTION;
USE SCHEMA STAGING;

-- CSV parsing rules
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1                 -- ignore header row
    TRIM_SPACE = TRUE               -- remove leading/trailing spaces
    NULL_IF = ('', 'NULL', 'null')  -- treat these as NULLs
    COMPRESSION = 'AUTO';           -- auto-decompress .gz on load

-- Create an internal stage to hold uploaded files
CREATE OR REPLACE STAGE TRANSACTIONS_STAGE
    FILE_FORMAT = CSV_FORMAT;

-- Optional validation checks (run manually if needed)
-- SHOW FILE FORMATS LIKE 'CSV_FORMAT';
-- DESCRIBE STAGE TRANSACTIONS_STAGE;
