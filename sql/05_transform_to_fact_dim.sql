/* ==================================================================
   Step 5: Transform to analytics layer
   PURPOSE: Create base view and deterministic data splits
   DETAILS:
     TRANSACTIONS_BASE adds boolean fraud flag
     TRANSACTIONS_SPLIT applies 80/10/10 train/valid/test partition
     Split uses HASH(ID) for reproducibility across runs
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA ANALYTICS;

-- Base view
CREATE OR REPLACE VIEW TRANSACTIONS_BASE AS
SELECT
    r.*,
    CASE WHEN r.CLASS = 1 THEN TRUE ELSE FALSE END AS IS_FRAUD  -- boolean fraud flag
FROM FRAUD_DETECTION.STAGING.TRANSACTIONS_RAW r;

-- Table with deterministic 80/10/10 split
CREATE OR REPLACE TABLE TRANSACTIONS_SPLIT AS
SELECT
    b.*,
    MOD(ABS(HASH(b.ID)), 10) AS FOLD,  -- 10 integers (0-9): 10 buckets
    CASE                               
        WHEN MOD(ABS(HASH(b.ID)), 10) < 8 THEN 'TRAIN'  -- 0-7 (80%)
        WHEN MOD(ABS(HASH(b.ID)), 10) = 8 THEN 'VALID'  --  8  (10%)
        ELSE 'TEST'                                     --  9  (10%)
    END AS SPLIT
FROM TRANSACTIONS_BASE b;

-- Verify total row count
SELECT COUNT(*) AS total_rows
FROM TRANSACTIONS_BASE;

-- Verify split distribution and fraud balance
SELECT
    SPLIT,
    COUNT(*) AS number_of_rows,
    SUM(CASE WHEN IS_FRAUD THEN 1 ELSE 0 END) AS fraud_rows,
    ROUND(100.0 * SUM(CASE WHEN IS_FRAUD THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS fraud_pct
FROM TRANSACTIONS_SPLIT
GROUP BY SPLIT
ORDER BY SPLIT;

-- SELECT * FROM TRANSACTIONS_BASE LIMIT 20;
-- SELECT * FROM TRANSACTIONS_SPLIT LIMIT 20;
