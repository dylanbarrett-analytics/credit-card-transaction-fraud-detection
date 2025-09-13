/* ==================================================================
   Step 6: Create reporting summaries
   PURPOSE: Provide high-level fraud rate summaries for quick inspection
   DETAILS:
     RPT_OVERALL_SUMMARY: one-row class balance
     RPT_SPLIT_SUMMARY: fraud rate by TRAIN / VALID / TEST
     RPT_FRAUD_RATE_BY_AMOUNT: fraud rate by amount bucket within each split
     Source table: ANALYTICS.TRANSACTIONS_SPLIT
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA REPORTING;

-- ------------------------------------------------------------------
-- Overall class balance (one row)
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_OVERALL_SUMMARY
COMMENT = 'One-row class balance from ANALYTICS.TRANSACTIONS_SPLIT' AS
SELECT
    COUNT(*)                                                      AS total_rows,
    SUM(IFF(IS_FRAUD,1,0))                                        AS fraud_rows,
    ROUND(100.0 * SUM(IFF(IS_FRAUD,1,0)) / NULLIF(COUNT(*),0), 2) AS fraud_pct
FROM ANALYTICS.TRANSACTIONS_SPLIT;

-- ------------------------------------------------------------------
-- Split summary (TRAIN / VALID / TEST)
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_SPLIT_SUMMARY
COMMENT = 'Fraud rate by TRAIN / VALID / TEST split' AS
SELECT
    SPLIT,
    COUNT(*)                                                      AS number_of_rows,
    SUM(IFF(IS_FRAUD,1,0))                                        AS fraud_rows,
    ROUND(100.0 * SUM(IFF(IS_FRAUD,1,0)) / NULLIF(COUNT(*),0), 2) AS fraud_pct
FROM ANALYTICS.TRANSACTIONS_SPLIT
GROUP BY SPLIT
ORDER BY SPLIT;

-- ------------------------------------------------------------------
-- Fraud rate by amount buckets within each split
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_FRAUD_RATE_BY_AMOUNT
COMMENT = 'Fraud rate by transaction amount bucket per split' AS
WITH BUCKETS AS (
    SELECT
        SPLIT,
        CASE
            WHEN AMOUNT IS NULL THEN 'NULL'
            WHEN AMOUNT <= 10   THEN '0-10'
            WHEN AMOUNT <= 50   THEN '11-50'
            WHEN AMOUNT <= 100  THEN '51-100'
            WHEN AMOUNT <= 500  THEN '101-500'
            WHEN AMOUNT <= 1000 THEN '501-1000'
            WHEN AMOUNT <= 5000 THEN '1001-5000'
            ELSE '5001+'
        END AS AMOUNT_BUCKET,
        IS_FRAUD
    FROM ANALYTICS.TRANSACTIONS_SPLIT
)
SELECT
    SPLIT,
    AMOUNT_BUCKET,
    COUNT(*)                                                      AS number_of_rows,
    SUM(IFF(IS_FRAUD,1,0))                                        AS fraud_rows,
    ROUND(100.0 * SUM(IFF(IS_FRAUD,1,0)) / NULLIF(COUNT(*),0), 2) AS fraud_pct
FROM BUCKETS
GROUP BY SPLIT, AMOUNT_BUCKET
ORDER BY
    SPLIT,
    CASE AMOUNT_BUCKET
        WHEN 'NULL'      THEN 0
        WHEN '0-10'      THEN 1
        WHEN '11-50'     THEN 2
        WHEN '51-100'    THEN 3
        WHEN '101-500'   THEN 4
        WHEN '501-1000'  THEN 5
        WHEN '1001-5000' THEN 6
        ELSE 7
    END;

-- SELECT * FROM RPT_OVERALL_SUMMARY;
-- SELECT * FROM RPT_SPLIT_SUMMARY;
-- SELECT * FROM RPT_FRAUD_RATE_BY_AMOUNT;

-- Cross-check totals for consistency
-- SELECT
--   (SELECT total_rows FROM RPT_OVERALL_SUMMARY) AS overall_total,
--   (SELECT SUM(number_of_rows) FROM RPT_SPLIT_SUMMARY) AS split_total;
    
