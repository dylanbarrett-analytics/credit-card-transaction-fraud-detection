/* ==================================================================
   Step 10a: Prepare ML features
   PURPOSE: Provide a modeling-ready feature matrix with label and splits
   DETAILS:
     Source: ANALYTICS.TRANSACTIONS_SPLIT
     Data types normalized to FLOAT; LABEL = 1/0
     Includes ID, SPLIT, FOLD, for joinability and cross-validation
     Consumers filter by SPLIT (TRAIN / VALID / TEST) as needed
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW ML_FEATURES
COMMENT = 'Modeling feature matrix with LABEL, SPLIT, FOLD; source ANALYTICS.TRANSACTIONS_SPLIT' AS
SELECT
    ID::INT               AS ID,
    SPLIT,
    FOLD,
    IFF(IS_FRAUD, 1, 0)   AS LABEL, -- target variable
    AMOUNT::FLOAT         AS AMOUNT,
     V1::FLOAT AS V1,   V2::FLOAT AS V2,   V3::FLOAT AS V3,   V4::FLOAT AS V4,
     V5::FLOAT AS V5,   V6::FLOAT AS V6,   V7::FLOAT AS V7,   V8::FLOAT AS V8,
     V9::FLOAT AS V9,  V10::FLOAT AS V10, V11::FLOAT AS V11, V12::FLOAT AS V12,
    V13::FLOAT AS V13, V14::FLOAT AS V14, V15::FLOAT AS V15, V16::FLOAT AS V16,
    V17::FLOAT AS V17, V18::FLOAT AS V18, V19::FLOAT AS V19, V20::FLOAT AS V20,
    V21::FLOAT AS V21, V22::FLOAT AS V22, V23::FLOAT AS V23, V24::FLOAT AS V24,
    V25::FLOAT AS V25, V26::FLOAT AS V26, V27::FLOAT AS V27, V28::FLOAT AS V28
FROM FRAUD_DETECTION.ANALYTICS.TRANSACTIONS_SPLIT;

-- SELECT SPLIT, COUNT(*) AS number_of_rows FROM ML_FEATURES GROUP BY SPLIT ORDER BY SPLIT;
-- SELECT SPLIT, LABEL, COUNT(*) AS rows_per_label FROM ML_FEATURES GROUP BY SPLIT, LABEL ORDER BY SPLIT, LABEL;