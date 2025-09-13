/* ==================================================================
   Step 8: Create dashboard feeds (parameter-driven)
   PURPOSE: Provide views for parameterized slicing and charting
   DETAILS:
     RPT_FEATURE_DRILLDOWN_STATS: mean/median/stddev per feature by class and split
     RPT_FEATURE_DRILLDOWN_BINS: normalized feature value distribution bins (class % within split/feature)
     Source table: ANALYTICS.TRANSACTIONS_SPLIT
     Includes splits: TRAIN, VALID, TEST
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA REPORTING;

-- ------------------------------------------------------------------
-- Stats per feature by class (FRAUD/LEGIT) and split
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_FEATURE_DRILL_DOWN_STATS
COMMENT = 'Mean/median/stddev per feature by FRAUD vs LEGIT within each split' AS
WITH CASTED AS (    -- UNPIVOT requires uniform data types
    SELECT
        SPLIT,
        IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
        AMOUNT::FLOAT AS AMOUNT,
         V1::FLOAT AS V1,   V2::FLOAT AS V2,   V3::FLOAT AS V3,   V4::FLOAT AS V4,
         V5::FLOAT AS V5,   V6::FLOAT AS V6,   V7::FLOAT AS V7,   V8::FLOAT AS V8,
         V9::FLOAT AS V9,  V10::FLOAT AS V10, V11::FLOAT AS V11, V12::FLOAT AS V12,
        V13::FLOAT AS V13, V14::FLOAT AS V14, V15::FLOAT AS V15, V16::FLOAT AS V16,
        V17::FLOAT AS V17, V18::FLOAT AS V18, V19::FLOAT AS V19, V20::FLOAT AS V20,
        V21::FLOAT AS V21, V22::FLOAT AS V22, V23::FLOAT AS V23, V24::FLOAT AS V24,
        V25::FLOAT AS V25, V26::FLOAT AS V26, V27::FLOAT AS V27, V28::FLOAT AS V28
    FROM FRAUD_DETECTION.ANALYTICS.TRANSACTIONS_SPLIT
),
UNPIVOTED AS (
    SELECT
        SPLIT,
        CLASS_LABEL,
        FEATURE,
        VALUE::FLOAT AS VALUE
    FROM CASTED
    UNPIVOT(VALUE FOR FEATURE IN (
        AMOUNT, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14,
        V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28
    ))
)
SELECT
    SPLIT,
    FEATURE,
    CLASS_LABEL,
    COUNT(*)      AS number_of_rows,
    AVG(VALUE)    AS avg_value,
    MEDIAN(VALUE) AS median_value,
    STDDEV(VALUE) AS stddev_value,
    MIN(VALUE)    AS min_value,
    MAX(VALUE)    AS max_value
FROM UNPIVOTED
GROUP BY SPLIT, FEATURE, CLASS_LABEL;

-- ------------------------------------------------------------------
-- Normalized feature value distribution bins per feature and split (class % within group)
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_FEATURE_DRILL_DOWN_BINS
COMMENT = 'Normalized feature value distribution bins per feature and split; % within FRAUD/LEGIT' AS
WITH CASTED AS (
    SELECT
        SPLIT,
        IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
        AMOUNT::FLOAT AS AMOUNT,
         V1::FLOAT AS V1,   V2::FLOAT AS V2,   V3::FLOAT AS V3,   V4::FLOAT AS V4,
         V5::FLOAT AS V5,   V6::FLOAT AS V6,   V7::FLOAT AS V7,   V8::FLOAT AS V8,
         V9::FLOAT AS V9,  V10::FLOAT AS V10, V11::FLOAT AS V11, V12::FLOAT AS V12,
        V13::FLOAT AS V13, V14::FLOAT AS V14, V15::FLOAT AS V15, V16::FLOAT AS V16,
        V17::FLOAT AS V17, V18::FLOAT AS V18, V19::FLOAT AS V19, V20::FLOAT AS V20,
        V21::FLOAT AS V21, V22::FLOAT AS V22, V23::FLOAT AS V23, V24::FLOAT AS V24,
        V25::FLOAT AS V25, V26::FLOAT AS V26, V27::FLOAT AS V27, V28::FLOAT AS V28
    FROM FRAUD_DETECTION.ANALYTICS.TRANSACTIONS_SPLIT
),
UNPIVOTED AS (
    SELECT
        SPLIT,
        CLASS_LABEL,
        FEATURE,
        VALUE::FLOAT AS VALUE
    FROM CASTED
    UNPIVOT (VALUE FOR FEATURE IN (
        AMOUNT, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14,
        V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28
    ))
),
BINS AS (   -- NTILE across FRAUD+LEGIT together
    SELECT
        SPLIT,
        FEATURE,
        CLASS_LABEL,
        NTILE(20) OVER (PARTITION BY SPLIT, feature ORDER BY VALUE) AS BIN,
        VALUE
    FROM UNPIVOTED
),
AGG AS (    -- counts and bins ranges
    SELECT
        SPLIT,
        FEATURE,
        CLASS_LABEL,
        BIN,
        COUNT(*)   AS rows_in_bin,
        MIN(VALUE) AS bin_min,
        MAX(VALUE) AS bin_max,
    FROM BINS
    GROUP BY SPLIT, FEATURE, CLASS_LABEL, BIN
),
WITH_TOTALS AS (    -- normalize within each (split, feature, class)
    SELECT
        AGG.*,
        SUM(rows_in_bin) OVER (PARTITION BY SPLIT, FEATURE, CLASS_LABEL) AS rows_per_group
    FROM AGG
),
WITH_CDFS AS (
    SELECT
        SPLIT,
        FEATURE,
        CLASS_LABEL,
        BIN,
        rows_in_bin,
        ROUND(100.0 * rows_in_bin / NULLIF(rows_per_group, 0), 2) AS pct_within_group,
        SUM(rows_in_bin) OVER (PARTITION BY SPLIT, FEATURE, CLASS_LABEL ORDER BY BIN) * 1.0
            / NULLIF(rows_per_group, 0) AS cdf_value,
        bin_min,
        bin_max
    FROM WITH_TOTALS
),
PIVOT_CDF AS (
    SELECT
        SPLIT,
        FEATURE,
        BIN,
        MAX(CASE WHEN CLASS_LABEL = 'FRAUD' THEN cdf_value END) AS fraud_cdf,
        MAX(CASE WHEN CLASS_LABEL = 'LEGIT' THEN cdf_value END) AS legit_cdf,
        MAX(bin_min) AS bin_min,
        MAX(bin_max) AS bin_max
    FROM WITH_CDFS
    GROUP BY SPLIT, FEATURE, BIN
)       
SELECT
    SPLIT,
    FEATURE,
    BIN,
    fraud_cdf,
    legit_cdf,
    ABS(fraud_cdf - legit_cdf) AS ks_gap_value,
    bin_min,
    bin_max
FROM PIVOT_CDF;

-- SELECT * FROM RPT_FEATURE_DRILL_DOWN_STATS ORDER BY SPLIT, FEATURE, CLASS_LABEL;
-- SELECT * FROM RPT_FEATURE_DRILL_DOWN_BINS ORDER BY SPLIT, FEATURE, BIN;
