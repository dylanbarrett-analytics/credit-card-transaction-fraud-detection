/* ==================================================================
   Step 9: Dashboard KPIs and scatter pairs
   PURPOSE: Provide header KPIs and fixed feature-pair scatter feeds
   DETAILS:
     RPT_DASHBOARD_HEADER: counts, fraud%, fraud-to-legit ratio, amount stats by split
     RPT_SCATTER_FEATURE_PAIRS: 4 fixed feature pairs on TEST split for charts
     Top features sourced from RPT_FEATURE_SEPARATION_SUMMARY
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA REPORTING;

-- ------------------------------------------------------------------
-- Dashboard header KPIs per split
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_DASHBOARD_HEADER
COMMENT = 'Header KPIs by split with counts, fraud%, ratio, and amount stats' AS
WITH CLASS_KPIS AS (
    SELECT
        SPLIT,
        COUNT(*)                                             AS total_rows,
        SUM(IFF(IS_FRAUD, 1, 0))                             AS fraud_rows,
        SUM(IFF(NOT IS_FRAUD, 1, 0))                         AS legit_rows,
        AVG(AMOUNT::FLOAT)                                   AS avg_amount_overall,
        MEDIAN(AMOUNT::FLOAT)                                AS median_amount_overall,
        AVG(IFF(IS_FRAUD, AMOUNT::FLOAT, NULL))              AS avg_amount_fraud,
        MEDIAN(IFF(IS_FRAUD, AMOUNT::FLOAT, NULL))           AS median_amount_fraud,
        AVG(IFF(NOT IS_FRAUD, AMOUNT::FLOAT, NULL))          AS avg_amount_legit,
        MEDIAN(IFF(NOT IS_FRAUD, AMOUNT::FLOAT, NULL))       AS median_amount_legit
    FROM FRAUD_DETECTION.ANALYTICS.TRANSACTIONS_SPLIT
    GROUP BY SPLIT
),
TOP_FEATURES AS (
    SELECT
        SPLIT,
        LISTAGG(FEATURE, ', ') WITHIN GROUP (ORDER BY SEPARATION DESC) AS top3_features
    FROM (
        SELECT
            SPLIT,
            FEATURE,
            SEPARATION,
            ROW_NUMBER() OVER (PARTITION BY SPLIT ORDER BY SEPARATION DESC) AS rn
        FROM RPT_FEATURE_SEPARATION_SUMMARY
    )
    WHERE rn <= 3
    GROUP BY SPLIT
)
SELECT
    c.SPLIT,
    c.total_rows,
    c.fraud_rows,
    c.legit_rows,
    ROUND(100.0 * c.fraud_rows / NULLIF(c.total_rows, 0), 2)    AS fraud_pct,
    ROUND(c.fraud_rows / NULLIF(c.legit_rows, 0), 2)            AS fraud_to_legit_ratio,
    c.avg_amount_overall,
    c.median_amount_overall,
    c.avg_amount_fraud,
    c.median_amount_fraud,
    c.avg_amount_legit,
    c.median_amount_legit,
    t.top3_features
FROM CLASS_KPIS c
LEFT JOIN TOP_FEATURES t ON c.SPLIT = t.SPLIT;

-- ------------------------------------------------------------------
-- Fixed feature-pair scatter feed on TEST split
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW RPT_SCATTER_FEATURE_PAIRS
COMMENT = '4 fixed feature pairs on TEST split, x/y values with class labels' AS
-- Pair 1: V14 vs V10
SELECT
    SPLIT,
    IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
    'V14_vs_V10' AS pair_name,
    'V14'        AS feature_x,
    'V10'        AS feature_y,
    V14::FLOAT   AS x_value,
    V10::FLOAT   AS y_value
FROM ANALYTICS.TRANSACTIONS_SPLIT
WHERE SPLIT = 'TEST'

UNION ALL
-- Pair 2: V12 vs V4
SELECT
    SPLIT,
    IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
    'V12_vs_V4' AS pair_name,
    'V12' AS feature_x,
    'V4' AS feature_y,
    V12::FLOAT AS x_value,
    V4::FLOAT AS y_value
FROM ANALYTICS.TRANSACTIONS_SPLIT
WHERE SPLIT = 'TEST'

UNION ALL
-- Pair 3: V3 vs V11
SELECT
    SPLIT,
    IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
    'V3_vs_V11' AS pair_name,
    'V3' AS feature_x,
    'V11' AS feature_y,
    V3::FLOAT AS x_value,
    V11::FLOAT AS y_value
FROM ANALYTICS.TRANSACTIONS_SPLIT
WHERE SPLIT = 'TEST'

UNION ALL
-- Pair 4: V17 vs V9
SELECT
    SPLIT,
    IFF(IS_FRAUD, 'FRAUD', 'LEGIT') AS CLASS_LABEL,
    'V17_vs_V9' AS pair_name,
    'V17' AS feature_x,
    'V9' AS feature_y,
    V17::FLOAT AS x_value,
    V9::FLOAT AS y_value
FROM ANALYTICS.TRANSACTIONS_SPLIT
WHERE SPLIT = 'TEST';

-- SELECT * FROM RPT_DASHBOARD_HEADER ORDER BY SPLIT;
-- SELECT * FROM RPT_SCATTER_FEATURE_PAIRS WHERE pair_name = 'V14_vs_V10' ORDER BY CLASS_LABEL;