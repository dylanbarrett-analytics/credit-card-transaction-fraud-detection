/* ==================================================================
   Step 8.5: Create feature profile view
   PURPOSE: Provide per-feature per-split summary with class stats and separation
   DETAILS:
     Source: REPORTING.RPT_FEATURE_DRILL_DOWN_STATS
     Output: REPORTING.RPT_FEATURE_PROFILE
     Grain: one row per SPLIT x FEATURE
   ================================================================== */

USE WAREHOUSE FRAUD_WH;
USE DATABASE FRAUD_DETECTION;
USE SCHEMA REPORTING;

CREATE OR REPLACE VIEW RPT_FEATURE_PROFILE
COMMENT = 'Per-feature per-split summary from RPT_FEATURE_DRILL_DOWN_STATS with class counts, means, medians, stddevs, DIFF, SEPARATION.' AS
SELECT
    SPLIT,
    FEATURE,
    -- class counts
    SUM(IFF(CLASS_LABEL = 'FRAUD', NUMBER_OF_ROWS, 0)) AS FRAUD_ROWS,
    SUM(IFF(CLASS_LABEL = 'LEGIT', NUMBER_OF_ROWS, 0)) AS LEGIT_ROWS,
    -- class means
    MAX(IFF(CLASS_LABEL = 'FRAUD', AVG_VALUE, NULL)) AS FRAUD_AVG,
    MAX(IFF(CLASS_LABEL = 'LEGIT', AVG_VALUE, NULL)) AS LEGIT_AVG,
    -- difference
    MAX(IFF(CLASS_LABEL = 'FRAUD', AVG_VALUE, NULL))
      - MAX(IFF(CLASS_LABEL = 'LEGIT', AVG_VALUE, NULL)) AS DIFF,
    -- absolute value of difference (i.e., separation)
    ABS(
      MAX(IFF(CLASS_LABEL = 'FRAUD', AVG_VALUE, NULL))
      - MAX(IFF(CLASS_LABEL = 'LEGIT', AVG_VALUE, NULL))
    )                                                AS SEPARATION,
    -- medians
    MAX(IFF(CLASS_LABEL = 'FRAUD', MEDIAN_VALUE, NULL)) AS FRAUD_MEDIAN,
    MAX(IFF(CLASS_LABEL = 'LEGIT', MEDIAN_VALUE, NULL)) AS LEGIT_MEDIAN,
    -- standard deviations
    MAX(IFF(CLASS_LABEL = 'FRAUD', STDDEV_VALUE, NULL)) AS FRAUD_STDDEV,
    MAX(IFF(CLASS_LABEL = 'LEGIT', STDDEV_VALUE, NULL)) AS LEGIT_STDDEV,
    -- rank by separation DESC
    RANK() OVER (PARTITION BY SPLIT ORDER BY   
        ABS(
        MAX(IFF(CLASS_LABEL = 'FRAUD', AVG_VALUE, NULL))
      - MAX(IFF(CLASS_LABEL = 'LEGIT', AVG_VALUE, NULL))
    ) / NULLIF(AVG(STDDEV_VALUE), 0) DESC
    ) AS FEATURE_RANK
FROM RPT_FEATURE_DRILL_DOWN_STATS
GROUP BY SPLIT, FEATURE;

-- SELECT * FROM RPT_FEATURE_PROFILE ORDER BY SPLIT, FEATURE_RANK;