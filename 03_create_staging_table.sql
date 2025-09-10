/* ==================================================================
   Step 3: Create staging table
   PURPOSE: Define raw table structure for incoming transaction data
   DETAILS:
     Table mirrors source CSV columns
     Data is stored as-is for immutability and traceability
     Transformations applied in later steps
   ================================================================== */

USE DATABASE FRAUD_DETECTION;
USE SCHEMA STAGING;

-- Create raw staging table matching source CSV
CREATE OR REPLACE TABLE TRANSACTIONS_RAW (
    id      INT,            -- transaction identifier
    -- V1-V28 = PCA-transformed numeric features
    V1  FLOAT,  V2  FLOAT,  V3  FLOAT,  V4  FLOAT,  V5  FLOAT,
    V6  FLOAT,  V7  FLOAT,  V8  FLOAT,  V9  FLOAT,  V10 FLOAT,
    V11 FLOAT,  V12 FLOAT,  V13 FLOAT,  V14 FLOAT,  V15 FLOAT,
    V16 FLOAT,  V17 FLOAT,  V18 FLOAT,  V19 FLOAT,  V20 FLOAT,
    V21 FLOAT,  V22 FLOAT,  V23 FLOAT,  V24 FLOAT,  V25 FLOAT,
    V26 FLOAT,  V27 FLOAT,  V28 FLOAT,
    
    Amount  NUMBER(18,2),   -- transaction amount
    Class   INT             -- label: 1 = fraud, 0 = legitimate
);