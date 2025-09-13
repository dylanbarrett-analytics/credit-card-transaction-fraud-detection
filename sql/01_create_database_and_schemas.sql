/* ==================================================================
   Step 1: Create database and schemas
   PURPOSE: Establish clean ELT pipeline with 3 layers
   DETAILS:
     STAGING   = raw data landing
     ANALYTICS = modeled transformations
     REPORTING = curated business intelligence views
     In production, consider IF NOT EXISTS to avoid replacement
   ================================================================== */

-- Create database, can be re-run safely
CREATE OR REPLACE DATABASE FRAUD_DETECTION;

-- Three-layer schema layout for lifecycle separation
CREATE OR REPLACE SCHEMA FRAUD_DETECTION.STAGING;    -- raw landing
CREATE OR REPLACE SCHEMA FRAUD_DETECTION.ANALYTICS;  -- transformations
CREATE OR REPLACE SCHEMA FRAUD_DETECTION.REPORTING;  -- curated views

-- Set context for subsequent steps
USE DATABASE FRAUD_DETECTION;

USE SCHEMA STAGING;
