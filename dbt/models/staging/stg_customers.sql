-- =============================================================================
-- STAGING MODEL: stg_customers
-- =============================================================================
--
-- PURPOSE:
--     Clean and standardize raw customer data.
--     Provides customer attributes for joining with transaction data.
--
-- SOURCE:
--     Raw Customers table from source system
--
-- OUTPUT:
--     Clean customer dimension data
--
-- GRAIN:
--     One row per customer (CUSTOMERID is unique)
--
-- =============================================================================

{{
    config(
        materialized='view',
        tags=['staging', 'thrive_cash']
    )
}}

WITH source_data AS (
    -- In production: SELECT * FROM {{ source('raw', 'customers') }}
    SELECT * FROM {{ ref('raw_customers') }}
),

cleaned AS (
    SELECT
        -- =====================================================================
        -- PRIMARY KEY
        -- =====================================================================
        CAST(CUSTOMERID AS INTEGER) AS customer_id,
        
        -- =====================================================================
        -- CUSTOMER ATTRIBUTES
        -- =====================================================================
        -- Name (first name only in this dataset)
        TRIM(FIRSTNAME) AS first_name,
        
        -- Email (masked for privacy in non-prod environments)
        -- In production, consider: MD5(EMAIL) AS email_hash
        TRIM(EMAIL) AS email,
        
        -- Location
        TRIM(CAST(BILLINGPOSTCODE AS VARCHAR)) AS billing_postal_code
        
    FROM source_data
    WHERE CUSTOMERID IS NOT NULL
)

SELECT * FROM cleaned
