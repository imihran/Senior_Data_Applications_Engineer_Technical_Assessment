-- =============================================================================
-- STAGING MODEL: stg_tc_data
-- =============================================================================
--
-- PURPOSE:
--     Clean and standardize raw Thrive Cash transaction data.
--     This is the first transformation layer - it prepares raw data
--     for downstream business logic.
--
-- WHAT STAGING MODELS DO:
--     1. Rename columns to consistent naming conventions
--     2. Cast data types explicitly
--     3. Handle null values
--     4. Add basic derived columns
--     5. NO business logic - just cleaning
--
-- SOURCE:
--     Raw TC_Data table from source system
--
-- OUTPUT:
--     Clean, typed, consistently named transaction data
--
-- GRAIN:
--     One row per transaction (TRANS_ID is unique)
--
-- =============================================================================

{{
    config(
        materialized='view',
        tags=['staging', 'thrive_cash']
    )
}}

WITH source_data AS (
    -- In production, this would reference a source table:
    -- SELECT * FROM {{ source('raw', 'tc_data') }}
    -- For this assessment, we assume data is loaded from CSV/Excel
    SELECT * FROM {{ ref('raw_tc_data') }}
),

cleaned AS (
    SELECT
        -- =====================================================================
        -- PRIMARY KEY
        -- =====================================================================
        -- Transaction ID is the unique identifier for each transaction
        CAST(TRANS_ID AS INTEGER) AS transaction_id,
        
        -- =====================================================================
        -- TRANSACTION ATTRIBUTES
        -- =====================================================================
        -- Transaction type: 'earned', 'spent', or 'expired'
        LOWER(TRIM(TCTYPE)) AS transaction_type,
        
        -- Transaction amount
        -- Positive for earned, negative for spent/expired
        CAST(AMOUNT AS DECIMAL(10, 2)) AS amount,
        
        -- Absolute value of amount (always positive, useful for aggregations)
        ABS(CAST(AMOUNT AS DECIMAL(10, 2))) AS amount_abs,
        
        -- Reason for the transaction (e.g., 'Refund', 'Promotion')
        TRIM(REASON) AS reason,
        
        -- =====================================================================
        -- DATES
        -- =====================================================================
        -- When the transaction was created
        CAST(CREATEDAT AS TIMESTAMP) AS created_at,
        
        -- When earned credits expire (null for spent/expired transactions)
        CAST(EXPIREDAT AS TIMESTAMP) AS expires_at,
        
        -- Extract date parts for easier filtering and grouping
        CAST(CREATEDAT AS DATE) AS created_date,
        DATE_TRUNC('month', CREATEDAT) AS created_month,
        
        -- =====================================================================
        -- FOREIGN KEYS
        -- =====================================================================
        -- Customer who owns this transaction
        CAST(CUSTOMERID AS INTEGER) AS customer_id,
        
        -- Order associated with spent transactions (null for earned/expired)
        CAST(ORDERID AS INTEGER) AS order_id,
        
        -- =====================================================================
        -- DERIVED FLAGS
        -- =====================================================================
        -- Boolean flags for easier filtering
        CASE WHEN LOWER(TCTYPE) = 'earned' THEN TRUE ELSE FALSE END AS is_earned,
        CASE WHEN LOWER(TCTYPE) = 'spent' THEN TRUE ELSE FALSE END AS is_spent,
        CASE WHEN LOWER(TCTYPE) = 'expired' THEN TRUE ELSE FALSE END AS is_expired,
        
        -- Is this transaction still active (not expired)?
        CASE 
            WHEN LOWER(TCTYPE) = 'earned' 
                 AND (EXPIREDAT IS NULL OR EXPIREDAT > CURRENT_TIMESTAMP())
            THEN TRUE 
            ELSE FALSE 
        END AS is_active
        
    FROM source_data
    
    -- Filter out any obviously invalid records
    WHERE TRANS_ID IS NOT NULL
      AND TCTYPE IS NOT NULL
      AND CUSTOMERID IS NOT NULL
)

SELECT * FROM cleaned
