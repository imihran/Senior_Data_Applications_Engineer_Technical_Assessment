-- =============================================================================
-- MART MODEL: fct_customer_current_balance
-- =============================================================================
--
-- PURPOSE:
--     Snapshot table showing the CURRENT Thrive Cash balance for each customer.
--     This is the most commonly queried table for finance reporting.
--
-- BUSINESS USE CASES:
--     - "What is customer X's current balance?"
--     - "List all customers with balance > $50"
--     - "Total Thrive Cash liability across all customers"
--     - "Month-end balance report"
--
-- GRAIN:
--     One row per customer (latest balance only)
--
-- KEY COLUMNS:
--     - customer_id: Customer identifier
--     - total_earned: Lifetime earned amount
--     - total_spent: Lifetime spent amount
--     - total_expired: Lifetime expired amount
--     - current_balance: Available balance right now
--     - last_activity_date: When they last had a transaction
--
-- MATERIALIZATION:
--     Table for fast lookups
--     Consider incremental for very large customer bases
--
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'thrive_cash']
    )
}}

WITH 
-- =============================================================================
-- STEP 1: Get the latest transaction for each customer
-- =============================================================================
latest_transactions AS (
    SELECT
        customer_id,
        MAX(transaction_date) AS last_activity_date
    FROM {{ ref('fct_customer_balance_history') }}
    GROUP BY customer_id
),

-- =============================================================================
-- STEP 2: Get balance at the latest transaction
-- =============================================================================
current_balances AS (
    SELECT
        h.customer_id,
        h.transaction_date AS balance_as_of_date,
        h.cumulative_earned AS total_earned,
        h.cumulative_spent AS total_spent,
        h.cumulative_expired AS total_expired,
        h.current_balance
    FROM {{ ref('fct_customer_balance_history') }} h
    INNER JOIN latest_transactions lt
        ON h.customer_id = lt.customer_id
        AND h.transaction_date = lt.last_activity_date
    -- Handle ties (multiple transactions on same date) by taking the last one
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY h.customer_id 
        ORDER BY h.transaction_id DESC
    ) = 1
),

-- =============================================================================
-- STEP 3: Add customer attributes and calculate metrics
-- =============================================================================
final AS (
    SELECT
        -- Customer identification
        cb.customer_id,
        c.first_name AS customer_first_name,
        c.email AS customer_email,
        
        -- Balance information
        cb.total_earned,
        cb.total_spent,
        cb.total_expired,
        cb.current_balance,
        
        -- Activity metrics
        cb.balance_as_of_date AS last_activity_date,
        
        -- Calculated metrics
        CASE 
            WHEN cb.total_earned > 0 
            THEN ROUND(100.0 * cb.total_spent / cb.total_earned, 2)
            ELSE 0 
        END AS redemption_rate_pct,
        
        CASE 
            WHEN cb.total_earned > 0 
            THEN ROUND(100.0 * cb.total_expired / cb.total_earned, 2)
            ELSE 0 
        END AS expiration_rate_pct,
        
        -- Balance status
        CASE
            WHEN cb.current_balance > 100 THEN 'High Balance'
            WHEN cb.current_balance > 0 THEN 'Active Balance'
            ELSE 'Zero Balance'
        END AS balance_status,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS _loaded_at
        
    FROM current_balances cb
    LEFT JOIN {{ ref('stg_customers') }} c
        ON cb.customer_id = c.customer_id
)

SELECT * FROM final
ORDER BY customer_id
