-- =============================================================================
-- MART MODEL: fct_customer_balance_history
-- =============================================================================
--
-- PURPOSE:
--     Fact table showing Thrive Cash balance history for each customer.
--     This is the primary table for finance team reporting.
--
-- BUSINESS USE CASES:
--     - "What is customer X's current balance?"
--     - "What was the balance on date Y?"
--     - "How much has customer X earned/spent/expired total?"
--     - "Show me the balance trend over time"
--
-- GRAIN:
--     One row per customer per transaction
--     Each row shows the balance AFTER that transaction
--
-- KEY COLUMNS:
--     - customer_id: Who this balance belongs to
--     - transaction_date: When this balance was recorded
--     - cumulative_earned: Total earned up to this point
--     - cumulative_spent: Total spent up to this point
--     - cumulative_expired: Total expired up to this point
--     - current_balance: Available balance (earned - spent - expired)
--
-- MATERIALIZATION:
--     Table (not view) for faster query performance
--     Finance team queries this frequently
--
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'thrive_cash'],
        -- Add clustering for query performance on large datasets
        -- cluster_by=['customer_id', 'transaction_date']
    )
}}

WITH 
-- =============================================================================
-- STEP 1: Calculate transaction impacts
-- =============================================================================
-- Categorize each transaction's effect on the balance
transaction_impacts AS (
    SELECT
        customer_id,
        transaction_id,
        transaction_type,
        created_at AS transaction_date,
        amount,
        -- Separate amounts by type for cumulative calculations
        -- Earned adds to balance
        CASE WHEN transaction_type = 'earned' THEN amount ELSE 0 END AS earned_impact,
        -- Spent subtracts from balance (stored as negative, so we use ABS)
        CASE WHEN transaction_type = 'spent' THEN amount_abs ELSE 0 END AS spent_impact,
        -- Expired subtracts from balance
        CASE WHEN transaction_type = 'expired' THEN amount_abs ELSE 0 END AS expired_impact
    FROM {{ ref('stg_tc_data') }}
),

-- =============================================================================
-- STEP 2: Calculate running totals
-- =============================================================================
-- Use window functions to compute cumulative sums
running_totals AS (
    SELECT
        customer_id,
        transaction_id,
        transaction_type,
        transaction_date,
        amount,
        
        -- Cumulative earned: running sum of all earned amounts
        SUM(earned_impact) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_earned,
        
        -- Cumulative spent: running sum of all spent amounts
        SUM(spent_impact) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_spent,
        
        -- Cumulative expired: running sum of all expired amounts
        SUM(expired_impact) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_expired
        
    FROM transaction_impacts
),

-- =============================================================================
-- STEP 3: Calculate current balance and add metadata
-- =============================================================================
final AS (
    SELECT
        -- Primary key (composite)
        customer_id,
        transaction_id,
        
        -- Transaction details
        transaction_type,
        transaction_date,
        amount AS transaction_amount,
        
        -- Cumulative totals
        cumulative_earned,
        cumulative_spent,
        cumulative_expired,
        
        -- Current balance = earned - spent - expired
        (cumulative_earned - cumulative_spent - cumulative_expired) AS current_balance,
        
        -- Metadata for tracking
        CURRENT_TIMESTAMP() AS _loaded_at
        
    FROM running_totals
)

SELECT * FROM final
ORDER BY customer_id, transaction_date, transaction_id
