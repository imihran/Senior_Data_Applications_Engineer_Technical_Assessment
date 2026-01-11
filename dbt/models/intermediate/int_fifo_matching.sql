-- =============================================================================
-- INTERMEDIATE MODEL: int_fifo_matching
-- =============================================================================
--
-- PURPOSE:
--     Implements the FIFO (First-In, First-Out) matching logic in SQL.
--     This model matches spent/expired transactions to earned transactions
--     in chronological order.
--
-- BUSINESS LOGIC:
--     For each customer:
--     1. Sort earned transactions by date (oldest first)
--     2. For each spent/expired transaction, match to oldest unmatched earned
--     3. Track which earned transaction was redeemed by which spent/expired
--
-- WHY SQL INSTEAD OF PYTHON?
--     - Runs directly in the data warehouse (Snowflake)
--     - Scales to millions of rows without memory issues
--     - Leverages warehouse compute power
--     - Easier to audit and debug
--
-- OUTPUT:
--     Transaction data with REDEEMID column added to earned transactions
--
-- COMPLEXITY NOTE:
--     FIFO matching in pure SQL is complex. This implementation uses
--     window functions and recursive CTEs where needed. For very large
--     datasets, consider a stored procedure or Python UDF.
--
-- =============================================================================

{{
    config(
        materialized='view',
        tags=['intermediate', 'thrive_cash', 'fifo']
    )
}}

WITH 
-- =============================================================================
-- STEP 1: Prepare earned transactions
-- =============================================================================
-- Get all earned transactions, numbered in FIFO order per customer
earned_transactions AS (
    SELECT
        transaction_id,
        customer_id,
        created_at,
        amount AS earned_amount,
        -- Assign a sequence number for FIFO ordering
        -- 1 = oldest earned transaction for this customer
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY created_at, transaction_id
        ) AS fifo_rank
    FROM {{ ref('stg_tc_data') }}
    WHERE transaction_type = 'earned'
),

-- =============================================================================
-- STEP 2: Prepare redemption transactions (spent + expired)
-- =============================================================================
-- Get all spent and expired transactions, numbered by date per customer
redemption_transactions AS (
    SELECT
        transaction_id AS redemption_id,
        customer_id,
        created_at AS redemption_date,
        amount_abs AS redemption_amount,
        transaction_type AS redemption_type,
        -- Assign a sequence number for processing order
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY created_at, transaction_id
        ) AS redemption_rank
    FROM {{ ref('stg_tc_data') }}
    WHERE transaction_type IN ('spent', 'expired')
),

-- =============================================================================
-- STEP 3: Match earned to redemptions using FIFO logic
-- =============================================================================
-- This is a simplified matching that assigns each redemption to the 
-- corresponding earned transaction based on FIFO rank.
-- 
-- NOTE: This is a simplified version. A full implementation would need
-- to handle partial matches (when redemption amount != earned amount).
-- For production, consider using a Python UDF or stored procedure.
fifo_matches AS (
    SELECT
        e.transaction_id AS earned_transaction_id,
        e.customer_id,
        e.created_at AS earned_date,
        e.earned_amount,
        e.fifo_rank,
        r.redemption_id,
        r.redemption_date,
        r.redemption_amount,
        r.redemption_type
    FROM earned_transactions e
    LEFT JOIN redemption_transactions r
        ON e.customer_id = r.customer_id
        AND e.fifo_rank = r.redemption_rank
        -- Only match if earned happened before redemption
        AND e.created_at < r.redemption_date
),

-- =============================================================================
-- STEP 4: Combine with original data
-- =============================================================================
-- Add REDEEMID to the original transaction data
final AS (
    SELECT
        t.transaction_id,
        t.transaction_type,
        t.created_at,
        t.expires_at,
        t.customer_id,
        t.order_id,
        t.amount,
        t.reason,
        -- Add REDEEMID for earned transactions
        CASE 
            WHEN t.transaction_type = 'earned' THEN m.redemption_id
            ELSE NULL
        END AS redeem_id,
        -- Add redemption details for analysis
        m.redemption_date,
        m.redemption_type
    FROM {{ ref('stg_tc_data') }} t
    LEFT JOIN fifo_matches m
        ON t.transaction_id = m.earned_transaction_id
)

SELECT * FROM final
