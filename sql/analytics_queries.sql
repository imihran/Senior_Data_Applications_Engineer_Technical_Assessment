-- =============================================================================
-- THRIVE CASH ANALYTICS QUERIES
-- =============================================================================
--
-- PURPOSE:
--     SQL queries for the finance team to analyze Thrive Cash data.
--     These queries provide insights into customer balances, redemption
--     patterns, and financial reporting metrics.
--
-- TARGET DATABASE: Snowflake (compatible with most SQL databases)
--
-- TABLES USED:
--     - tc_data: Raw Thrive Cash transactions (with REDEEMID after FIFO matching)
--     - customers: Customer information
--     - sales: Order/sales data
--
-- AUTHOR: Data Applications Team
-- DATE: 2024
-- =============================================================================


-- =============================================================================
-- PART 5A: CUSTOMER BALANCES OVER TIME
-- =============================================================================
-- This query creates a view showing the running balance for each customer
-- at each transaction point. Finance can query this to see:
--   - Current balance for any customer
--   - Historical balance at any point in time
--   - Cumulative earned, spent, and expired amounts
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW: customer_balance_history
-- -----------------------------------------------------------------------------
-- Creates a running total of Thrive Cash for each customer over time.
-- Each row represents a transaction and shows the balance AFTER that transaction.
--
-- HOW TO READ THIS:
--   - cumulative_earned: Total amount earned up to this point
--   - cumulative_spent: Total amount spent up to this point  
--   - cumulative_expired: Total amount expired up to this point
--   - current_balance: What the customer has available (earned - spent - expired)
--
-- EXAMPLE OUTPUT:
--   customer_id | transaction_date | cumulative_earned | cumulative_spent | current_balance
--   100         | 2023-01-01       | 20.00            | 0.00             | 20.00
--   100         | 2023-02-01       | 20.00            | 15.00            | 5.00
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW customer_balance_history AS
WITH transaction_amounts AS (
    -- First, categorize each transaction's impact on the balance
    -- Earned adds to balance, spent/expired subtract from balance
    SELECT 
        CUSTOMERID as customer_id,
        TRANS_ID as transaction_id,
        TCTYPE as transaction_type,
        CREATEDAT as transaction_date,
        AMOUNT as amount,
        -- Separate amounts by type for cumulative calculations
        CASE WHEN TCTYPE = 'earned' THEN AMOUNT ELSE 0 END as earned_amount,
        CASE WHEN TCTYPE = 'spent' THEN ABS(AMOUNT) ELSE 0 END as spent_amount,
        CASE WHEN TCTYPE = 'expired' THEN ABS(AMOUNT) ELSE 0 END as expired_amount
    FROM tc_data
),
running_totals AS (
    -- Calculate running totals using window functions
    -- Window functions are perfect for this - they calculate cumulative sums
    -- partitioned by customer and ordered by date
    SELECT 
        customer_id,
        transaction_id,
        transaction_type,
        transaction_date,
        amount,
        -- Cumulative earned: sum of all earned amounts up to this row
        SUM(earned_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_earned,
        -- Cumulative spent: sum of all spent amounts up to this row
        SUM(spent_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_spent,
        -- Cumulative expired: sum of all expired amounts up to this row
        SUM(expired_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_expired
    FROM transaction_amounts
)
SELECT 
    customer_id,
    transaction_id,
    transaction_type,
    transaction_date,
    amount,
    cumulative_earned,
    cumulative_spent,
    cumulative_expired,
    -- Current balance = what they've earned minus what they've used
    (cumulative_earned - cumulative_spent - cumulative_expired) as current_balance
FROM running_totals
ORDER BY customer_id, transaction_date, transaction_id;


-- -----------------------------------------------------------------------------
-- VIEW: customer_current_balance
-- -----------------------------------------------------------------------------
-- Simplified view showing just the CURRENT balance for each customer.
-- This is what finance typically needs for month-end reporting.
--
-- HOW IT WORKS:
--   Takes the most recent row from customer_balance_history for each customer.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW customer_current_balance AS
WITH latest_transaction AS (
    -- Find the most recent transaction for each customer
    SELECT 
        customer_id,
        MAX(transaction_date) as latest_date
    FROM customer_balance_history
    GROUP BY customer_id
)
SELECT 
    cbh.customer_id,
    cbh.transaction_date as balance_as_of,
    cbh.cumulative_earned as total_earned,
    cbh.cumulative_spent as total_spent,
    cbh.cumulative_expired as total_expired,
    cbh.current_balance
FROM customer_balance_history cbh
INNER JOIN latest_transaction lt 
    ON cbh.customer_id = lt.customer_id 
    AND cbh.transaction_date = lt.latest_date;


-- =============================================================================
-- PART 5B: SAMPLE QUERY - BALANCE ON SPECIFIC DATE
-- =============================================================================
-- Query to answer: "What was the Thrive Cash balance for customers 
-- 23306353 and 16161481 on 2023-03-21?"
--
-- APPROACH:
--   Find the last transaction on or before the target date for each customer,
--   then return the balance at that point.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- QUERY: Balance for specific customers on specific date
-- -----------------------------------------------------------------------------
-- This query finds the balance as of a specific date by:
-- 1. Filtering to transactions on or before the target date
-- 2. Finding the most recent transaction for each customer
-- 3. Returning the balance at that point
--
-- NOTE: If a customer had no transactions before the target date,
--       they won't appear in results (balance would be 0)
-- -----------------------------------------------------------------------------

WITH balance_on_date AS (
    -- Get all transactions up to and including the target date
    SELECT 
        customer_id,
        transaction_date,
        cumulative_earned,
        cumulative_spent,
        cumulative_expired,
        current_balance,
        -- Rank transactions to find the most recent one per customer
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date DESC, transaction_id DESC
        ) as rn
    FROM customer_balance_history
    WHERE transaction_date <= '2023-03-21'  -- Target date
      AND customer_id IN (23306353, 16161481)  -- Target customers
)
SELECT 
    customer_id,
    '2023-03-21' as balance_date,
    transaction_date as last_transaction_date,
    cumulative_earned as total_earned,
    cumulative_spent as total_spent,
    cumulative_expired as total_expired,
    current_balance as balance_on_date
FROM balance_on_date
WHERE rn = 1  -- Only the most recent transaction per customer
ORDER BY customer_id;

-- -----------------------------------------------------------------------------
-- EXPECTED OUTPUT (based on sample data):
-- -----------------------------------------------------------------------------
-- customer_id | balance_date | last_transaction_date | total_earned | total_spent | total_expired | balance_on_date
-- 16161481    | 2023-03-21   | 2023-03-08           | 21.57        | 1.50        | 5.00          | 15.07
-- 23306353    | 2023-03-21   | 2023-03-09           | 130.00       | 130.00      | 0.00          | 0.00
-- -----------------------------------------------------------------------------


-- =============================================================================
-- ADDITIONAL ANALYTICS QUERIES
-- =============================================================================
-- These queries provide additional insights for the finance team.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- QUERY: Monthly Thrive Cash Summary
-- -----------------------------------------------------------------------------
-- Aggregates Thrive Cash activity by month for trend analysis.
-- Useful for understanding seasonal patterns and growth.
-- -----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('month', CREATEDAT) as month,
    COUNT(CASE WHEN TCTYPE = 'earned' THEN 1 END) as earned_count,
    COUNT(CASE WHEN TCTYPE = 'spent' THEN 1 END) as spent_count,
    COUNT(CASE WHEN TCTYPE = 'expired' THEN 1 END) as expired_count,
    SUM(CASE WHEN TCTYPE = 'earned' THEN AMOUNT ELSE 0 END) as total_earned,
    SUM(CASE WHEN TCTYPE = 'spent' THEN ABS(AMOUNT) ELSE 0 END) as total_spent,
    SUM(CASE WHEN TCTYPE = 'expired' THEN ABS(AMOUNT) ELSE 0 END) as total_expired,
    -- Net change in liability (what we owe customers)
    SUM(CASE WHEN TCTYPE = 'earned' THEN AMOUNT ELSE 0 END) -
    SUM(CASE WHEN TCTYPE IN ('spent', 'expired') THEN ABS(AMOUNT) ELSE 0 END) as net_liability_change
FROM tc_data
GROUP BY DATE_TRUNC('month', CREATEDAT)
ORDER BY month;


-- -----------------------------------------------------------------------------
-- QUERY: FIFO Matching Summary
-- -----------------------------------------------------------------------------
-- Shows how earned transactions were matched to spent/expired.
-- Useful for auditing the FIFO matching process.
-- -----------------------------------------------------------------------------

SELECT 
    e.TRANS_ID as earned_trans_id,
    e.CUSTOMERID as customer_id,
    e.CREATEDAT as earned_date,
    e.AMOUNT as earned_amount,
    e.REASON as earned_reason,
    e.REDEEMID as matched_to_trans_id,
    r.TCTYPE as redemption_type,
    r.CREATEDAT as redemption_date,
    r.AMOUNT as redemption_amount
FROM tc_data e
LEFT JOIN tc_data r ON e.REDEEMID = r.TRANS_ID
WHERE e.TCTYPE = 'earned'
ORDER BY e.CUSTOMERID, e.CREATEDAT;


-- -----------------------------------------------------------------------------
-- QUERY: Customers with Expiring Balances
-- -----------------------------------------------------------------------------
-- Identifies customers with earned credits that will expire soon.
-- Useful for marketing to send reminder emails.
-- -----------------------------------------------------------------------------

SELECT 
    CUSTOMERID as customer_id,
    TRANS_ID as transaction_id,
    AMOUNT as expiring_amount,
    CREATEDAT as earned_date,
    EXPIREDAT as expiration_date,
    DATEDIFF('day', CURRENT_DATE(), EXPIREDAT) as days_until_expiration
FROM tc_data
WHERE TCTYPE = 'earned'
  AND REDEEMID IS NULL  -- Not yet redeemed
  AND EXPIREDAT IS NOT NULL
  AND EXPIREDAT > CURRENT_DATE()  -- Not yet expired
  AND EXPIREDAT <= DATEADD('day', 30, CURRENT_DATE())  -- Expires within 30 days
ORDER BY EXPIREDAT;


-- -----------------------------------------------------------------------------
-- QUERY: Redemption Rate by Reason
-- -----------------------------------------------------------------------------
-- Analyzes which types of earned credits get redeemed vs expire.
-- Useful for understanding which promotions are most effective.
-- -----------------------------------------------------------------------------

SELECT 
    REASON as earned_reason,
    COUNT(*) as total_earned,
    COUNT(CASE WHEN REDEEMID IS NOT NULL THEN 1 END) as redeemed_count,
    COUNT(CASE WHEN REDEEMID IS NULL AND EXPIREDAT < CURRENT_DATE() THEN 1 END) as expired_count,
    COUNT(CASE WHEN REDEEMID IS NULL AND (EXPIREDAT IS NULL OR EXPIREDAT >= CURRENT_DATE()) THEN 1 END) as pending_count,
    ROUND(
        100.0 * COUNT(CASE WHEN REDEEMID IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0), 
        2
    ) as redemption_rate_pct
FROM tc_data
WHERE TCTYPE = 'earned'
GROUP BY REASON
ORDER BY total_earned DESC;


-- =============================================================================
-- DATA QUALITY CHECKS (SQL-based)
-- =============================================================================
-- These queries can be run to validate data quality directly in SQL.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CHECK: No negative balances
-- -----------------------------------------------------------------------------
-- Customers should never have a negative Thrive Cash balance.
-- If this returns rows, there's a data issue.
-- -----------------------------------------------------------------------------

SELECT 
    customer_id,
    transaction_date,
    current_balance
FROM customer_balance_history
WHERE current_balance < 0
ORDER BY customer_id, transaction_date;


-- -----------------------------------------------------------------------------
-- CHECK: REDEEMID references valid transactions
-- -----------------------------------------------------------------------------
-- Every REDEEMID should reference an actual spent or expired transaction.
-- -----------------------------------------------------------------------------

SELECT 
    e.TRANS_ID as earned_trans_id,
    e.REDEEMID as invalid_redeemid
FROM tc_data e
LEFT JOIN tc_data r ON e.REDEEMID = r.TRANS_ID
WHERE e.REDEEMID IS NOT NULL
  AND (r.TRANS_ID IS NULL OR r.TCTYPE NOT IN ('spent', 'expired'));


-- -----------------------------------------------------------------------------
-- CHECK: Chronological consistency
-- -----------------------------------------------------------------------------
-- Earned transactions should only be matched to redemptions that happened AFTER.
-- -----------------------------------------------------------------------------

SELECT 
    e.TRANS_ID as earned_trans_id,
    e.CREATEDAT as earned_date,
    e.REDEEMID as redemption_id,
    r.CREATEDAT as redemption_date
FROM tc_data e
INNER JOIN tc_data r ON e.REDEEMID = r.TRANS_ID
WHERE e.TCTYPE = 'earned'
  AND e.CREATEDAT > r.CREATEDAT;  -- Earned AFTER redemption = error
