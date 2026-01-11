-- =============================================================================
-- CUSTOM DBT TEST: Balance Should Never Be Negative
-- =============================================================================
--
-- PURPOSE:
--     Verify that no customer ever has a negative Thrive Cash balance.
--     A negative balance would indicate either:
--     - A bug in the FIFO matching logic
--     - Bad source data (spent more than earned)
--     - A data quality issue that needs investigation
--
-- HOW DBT TESTS WORK:
--     - If this query returns ANY rows, the test FAILS
--     - If this query returns ZERO rows, the test PASSES
--     - Failed tests can block deployments in CI/CD
--
-- BUSINESS RULE:
--     Customers cannot have a negative Thrive Cash balance because
--     they can only spend credits they've already earned.
--
-- =============================================================================

-- Find any rows where the balance is negative
-- If this returns rows, something is wrong!

SELECT
    customer_id,
    transaction_id,
    transaction_date,
    current_balance,
    cumulative_earned,
    cumulative_spent,
    cumulative_expired
FROM {{ ref('fct_customer_balance_history') }}
WHERE current_balance < 0
