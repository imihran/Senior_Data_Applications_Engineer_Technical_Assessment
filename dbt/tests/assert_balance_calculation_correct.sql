-- =============================================================================
-- CUSTOM DBT TEST: Balance Calculation Is Correct
-- =============================================================================
--
-- PURPOSE:
--     Verify that current_balance = cumulative_earned - cumulative_spent - cumulative_expired
--     This catches any bugs in the balance calculation logic.
--
-- HOW IT WORKS:
--     Compares the stored current_balance with a recalculated value.
--     If they don't match (within a small tolerance for floating point),
--     the test fails.
--
-- =============================================================================

SELECT
    customer_id,
    transaction_id,
    current_balance AS stored_balance,
    (cumulative_earned - cumulative_spent - cumulative_expired) AS calculated_balance,
    ABS(current_balance - (cumulative_earned - cumulative_spent - cumulative_expired)) AS difference
FROM {{ ref('fct_customer_balance_history') }}
WHERE ABS(current_balance - (cumulative_earned - cumulative_spent - cumulative_expired)) > 0.01
