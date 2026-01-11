"""
=============================================================================
ANALYTICS RUNNER - SAMPLE QUERIES FOR FINANCE TEAM
=============================================================================

PURPOSE:
    Demonstrates the analytics queries from Part 5 of the assessment.
    This script answers the sample question:
    
    "What was the Thrive Cash balance for customers 23306353 and 16161481 
    on 2023-03-21?"

HOW TO RUN:
    python src/run_analytics.py

OUTPUT:
    - Customer balance history table
    - Answer to the sample query
    - Summary statistics

AUTHOR: Data Applications Team
DATE: 2024
=============================================================================
"""

import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_customer_balance_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a customer balance history table showing running totals.
    
    WHAT THIS DOES:
        For each customer, calculates cumulative earned, spent, expired,
        and current balance at each transaction point.
    
    This is the Python equivalent of the SQL view in analytics_queries.sql
    
    PARAMETERS:
        df: DataFrame with TC_Data (including REDEEMID from FIFO matching)
    
    RETURNS:
        DataFrame with columns:
        - customer_id
        - transaction_date
        - transaction_id
        - transaction_type
        - amount
        - cumulative_earned
        - cumulative_spent
        - cumulative_expired
        - current_balance
    """
    logger.info("Building customer balance history...")
    
    balance_records = []
    
    for customer_id in df['CUSTOMERID'].unique():
        # Get this customer's transactions sorted by date
        customer_df = df[df['CUSTOMERID'] == customer_id].sort_values('CREATEDAT')
        
        # Initialize running totals
        cumulative_earned = 0
        cumulative_spent = 0
        cumulative_expired = 0
        
        for _, row in customer_df.iterrows():
            # Update running totals based on transaction type
            if row['TCTYPE'] == 'earned':
                cumulative_earned += row['AMOUNT']
            elif row['TCTYPE'] == 'spent':
                cumulative_spent += abs(row['AMOUNT'])
            elif row['TCTYPE'] == 'expired':
                cumulative_expired += abs(row['AMOUNT'])
            
            # Calculate current balance
            current_balance = cumulative_earned - cumulative_spent - cumulative_expired
            
            # Record this point in time
            balance_records.append({
                'customer_id': customer_id,
                'transaction_date': row['CREATEDAT'],
                'transaction_id': row['TRANS_ID'],
                'transaction_type': row['TCTYPE'],
                'amount': row['AMOUNT'],
                'cumulative_earned': round(cumulative_earned, 2),
                'cumulative_spent': round(cumulative_spent, 2),
                'cumulative_expired': round(cumulative_expired, 2),
                'current_balance': round(current_balance, 2)
            })
    
    return pd.DataFrame(balance_records)


def get_balance_on_date(
    balance_history: pd.DataFrame,
    customer_ids: list,
    target_date: str
) -> pd.DataFrame:
    """
    Get the Thrive Cash balance for specific customers on a specific date.
    
    WHAT THIS DOES:
        Finds the last transaction on or before the target date for each
        customer and returns their balance at that point.
    
    This answers the sample query from Part 5B:
    "What was the Thrive Cash balance for customers 23306353 and 16161481 
    on 2023-03-21?"
    
    PARAMETERS:
        balance_history: DataFrame from build_customer_balance_history()
        customer_ids: List of customer IDs to query
        target_date: Date string in 'YYYY-MM-DD' format
    
    RETURNS:
        DataFrame with balance information for each customer
    """
    logger.info(f"Getting balance for customers {customer_ids} on {target_date}")
    
    target_dt = pd.to_datetime(target_date)
    
    results = []
    
    for customer_id in customer_ids:
        # Filter to this customer's transactions on or before target date
        customer_history = balance_history[
            (balance_history['customer_id'] == customer_id) &
            (balance_history['transaction_date'] <= target_dt)
        ]
        
        if len(customer_history) == 0:
            # No transactions before target date
            results.append({
                'customer_id': customer_id,
                'balance_date': target_date,
                'last_transaction_date': None,
                'total_earned': 0,
                'total_spent': 0,
                'total_expired': 0,
                'balance_on_date': 0
            })
        else:
            # Get the most recent transaction
            latest = customer_history.iloc[-1]
            
            results.append({
                'customer_id': customer_id,
                'balance_date': target_date,
                'last_transaction_date': latest['transaction_date'],
                'total_earned': latest['cumulative_earned'],
                'total_spent': latest['cumulative_spent'],
                'total_expired': latest['cumulative_expired'],
                'balance_on_date': latest['current_balance']
            })
    
    return pd.DataFrame(results)


def run_analytics():
    """
    Run all analytics queries and display results.
    
    This demonstrates the analytics capabilities from Part 5.
    """
    print("=" * 70)
    print("THRIVE CASH ANALYTICS")
    print("=" * 70)
    
    # Load the FIFO-matched data
    logger.info("Loading FIFO-matched data...")
    df = pd.read_csv('output/tc_data_with_redemptions.csv')
    df['CREATEDAT'] = pd.to_datetime(df['CREATEDAT'])
    
    print(f"\nLoaded {len(df)} transactions for {df['CUSTOMERID'].nunique()} customers")
    
    # =========================================================================
    # PART 5A: Build Customer Balance History
    # =========================================================================
    print("\n" + "=" * 70)
    print("PART 5A: CUSTOMER BALANCE HISTORY")
    print("=" * 70)
    
    balance_history = build_customer_balance_history(df)
    
    print("\nBalance history (showing key columns):")
    print(balance_history[[
        'customer_id', 'transaction_date', 'transaction_type',
        'amount', 'cumulative_earned', 'cumulative_spent', 
        'cumulative_expired', 'current_balance'
    ]].to_string(index=False))
    
    # Save to CSV
    balance_history.to_csv('output/customer_balance_history.csv', index=False)
    print("\n✓ Saved to output/customer_balance_history.csv")
    
    # =========================================================================
    # PART 5B: Sample Query - Balance on Specific Date
    # =========================================================================
    print("\n" + "=" * 70)
    print("PART 5B: SAMPLE QUERY")
    print("=" * 70)
    print("\nQuestion: What was the Thrive Cash balance for customers")
    print("          23306353 and 16161481 on 2023-03-21?")
    print("-" * 70)
    
    # Run the query
    target_customers = [23306353, 16161481]
    target_date = '2023-03-21'
    
    result = get_balance_on_date(balance_history, target_customers, target_date)
    
    print("\nAnswer:")
    print(result.to_string(index=False))
    
    # =========================================================================
    # SUMMARY STATISTICS
    # =========================================================================
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    
    # Current balances (latest for each customer)
    current_balances = balance_history.groupby('customer_id').last().reset_index()
    
    print("\nCurrent Customer Balances:")
    print(current_balances[[
        'customer_id', 'cumulative_earned', 'cumulative_spent',
        'cumulative_expired', 'current_balance'
    ]].to_string(index=False))
    
    # Overall totals
    total_earned = df[df['TCTYPE'] == 'earned']['AMOUNT'].sum()
    total_spent = abs(df[df['TCTYPE'] == 'spent']['AMOUNT'].sum())
    total_expired = abs(df[df['TCTYPE'] == 'expired']['AMOUNT'].sum())
    total_liability = total_earned - total_spent - total_expired
    
    print(f"\nOverall Thrive Cash Metrics:")
    print(f"  Total Earned:   ${total_earned:,.2f}")
    print(f"  Total Spent:    ${total_spent:,.2f}")
    print(f"  Total Expired:  ${total_expired:,.2f}")
    print(f"  Net Liability:  ${total_liability:,.2f}")
    
    print("\n" + "=" * 70)
    print("ANALYTICS COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    run_analytics()
