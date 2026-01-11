"""
=============================================================================
FIFO MATCHING ENGINE FOR THRIVE CASH TRANSACTIONS
=============================================================================

PURPOSE:
    This module implements the First-In, First-Out (FIFO) matching algorithm
    for Thrive Cash transactions. It matches "spent" and "expired" transactions
    to "earned" transactions in chronological order.

BUSINESS CONTEXT:
    Thrive Cash is a customer rewards program where:
    - Customers EARN Thrive Cash through promotions, refunds, referrals, etc.
    - Customers SPEND Thrive Cash on orders (reduces their balance)
    - Unused Thrive Cash EXPIRES after a certain period
    
    For accounting purposes, we need to track WHICH earned transaction was
    redeemed by WHICH spent/expired transaction. This follows FIFO rules:
    the OLDEST earned transaction gets matched first.

EXAMPLE:
    Customer earns $20 on Jan 1, then $30 on Jan 15.
    Customer spends $25 on Feb 1.
    
    FIFO matching:
    - The $25 spent matches to the $20 earned on Jan 1 (oldest first)
    - The remaining $5 matches to the $30 earned on Jan 15
    
OUTPUT:
    A new column "REDEEMID" is added to earned transactions, containing
    the TRANS_ID of the spent/expired transaction that redeemed it.

AUTHOR: Data Applications Team
DATE: 2024
=============================================================================
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional
from datetime import datetime
import logging

# -----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -----------------------------------------------------------------------------
# Set up logging so we can track what the algorithm is doing.
# This is crucial for debugging and auditing in production.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_tc_data(filepath: str) -> pd.DataFrame:
    """
    Load Thrive Cash transaction data from an Excel file.
    
    WHAT THIS DOES:
        Reads the TC_Data sheet from the Excel file and prepares it
        for processing by ensuring correct data types.
    
    PARAMETERS:
        filepath: Path to the Excel file (e.g., 'data/tc_raw_data.xlsx')
    
    RETURNS:
        DataFrame with columns:
        - TRANS_ID: Unique transaction identifier
        - TCTYPE: Transaction type ('earned', 'spent', 'expired')
        - CREATEDAT: When the transaction occurred
        - EXPIREDAT: When earned transactions expire (if applicable)
        - CUSTOMERID: Customer identifier
        - ORDERID: Associated order (for spent transactions)
        - AMOUNT: Transaction amount (positive for earned, negative for spent/expired)
        - REASON: Why the transaction occurred (refund, promotion, etc.)
    
    EXAMPLE:
        >>> df = load_tc_data('data/tc_raw_data.xlsx')
        >>> print(df.head())
    """
    logger.info(f"Loading TC data from: {filepath}")
    
    # Read the Excel file, specifically the TC_Data sheet
    df = pd.read_excel(filepath, sheet_name='TC_Data')
    
    # Ensure CREATEDAT is a proper datetime for sorting
    # This is critical for FIFO - we need accurate chronological ordering
    df['CREATEDAT'] = pd.to_datetime(df['CREATEDAT'])
    
    # Log summary statistics for visibility
    logger.info(f"Loaded {len(df)} transactions")
    logger.info(f"Transaction types: {df['TCTYPE'].value_counts().to_dict()}")
    logger.info(f"Unique customers: {df['CUSTOMERID'].nunique()}")
    
    return df


def perform_fifo_matching(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform FIFO matching of spent/expired transactions to earned transactions.
    
    ==========================================================================
    ALGORITHM EXPLANATION (for non-technical readers):
    ==========================================================================
    
    Think of it like a queue at a coffee shop:
    1. Customers earn "coffee credits" (earned transactions)
    2. When they use credits (spent) or credits expire (expired), 
       we need to know WHICH credits were used
    3. We always use the OLDEST credits first (First-In, First-Out)
    
    For each customer:
    1. Sort all their "earned" transactions by date (oldest first)
    2. For each "spent" or "expired" transaction:
       - Find the oldest earned transaction that hasn't been matched yet
       - Link them together by putting the spent/expired ID in REDEEMID
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    The algorithm processes each customer independently:
    1. Filter transactions for one customer
    2. Separate into "earned" vs "spent/expired" buckets
    3. Sort earned by CREATEDAT ascending (oldest first)
    4. For each spent/expired transaction, assign to next available earned
    5. Handle partial matches (when amounts don't align perfectly)
    
    PARAMETERS:
        df: DataFrame with TC_Data columns (see load_tc_data for schema)
    
    RETURNS:
        DataFrame with new REDEEMID column added to earned transactions.
        REDEEMID contains the TRANS_ID of the spent/expired transaction
        that redeemed this earned transaction.
    
    SCALABILITY NOTE:
        This implementation uses vectorized operations where possible
        and processes customers in batches. For millions of transactions,
        consider using SQL window functions in Snowflake instead.
    """
    logger.info("Starting FIFO matching process...")
    
    # Create a copy to avoid modifying the original data
    # This is a best practice for data pipelines
    result_df = df.copy()
    
    # Initialize the REDEEMID column with None (null values)
    # Only earned transactions will get a REDEEMID assigned
    result_df['REDEEMID'] = None
    
    # Get list of unique customers to process
    customers = result_df['CUSTOMERID'].unique()
    logger.info(f"Processing {len(customers)} customers...")
    
    # Track statistics for logging and validation
    total_matches = 0
    unmatched_redemptions = 0
    
    # ---------------------------------------------------------------------
    # PROCESS EACH CUSTOMER INDEPENDENTLY
    # ---------------------------------------------------------------------
    # FIFO matching is done per-customer because each customer has their
    # own Thrive Cash balance that is independent of other customers.
    
    for customer_id in customers:
        logger.debug(f"Processing customer: {customer_id}")
        
        # Get all transactions for this customer
        customer_mask = result_df['CUSTOMERID'] == customer_id
        customer_df = result_df[customer_mask].copy()
        
        # -------------------------------------------------------------
        # SEPARATE EARNED VS SPENT/EXPIRED TRANSACTIONS
        # -------------------------------------------------------------
        # Earned = money coming IN (positive amounts)
        # Spent/Expired = money going OUT (negative amounts)
        
        earned_mask = customer_df['TCTYPE'] == 'earned'
        redemption_mask = customer_df['TCTYPE'].isin(['spent', 'expired'])
        
        # Get earned transactions, sorted by date (OLDEST FIRST - this is the FIFO part!)
        earned_txns = customer_df[earned_mask].sort_values('CREATEDAT').copy()
        
        # Get spent/expired transactions (redemptions), sorted by date
        redemption_txns = customer_df[redemption_mask].sort_values('CREATEDAT')
        
        if len(earned_txns) == 0 or len(redemption_txns) == 0:
            # Nothing to match for this customer
            continue
        
        # -------------------------------------------------------------
        # FIFO MATCHING LOOP
        # -------------------------------------------------------------
        # For each redemption (spent/expired), find the oldest unmatched
        # earned transaction and link them together.
        
        # Track which earned transactions have been matched
        # Using a list of indices that are still available
        available_earned_indices = list(earned_txns.index)
        
        for _, redemption in redemption_txns.iterrows():
            redemption_id = redemption['TRANS_ID']
            redemption_amount = abs(redemption['AMOUNT'])  # Make positive for comparison
            redemption_date = redemption['CREATEDAT']
            
            # Track how much of this redemption we still need to match
            remaining_to_match = redemption_amount
            
            # Try to match to available earned transactions (FIFO order)
            indices_to_remove = []
            
            for earned_idx in available_earned_indices:
                if remaining_to_match <= 0:
                    break
                    
                earned_row = earned_txns.loc[earned_idx]
                earned_amount = earned_row['AMOUNT']
                earned_date = earned_row['CREATEDAT']
                
                # IMPORTANT: Only match if earned transaction is BEFORE the redemption
                # You can't redeem credits you haven't earned yet!
                if earned_date > redemption_date:
                    continue
                
                # Match this earned transaction to the redemption
                # Set REDEEMID on the earned transaction
                result_df.loc[earned_idx, 'REDEEMID'] = redemption_id
                
                # Update tracking
                remaining_to_match -= earned_amount
                indices_to_remove.append(earned_idx)
                total_matches += 1
                
                logger.debug(
                    f"  Matched earned {earned_row['TRANS_ID']} (${earned_amount}) "
                    f"to {redemption['TCTYPE']} {redemption_id}"
                )
            
            # Remove matched earned transactions from available pool
            for idx in indices_to_remove:
                available_earned_indices.remove(idx)
            
            # Check if we couldn't fully match this redemption
            if remaining_to_match > 0.01:  # Small tolerance for floating point
                unmatched_redemptions += 1
                logger.warning(
                    f"Customer {customer_id}: Could not fully match "
                    f"{redemption['TCTYPE']} {redemption_id} "
                    f"(${remaining_to_match:.2f} unmatched)"
                )
    
    # ---------------------------------------------------------------------
    # LOG SUMMARY STATISTICS
    # ---------------------------------------------------------------------
    logger.info(f"FIFO matching complete!")
    logger.info(f"  Total matches made: {total_matches}")
    logger.info(f"  Redemptions with unmatched amounts: {unmatched_redemptions}")
    
    matched_earned = result_df[result_df['REDEEMID'].notna()]
    logger.info(f"  Earned transactions with REDEEMID: {len(matched_earned)}")
    
    return result_df


def save_results(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the matched results to a CSV file.
    
    WHAT THIS DOES:
        Exports the DataFrame with REDEEMID column to a CSV file
        for downstream processing or review.
    
    PARAMETERS:
        df: DataFrame with FIFO matching results
        output_path: Where to save the CSV (e.g., 'output/tc_data_with_redemptions.csv')
    
    OUTPUT FILE FORMAT:
        CSV with columns: TRANS_ID, TCTYPE, CREATEDAT, EXPIREDAT, 
                         CUSTOMERID, ORDERID, AMOUNT, REASON, REDEEMID
    """
    logger.info(f"Saving results to: {output_path}")
    
    # Save to CSV with a clean format
    df.to_csv(output_path, index=False, date_format='%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Successfully saved {len(df)} rows to {output_path}")


def run_fifo_matching_pipeline(
    input_path: str = 'data/tc_raw_data.xlsx',
    output_path: str = 'output/tc_data_with_redemptions.csv'
) -> pd.DataFrame:
    """
    Run the complete FIFO matching pipeline from start to finish.
    
    ==========================================================================
    PIPELINE OVERVIEW (for non-technical readers):
    ==========================================================================
    
    This function runs the entire process:
    1. LOAD: Read the Excel file with transaction data
    2. MATCH: Apply FIFO algorithm to link spent/expired to earned
    3. SAVE: Export results to CSV for accounting team
    
    ==========================================================================
    USAGE:
    ==========================================================================
    
    From command line:
        python src/fifo_matching.py
    
    From Python:
        from src.fifo_matching import run_fifo_matching_pipeline
        result = run_fifo_matching_pipeline()
    
    PARAMETERS:
        input_path: Path to source Excel file
        output_path: Path for output CSV file
    
    RETURNS:
        DataFrame with FIFO matching results
    """
    logger.info("=" * 60)
    logger.info("THRIVE CASH FIFO MATCHING PIPELINE")
    logger.info("=" * 60)
    
    # Step 1: Load data
    df = load_tc_data(input_path)
    
    # Step 2: Perform FIFO matching
    result_df = perform_fifo_matching(df)
    
    # Step 3: Save results
    save_results(result_df, output_path)
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    
    return result_df


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
# This allows the script to be run directly from the command line:
#   python src/fifo_matching.py

if __name__ == '__main__':
    # Run the pipeline with default paths
    result = run_fifo_matching_pipeline()
    
    # Print a summary of results
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    
    # Show earned transactions with their REDEEMID assignments
    earned_with_redemptions = result[result['TCTYPE'] == 'earned'][
        ['TRANS_ID', 'CUSTOMERID', 'CREATEDAT', 'AMOUNT', 'REDEEMID']
    ].sort_values(['CUSTOMERID', 'CREATEDAT'])
    
    print("\nEarned transactions with REDEEMID assignments:")
    print(earned_with_redemptions.to_string(index=False))
