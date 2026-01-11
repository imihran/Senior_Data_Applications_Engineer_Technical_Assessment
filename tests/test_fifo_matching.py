"""
=============================================================================
UNIT TESTS FOR FIFO MATCHING ENGINE
=============================================================================

PURPOSE:
    This module contains comprehensive tests for the FIFO matching algorithm.
    Tests verify that the business logic works correctly under various scenarios.

WHY TESTING MATTERS:
    - Financial data requires high accuracy
    - FIFO matching affects accounting reports
    - Tests catch bugs before they reach production
    - Tests serve as documentation of expected behavior

TEST CATEGORIES:
    1. BASIC FUNCTIONALITY: Simple matching scenarios
    2. EDGE CASES: Empty data, single transactions, etc.
    3. BUSINESS RULES: FIFO order, chronological constraints
    4. DATA QUALITY: Validation checks work correctly

RUNNING TESTS:
    From the Repo directory:
        pytest tests/test_fifo_matching.py -v
    
    With coverage:
        pytest tests/test_fifo_matching.py -v --cov=src

AUTHOR: Data Applications Team
DATE: 2024
=============================================================================
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from fifo_matching import perform_fifo_matching, load_tc_data
from data_quality import validate_source_data, validate_fifo_results


# =============================================================================
# TEST FIXTURES
# =============================================================================
# Fixtures provide reusable test data. They run before each test that uses them.

@pytest.fixture
def sample_tc_data():
    """
    Create a sample DataFrame that mimics the TC_Data structure.
    
    WHAT THIS IS:
        A small, controlled dataset for testing. We know exactly what
        the expected output should be, so we can verify the algorithm.
    
    SCENARIO:
        Customer 1: Earns $20, then $30, then spends $25
        Expected: The $25 spent should match to the $20 earned (oldest first)
    """
    data = {
        'TRANS_ID': [1001, 1002, 1003],
        'TCTYPE': ['earned', 'earned', 'spent'],
        'CREATEDAT': [
            datetime(2023, 1, 1, 10, 0, 0),  # Earned first
            datetime(2023, 1, 15, 10, 0, 0),  # Earned second
            datetime(2023, 2, 1, 10, 0, 0),   # Spent later
        ],
        'EXPIREDAT': [
            datetime(2023, 3, 1),
            datetime(2023, 3, 15),
            None,
        ],
        'CUSTOMERID': [100, 100, 100],
        'ORDERID': [None, None, 5001],
        'AMOUNT': [20.0, 30.0, -25.0],
        'REASON': ['Promotion', 'Refund', None],
    }
    return pd.DataFrame(data)


@pytest.fixture
def multi_customer_data():
    """
    Test data with multiple customers.
    
    SCENARIO:
        Customer 100: Earns $20, spends $15
        Customer 200: Earns $50, earns $25, spends $60
        
    This tests that FIFO matching is done independently per customer.
    """
    data = {
        'TRANS_ID': [1001, 1002, 1003, 2001, 2002, 2003],
        'TCTYPE': ['earned', 'spent', 'earned', 'earned', 'spent', 'earned'],
        'CREATEDAT': [
            datetime(2023, 1, 1),   # Customer 100 earns
            datetime(2023, 2, 1),   # Customer 100 spends
            datetime(2023, 1, 5),   # Customer 200 earns (first)
            datetime(2023, 1, 10),  # Customer 200 earns (second)
            datetime(2023, 2, 15),  # Customer 200 spends
            datetime(2023, 1, 1),   # Customer 100 earns more
        ],
        'EXPIREDAT': [None] * 6,
        'CUSTOMERID': [100, 100, 200, 200, 200, 100],
        'ORDERID': [None, 5001, None, None, 5002, None],
        'AMOUNT': [20.0, -15.0, 50.0, 25.0, -60.0, 10.0],
        'REASON': [None] * 6,
    }
    return pd.DataFrame(data)


@pytest.fixture
def expired_transactions_data():
    """
    Test data with expired transactions.
    
    SCENARIO:
        Customer earns $10, then $20
        $10 expires before being spent
        Customer spends $15
        
    Expected: The $15 spent should match to the $20 earned (since $10 expired)
    """
    data = {
        'TRANS_ID': [1001, 1002, 1003, 1004],
        'TCTYPE': ['earned', 'earned', 'expired', 'spent'],
        'CREATEDAT': [
            datetime(2023, 1, 1),   # Earn $10
            datetime(2023, 1, 15),  # Earn $20
            datetime(2023, 2, 1),   # $10 expires
            datetime(2023, 2, 15),  # Spend $15
        ],
        'EXPIREDAT': [
            datetime(2023, 2, 1),
            datetime(2023, 3, 15),
            None,
            None,
        ],
        'CUSTOMERID': [100, 100, 100, 100],
        'ORDERID': [None, None, None, 5001],
        'AMOUNT': [10.0, 20.0, -10.0, -15.0],
        'REASON': [None] * 4,
    }
    return pd.DataFrame(data)


# =============================================================================
# BASIC FUNCTIONALITY TESTS
# =============================================================================

class TestBasicFIFOMatching:
    """
    Tests for basic FIFO matching functionality.
    
    These tests verify that the core algorithm works correctly
    in straightforward scenarios.
    """
    
    def test_simple_fifo_matching(self, sample_tc_data):
        """
        Test that FIFO matching assigns REDEEMID correctly.
        
        SCENARIO:
            Customer earns $20 (ID 1001), then $30 (ID 1002), then spends $25 (ID 1003)
        
        EXPECTED:
            The $20 earned (oldest) should get REDEEMID = 1003 (the spent transaction)
        """
        result = perform_fifo_matching(sample_tc_data)
        
        # Check that REDEEMID column was added
        assert 'REDEEMID' in result.columns, "REDEEMID column should be added"
        
        # Check that the oldest earned transaction got matched
        earned_1001 = result[result['TRANS_ID'] == 1001].iloc[0]
        assert earned_1001['REDEEMID'] == 1003, \
            "Oldest earned (1001) should be matched to spent (1003)"
    
    def test_fifo_order_respected(self, sample_tc_data):
        """
        Test that FIFO order is respected (oldest earned matched first).
        
        EXPECTED:
            Transaction 1001 (older) should be matched before 1002 (newer)
        """
        result = perform_fifo_matching(sample_tc_data)
        
        # Get the earned transactions
        earned_1001 = result[result['TRANS_ID'] == 1001].iloc[0]
        earned_1002 = result[result['TRANS_ID'] == 1002].iloc[0]
        
        # 1001 should be matched (has REDEEMID)
        assert pd.notna(earned_1001['REDEEMID']), \
            "Older earned transaction should be matched first"
        
        # 1002 might or might not be matched depending on amounts
        # In this case, $25 spent > $20 earned, so 1002 should also be partially matched
    
    def test_redeemid_only_on_earned(self, sample_tc_data):
        """
        Test that REDEEMID is only assigned to earned transactions.
        
        EXPECTED:
            Spent and expired transactions should NOT have REDEEMID values
        """
        result = perform_fifo_matching(sample_tc_data)
        
        # Check spent transaction doesn't have REDEEMID
        spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
        assert pd.isna(spent_row['REDEEMID']), \
            "Spent transactions should not have REDEEMID"


class TestMultiCustomerMatching:
    """
    Tests for FIFO matching with multiple customers.
    
    FIFO matching should be independent per customer - one customer's
    transactions should not affect another customer's matching.
    """
    
    def test_customers_matched_independently(self, multi_customer_data):
        """
        Test that each customer's transactions are matched independently.
        
        EXPECTED:
            Customer 100's earned transactions should only be matched to
            Customer 100's spent transactions, not Customer 200's.
        """
        result = perform_fifo_matching(multi_customer_data)
        
        # Get Customer 100's earned transactions with REDEEMID
        customer_100_earned = result[
            (result['CUSTOMERID'] == 100) & 
            (result['TCTYPE'] == 'earned') &
            (result['REDEEMID'].notna())
        ]
        
        # Check that their REDEEMIDs reference Customer 100's spent transactions
        customer_100_spent_ids = set(
            result[(result['CUSTOMERID'] == 100) & (result['TCTYPE'] == 'spent')]['TRANS_ID']
        )
        
        for _, row in customer_100_earned.iterrows():
            assert row['REDEEMID'] in customer_100_spent_ids, \
                f"Customer 100's earned should only match to Customer 100's spent"
    
    def test_no_cross_customer_matching(self, multi_customer_data):
        """
        Test that there's no cross-customer matching.
        
        EXPECTED:
            Customer 200's spent transaction (2003) should NOT appear as
            REDEEMID for Customer 100's earned transactions.
        """
        result = perform_fifo_matching(multi_customer_data)
        
        # Get all REDEEMIDs for Customer 100
        customer_100_redeemids = set(
            result[result['CUSTOMERID'] == 100]['REDEEMID'].dropna()
        )
        
        # Get Customer 200's spent transaction IDs
        customer_200_spent_ids = set(
            result[(result['CUSTOMERID'] == 200) & (result['TCTYPE'] == 'spent')]['TRANS_ID']
        )
        
        # There should be no overlap
        overlap = customer_100_redeemids & customer_200_spent_ids
        assert len(overlap) == 0, \
            f"Found cross-customer matching: {overlap}"


class TestExpiredTransactions:
    """
    Tests for handling expired transactions.
    
    Expired transactions should also be matched to earned transactions
    using FIFO rules, just like spent transactions.
    """
    
    def test_expired_matches_to_earned(self, expired_transactions_data):
        """
        Test that expired transactions are matched to earned transactions.
        
        EXPECTED:
            The expired transaction (1003) should be matched to the oldest
            earned transaction (1001).
        """
        result = perform_fifo_matching(expired_transactions_data)
        
        # Find the earned transaction that was matched to the expired
        earned_matched_to_expired = result[
            (result['TCTYPE'] == 'earned') & 
            (result['REDEEMID'] == 1003)  # 1003 is the expired transaction
        ]
        
        assert len(earned_matched_to_expired) > 0, \
            "Expired transaction should be matched to an earned transaction"


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestEdgeCases:
    """
    Tests for edge cases and boundary conditions.
    
    These tests ensure the algorithm handles unusual situations gracefully.
    """
    
    def test_empty_dataframe(self):
        """
        Test handling of empty DataFrame.
        
        EXPECTED:
            Should return empty DataFrame with REDEEMID column, no errors.
        """
        empty_df = pd.DataFrame(columns=[
            'TRANS_ID', 'TCTYPE', 'CREATEDAT', 'EXPIREDAT', 
            'CUSTOMERID', 'ORDERID', 'AMOUNT', 'REASON'
        ])
        
        result = perform_fifo_matching(empty_df)
        
        assert 'REDEEMID' in result.columns
        assert len(result) == 0
    
    def test_only_earned_transactions(self):
        """
        Test when there are only earned transactions (nothing to match).
        
        EXPECTED:
            All REDEEMID values should be None.
        """
        data = {
            'TRANS_ID': [1001, 1002],
            'TCTYPE': ['earned', 'earned'],
            'CREATEDAT': [datetime(2023, 1, 1), datetime(2023, 1, 15)],
            'EXPIREDAT': [None, None],
            'CUSTOMERID': [100, 100],
            'ORDERID': [None, None],
            'AMOUNT': [20.0, 30.0],
            'REASON': [None, None],
        }
        df = pd.DataFrame(data)
        
        result = perform_fifo_matching(df)
        
        assert result['REDEEMID'].isna().all(), \
            "With no spent/expired, all REDEEMID should be None"
    
    def test_only_spent_transactions(self):
        """
        Test when there are only spent transactions (nothing to match to).
        
        EXPECTED:
            Should complete without error, no REDEEMID assignments.
        """
        data = {
            'TRANS_ID': [1001, 1002],
            'TCTYPE': ['spent', 'spent'],
            'CREATEDAT': [datetime(2023, 1, 1), datetime(2023, 1, 15)],
            'EXPIREDAT': [None, None],
            'CUSTOMERID': [100, 100],
            'ORDERID': [5001, 5002],
            'AMOUNT': [-20.0, -30.0],
            'REASON': [None, None],
        }
        df = pd.DataFrame(data)
        
        result = perform_fifo_matching(df)
        
        # Should complete without error
        assert 'REDEEMID' in result.columns
    
    def test_spent_before_earned(self):
        """
        Test when spent transaction occurs before any earned.
        
        EXPECTED:
            The spent transaction should NOT be matched to the earned
            (can't spend credits you haven't earned yet).
        """
        data = {
            'TRANS_ID': [1001, 1002],
            'TCTYPE': ['spent', 'earned'],
            'CREATEDAT': [
                datetime(2023, 1, 1),   # Spent first
                datetime(2023, 2, 1),   # Earned later
            ],
            'EXPIREDAT': [None, None],
            'CUSTOMERID': [100, 100],
            'ORDERID': [5001, None],
            'AMOUNT': [-20.0, 30.0],
            'REASON': [None, None],
        }
        df = pd.DataFrame(data)
        
        result = perform_fifo_matching(df)
        
        # The earned transaction should NOT be matched to the spent
        # because the spent happened before the earned
        earned_row = result[result['TRANS_ID'] == 1002].iloc[0]
        assert pd.isna(earned_row['REDEEMID']), \
            "Earned after spent should not be matched to that spent"


# =============================================================================
# DATA QUALITY VALIDATION TESTS
# =============================================================================

class TestDataQualityValidation:
    """
    Tests for the data quality validation framework.
    
    These tests ensure our validation checks catch data issues correctly.
    """
    
    def test_source_validation_passes_good_data(self, sample_tc_data):
        """
        Test that valid data passes source validation.
        """
        report = validate_source_data(sample_tc_data)
        
        assert report.passed, \
            f"Valid data should pass validation. Errors: {report.error_count}"
    
    def test_source_validation_catches_null_trans_id(self):
        """
        Test that null TRANS_ID is caught by validation.
        """
        data = {
            'TRANS_ID': [1001, None],  # Second row has null ID
            'TCTYPE': ['earned', 'spent'],
            'CREATEDAT': [datetime(2023, 1, 1), datetime(2023, 2, 1)],
            'EXPIREDAT': [None, None],
            'CUSTOMERID': [100, 100],
            'ORDERID': [None, 5001],
            'AMOUNT': [20.0, -15.0],
            'REASON': [None, None],
        }
        df = pd.DataFrame(data)
        
        report = validate_source_data(df)
        
        # Should have at least one error for null TRANS_ID
        null_check = next(
            (r for r in report.results if 'TRANS_ID' in r.check_name and 'Null' in r.check_name),
            None
        )
        assert null_check is not None and not null_check.passed, \
            "Should catch null TRANS_ID"
    
    def test_source_validation_catches_invalid_type(self):
        """
        Test that invalid transaction types are caught.
        """
        data = {
            'TRANS_ID': [1001, 1002],
            'TCTYPE': ['earned', 'invalid_type'],  # Invalid type
            'CREATEDAT': [datetime(2023, 1, 1), datetime(2023, 2, 1)],
            'EXPIREDAT': [None, None],
            'CUSTOMERID': [100, 100],
            'ORDERID': [None, 5001],
            'AMOUNT': [20.0, -15.0],
            'REASON': [None, None],
        }
        df = pd.DataFrame(data)
        
        report = validate_source_data(df)
        
        # Should have error for invalid type
        type_check = next(
            (r for r in report.results if 'Transaction Types' in r.check_name),
            None
        )
        assert type_check is not None and not type_check.passed, \
            "Should catch invalid transaction type"
    
    def test_fifo_validation_passes_correct_results(self, sample_tc_data):
        """
        Test that correctly matched data passes FIFO validation.
        """
        matched_df = perform_fifo_matching(sample_tc_data)
        report = validate_fifo_results(sample_tc_data, matched_df)
        
        assert report.passed, \
            f"Correctly matched data should pass validation. Errors: {report.error_count}"


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestIntegration:
    """
    Integration tests that test the full pipeline.
    
    These tests use the actual data file to verify end-to-end functionality.
    """
    
    def test_load_actual_data(self):
        """
        Test loading the actual Excel file.
        """
        # This test requires the actual data file
        try:
            df = load_tc_data('data/tc_raw_data.xlsx')
            
            assert len(df) > 0, "Should load some data"
            assert 'TRANS_ID' in df.columns
            assert 'TCTYPE' in df.columns
            assert 'CUSTOMERID' in df.columns
        except FileNotFoundError:
            pytest.skip("Data file not found - skipping integration test")
    
    def test_full_pipeline_on_actual_data(self):
        """
        Test the full FIFO matching pipeline on actual data.
        """
        try:
            # Load data
            df = load_tc_data('data/tc_raw_data.xlsx')
            
            # Validate source
            source_report = validate_source_data(df)
            assert source_report.passed, "Source validation should pass"
            
            # Perform matching
            matched_df = perform_fifo_matching(df)
            
            # Validate results
            fifo_report = validate_fifo_results(df, matched_df)
            assert fifo_report.passed, "FIFO validation should pass"
            
            # Check output has expected structure
            assert 'REDEEMID' in matched_df.columns
            assert len(matched_df) == len(df)
            
        except FileNotFoundError:
            pytest.skip("Data file not found - skipping integration test")


# =============================================================================
# PERFORMANCE TESTS
# =============================================================================

class TestPerformance:
    """
    Performance tests to ensure the algorithm scales.
    
    These tests verify that the algorithm can handle larger datasets
    within acceptable time limits.
    """
    
    def test_handles_large_dataset(self):
        """
        Test that the algorithm can handle a larger dataset.
        
        Creates 1000 transactions and verifies completion within 5 seconds.
        """
        import time
        
        # Generate larger dataset
        n_transactions = 1000
        n_customers = 50
        
        np.random.seed(42)  # For reproducibility
        
        data = {
            'TRANS_ID': list(range(1, n_transactions + 1)),
            'TCTYPE': np.random.choice(['earned', 'spent', 'expired'], n_transactions, p=[0.5, 0.35, 0.15]),
            'CREATEDAT': [datetime(2023, 1, 1) + timedelta(hours=i) for i in range(n_transactions)],
            'EXPIREDAT': [None] * n_transactions,
            'CUSTOMERID': np.random.choice(range(1, n_customers + 1), n_transactions),
            'ORDERID': [None] * n_transactions,
            'AMOUNT': [20.0 if t == 'earned' else -15.0 for t in np.random.choice(['earned', 'spent', 'expired'], n_transactions, p=[0.5, 0.35, 0.15])],
            'REASON': [None] * n_transactions,
        }
        
        # Fix amounts based on actual types
        df = pd.DataFrame(data)
        df.loc[df['TCTYPE'] == 'earned', 'AMOUNT'] = np.random.uniform(10, 50, (df['TCTYPE'] == 'earned').sum())
        df.loc[df['TCTYPE'] == 'spent', 'AMOUNT'] = -np.random.uniform(5, 30, (df['TCTYPE'] == 'spent').sum())
        df.loc[df['TCTYPE'] == 'expired', 'AMOUNT'] = -np.random.uniform(5, 20, (df['TCTYPE'] == 'expired').sum())
        
        # Time the execution
        start_time = time.time()
        result = perform_fifo_matching(df)
        elapsed_time = time.time() - start_time
        
        assert elapsed_time < 5.0, \
            f"Should complete 1000 transactions in under 5 seconds, took {elapsed_time:.2f}s"
        assert len(result) == n_transactions


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == '__main__':
    # Run tests with verbose output
    pytest.main([__file__, '-v', '--tb=short'])
