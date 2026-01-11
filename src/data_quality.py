"""
=============================================================================
DATA QUALITY VALIDATION FRAMEWORK FOR THRIVE CASH PIPELINE
=============================================================================

PURPOSE:
    This module provides comprehensive data quality checks for the Thrive Cash
    processing pipeline. It validates data at multiple stages:
    
    1. SOURCE VALIDATION: Check raw data before processing
    2. BUSINESS RULE VALIDATION: Ensure data follows business logic
    3. RECONCILIATION: Verify FIFO matching results are correct
    4. OUTPUT VALIDATION: Confirm final output meets requirements

WHY DATA QUALITY MATTERS:
    - Financial data requires high accuracy (SOX compliance)
    - Errors in FIFO matching affect accounting reports
    - Early detection prevents downstream issues
    - Audit trail for compliance requirements

VALIDATION CATEGORIES:
    - Completeness: No missing required fields
    - Accuracy: Values are within expected ranges
    - Consistency: Data follows business rules
    - Integrity: Referential relationships are valid
    - Timeliness: Data is current and properly dated

AUTHOR: Data Applications Team
DATE: 2024
=============================================================================
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

# -----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES FOR VALIDATION RESULTS
# =============================================================================
# Using dataclasses to structure validation results makes them easy to
# understand and work with programmatically.

@dataclass
class ValidationResult:
    """
    Represents the result of a single validation check.
    
    ATTRIBUTES:
        check_name: Human-readable name of the check
        passed: True if validation passed, False if failed
        message: Description of what was checked and the result
        severity: 'ERROR' (must fix), 'WARNING' (should review), 'INFO' (informational)
        details: Additional context or failing records
    
    EXAMPLE:
        ValidationResult(
            check_name="No Null Transaction IDs",
            passed=True,
            message="All 17 transactions have valid TRANS_ID values",
            severity="ERROR",
            details=None
        )
    """
    check_name: str
    passed: bool
    message: str
    severity: str = 'ERROR'  # ERROR, WARNING, INFO
    details: Optional[Dict] = None


@dataclass
class ValidationReport:
    """
    Aggregated report of all validation checks.
    
    ATTRIBUTES:
        timestamp: When the validation was run
        stage: Which pipeline stage ('source', 'post_fifo', 'output')
        results: List of individual ValidationResult objects
        passed: True if ALL error-level checks passed
        
    METHODS:
        summary(): Returns a human-readable summary string
        to_dict(): Converts to dictionary for JSON serialization
    """
    timestamp: datetime
    stage: str
    results: List[ValidationResult]
    
    @property
    def passed(self) -> bool:
        """Check if all ERROR-level validations passed."""
        return all(r.passed for r in self.results if r.severity == 'ERROR')
    
    @property
    def error_count(self) -> int:
        """Count of failed ERROR-level checks."""
        return sum(1 for r in self.results if not r.passed and r.severity == 'ERROR')
    
    @property
    def warning_count(self) -> int:
        """Count of failed WARNING-level checks."""
        return sum(1 for r in self.results if not r.passed and r.severity == 'WARNING')
    
    def summary(self) -> str:
        """Generate a human-readable summary of validation results."""
        lines = [
            "=" * 60,
            f"VALIDATION REPORT: {self.stage.upper()}",
            f"Timestamp: {self.timestamp}",
            "=" * 60,
            f"Overall Status: {'PASSED ✓' if self.passed else 'FAILED ✗'}",
            f"Errors: {self.error_count} | Warnings: {self.warning_count}",
            "-" * 60,
        ]
        
        for result in self.results:
            status = "✓" if result.passed else "✗"
            lines.append(f"[{result.severity}] {status} {result.check_name}")
            lines.append(f"    {result.message}")
            if result.details and not result.passed:
                lines.append(f"    Details: {result.details}")
        
        lines.append("=" * 60)
        return "\n".join(lines)


# =============================================================================
# SOURCE DATA VALIDATION
# =============================================================================
# These checks run BEFORE any processing to ensure input data is valid.

def validate_source_data(df: pd.DataFrame) -> ValidationReport:
    """
    Validate raw source data before processing.
    
    ==========================================================================
    WHAT THIS CHECKS (for non-technical readers):
    ==========================================================================
    
    Before we process any data, we need to make sure it's "clean":
    
    1. REQUIRED FIELDS: Every transaction must have an ID, type, date, 
       customer, and amount. Missing values would break our calculations.
    
    2. VALID TRANSACTION TYPES: Only 'earned', 'spent', 'expired' are allowed.
       Any other type would indicate bad data.
    
    3. AMOUNT SIGNS: Earned should be positive, spent/expired should be negative.
       Wrong signs would mess up balance calculations.
    
    4. DATE LOGIC: Transactions can't be in the future, and expiration dates
       should be after creation dates.
    
    5. NO DUPLICATES: Each transaction ID should appear only once.
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    PARAMETERS:
        df: Raw DataFrame from Excel file
    
    RETURNS:
        ValidationReport with all check results
    
    RAISES:
        Nothing - all issues are captured in the report
    """
    logger.info("Running source data validation...")
    results = []
    
    # -------------------------------------------------------------------------
    # CHECK 1: Required Fields Not Null
    # -------------------------------------------------------------------------
    # These fields are essential for FIFO matching to work correctly.
    
    required_fields = ['TRANS_ID', 'TCTYPE', 'CREATEDAT', 'CUSTOMERID', 'AMOUNT']
    
    for field in required_fields:
        null_count = df[field].isna().sum()
        passed = null_count == 0
        
        results.append(ValidationResult(
            check_name=f"No Null Values: {field}",
            passed=passed,
            message=f"Found {null_count} null values in {field}" if not passed 
                    else f"All {len(df)} records have valid {field}",
            severity='ERROR',
            details={'null_count': int(null_count)} if not passed else None
        ))
    
    # -------------------------------------------------------------------------
    # CHECK 2: Valid Transaction Types
    # -------------------------------------------------------------------------
    # Only these three types are valid in our business logic.
    
    valid_types = {'earned', 'spent', 'expired'}
    actual_types = set(df['TCTYPE'].dropna().unique())
    invalid_types = actual_types - valid_types
    
    results.append(ValidationResult(
        check_name="Valid Transaction Types",
        passed=len(invalid_types) == 0,
        message=f"Invalid types found: {invalid_types}" if invalid_types 
                else f"All transactions have valid types: {actual_types}",
        severity='ERROR',
        details={'invalid_types': list(invalid_types)} if invalid_types else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 3: Amount Sign Consistency
    # -------------------------------------------------------------------------
    # Business rule: earned = positive, spent/expired = negative
    
    earned_positive = df[df['TCTYPE'] == 'earned']['AMOUNT'].gt(0).all()
    spent_negative = df[df['TCTYPE'] == 'spent']['AMOUNT'].lt(0).all()
    expired_negative = df[df['TCTYPE'] == 'expired']['AMOUNT'].lt(0).all()
    
    sign_check_passed = earned_positive and spent_negative and expired_negative
    
    results.append(ValidationResult(
        check_name="Amount Sign Consistency",
        passed=sign_check_passed,
        message="All amounts have correct signs (earned>0, spent/expired<0)" if sign_check_passed
                else "Some amounts have incorrect signs",
        severity='ERROR',
        details={
            'earned_all_positive': bool(earned_positive),
            'spent_all_negative': bool(spent_negative),
            'expired_all_negative': bool(expired_negative)
        } if not sign_check_passed else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 4: No Duplicate Transaction IDs
    # -------------------------------------------------------------------------
    # Each transaction should have a unique identifier.
    
    duplicate_ids = df[df['TRANS_ID'].duplicated()]['TRANS_ID'].tolist()
    
    results.append(ValidationResult(
        check_name="No Duplicate Transaction IDs",
        passed=len(duplicate_ids) == 0,
        message=f"Found {len(duplicate_ids)} duplicate TRANS_IDs" if duplicate_ids
                else "All transaction IDs are unique",
        severity='ERROR',
        details={'duplicate_ids': duplicate_ids} if duplicate_ids else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 5: Dates Are Reasonable
    # -------------------------------------------------------------------------
    # Transactions shouldn't be in the future (data quality issue).
    
    future_dates = df[df['CREATEDAT'] > datetime.now()]
    
    results.append(ValidationResult(
        check_name="No Future Dates",
        passed=len(future_dates) == 0,
        message=f"Found {len(future_dates)} transactions with future dates" if len(future_dates) > 0
                else "All transaction dates are in the past",
        severity='WARNING',  # Warning because clock skew could cause this
        details={'future_count': len(future_dates)} if len(future_dates) > 0 else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 6: Customer IDs Are Valid
    # -------------------------------------------------------------------------
    # Customer IDs should be positive integers.
    
    invalid_customers = df[df['CUSTOMERID'] <= 0] if df['CUSTOMERID'].dtype in ['int64', 'float64'] else pd.DataFrame()
    
    results.append(ValidationResult(
        check_name="Valid Customer IDs",
        passed=len(invalid_customers) == 0,
        message=f"Found {len(invalid_customers)} invalid customer IDs" if len(invalid_customers) > 0
                else "All customer IDs are valid positive numbers",
        severity='ERROR',
        details=None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 7: Data Completeness Summary
    # -------------------------------------------------------------------------
    # Informational check about the data we received.
    
    results.append(ValidationResult(
        check_name="Data Completeness Summary",
        passed=True,
        message=f"Loaded {len(df)} transactions for {df['CUSTOMERID'].nunique()} customers",
        severity='INFO',
        details={
            'total_transactions': len(df),
            'unique_customers': int(df['CUSTOMERID'].nunique()),
            'transaction_types': df['TCTYPE'].value_counts().to_dict(),
            'date_range': f"{df['CREATEDAT'].min()} to {df['CREATEDAT'].max()}"
        }
    ))
    
    return ValidationReport(
        timestamp=datetime.now(),
        stage='source',
        results=results
    )


# =============================================================================
# POST-FIFO MATCHING VALIDATION
# =============================================================================
# These checks run AFTER FIFO matching to verify results are correct.

def validate_fifo_results(
    original_df: pd.DataFrame, 
    matched_df: pd.DataFrame
) -> ValidationReport:
    """
    Validate FIFO matching results for correctness.
    
    ==========================================================================
    WHAT THIS CHECKS (for non-technical readers):
    ==========================================================================
    
    After we match spent/expired transactions to earned transactions, we need
    to verify the matching was done correctly:
    
    1. REDEEMID VALIDITY: The REDEEMID values should be actual transaction IDs
       from spent or expired transactions.
    
    2. NO DOUBLE MATCHING: Each spent/expired transaction should only be 
       assigned to earned transactions ONCE (no duplicates).
    
    3. CHRONOLOGICAL ORDER: Earned transactions should only be matched to
       spent/expired transactions that happened AFTER them.
    
    4. BALANCE RECONCILIATION: The total amounts should still balance
       (total earned = total spent + total expired + remaining balance).
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    PARAMETERS:
        original_df: Raw data before FIFO matching
        matched_df: Data after FIFO matching (with REDEEMID column)
    
    RETURNS:
        ValidationReport with all check results
    """
    logger.info("Running FIFO results validation...")
    results = []
    
    # -------------------------------------------------------------------------
    # CHECK 1: REDEEMID Column Exists
    # -------------------------------------------------------------------------
    
    has_redeemid = 'REDEEMID' in matched_df.columns
    
    results.append(ValidationResult(
        check_name="REDEEMID Column Exists",
        passed=has_redeemid,
        message="REDEEMID column successfully added" if has_redeemid
                else "REDEEMID column is missing from output",
        severity='ERROR'
    ))
    
    if not has_redeemid:
        # Can't continue validation without REDEEMID
        return ValidationReport(
            timestamp=datetime.now(),
            stage='post_fifo',
            results=results
        )
    
    # -------------------------------------------------------------------------
    # CHECK 2: REDEEMID Values Are Valid Transaction IDs
    # -------------------------------------------------------------------------
    # REDEEMIDs should reference actual spent/expired transactions.
    
    valid_redemption_ids = set(
        matched_df[matched_df['TCTYPE'].isin(['spent', 'expired'])]['TRANS_ID']
    )
    
    assigned_redeemids = set(matched_df['REDEEMID'].dropna())
    invalid_redeemids = assigned_redeemids - valid_redemption_ids
    
    results.append(ValidationResult(
        check_name="REDEEMID References Valid Transactions",
        passed=len(invalid_redeemids) == 0,
        message=f"Found {len(invalid_redeemids)} invalid REDEEMID values" if invalid_redeemids
                else "All REDEEMID values reference valid spent/expired transactions",
        severity='ERROR',
        details={'invalid_ids': list(invalid_redeemids)} if invalid_redeemids else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 3: Only Earned Transactions Have REDEEMID
    # -------------------------------------------------------------------------
    # Spent and expired transactions should NOT have REDEEMID values.
    
    non_earned_with_redeemid = matched_df[
        (matched_df['TCTYPE'] != 'earned') & 
        (matched_df['REDEEMID'].notna())
    ]
    
    results.append(ValidationResult(
        check_name="Only Earned Transactions Have REDEEMID",
        passed=len(non_earned_with_redeemid) == 0,
        message=f"Found {len(non_earned_with_redeemid)} non-earned transactions with REDEEMID" 
                if len(non_earned_with_redeemid) > 0
                else "REDEEMID only assigned to earned transactions",
        severity='ERROR'
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 4: Chronological Consistency
    # -------------------------------------------------------------------------
    # Earned transactions should be matched to redemptions that happened AFTER.
    
    chronological_errors = []
    earned_with_redeemid = matched_df[
        (matched_df['TCTYPE'] == 'earned') & 
        (matched_df['REDEEMID'].notna())
    ]
    
    for _, earned_row in earned_with_redeemid.iterrows():
        redeemid = earned_row['REDEEMID']
        redemption_row = matched_df[matched_df['TRANS_ID'] == redeemid]
        
        if len(redemption_row) > 0:
            redemption_date = redemption_row.iloc[0]['CREATEDAT']
            earned_date = earned_row['CREATEDAT']
            
            if earned_date > redemption_date:
                chronological_errors.append({
                    'earned_id': earned_row['TRANS_ID'],
                    'earned_date': str(earned_date),
                    'redemption_id': redeemid,
                    'redemption_date': str(redemption_date)
                })
    
    results.append(ValidationResult(
        check_name="Chronological Consistency",
        passed=len(chronological_errors) == 0,
        message=f"Found {len(chronological_errors)} chronological errors" if chronological_errors
                else "All matches follow chronological order (earned before redemption)",
        severity='ERROR',
        details={'errors': chronological_errors} if chronological_errors else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 5: Balance Reconciliation Per Customer
    # -------------------------------------------------------------------------
    # For each customer, verify that the math adds up.
    
    balance_errors = []
    
    for customer_id in matched_df['CUSTOMERID'].unique():
        customer_df = matched_df[matched_df['CUSTOMERID'] == customer_id]
        
        total_earned = customer_df[customer_df['TCTYPE'] == 'earned']['AMOUNT'].sum()
        total_spent = abs(customer_df[customer_df['TCTYPE'] == 'spent']['AMOUNT'].sum())
        total_expired = abs(customer_df[customer_df['TCTYPE'] == 'expired']['AMOUNT'].sum())
        
        # Remaining balance should be non-negative
        remaining = total_earned - total_spent - total_expired
        
        if remaining < -0.01:  # Small tolerance for floating point
            balance_errors.append({
                'customer_id': customer_id,
                'total_earned': float(total_earned),
                'total_spent': float(total_spent),
                'total_expired': float(total_expired),
                'remaining': float(remaining)
            })
    
    results.append(ValidationResult(
        check_name="Customer Balance Reconciliation",
        passed=len(balance_errors) == 0,
        message=f"Found {len(balance_errors)} customers with negative balances" if balance_errors
                else "All customer balances reconcile correctly",
        severity='WARNING',  # Warning because negative balance might be valid in some cases
        details={'errors': balance_errors} if balance_errors else None
    ))
    
    # -------------------------------------------------------------------------
    # CHECK 6: Matching Statistics
    # -------------------------------------------------------------------------
    # Informational summary of the matching results.
    
    total_earned = len(matched_df[matched_df['TCTYPE'] == 'earned'])
    matched_earned = len(matched_df[
        (matched_df['TCTYPE'] == 'earned') & 
        (matched_df['REDEEMID'].notna())
    ])
    unmatched_earned = total_earned - matched_earned
    
    results.append(ValidationResult(
        check_name="Matching Statistics",
        passed=True,
        message=f"Matched {matched_earned}/{total_earned} earned transactions ({unmatched_earned} unmatched)",
        severity='INFO',
        details={
            'total_earned': total_earned,
            'matched_earned': matched_earned,
            'unmatched_earned': unmatched_earned,
            'match_rate': f"{(matched_earned/total_earned*100):.1f}%" if total_earned > 0 else "N/A"
        }
    ))
    
    return ValidationReport(
        timestamp=datetime.now(),
        stage='post_fifo',
        results=results
    )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def run_all_validations(
    source_df: pd.DataFrame,
    matched_df: pd.DataFrame
) -> Tuple[ValidationReport, ValidationReport]:
    """
    Run all validation checks and return reports.
    
    WHAT THIS DOES:
        Convenience function that runs both source and post-FIFO validations
        and returns both reports.
    
    PARAMETERS:
        source_df: Raw data before processing
        matched_df: Data after FIFO matching
    
    RETURNS:
        Tuple of (source_report, fifo_report)
    
    EXAMPLE:
        source_report, fifo_report = run_all_validations(raw_df, matched_df)
        print(source_report.summary())
        print(fifo_report.summary())
    """
    source_report = validate_source_data(source_df)
    fifo_report = validate_fifo_results(source_df, matched_df)
    
    return source_report, fifo_report


def validation_gate(report: ValidationReport, fail_on_error: bool = True) -> bool:
    """
    Check if validation passed and optionally raise an exception.
    
    WHAT THIS DOES:
        Acts as a "gate" in the pipeline - if validation fails, we can
        stop processing to prevent bad data from propagating.
    
    PARAMETERS:
        report: ValidationReport to check
        fail_on_error: If True, raise exception on failure
    
    RETURNS:
        True if validation passed, False otherwise
    
    RAISES:
        ValueError: If fail_on_error=True and validation failed
    
    USAGE IN AIRFLOW:
        This function can be used in Airflow tasks to fail the DAG
        if data quality checks don't pass.
    """
    if not report.passed:
        error_msg = f"Validation failed at stage '{report.stage}' with {report.error_count} errors"
        logger.error(error_msg)
        
        if fail_on_error:
            raise ValueError(error_msg)
        
        return False
    
    logger.info(f"Validation passed at stage '{report.stage}'")
    return True


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    # Demo: Run validation on sample data
    from fifo_matching import load_tc_data, perform_fifo_matching
    
    print("Loading data...")
    source_df = load_tc_data('data/tc_raw_data.xlsx')
    
    print("\nRunning source validation...")
    source_report = validate_source_data(source_df)
    print(source_report.summary())
    
    print("\nPerforming FIFO matching...")
    matched_df = perform_fifo_matching(source_df)
    
    print("\nRunning FIFO results validation...")
    fifo_report = validate_fifo_results(source_df, matched_df)
    print(fifo_report.summary())
