"""
=============================================================================
THRIVE CASH PROCESSING DAG - AIRFLOW ORCHESTRATION
=============================================================================

PURPOSE:
    This Airflow DAG orchestrates the complete Thrive Cash FIFO matching 
    pipeline. It automates the monthly process of matching spent/expired 
    transactions to earned transactions for accounting purposes.

BUSINESS CONTEXT:
    Every month-end, the finance team needs to know which Thrive Cash 
    credits were redeemed. This DAG automates what was previously a 
    manual, hours-long process.

PIPELINE FLOW:
    
    download_data → validate_source → perform_fifo_matching → 
    validate_results → build_analytics → send_alerts
    
    1. DOWNLOAD DATA: Fetch latest transaction data from source
    2. VALIDATE SOURCE: Check data quality before processing
    3. FIFO MATCHING: Match spent/expired to earned transactions
    4. VALIDATE RESULTS: Verify matching is correct
    5. BUILD ANALYTICS: Create reporting tables for finance team
    6. SEND ALERTS: Notify team of completion or failures

SCHEDULE:
    Runs daily at 6 AM UTC, but primary use is month-end close.
    Can also be triggered manually for ad-hoc processing.

ERROR HANDLING:
    - Each task has retry logic (3 retries with 5-minute delays)
    - Failures trigger email alerts to the data team
    - Validation failures stop the pipeline to prevent bad data

MONITORING:
    - Task durations are logged for performance tracking
    - Data quality metrics are captured at each stage
    - Alerts sent on anomalies (e.g., unusual transaction volumes)

AUTHOR: Data Applications Team
DATE: 2024
=============================================================================
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import logging

# -----------------------------------------------------------------------------
# AIRFLOW IMPORTS
# -----------------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# -----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# =============================================================================
# DAG CONFIGURATION
# =============================================================================
# These settings control how the DAG behaves in production.

# Default arguments applied to all tasks in the DAG
DEFAULT_ARGS = {
    # -------------------------------------------------------------------------
    # OWNERSHIP & NOTIFICATIONS
    # -------------------------------------------------------------------------
    'owner': 'data-applications-team',  # Team responsible for this DAG
    'email': ['data-team@thrivemarket.com'],  # Alert recipients
    'email_on_failure': True,  # Send email when task fails
    'email_on_retry': False,  # Don't spam on retries
    
    # -------------------------------------------------------------------------
    # RETRY CONFIGURATION
    # -------------------------------------------------------------------------
    # If a task fails, Airflow will retry it automatically.
    # This handles transient issues like network timeouts.
    'retries': 3,  # Number of retry attempts
    'retry_delay': timedelta(minutes=5),  # Wait between retries
    'retry_exponential_backoff': True,  # Increase delay each retry
    'max_retry_delay': timedelta(minutes=30),  # Cap on retry delay
    
    # -------------------------------------------------------------------------
    # EXECUTION SETTINGS
    # -------------------------------------------------------------------------
    'execution_timeout': timedelta(hours=2),  # Max time per task
    'depends_on_past': False,  # Don't wait for previous runs
    'start_date': datetime(2024, 1, 1),  # DAG start date
}

# DAG-level configuration
DAG_CONFIG = {
    'dag_id': 'thrive_cash_fifo_processing',
    'description': 'FIFO matching pipeline for Thrive Cash transactions',
    'schedule_interval': '0 6 * * *',  # Daily at 6 AM UTC
    'catchup': False,  # Don't backfill historical runs
    'max_active_runs': 1,  # Only one run at a time
    'tags': ['finance', 'thrive-cash', 'fifo', 'production'],
}


# =============================================================================
# TASK FUNCTIONS
# =============================================================================
# Each function below represents one step in the pipeline.
# They are designed to be idempotent (safe to re-run).


def download_data(**context) -> Dict[str, Any]:
    """
    TASK 1: Download source data from the data warehouse.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    This task fetches the latest Thrive Cash transaction data from our 
    data warehouse (Snowflake). It's like downloading a fresh spreadsheet
    of all transactions that need to be processed.
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    In production, this would:
    1. Connect to Snowflake using secure credentials
    2. Query the TC_DATA, SALES, and CUSTOMERS tables
    3. Save results to a staging location (S3 or local)
    
    For this assessment, we're reading from a local Excel file.
    
    PARAMETERS:
        **context: Airflow context with execution date, task instance, etc.
    
    RETURNS:
        Dict with file paths and row counts for downstream tasks
    
    XCOM:
        Pushes 'source_file_path' and 'row_count' for downstream tasks
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("TASK: download_data")
    logger.info("=" * 60)
    
    # In production, this would be a Snowflake query
    # For assessment, we read from the Excel file
    source_path = 'data/tc_raw_data.xlsx'
    
    logger.info(f"Reading data from: {source_path}")
    
    # Load all sheets
    tc_data = pd.read_excel(source_path, sheet_name='TC_Data')
    sales = pd.read_excel(source_path, sheet_name='Sales')
    customers = pd.read_excel(source_path, sheet_name='Customers')
    
    # Log summary statistics
    logger.info(f"TC_Data: {len(tc_data)} rows")
    logger.info(f"Sales: {len(sales)} rows")
    logger.info(f"Customers: {len(customers)} rows")
    
    # Store metadata for downstream tasks using XCom
    # XCom is Airflow's way of passing data between tasks
    result = {
        'source_file_path': source_path,
        'tc_data_rows': len(tc_data),
        'sales_rows': len(sales),
        'customers_rows': len(customers),
        'execution_date': str(context['execution_date'])
    }
    
    logger.info(f"Download complete: {result}")
    
    return result


def validate_source(**context) -> Dict[str, Any]:
    """
    TASK 2: Validate source data quality before processing.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    Before we process the data, we check that it's "clean" and valid.
    This is like proofreading a document before publishing - we want to
    catch errors early before they cause bigger problems.
    
    If validation fails, the pipeline STOPS here to prevent bad data
    from being processed.
    
    ==========================================================================
    CHECKS PERFORMED:
    ==========================================================================
    
    1. No missing required fields (transaction ID, type, date, etc.)
    2. Transaction types are valid (earned, spent, expired)
    3. Amounts have correct signs (earned positive, spent/expired negative)
    4. No duplicate transaction IDs
    5. Dates are reasonable (not in the future)
    
    PARAMETERS:
        **context: Airflow context
    
    RETURNS:
        Dict with validation results
    
    RAISES:
        ValueError: If critical validation checks fail
    """
    import pandas as pd
    import sys
    sys.path.insert(0, 'src')
    from data_quality import validate_source_data, validation_gate
    from fifo_matching import load_tc_data
    
    logger.info("=" * 60)
    logger.info("TASK: validate_source")
    logger.info("=" * 60)
    
    # Get file path from previous task
    ti = context['ti']
    download_result = ti.xcom_pull(task_ids='download_data')
    source_path = download_result['source_file_path']
    
    logger.info(f"Validating data from: {source_path}")
    
    # Load and validate
    df = load_tc_data(source_path)
    report = validate_source_data(df)
    
    # Log the validation report
    logger.info(report.summary())
    
    # This will raise an exception if validation fails
    # causing the DAG to stop and alert the team
    validation_gate(report, fail_on_error=True)
    
    return {
        'validation_passed': report.passed,
        'error_count': report.error_count,
        'warning_count': report.warning_count
    }


def perform_fifo_matching(**context) -> Dict[str, Any]:
    """
    TASK 3: Execute the FIFO matching algorithm.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    This is the main business logic task. It takes all the Thrive Cash
    transactions and figures out which "spent" or "expired" transactions
    used up which "earned" transactions.
    
    We use FIFO (First-In, First-Out) rules: the oldest earned credits
    get used first, just like how a grocery store rotates stock.
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    1. Load validated source data
    2. For each customer, sort earned transactions by date
    3. Match spent/expired to oldest available earned
    4. Add REDEEMID column to track the matching
    5. Save results to output file
    
    PARAMETERS:
        **context: Airflow context
    
    RETURNS:
        Dict with output file path and matching statistics
    """
    import sys
    sys.path.insert(0, 'src')
    from fifo_matching import run_fifo_matching_pipeline
    
    logger.info("=" * 60)
    logger.info("TASK: perform_fifo_matching")
    logger.info("=" * 60)
    
    # Get file path from download task
    ti = context['ti']
    download_result = ti.xcom_pull(task_ids='download_data')
    source_path = download_result['source_file_path']
    
    # Define output path with execution date for versioning
    execution_date = context['execution_date'].strftime('%Y%m%d')
    output_path = f'output/tc_data_with_redemptions_{execution_date}.csv'
    
    logger.info(f"Input: {source_path}")
    logger.info(f"Output: {output_path}")
    
    # Run the FIFO matching pipeline
    result_df = run_fifo_matching_pipeline(
        input_path=source_path,
        output_path=output_path
    )
    
    # Calculate statistics
    total_earned = len(result_df[result_df['TCTYPE'] == 'earned'])
    matched_earned = len(result_df[
        (result_df['TCTYPE'] == 'earned') & 
        (result_df['REDEEMID'].notna())
    ])
    
    return {
        'output_path': output_path,
        'total_transactions': len(result_df),
        'total_earned': total_earned,
        'matched_earned': matched_earned,
        'match_rate': f"{(matched_earned/total_earned*100):.1f}%" if total_earned > 0 else "N/A"
    }


def validate_results(**context) -> Dict[str, Any]:
    """
    TASK 4: Validate FIFO matching results.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    After matching, we double-check that everything was done correctly.
    This is like a quality control step in manufacturing - we verify
    the output meets our standards before sending it to the finance team.
    
    ==========================================================================
    CHECKS PERFORMED:
    ==========================================================================
    
    1. REDEEMID values reference valid transactions
    2. Only earned transactions have REDEEMID
    3. Matches follow chronological order
    4. Customer balances reconcile correctly
    
    PARAMETERS:
        **context: Airflow context
    
    RETURNS:
        Dict with validation results
    """
    import pandas as pd
    import sys
    sys.path.insert(0, 'src')
    from data_quality import validate_fifo_results, validation_gate
    from fifo_matching import load_tc_data
    
    logger.info("=" * 60)
    logger.info("TASK: validate_results")
    logger.info("=" * 60)
    
    ti = context['ti']
    
    # Get paths from previous tasks
    download_result = ti.xcom_pull(task_ids='download_data')
    fifo_result = ti.xcom_pull(task_ids='perform_fifo_matching')
    
    source_path = download_result['source_file_path']
    output_path = fifo_result['output_path']
    
    logger.info(f"Validating FIFO results: {output_path}")
    
    # Load original and matched data
    original_df = load_tc_data(source_path)
    matched_df = pd.read_csv(output_path)
    matched_df['CREATEDAT'] = pd.to_datetime(matched_df['CREATEDAT'])
    
    # Run validation
    report = validate_fifo_results(original_df, matched_df)
    
    # Log the report
    logger.info(report.summary())
    
    # Gate check - fail if critical errors
    validation_gate(report, fail_on_error=True)
    
    return {
        'validation_passed': report.passed,
        'error_count': report.error_count,
        'warning_count': report.warning_count
    }


def build_analytics(**context) -> Dict[str, Any]:
    """
    TASK 5: Build analytics tables for the finance team.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    This task creates summary tables that the finance team can use for
    reporting. Instead of looking at raw transaction data, they get
    clean, aggregated views like:
    
    - Customer balances over time
    - Total earned/spent/expired by period
    - Redemption patterns and trends
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    Creates the following analytics outputs:
    1. customer_balances.csv - Running balance for each customer
    2. Summary statistics for the period
    
    In production, these would be written to Snowflake tables.
    
    PARAMETERS:
        **context: Airflow context
    
    RETURNS:
        Dict with analytics output paths
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("TASK: build_analytics")
    logger.info("=" * 60)
    
    ti = context['ti']
    fifo_result = ti.xcom_pull(task_ids='perform_fifo_matching')
    output_path = fifo_result['output_path']
    
    # Load matched data
    df = pd.read_csv(output_path)
    df['CREATEDAT'] = pd.to_datetime(df['CREATEDAT'])
    
    logger.info("Building customer balance analytics...")
    
    # ---------------------------------------------------------------------
    # BUILD CUSTOMER BALANCES OVER TIME
    # ---------------------------------------------------------------------
    # This creates a running total of earned, spent, expired for each customer
    
    balance_records = []
    
    for customer_id in df['CUSTOMERID'].unique():
        customer_df = df[df['CUSTOMERID'] == customer_id].sort_values('CREATEDAT')
        
        cumulative_earned = 0
        cumulative_spent = 0
        cumulative_expired = 0
        
        for _, row in customer_df.iterrows():
            if row['TCTYPE'] == 'earned':
                cumulative_earned += row['AMOUNT']
            elif row['TCTYPE'] == 'spent':
                cumulative_spent += abs(row['AMOUNT'])
            elif row['TCTYPE'] == 'expired':
                cumulative_expired += abs(row['AMOUNT'])
            
            balance_records.append({
                'customer_id': customer_id,
                'transaction_date': row['CREATEDAT'],
                'transaction_id': row['TRANS_ID'],
                'transaction_type': row['TCTYPE'],
                'amount': row['AMOUNT'],
                'cumulative_earned': cumulative_earned,
                'cumulative_spent': cumulative_spent,
                'cumulative_expired': cumulative_expired,
                'current_balance': cumulative_earned - cumulative_spent - cumulative_expired
            })
    
    balance_df = pd.DataFrame(balance_records)
    
    # Save analytics output
    execution_date = context['execution_date'].strftime('%Y%m%d')
    analytics_path = f'output/customer_balances_{execution_date}.csv'
    balance_df.to_csv(analytics_path, index=False)
    
    logger.info(f"Analytics saved to: {analytics_path}")
    logger.info(f"Total records: {len(balance_df)}")
    
    return {
        'analytics_path': analytics_path,
        'total_records': len(balance_df),
        'unique_customers': balance_df['customer_id'].nunique()
    }


def send_alerts(**context) -> Dict[str, Any]:
    """
    TASK 6: Send completion alerts and summary report.
    
    ==========================================================================
    WHAT THIS DOES (for non-technical readers):
    ==========================================================================
    
    When the pipeline finishes (successfully or with errors), this task
    sends notifications to the team. This includes:
    
    - Success: Summary of what was processed
    - Failure: Details about what went wrong
    - Anomalies: Warnings about unusual patterns
    
    ==========================================================================
    TECHNICAL DETAILS:
    ==========================================================================
    
    In production, this would:
    1. Send Slack messages to #data-alerts channel
    2. Send email summary to finance team
    3. Update monitoring dashboard
    4. Log metrics to observability platform
    
    PARAMETERS:
        **context: Airflow context
    
    RETURNS:
        Dict with alert status
    """
    logger.info("=" * 60)
    logger.info("TASK: send_alerts")
    logger.info("=" * 60)
    
    ti = context['ti']
    
    # Gather results from all tasks
    download_result = ti.xcom_pull(task_ids='download_data') or {}
    source_validation = ti.xcom_pull(task_ids='validate_source') or {}
    fifo_result = ti.xcom_pull(task_ids='perform_fifo_matching') or {}
    results_validation = ti.xcom_pull(task_ids='validate_results') or {}
    analytics_result = ti.xcom_pull(task_ids='build_analytics') or {}
    
    # Build summary message
    summary = f"""
    ============================================================
    THRIVE CASH PROCESSING COMPLETE
    ============================================================
    Execution Date: {context['execution_date']}
    
    DATA SUMMARY:
    - Transactions processed: {fifo_result.get('total_transactions', 'N/A')}
    - Earned transactions: {fifo_result.get('total_earned', 'N/A')}
    - Matched transactions: {fifo_result.get('matched_earned', 'N/A')}
    - Match rate: {fifo_result.get('match_rate', 'N/A')}
    
    VALIDATION:
    - Source validation: {'PASSED' if source_validation.get('validation_passed') else 'FAILED'}
    - Results validation: {'PASSED' if results_validation.get('validation_passed') else 'FAILED'}
    
    OUTPUT FILES:
    - FIFO results: {fifo_result.get('output_path', 'N/A')}
    - Analytics: {analytics_result.get('analytics_path', 'N/A')}
    ============================================================
    """
    
    logger.info(summary)
    
    # In production, send to Slack/email here
    # slack_client.send_message(channel='#data-alerts', text=summary)
    # email_client.send(to='finance-team@thrivemarket.com', subject='TC Processing Complete', body=summary)
    
    return {
        'alert_sent': True,
        'summary': summary
    }


def handle_failure(context):
    """
    Callback function for task failures.
    
    WHAT THIS DOES:
        When any task fails, this function is called to send alerts
        and log the failure details for debugging.
    
    In production, this would:
    1. Send PagerDuty alert for critical failures
    2. Post to Slack with error details
    3. Create incident ticket
    """
    task_instance = context['task_instance']
    exception = context.get('exception')
    
    error_message = f"""
    ⚠️ THRIVE CASH PIPELINE FAILURE
    
    Task: {task_instance.task_id}
    DAG: {task_instance.dag_id}
    Execution Date: {context['execution_date']}
    
    Error: {str(exception)}
    
    Please investigate immediately.
    """
    
    logger.error(error_message)
    
    # In production: send PagerDuty alert, Slack message, etc.


# =============================================================================
# DAG DEFINITION
# =============================================================================
# This is where we define the DAG and wire up all the tasks.

with DAG(
    dag_id=DAG_CONFIG['dag_id'],
    description=DAG_CONFIG['description'],
    schedule_interval=DAG_CONFIG['schedule_interval'],
    default_args=DEFAULT_ARGS,
    catchup=DAG_CONFIG['catchup'],
    max_active_runs=DAG_CONFIG['max_active_runs'],
    tags=DAG_CONFIG['tags'],
    on_failure_callback=handle_failure,
) as dag:
    
    # -------------------------------------------------------------------------
    # TASK DEFINITIONS
    # -------------------------------------------------------------------------
    # Each task is a step in the pipeline.
    
    # Start marker (useful for complex DAGs with multiple entry points)
    start = EmptyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )
    
    # Task 1: Download data from source
    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        doc="""
        Downloads Thrive Cash transaction data from the data warehouse.
        In production, this queries Snowflake. For testing, reads from Excel.
        """
    )
    
    # Task 2: Validate source data
    validate_source_task = PythonOperator(
        task_id='validate_source',
        python_callable=validate_source,
        doc="""
        Validates source data quality before processing.
        Checks for nulls, valid types, correct signs, duplicates.
        Pipeline stops here if validation fails.
        """
    )
    
    # Task 3: Perform FIFO matching
    fifo_matching_task = PythonOperator(
        task_id='perform_fifo_matching',
        python_callable=perform_fifo_matching,
        doc="""
        Core business logic: matches spent/expired transactions to
        earned transactions using FIFO (First-In, First-Out) rules.
        """
    )
    
    # Task 4: Validate results
    validate_results_task = PythonOperator(
        task_id='validate_results',
        python_callable=validate_results,
        doc="""
        Validates FIFO matching results for correctness.
        Checks chronological order, balance reconciliation, etc.
        """
    )
    
    # Task 5: Build analytics
    build_analytics_task = PythonOperator(
        task_id='build_analytics',
        python_callable=build_analytics,
        doc="""
        Creates analytics tables for the finance team.
        Includes customer balances over time and summary statistics.
        """
    )
    
    # Task 6: Send alerts
    send_alerts_task = PythonOperator(
        task_id='send_alerts',
        python_callable=send_alerts,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream fails
        doc="""
        Sends completion notifications to the team.
        Runs regardless of upstream success/failure to ensure alerts are sent.
        """
    )
    
    # End marker
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,
        doc='Pipeline end marker'
    )
    
    # -------------------------------------------------------------------------
    # TASK DEPENDENCIES
    # -------------------------------------------------------------------------
    # This defines the order in which tasks run.
    # The >> operator means "runs before"
    
    # Linear pipeline flow:
    # start → download → validate_source → fifo_matching → 
    # validate_results → build_analytics → send_alerts → end
    
    start >> download_data_task >> validate_source_task >> fifo_matching_task
    fifo_matching_task >> validate_results_task >> build_analytics_task
    build_analytics_task >> send_alerts_task >> end


# =============================================================================
# DAG DOCUMENTATION
# =============================================================================
# This docstring appears in the Airflow UI

dag.doc_md = """
# Thrive Cash FIFO Processing Pipeline

## Overview
This DAG processes Thrive Cash transactions and performs FIFO matching
to link spent/expired transactions to earned transactions.

## Schedule
- Runs daily at 6 AM UTC
- Primary use is month-end close processing
- Can be triggered manually for ad-hoc runs

## Tasks
1. **download_data**: Fetch transaction data from source
2. **validate_source**: Check data quality before processing
3. **perform_fifo_matching**: Execute FIFO matching algorithm
4. **validate_results**: Verify matching correctness
5. **build_analytics**: Create reporting tables
6. **send_alerts**: Notify team of completion

## Contacts
- Owner: Data Applications Team
- Slack: #data-applications
- Email: data-team@thrivemarket.com

## Runbook
- [Troubleshooting Guide](https://wiki.thrivemarket.com/tc-pipeline)
- [Data Quality Checks](https://wiki.thrivemarket.com/tc-dq)
"""
