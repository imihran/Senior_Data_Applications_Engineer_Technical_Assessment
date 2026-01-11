

# Before we start the project please note the following:

Thank you Belinda and Sean for considering me for Senior Data Applications Engineer. If would you like to skip directly to the solutions:

- [Part 1: Airflow DAG Orchestration](#part-1-airflow-dag)
- [Part 2: FIFO Matching Business Logic](#part-2-fifo-matching-logic)
- [Part 3: Data Quality & Testing](#part-3-data-quality--testing)
- [Part 4: Data Modeling & Transformation Framework](#part-4-dbt-models)
- [Part 5: Analytics & Reporting](#part-5-analytics--reporting)

# Thrive Cash FIFO Processing Pipeline

## Overview

This project implements an automated data pipeline for processing Thrive Cash transactions. It solves the business problem of matching "spent" and "expired" transactions to "earned" transactions using FIFO (First-In, First-Out) accounting rules.

Before diving in to the approach and easy of navigation: 

[Link Text](#part-1-airflow-dag)

### What is Thrive Cash?

Thrive Cash is a customer rewards program where:
- Customers **earn** credits through promotions, refunds, referrals, etc.
- Customers **spend** credits on orders (reduces their balance)
- Unused credits **expire** after a certain period

For accounting purposes, we need to track which earned credits were redeemed by which spent/expired transactions.

### The Problem This Solves

**Before:** Manual FIFO matching took hours each month-end close, was error-prone, and didn't scale.

**After:** Automated pipeline runs daily, validates data quality, and produces audit-ready results in minutes.

---

## Project Structure

```
Repo/
├── dags/
│   └── thrive_cash_processing_dag.py    # Airflow DAG orchestration
├── src/
│   ├── fifo_matching.py                 # Core FIFO matching algorithm
│   └── data_quality.py                  # Data validation framework
├── sql/
│   └── analytics_queries.sql            # SQL queries for finance team
├── dbt/
│   ├── dbt_project.yml                  # dbt configuration
│   ├── models/
│   │   ├── staging/                     # Raw data cleaning
│   │   ├── intermediate/                # Business logic
│   │   └── marts/                       # Final reporting tables
│   └── tests/                           # Data quality tests
├── tests/
│   └── test_fifo_matching.py            # Python unit tests
├── data/
│   └── tc_raw_data.xlsx                 # Source data
├── output/                              # Generated output files
├── requirements.txt                     # Python dependencies
└── README.md                            # This file
```

---

## Quick Start

### Prerequisites

- Python 3.9+
- pip (Python package manager)

### Installation

```bash
# Clone or navigate to the project
cd Repo

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run the FIFO Matching

```bash
# Run the core FIFO matching algorithm
python src/fifo_matching.py
```

This will:
1. Load data from `data/tc_raw_data.xlsx`
2. Perform FIFO matching
3. Save results to `output/tc_data_with_redemptions.csv`

### Run Data Quality Validation

```bash
# Run validation checks
python src/data_quality.py
```

### Run Tests

```bash
# Run all unit tests
pytest tests/ -v

# Run with coverage report
pytest tests/ -v --cov=src
```

---

## Component Details

### Part 1: Airflow DAG

**File:** `dags/thrive_cash_processing_dag.py`

The DAG orchestrates the complete pipeline:

```
start → download_data → validate_source → perform_fifo_matching → 
        validate_results → build_analytics → send_alerts → end
```

**Key Features:**
- Retry logic (3 retries with exponential backoff)
- Timeout configurations (2 hours per task)
- Data quality gates (pipeline stops on validation failure)
- Alerting on failures
- XCom for passing data between tasks

**Schedule:** Daily at 6 AM UTC (configurable)

### Part 2: FIFO Matching Logic

**File:** `src/fifo_matching.py`

The core algorithm that matches spent/expired transactions to earned transactions.

**How FIFO Works:**
1. For each customer, sort earned transactions by date (oldest first)
2. For each spent/expired transaction, find the oldest unmatched earned
3. Link them by setting REDEEMID on the earned transaction

**Example:**
```
Customer earns $20 on Jan 1 (ID: 1001)
Customer earns $30 on Jan 15 (ID: 1002)
Customer spends $25 on Feb 1 (ID: 1003)

Result: Transaction 1001 gets REDEEMID = 1003 (oldest matched first)
```

**Output:** CSV file with REDEEMID column added to earned transactions

### Part 3: Data Quality & Testing

**Files:** 
- `src/data_quality.py` - Validation framework
- `tests/test_fifo_matching.py` - Unit tests

**Source Validation Checks:**
- No null values in required fields
- Valid transaction types (earned, spent, expired)
- Correct amount signs (earned positive, spent/expired negative)
- No duplicate transaction IDs
- Reasonable dates (not in future)

**FIFO Results Validation:**
- REDEEMID references valid transactions
- Only earned transactions have REDEEMID
- Chronological consistency (earned before redemption)
- Balance reconciliation per customer

**Test Coverage:**
- Basic FIFO matching scenarios
- Multi-customer independence
- Edge cases (empty data, only earned, etc.)
- Performance tests (1000+ transactions)

### Part 4: dbt Models

**Directory:** `dbt/`

Transformation framework using dbt (data build tool).

**Model Layers:**
1. **Staging** (`models/staging/`) - Clean raw data
   - `stg_tc_data.sql` - Standardized transactions
   - `stg_customers.sql` - Customer dimension

2. **Intermediate** (`models/intermediate/`) - Business logic
   - `int_fifo_matching.sql` - SQL-based FIFO matching

3. **Marts** (`models/marts/`) - Final reporting tables
   - `fct_customer_balance_history.sql` - Balance over time
   - `fct_customer_current_balance.sql` - Current balance snapshot

**dbt Tests:**
- Unique and not-null constraints
- Accepted values for transaction types
- Custom tests for balance validation

### Part 5: Analytics & Reporting

**File:** `sql/analytics_queries.sql`

SQL queries for the finance team:

**A) Customer Balances Over Time:**
- `customer_balance_history` view - Running balance at each transaction
- `customer_current_balance` view - Latest balance per customer

**B) Sample Query - Balance on Specific Date:**
```sql
-- What was the balance for customers 23306353 and 16161481 on 2023-03-21?
SELECT customer_id, balance_on_date
FROM balance_on_date
WHERE customer_id IN (23306353, 16161481)
  AND balance_date = '2023-03-21';
```

**Additional Analytics:**
- Monthly Thrive Cash summary
- FIFO matching audit trail
- Customers with expiring balances
- Redemption rate by reason

---

## Output Files

After running the pipeline, you'll find:

| File | Description |
|------|-------------|
| `output/tc_data_with_redemptions.csv` | Transaction data with REDEEMID column |
| `output/customer_balances_YYYYMMDD.csv` | Customer balance history |

---

## Sample Results

### FIFO Matching Output

| TRANS_ID | TCTYPE | CUSTOMERID | AMOUNT | REDEEMID |
|----------|--------|------------|--------|----------|
| 11315530 | earned | 23306353 | 40.00 | 11320645 |
| 11315638 | earned | 23306353 | 40.00 | 11320645 |
| 11315932 | earned | 23306353 | 30.00 | 11320645 |
| 11330728 | earned | 23306353 | 20.00 | 11562841 |

### Balance Query Results (2023-03-21)

| customer_id | total_earned | total_spent | current_balance |
|-------------|--------------|-------------|-----------------|
| 16161481 | 21.57 | 1.50 | 15.07 |
| 23306353 | 130.00 | 130.00 | 0.00 |

---

## Design Decisions

### Why Python for FIFO Matching?

- Complex business logic is easier to express in Python
- Better debugging and logging capabilities
- Easier to test with unit tests
- Can handle edge cases more gracefully

### Why dbt for Transformations?

- Industry standard for data transformation
- Built-in testing and documentation
- Version control friendly
- Scales well with Snowflake

### Why Airflow for Orchestration?

- Robust retry and error handling
- Visual DAG monitoring
- XCom for task communication
- Industry standard for data pipelines

---

## Scalability Considerations

For millions of transactions:

1. **Database-side processing:** Move FIFO logic to Snowflake stored procedure
2. **Partitioning:** Process by customer_id or date range
3. **Incremental processing:** Only process new transactions
4. **Parallel execution:** Use Airflow's parallelism features

---

## Monitoring & Alerting

The pipeline includes:

- **Logging:** Detailed logs at each step
- **Validation gates:** Pipeline stops on data quality failures
- **Alerts:** Email notifications on failure
- **Metrics:** Row counts, match rates, execution times

---

## Future Enhancements

1. **Real-time processing:** Stream processing for immediate matching
2. **ML anomaly detection:** Flag unusual transaction patterns
3. **Self-service dashboard:** Retool/Streamlit interface for finance
4. **Automated reconciliation:** Compare with source systems

---

## Contact

**Team:** Data Applications  
**Slack:** #data-applications  
**Email:** data-team@thrivemarket.com
