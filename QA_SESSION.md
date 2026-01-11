# Thrive Cash Project - Q&A Session

---

*Questions and answers to help understand the project better.*

---

## Questions

### Q1: The SQL file uses `tc_data` table. Which workflow creates it?

**A:** The `tc_data` table is assumed to already exist in the data warehouse (Snowflake). It's loaded from the source Excel file `data/tc_raw_data.xlsx` by the Airflow DAG's `download_data` task, which reads the `TC_Data` sheet. In production, this would be an existing table populated by upstream systems—the pipeline consumes it, not creates it.

---

### Q2: So the DAG only handles orchestration? The actual logic lives in the SRC folder?

**A:** Correct. The DAG handles orchestration (task order, retries, alerts, scheduling). The actual business logic lives in `src/`:
- `fifo_matching.py` — FIFO algorithm
- `data_quality.py` — validation checks

The DAG imports and calls these modules via `PythonOperator`.

---

### Q3: Can you do a high level of DAG flow?

**A:** 

```
start → download_data → validate_source → perform_fifo_matching → validate_results → build_analytics → send_alerts → end
```

1. **download_data** — Load Excel data (simulates Snowflake query)
2. **validate_source** — Check data quality before processing
3. **perform_fifo_matching** — Run FIFO algorithm, output CSV with REDEEMID
4. **validate_results** — Verify matching is correct
5. **build_analytics** — Create customer balance history tables
6. **send_alerts** — Notify team of success/failure

If any validation fails, the pipeline stops.

---
