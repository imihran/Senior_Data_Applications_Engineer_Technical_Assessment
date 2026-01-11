# Thrive Cash Assessment - Q&A Preparation Guide

*Answers written in plain language for interview discussions*

---

## FIFO Algorithm Questions

### Q: Walk me through how your FIFO matching handles partial amounts - what happens when a customer spends $25 but their oldest earned credit is only $20?

**A:** Right now, my implementation matches the entire $20 earned transaction to that $25 spent transaction. The algorithm then moves to the next oldest earned credit to cover the remaining $5.

Think of it like paying with gift cards at a store. If you have a $20 gift card and a $30 gift card, and you buy something for $25, the cashier uses up the $20 card completely, then takes $5 from the $30 card.

In my current code, both earned transactions would get the same REDEEMID (the spent transaction ID), showing they were both used for that purchase. The limitation is I'm not tracking that only $5 of the second credit was used - that's something I'd improve for production.

---

### Q: How would you modify it to track partial redemptions?

**A:** I'd add a new column called something like `AMOUNT_REDEEMED` to track how much of each earned credit was actually used.

Instead of just marking "this earned credit was used by this spent transaction," I'd track "this earned credit had $15 of its $20 used by this spent transaction."

This would require changing from a simple one-to-one matching to a more detailed ledger approach - similar to how a bank tracks partial payments against a loan balance.

---

### Q: What's the time complexity? How would it perform with 10 million transactions?

**A:** Honestly, my current Python approach would struggle at that scale. It processes each customer one at a time, and for each customer, it loops through their transactions. With 10 million transactions, we'd likely hit memory limits and it could take hours.

For that scale, I'd move the logic into Snowflake itself - let the database do the heavy lifting. Snowflake is built to handle billions of rows efficiently. The same logic written as SQL window functions would run in minutes instead of hours, and we wouldn't need to pull all that data into Python's memory.

---

### Q: What happens if two transactions have the exact same timestamp?

**A:** This is a real edge case I thought about. When two transactions happen at the exact same moment, I use the transaction ID as a tiebreaker. Since IDs are assigned sequentially, the lower ID is treated as "first."

It's not perfect - in reality, the transactions might have happened in a different order - but it gives us consistent, reproducible results. Every time we run the pipeline, we get the same answer, which is important for auditing.

---

## Scalability Questions

### Q: You mention moving to Snowflake stored procedures. Can you sketch out what that would look like?

**A:** The core idea is the same, just written in SQL instead of Python.

I'd use window functions to rank each customer's earned transactions by date (oldest = rank 1), then rank their spent/expired transactions the same way. Then I join them together - rank 1 earned matches to rank 1 spent, rank 2 to rank 2, and so on.

The beauty is Snowflake handles all the heavy lifting - partitioning by customer, sorting by date, joining millions of rows. It's what databases are designed to do.

---

### Q: How would you implement incremental processing?

**A:** Instead of reprocessing all transactions every day, I'd only process what's new.

The approach would be:
1. Keep track of the last time we ran (like a "high water mark")
2. Only pull transactions created after that timestamp
3. For customers with new transactions, recalculate just their matches
4. Merge the new results with the existing data

This could reduce processing time by 90% or more on a typical day, since most customers don't have new transactions.

---

### Q: How would you handle late-arriving data?

**A:** Late data is tricky because we might have already "closed the books" on that period.

My approach would be:
1. Flag the late transaction when it arrives
2. Identify which customer it affects
3. Reprocess just that customer's entire history (it's usually small per customer)
4. Log that we made a correction for audit purposes

The key is making the pipeline "idempotent" - meaning we can safely rerun it and get the same correct answer, even if data arrives out of order.

---

## Data Quality Questions

### Q: Your validation catches negative balances as a WARNING, not ERROR. Why?

**A:** In a perfect world, negative balances should never happen - you can't spend money you don't have.

But in reality, there are edge cases. Maybe a refund was processed before the original purchase showed up in our data. Maybe there's a timing issue between systems. Maybe there's a legitimate business reason I don't know about.

By making it a WARNING instead of ERROR, the pipeline continues running but flags the issue for a human to review. If I made it an ERROR, we'd block the entire month-end close over something that might be a known exception.

That said, if we consistently see negative balances, that's a sign something is wrong upstream and needs investigation.

---

### Q: What's your strategy for quarantining bad records vs. failing the entire pipeline?

**A:** It depends on how bad the problem is.

For critical issues - like the source file is completely empty or corrupted - we fail fast. No point continuing if the foundation is broken.

For isolated issues - like 3 out of 10,000 transactions have weird data - I'd quarantine those records, process the rest, and generate a report of what was skipped. Finance can still get their 99.97% of results on time, and we can investigate the exceptions separately.

The key is visibility. Whatever we do, we log it and alert on it. No silent failures.

---

### Q: How would you detect data drift - like suddenly 50% more expired transactions than usual?

**A:** I'd set up monitoring that tracks patterns over time:

- Average number of transactions per day/week/month
- Ratio of earned vs spent vs expired
- Average transaction amounts
- Number of unique customers

If any of these metrics suddenly jump outside their normal range - say, more than 2 standard deviations from the 30-day average - we'd trigger an alert.

It might be legitimate (big promotion, holiday season) or it might indicate a problem (duplicate data, system error). Either way, a human should look at it before we publish the results.

---

## Architecture Questions

### Q: Why did you choose TriggerRule.ALL_DONE for the alerts task?

**A:** I want the alerts task to run no matter what - whether the pipeline succeeded or failed.

If everything works, we send a "success" summary to the team. If something breaks, we send a "failure" alert so people know to investigate.

Without ALL_DONE, if an upstream task fails, Airflow would skip the alerts task entirely. That means failures would go unnoticed until someone manually checks the dashboard. Not good for a critical finance process.

---

### Q: Your DAG has max_active_runs=1. What problems could arise with concurrent runs?

**A:** Imagine two runs happening at the same time, both trying to write to the same output file. One might overwrite the other's results, or we'd get corrupted data.

Even worse, if both runs are processing the same transactions, we might double-count things or create duplicate REDEEMID assignments.

By limiting to one run at a time, we avoid these race conditions. The tradeoff is if a run takes too long, the next scheduled run has to wait. But for a daily pipeline that takes minutes, that's rarely a problem.

---

### Q: You have FIFO logic in both Python and SQL. Which would you use in production?

**A:** For production at scale, I'd use SQL (the dbt model).

Python is great for development and testing - it's easier to debug, write unit tests, and handle complex edge cases. But it doesn't scale well because all the data has to fit in memory.

SQL runs directly in Snowflake, which is built to handle massive datasets. The database optimizer figures out the most efficient way to execute the query. Plus, it's easier to audit - anyone can read the SQL and understand what's happening.

I'd keep the Python version for local testing and as a reference implementation, but production would run the SQL version.

---

### Q: What edge cases does your SQL FIFO implementation miss compared to Python?

**A:** The SQL version uses a simplified rank-based matching - earned rank 1 matches to spent rank 1, rank 2 to rank 2, etc.

This works when amounts align nicely, but it doesn't handle partial matches well. If a customer earns $10, then $20, then spends $25, the Python version correctly matches both earned transactions to that one spent. The SQL version would only match the first earned to the first spent.

For a production SQL implementation, I'd need to use a more sophisticated approach - probably a recursive CTE or a stored procedure that can track running balances.

---

## Business & Production Questions

### Q: Finance needs results by 5 PM. Your pipeline fails at 4 PM. What's your runbook?

**A:** First, don't panic. We have options.

1. **Check the error** - Is it a transient issue (network timeout) or a real problem (bad data)? If transient, just restart the pipeline.

2. **Use the last successful run** - If today's data isn't critical, we might be able to use yesterday's results with a note that today's transactions aren't included.

3. **Manual intervention** - If it's a data issue affecting a small number of records, we might quarantine those and rerun.

4. **Communicate early** - Let finance know there's an issue and give them an ETA. Surprises at 4:59 PM are worse than a heads-up at 4:15 PM.

The key is having these options documented before we need them. During a crisis is not the time to figure out the plan.

---

### Q: How do you handle rule changes mid-month - like "promotional credits should expire first"?

**A:** This is a business decision more than a technical one.

First question: Does the new rule apply retroactively, or only going forward? If retroactive, we need to reprocess historical data, which could change previously reported numbers. That's a big deal for accounting.

Technically, the change is straightforward - I'd add a priority field to the sorting logic. Instead of just sorting by date, we'd sort by priority first, then date.

But the real work is coordinating with finance and accounting to understand the implications, document the change, and ensure everyone agrees on how to handle the transition period.

---

### Q: An auditor asks to prove customer X's balance of $15.07 on March 21st is correct. How do you trace that?

**A:** I can walk through it step by step using the balance history table.

1. Pull all transactions for that customer up to March 21st
2. Show each transaction: date, type (earned/spent/expired), amount
3. Show the running balance after each transaction
4. The final row shows the balance as of their last transaction before March 21st

It's like a bank statement - every deposit and withdrawal is listed, and you can see exactly how we got to that final number.

The REDEEMID column adds another layer - for any earned credit that was used, I can show exactly which purchase it was applied to.

---

### Q: How would you implement GDPR data retention - deleting customer data after 7 years?

**A:** This requires careful planning because we can't just delete rows - that would break our audit trail and balance calculations.

My approach would be:
1. **Anonymize rather than delete** - Replace customer IDs with a hash or generic placeholder after 7 years
2. **Keep aggregates** - We can keep summary statistics without personal data
3. **Document the policy** - Clear rules about what gets deleted when
4. **Automate it** - A scheduled job that identifies and processes old records

The tricky part is transactions that span the boundary - like an earned credit from 7 years ago that was spent 6 years ago. We'd need clear rules about how to handle those cases.

---

## Monitoring & Operations Questions

### Q: What metrics would you track to know if this pipeline is healthy?

**A:** I'd watch several things:

**Performance metrics:**
- How long did each task take?
- How many rows were processed?
- Did we hit any memory or timeout limits?

**Data quality metrics:**
- How many validation warnings/errors?
- What percentage of earned transactions got matched?
- Any customers with unusual patterns?

**Business metrics:**
- Total Thrive Cash liability (what we owe customers)
- Net change from yesterday
- Number of active customers

If any of these suddenly change dramatically, something might be wrong - or something interesting happened in the business that we should know about.

---

### Q: What would trigger an alert?

**A:** Different severity levels:

**Page someone immediately:**
- Pipeline fails completely
- Critical validation errors (data corruption)
- Results not delivered by deadline

**Send a Slack message:**
- Pipeline takes 2x longer than usual
- Warning-level validation issues
- Unusual data patterns (potential drift)

**Log for review:**
- Minor anomalies
- Performance slightly degraded
- Edge cases that were handled automatically

The goal is to catch real problems without crying wolf. Too many false alarms and people start ignoring alerts.

---

## Reflection Questions

### Q: What would you do differently if you had more time?

**A:** A few things:

1. **Better partial matching** - Track exactly how much of each earned credit was used, not just that it was used

2. **Real-time processing** - Instead of daily batches, process transactions as they happen using streaming

3. **Self-service dashboard** - Let finance query balances themselves instead of waiting for reports

4. **Anomaly detection** - Use ML to automatically flag unusual patterns instead of manual threshold alerts

5. **More comprehensive testing** - Property-based tests that generate random scenarios, not just the cases I thought of

---

### Q: What's the biggest risk in this pipeline?

**A:** Honestly, it's the partial amount matching limitation.

Right now, if the amounts don't align perfectly, we might not match things correctly. In most cases it works fine, but edge cases could slip through.

For a financial system, "mostly correct" isn't good enough. Before going to production, I'd want to either fix the partial matching logic or add validation that catches when it might have gone wrong.

The second risk is scale. The Python implementation works great for thousands of transactions but would struggle with millions. We'd need to migrate to SQL before the business grows to that point.

---

### Q: How would you onboard a new team member to this codebase?

**A:** I'd start with the big picture before diving into code:

1. **Business context first** - Explain what Thrive Cash is, why FIFO matters, who uses the results

2. **Walk through the pipeline** - Show the DAG, explain each step at a high level

3. **Pair on a small change** - Nothing teaches like doing. Maybe add a new validation check together

4. **Point to the docs** - The README, this Q&A doc, the code comments

5. **Introduce them to stakeholders** - Meet the finance team, understand their needs firsthand

The code is well-documented, but context is what makes someone truly effective. Understanding *why* we built it this way matters as much as *how* it works.

---

## Quick Reference: Key Numbers

| Metric | Current State | Production Target |
|--------|---------------|-------------------|
| Processing time | ~5 minutes | <10 min at 10x scale |
| Test coverage | ~85% | 90%+ |
| Match accuracy | 100% (validated) | 100% maintained |
| Max transactions tested | 1,000 | 10M+ |

---

*Last updated: January 2026*
*Prepared for: Senior Data Applications Engineer Assessment Review*
