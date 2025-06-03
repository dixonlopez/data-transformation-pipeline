-- models/marts/metrics/mrr_monthly.sql

/*
Metric: Monthly Recurring Revenue (MRR)

Definition:
The predictable recurring revenue a business expects to receive every month from its active subscriptions.

Interpretation:
A key indicator of business health and growth. Consistent MRR growth suggests a strong customer base and successful acquisition/retention efforts. Declining MRR is a major red flag, indicating potential issues with customer churn or new customer acquisition. It's a fundamental metric for understanding the ongoing financial performance of a subscription-based business.
*/

{{ config(
    materialized='table',
    partition_by={
        "field": "month_start_date",
        "data_type": "date",
        "granularity": "month"
    }
) }}

with daily_summary as (
    select
        summary_date,
        total_mrr_usd
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_mrr as (
    select
        -- Extract the first day of the month for aggregation
        date_trunc(summary_date, month) as month_start_date,
        -- Sum all MRR generated within that month
        sum(total_mrr_usd) as monthly_recurring_revenue_usd
    from daily_summary
    group by 1
)

select *
from monthly_mrr
