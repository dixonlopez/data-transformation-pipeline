-- models/marts/metrics/revenue_by_country_monthly.sql

/*
Metric: Revenue by Country - Monthly

Definition:
The total revenue (e.g., total amount paid or MRR) generated from customers in different billing countries within a given month.

Interpretation:
Identifies top-performing geographical markets and areas for potential expansion or focused marketing efforts. Helps in understanding regional market dynamics, identifying market fit, and optimizing localized strategies. It can highlight opportunities or challenges in specific regions.
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
        billing_country,
        total_amount_paid_usd,
        total_mrr_usd
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_revenue_by_country as (
    select
        date_trunc(summary_date, month) as month_start_date,
        billing_country,
        sum(total_amount_paid_usd) as total_revenue_usd,
        sum(total_mrr_usd) as total_mrr_usd
    from daily_summary
    group by 1, 2
    order by 1, 2
)

select *
from monthly_revenue_by_country
