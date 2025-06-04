/*
Metric: Total Revenue

Definition:
The total gross amount of money collected from all transactions within a given period (e.g., a month). This includes both recurring and non-recurring payments.

Interpretation:
Represents the overall financial intake of the business. While MRR focuses on predictable recurring income, Total Revenue provides a comprehensive view of all cash inflows from sales. It's important to track alongside MRR to understand the full financial picture, especially if there are significant one-off payments or initial large annual payments that are not fully reflected in MRR for the current month.
*/

{{ config( materialized='table' ) }}

with daily_summary as (
    select
        summary_date,
        total_amount_paid_usd,
        total_transactions
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_total_revenue as (
    select
        -- Extract the first day of the month for aggregation
        date_trunc(summary_date, month) as month_start_date,
        -- Sum all actual amounts paid within that month
        sum(total_amount_paid_usd) as total_revenue_usd,
        -- Sum total count of transactions
        sum(total_transactions) as total_transactions
    from daily_summary
    group by 1
    order by 1
)

select *
from monthly_total_revenue
