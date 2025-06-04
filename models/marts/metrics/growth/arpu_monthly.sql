/*
Metric: Average Revenue Per User (ARPU) - Monthly

Definition:
The average amount of revenue generated per active user over a specific period (monthly in this case).

Interpretation:
Shows how much value, on average, each active user brings to the business. An increasing ARPU can indicate successful upselling, cross-selling, or the acquisition of higher-value new customers. It's a good metric to assess the monetization efficiency of your user base and the perceived value of your product.
*/

{{ config( materialized='table' ) }}

with daily_summary as (
    select
        summary_date,
        total_amount_paid_usd,
        distinct_users
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_arpu as (
    select
        date_trunc(summary_date, month) as month_start_date,
        sum(total_amount_paid_usd) as total_revenue_in_month,
        sum(distinct_users) as total_distinct_users_in_month
    from daily_summary
    group by 1
    order by 1
)

select
    month_start_date,
    total_revenue_in_month,
    total_distinct_users_in_month,
    -- Calculate ARPU, handling division by zero
    case
        when total_distinct_users_in_month > 0
        then cast(total_revenue_in_month as numeric) / total_distinct_users_in_month
        else 0
    end as average_revenue_per_user_usd
from monthly_arpu
