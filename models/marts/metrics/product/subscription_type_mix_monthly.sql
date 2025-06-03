-- models/marts/metrics/subscription_type_mix_monthly.sql

/*
Metric: Subscription Type Mix - Monthly

Definition:
The proportion of monthly versus annual subscriptions (based on MRR or distinct users) within a given month.

Interpretation:
Annual subscriptions often indicate higher customer commitment and provide more upfront cash flow, which can be beneficial for financial planning. Monitoring this mix helps assess customer loyalty, payment preferences, and the effectiveness of incentives for annual plans. A higher proportion of annual plans can contribute to more stable recurring revenue.
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
        subscription_type,
        total_mrr_usd,
        distinct_users
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_subscription_mix as (
    select
        date_trunc(summary_date, month) as month_start_date,
        subscription_type,
        sum(total_mrr_usd) as monthly_mrr_by_subscription_type_usd,
        sum(distinct_users) as monthly_distinct_users_by_subscription_type
    from daily_summary
    group by 1, 2
    order by 1, 2
)

select *
from monthly_subscription_mix
