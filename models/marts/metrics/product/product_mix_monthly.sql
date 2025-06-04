/*
Metric: Product Mix - Monthly

Definition:
The distribution of revenue (e.g., MRR) or active users across different product levels (PRO, GURU, BUSINESS) within a given month.

Interpretation:
Helps understand which products are most popular or generate the most revenue. A positive shift in the product mix towards higher-tier products (GURU, BUSINESS) indicates successful upselling and that customers are finding more value in advanced features, which is generally desirable for revenue growth. It's crucial for product strategy and pricing evaluation.
*/

{{ config( materialized='table' ) }}

with daily_summary as (
    select
        summary_date,
        product,
        total_mrr_usd,
        distinct_users
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_product_mix as (
    select
        date_trunc(summary_date, month) as month_start_date,
        product,
        sum(total_mrr_usd) as monthly_mrr_by_product_usd,
        sum(distinct_users) as monthly_distinct_users_by_product
    from daily_summary
    group by 1, 2
    order by 1, 2
)

select *
from monthly_product_mix
