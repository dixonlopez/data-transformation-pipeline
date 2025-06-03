-- models/marts/metrics/discount_rate_monthly.sql

/*
Metric: Discount Rate - Monthly

Definition:
The percentage of total potential revenue (gross amount) that was given away as discounts within a given month.

Interpretation:
Helps evaluate the effectiveness of pricing strategies and promotional offers. A high or increasing discount rate might indicate over-reliance on discounts to acquire or retain customers, potentially signaling a perceived lower value of the product or unsustainable pricing. It's crucial to balance acquisition/retention with profitability.
*/

{{ config( materialized='table' ) }}

with daily_summary as (
    select
        summary_date,
        total_discount_usd,
        total_gross_amount_usd
    from {{ ref('fact_payments_daily_summary') }}
),

monthly_discount_summary as (
    select
        date_trunc(summary_date, month) as month_start_date,
        sum(total_discount_usd) as total_monthly_discount_usd,
        sum(total_gross_amount_usd) as total_monthly_gross_amount_usd
    from daily_summary
    group by 1
    order by 1
)

select
    month_start_date,
    total_monthly_discount_usd,
    total_monthly_gross_amount_usd,
    -- Calculate discount rate, handling division by zero
    case
        when total_monthly_gross_amount_usd > 0
        then cast(total_monthly_discount_usd as numeric) / total_monthly_gross_amount_usd
        else 0
    end as monthly_discount_rate
from monthly_discount_summary
