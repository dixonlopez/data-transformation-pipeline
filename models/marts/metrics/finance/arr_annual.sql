-- models/marts/metrics/arr_annual.sql

/*
Metric: Annual Recurring Revenue (ARR)

Definition:
The annualized value of a company's recurring revenue streams. It's typically calculated as MRR multiplied by 12.

Interpretation:
Provides a longer-term view of the business's recurring revenue potential. Useful for forecasting and evaluating the overall scale of the subscription business. It helps stakeholders understand the company's annual financial run rate based on its current recurring revenue. While MRR is good for short-term operational insights, ARR is better for long-term strategic planning and valuation.
*/

{{ config( materialized='table' ) }}

with mrr_data as (
    select
        month_start_date,
        monthly_recurring_revenue_usd
    from {{ ref('mrr_monthly') }}
)

select
    month_start_date,
    -- Calculate ARR by multiplying monthly MRR by 12
    monthly_recurring_revenue_usd * 12 as annual_recurring_revenue_usd
from mrr_data
