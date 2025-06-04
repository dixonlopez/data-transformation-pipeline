/*
Metric: Customer Lifetime Value (CLV) - Historical

Definition:
The total revenue a business has received from a single customer account throughout their entire relationship with the company, based on historical transactions.

Interpretation:
A higher CLV indicates that customers are more valuable over time, suggesting effective retention strategies and product satisfaction. It helps in understanding the long-term profitability of individual customer segments and informs decisions on customer acquisition cost (CAC). This historical CLV serves as a foundational component for more advanced predictive CLV models.
*/

{{ config(materialized='table') }}

with user_payments as (
    select
        user_id,
        amount_paid_usd
    from {{ ref('dim_user_subscriptions_history') }} -- Using dim_user_subscriptions_history for user_id granularity
),

user_clv as (
    select
        user_id,
        -- Sum all amounts paid by each unique user to get their historical CLV
        sum(amount_paid_usd) as historical_clv_usd
    from user_payments
    group by 1
    order by 1
)

select *
from user_clv
