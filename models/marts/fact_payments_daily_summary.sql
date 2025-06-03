{{ config(
    materialized='table'
) }}

/*
Given that the payment table is immutable in the past (it is an assumption), I would have created it as incremental since the past does not change. 
I didn't do it now because I am working with the free tier of dbt Cloud and it does not allow me to.

{{ config(
    materialized='incremental',
    unique_key=['summary_date', 'billing_country', 'product', 'subscription_type'],
    partition_by={
        "field": "summary_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}
*/


with payments as (
    select
        transaction_date,
        billing_country,
        product,
        product_level,
        subscription_type,
        amount_paid_usd,
        mrr_per_transaction_usd,
        discount_amount_usd,
        gross_amount_usd,
        user_id
    from {{ ref('stg_payment_system__payments') }}
    {% if is_incremental() %}
        -- Incremental load: only processes new data since the last run.
        -- We subtract 1 day to ensure no data is missed if the incremental run occurs mid-day
        -- or if there's a slight latency in data ingestion.
        where transaction_date >= (select date_sub(max(summary_date), interval 1 day) from {{ this }})
    {% endif %}
),

daily_summary as (
    select
        transaction_date as summary_date,
        billing_country,
        product,
        product_level,
        subscription_type,
        -- Total sum of amounts paid in USD
        sum(amount_paid_usd) as total_amount_paid_usd,
        -- Total sum of MRR generated
        sum(mrr_per_transaction_usd) as total_mrr_usd,
        -- Total sum of discounts applied
        sum(discount_amount_usd) as total_discount_usd,
        -- Total sum of the gross amount (before discounts)
        sum(gross_amount_usd) as total_gross_amount_usd,
        -- Count of distinct users who made payments
        count(distinct user_id) as distinct_users,
        -- Total count of transactions
        count(*) as total_transactions
    from payments
    group by 1, 2, 3, 4, 5
)

select *
from daily_summary
