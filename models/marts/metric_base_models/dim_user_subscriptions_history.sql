{{ config(materialized='table') }}

with user_payments as (
    select
        user_id,
        transaction_date,
        transaction_at,
        product,
        product_level,
        amount_paid_usd,
        period,
        mrr_per_transaction_usd,
        -- Identifies the user's first transaction
        min(transaction_at) over (partition by user_id) as first_transaction_at,
        min(transaction_date) over (partition by user_id) as first_transaction_date,
        -- Uses window functions to get the previous/next product and level
        lag(product_level) over (partition by user_id order by transaction_at) as previous_product_level,
        lag(product) over (partition by user_id order by transaction_at) as previous_product,
        lead(transaction_at) over (partition by user_id order by transaction_at) as next_transaction_at,
        lead(product_level) over (partition by user_id order by transaction_at) as next_product_level,
        lead(product) over (partition by user_id order by transaction_at) as next_product,
        -- Calculates the estimated subscription end date
        cast(DATETIME_ADD(cast(transaction_at as datetime), interval period month) as timestamp) as estimated_subscription_end_at

    from {{ ref('stg_payment_system__payments') }}
),

user_subscription_events as (
    select
        user_id,
        transaction_date,
        transaction_at,
        product,
        product_level,
        amount_paid_usd,
        period,
        mrr_per_transaction_usd,
        first_transaction_date,
        estimated_subscription_end_at,
        previous_product,
        previous_product_level,
        next_product,
        next_product_level,
        -- Classifies the subscription event type
        case
            when transaction_at = first_transaction_at then 'New Acquisition'
            when product_level > previous_product_level then 'Upgrade'
            when product_level < previous_product_level then 'Downgrade'
            when product_level = previous_product_level then 'Renewal/Retention'
            else 'Other Event' -- For any unforeseen cases
        end as subscription_event_type,
        -- Identifies if this is the last transaction for a user and their subscription has ended.
        -- This is an approximation for churn in the absence of explicit cancellation data.
        case
            when next_transaction_at is null and current_timestamp() > estimated_subscription_end_at
            then true
            else false
        end as is_potential_churn_event_last_transaction
    from user_payments
)

select *
from user_subscription_events
order by user_id, transaction_at
