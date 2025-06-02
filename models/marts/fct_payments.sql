{{ config(materialized="table") }}

select
    payment_uid,
    user_id,
    billing_country,
    transaction_at,
    transaction_date,
    product,
    product_level,
    price_usd,
    amount_paid_usd,
    gross_amount_usd,
    discount_amount_usd,
    extra_charge_amount_usd,
    period,
    subscription_type,
    mrr_per_transaction_usd
from {{ ref("stg_payment_system__payments") }}
