{{ config(materialized="table") }}

with
    user_latest_product as (
        select user_id, product as current_product
        from {{ ref("fct_payments") }}
        qualify
            row_number() over (partition by user_id order by transaction_at desc) = 1
    )
select
    payments.user_id,
    min(payments.transaction_date) as first_transaction_date,
    max(payments.transaction_date) as last_transaction_date,
    sum(payments.amount_paid_usd) as total_lifetime_spend_usd,
    count(payments.payment_uid) as number_of_transactions,
    any_value(user_latest_product.current_product) as current_product
from {{ ref("fct_payments") }} as payments
left join user_latest_product on payments.user_id = user_latest_product.user_id
group by payments.user_id
