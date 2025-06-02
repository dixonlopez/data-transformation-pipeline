{{ config(materialized="table") }}

with

    source as (select * from {{ source("payments", "synthetical_payments") }}),

    renamed as (

        select
            to_base64(
                sha256(concat(userid, billingcountry, transactiontime))
            ) as payment_uid,
            cast(userid as string) as user_id,
            billingcountry as billing_country,
            transactiontime as transaction_time,
            timestamp_seconds(transactiontime) as transaction_at,
            cast(date(timestamp_seconds(transactiontime)) as date) as transaction_date,
            product,
            case
                when product = 'PRO'
                then 1
                when product = 'GURU'
                then 2
                when product = 'BUSINESS'
                then 3
            end as product_level,
            price as price_usd,
            amount as amount_paid_usd,
            cast(period as int64) as period,
            cast((price * period) as numeric) as gross_amount_usd,
            case
                when (price * period) - amount > 0
                then cast((price * period) - amount as numeric)
                else 0
            end as discount_amount_usd,
            case
                when (price * period) - amount < 0
                then abs(cast((price * period) - amount as numeric))
                else 0
            end as extra_charge_amount_usd,
            case
                when period = 12
                then 'Annual'
                when period = 1
                then 'Monthly'
                else 'Other'
            end as subscription_type,
            cast(amount / period as numeric) as mrr_per_transaction_usd

        from source

    )

select *
from renamed
