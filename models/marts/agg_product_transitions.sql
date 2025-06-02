{{ config(materialized="table") }}

with
    user_product_history as (
        select
            fp.user_id,
            fp.product as current_product,
            fp.product_level as current_product_level,
            fp.transaction_at as transition_at,
            lag(fp.product, 1) over (
                partition by fp.user_id order by fp.transaction_at
            ) as previous_product,
            lag(fp.product_level, 1) over (
                partition by fp.user_id order by fp.transaction_at
            ) as previous_product_level,
            lag(fp.transaction_at, 1) over (
                partition by fp.user_id order by fp.transaction_at
            ) as previous_transaction_at
        from {{ ref("fct_payments") }} fp
    )
select
    uph.user_id,
    uph.previous_product,
    uph.current_product,
    uph.transition_at,
    timestamp_diff(
        uph.transition_at, uph.previous_transaction_at, day
    ) as days_between_transitions,
    case
        when uph.current_product_level > uph.previous_product_level
        then 'Upgrade'
        when uph.current_product_level < uph.previous_product_level
        then 'Downgrade'
    end as transition_type
from user_product_history uph
where uph.previous_product is not null and uph.current_product != uph.previous_product
order by uph.user_id, uph.transition_at
