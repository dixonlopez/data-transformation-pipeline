{{ config(materialized="table") }}

select
    cast(format_date('%Y-%m-01', transaction_date) as date) as reporting_month,
    product,
    billing_country,
    sum(mrr_per_transaction_usd) as total_mrr_usd
from {{ ref("fct_payments") }}
group by 1, 2, 3
order by 1, 2, 3
