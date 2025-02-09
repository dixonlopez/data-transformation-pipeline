{{ config(materialized="table") }}

select * from {{ ref('stg_globepay_pay_acceptance_with_chargebacks') }}