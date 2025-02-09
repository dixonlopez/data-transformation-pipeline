{{ config(materialized="view") }}

with
    pay_acceptance as (

        select *
        from {{ ref("base_globepay__pay_acceptance_report") }}
        where amount_charged_local_currency >= 0  -- There was only one negative value so I assume it is an error and that's why I filter it out.

    ),

    pay_chargeback as (select * from {{ ref("base_globepay__pay_chargeback_report") }}),

    combined as (

        select
            {{
                dbt_utils.star(
                    from=ref("base_globepay__pay_acceptance_report"),
                    relation_alias="pay_acceptance"
                )
            }},
            pay_chargeback.was_payment_charged_back,
            case
                when pay_chargeback.was_payment_charged_back is not null
                then true
                else false
            end as has_chargedback_information

        from pay_acceptance
        left join pay_chargeback on pay_acceptance.payment_key = pay_chargeback.payment_key

    )
select 
    source_system,
    payment_key,
    transaction_processed_at,
    was_transaction_accepted,
    was_card_verification_value_provided,
    iso_country_code,
    iso_currency_code,
    amount_charged_local_currency,
    exchange_rate,
    amount_charged_usd,
    was_payment_charged_back

from combined