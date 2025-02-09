{{ config(materialized="view") }}

with

    source as (

        select *, to_variant(rates) as rates_variant
        from {{ source("globepay", "globe_pay_acceptance_report") }}
    ),

    renamed as (

        select
            external_ref::char(21) as payment_external_id,
            -- status, -- I do not leave it visible because it is always true, because it is the table of accepted payments.
            source::varchar(20) as source_system,  -- I leave it visible in case there are other sources of payment in the future.
            ref::char(27) as payment_internal_id,
            {{
                dbt_utils.generate_surrogate_key(
                    ["external_ref::char(21)", "source::varchar(20)"],
                )
            }} as payment_key,
            date_time::timestamp as transaction_processed_at,
            case
                when lower(state) = 'accepted'
                then true
                when lower(state) = 'declined'
                then false
                else null
            end as was_transaction_accepted,
            case
                when cvv_provided = true
                then true
                when cvv_provided = false
                then false
                else null
            end as was_card_verification_value_provided,
            amount::decimal(10, 2) as amount_charged_local_currency,  -- local means in the currency of the transaction
            country::char(2) as iso_country_code,
            currency::char(3) as iso_currency_code,
            rates as rates_json,
            (
                case
                    when currency = 'CAD'
                    then try_parse_json(rates):CAD
                    when currency = 'EUR'
                    then try_parse_json(rates):EUR
                    when currency = 'MXN'
                    then try_parse_json(rates):MXN
                    when currency = 'USD'
                    then try_parse_json(rates):USD
                    when currency = 'SGD'
                    then try_parse_json(rates):SGD
                    when currency = 'AUD'
                    then try_parse_json(rates):AUD
                    when currency = 'GBP'
                    then try_parse_json(rates):GBP
                    else null
                end
            )::decimal(14, 12) as exchange_rate,
            amount_charged_local_currency * exchange_rate as amount_charged_usd

        from source

    )

select *
from renamed
