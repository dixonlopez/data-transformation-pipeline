{{ config(materialized="view") }}

with 

source as (

    select * from {{ source('globepay', 'globe_pay_chargeback_report') }}

),

renamed as (

    select
        external_ref::char(21) as payment_external_id,
        -- status, -- I do not leave it visible because it is always true
        source::varchar(20) as source_system,
        {{
            dbt_utils.generate_surrogate_key(
                ["external_ref::char(21)", "source::varchar(20)"],
            )
        }} as payment_key,        
        chargeback::boolean as was_payment_charged_back

    from source

)

select * from renamed
