

/*
    Prueba: verificar si todos los importes son positivos
    Resultado: un importe no es positivo, se asume que es un error porque por el concepto de pago no debería ser negativo

*/
select amount_charged_local_currency >= 0 as is_positive, count(*)
from {{ ref("base_globepay__pay_acceptance_report") }}
group by 1;

/*
    Prueba: verificar que todas las filas hayan quedado con un exchange_rate una vez se extrajo del json
    Resultado: todas las filas tienen exchange_rate

*/
select count(*)
from {{ ref("base_globepay__pay_acceptance_report") }}
where exchange_rate is null;

/*
    Prueba: verificación de que el external_id es único para ver si se puede utilizar en la llave primaria
    Resultado: el external_id es único 
*/
select payment_external_id, count(*)
from {{ ref("base_globepay__pay_acceptance_report") }}
group by 1
having count(*) > 1;

/*
    Prueba: verificación de que el external_id es único para ver si se puede utilizar en la llave primaria
    Resultado: el external_id es único 
*/
select payment_external_id, count(*)
from {{ ref("base_globepay__pay_chargeback_report") }}
group by 1
having count(*) > 1;

/*
    Prueba: verificación de que en el acceptance report y chargeback report todas las transacciones que están en una fuente están tambien en la otra para saber el tipo de join que se podria requerir
    Resultado: todos los pagos de una fuente están en la otra también
*/

with
    acceptance_report as (
        select payment_external_id
        from {{ ref("base_globepay__pay_acceptance_report") }}
    ),
    chargeback_report as (
        select payment_external_id
        from {{ ref("base_globepay__pay_chargeback_report") }}
    )

select
    -- Pagos solo en la tabla de aceptación
    sum(
        case
            when acceptance_report.payment_external_id is not null and chargeback_report.payment_external_id is null
            then 1
            else 0
        end
    ) as acceptance_only_count,

    -- Pagos solo en la tabla de chargebacks
    sum(
        case
            when chargeback_report.payment_external_id is not null and acceptance_report.payment_external_id is null
            then 1
            else 0
        end
    ) as chargeback_only_count,

    -- Pagos presentes en ambas tablas
    sum(
        case
            when acceptance_report.payment_external_id is not null and chargeback_report.payment_external_id is not null
            then 1
            else 0
        end
    ) as both_sources_count

from acceptance_report 
full outer join chargeback_report on acceptance_report.payment_external_id = chargeback_report.payment_external_id;

-- answering questions de la parte 2:


/*
    Prueba: What is the acceptance rate over time? Voy a responder a esta pregunta para todo el dataset
    Resultado: 0.695524 (66%)
*/

select 
    count(case when was_transaction_accepted = true then 1 end) * 1.0 / count(*) as acceptance_rate
    -- count(case when was_transaction_accepted = true then 1 end) * 1.0  as numerator, -- added for testing
    -- count(*) as denominator -- added for testing
from {{ ref('payments') }};



/*
    Prueba: What is the acceptance rate over time? Voy a responder a esta pregunta a con granularidad semanal
    Resultado: para la semana del 2018-12-31 el acceptance_rate es de 0.731844 (73%) , para la semana del 2019-01-07 es de 0.62381 (62,4%) y asi sucesivamente
    Informacion extra para analista: si se requiere cambiar la granularidad, bastará con modificar la palabra 'week' por la granularidad deseada
*/

select 
    'week' as date_granularity,
    date_trunc('week', transaction_processed_at) as event_date,
    count(case when was_transaction_accepted = true then 1 end) * 1.0 / count(*) as acceptance_rate
    -- count(case when was_transaction_accepted = true then 1 end) * 1.0  as numerator, -- added for testing
    -- count(*) as denominator -- added for testing
from {{ ref('payments') }}
group by 1,2
order by 2;

/*
    Prueba:  List the countries where the amount of declined transactions went over $25M
    Resultado: AE (United Arab Emirates) , US (United states), CA (Canadá)
*/

select
    iso_country_code
from {{ ref('payments') }}
where was_transaction_accepted = false
group by 1
having sum(amount_charged_local_currency) > 25000000;


/*
    Prueba:  Which transactions are missing chargeback data?
    Resultado: En ninguna transaccion faltan los datos del chargeback
    Informacion extra para analista: al momento de unir los reportes de pay_acceptance y pay_charge_back si was_payment_charged_back estuviese null para algún pago significaría que no fue posible unir chargeback porque la transacción no existía en esa tabla
*/

select payment_key
from {{ ref('payments') }}
where was_payment_charged_back is null;





