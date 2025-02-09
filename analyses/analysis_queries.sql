/**********************************************************************
 *                                                                    *
 *                           Part 2 answers                             *
 *                                                                    *
 ***********************************************************************/
/*
    In this analysis, I address the 3 questions proposed in the test statement
*/
/*
    Test: What is the acceptance rate over time? Answering this question for the entire dataset
    Result: 0.695524 (66%)
*/
select
    count(case when was_transaction_accepted = true then 1 end)
    * 1.0
    / count(*) as acceptance_rate
from {{ ref("payments") }}
;

/*
    Test: What is the acceptance rate over time? Answering this question with weekly granularity
    Result: For the week of 2018-12-31 the acceptance_rate is 0.731844 (73%), for the week of 2019-01-07 it is 0.62381 (62.4%), and so on
    Extra information for analysts: To change the granularity, simply replace 'week' with the desired granularity
*/
select
    'week' as date_granularity,
    date_trunc('week', transaction_processed_at) as event_date,
    count(case when was_transaction_accepted = true then 1 end)
    * 1.0
    / count(*) as acceptance_rate
from {{ ref("payments") }}
group by 1, 2
order by 2
;

/*
    Test: List the countries where the amount of declined transactions exceeded $25M
    Result: United States (US), United Arab Emirates (AE), Canada (CA), and Mexico (MX)
*/
select iso_country_code, sum(amount_charged_usd)
from {{ ref("payments") }}
where was_transaction_accepted = false
group by 1
having sum(amount_charged_usd) > 25000000
order by sum(amount_charged_usd)
;

/*
    Test: Which transactions are missing chargeback data?
    Result: No transactions are missing chargeback data
    Extra information for analysts: When joining the pay_acceptance and pay_chargeback reports, if was_payment_charged_back is NULL for any payment, it means the transaction was not found in the chargeback table
*/
select payment_key
from {{ ref("payments") }}
where was_payment_charged_back is null
;
