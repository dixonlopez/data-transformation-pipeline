-- models/marts/metrics/new_customer_acquisition_monthly.sql

/*
Metric: New Customer Acquisition - Monthly

Definition:
The number of unique users who made their very first purchase (i.e., had their first transaction recorded) within a given month.

Interpretation:
A direct measure of the effectiveness of marketing and sales efforts in attracting new customers. Consistent new customer acquisition is vital for business expansion and offsetting customer churn. A healthy trend in this metric indicates that the top of your sales funnel is performing well.
*/

{{ config( materialized='table' ) }}

with user_history as (
    select
        user_id,
        transaction_date,
        subscription_event_type
    from {{ ref('dim_user_subscriptions_history') }}
    where subscription_event_type = 'New Acquisition' -- Filter for new acquisition events only
),

monthly_new_customers as (
    select
        -- Group by the month of the first transaction
        date_trunc(transaction_date, month) as month_start_date,
        -- Count distinct users who had their first transaction in this month
        count(distinct user_id) as new_customer_count
    from user_history
    group by 1
    order by 1
)

select *
from monthly_new_customers
