/*
Metric: Churn Rate (User Churn Rate) - Monthly Approximation

Definition:
The percentage of customers lost over a given period (monthly in this case). This model provides an *event-based user churn rate* approximation by identifying users whose last known subscription has ended and no new transaction has occurred.

Interpretation:
A critical metric for SaaS. A high or increasing churn rate indicates customer dissatisfaction, poor retention, or a mismatch between product and market needs. Lower churn is essential for sustainable growth and maximizing customer lifetime value. Monitoring this metric helps identify issues that impact customer loyalty and recurring revenue.
*/

{{ config( materialized='table' ) }}

with user_events as (
    select
        user_id,
        transaction_date,
        is_potential_churn_event_last_transaction
    from {{ ref('dim_user_subscriptions_history') }}
),

monthly_user_status as (
    select
        user_id,
        date_trunc(transaction_date, month) as month_start_date,
        -- Identify if a user was active (had any transaction) in a given month
        true as is_active_in_month,
        -- Identify if this transaction was a potential churn event (last known transaction and subscription ended)
        max(case when is_potential_churn_event_last_transaction then 1 else 0 end) as is_churn_event_in_month
    from user_events
    group by 1, 2
),

lagged_status as (
    select
        month_start_date,
        user_id,
        is_active_in_month,
        is_churn_event_in_month,
        -- Check if the user was active in the previous month
        lag(is_active_in_month, 1, false) over (partition by user_id order by month_start_date) as was_active_previous_month
    from monthly_user_status
),

monthly_churn_calculation as (
    select
        month_start_date,
        -- Count users who were active last month but are a potential churn event this month
        count(distinct case when was_active_previous_month and is_churn_event_in_month = 1 then user_id end) as churned_users_count,
        -- Count users who were active in the previous month (base for churn rate)
        count(distinct case when was_active_previous_month then user_id end) as active_users_previous_month_count
    from lagged_status
    group by 1
    order by 1
),
monthly_new_customers as (
    select
        date_trunc(transaction_date, month) as month_start_date,
        count(distinct user_id) as new_customer_count
    from {{ ref('dim_user_subscriptions_history') }}
    where subscription_event_type = 'New Acquisition'
    group by 1
)

select
    mc.month_start_date,
    mc.churned_users_count,
    mc.active_users_previous_month_count,
    nc.new_customer_count,
    case
        when mc.active_users_previous_month_count > 0
        then cast(mc.churned_users_count as numeric) / mc.active_users_previous_month_count
        else 0
    end as monthly_churn_rate
from monthly_churn_calculation mc
left join monthly_new_customers nc
  on mc.month_start_date = nc.month_start_date
order by mc.month_start_date
