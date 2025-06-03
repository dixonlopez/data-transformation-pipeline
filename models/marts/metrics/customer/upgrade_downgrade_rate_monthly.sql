-- models/marts/metrics/upgrade_downgrade_rate_monthly.sql

/*
Metric: Upgrade/Downgrade Rate - Monthly

Definition:
The percentage of users who change their subscription plan within a given month, specifically differentiating between upgrading to a higher tier and downgrading to a lower one.

Interpretation:
Reflects customer satisfaction and perceived value of different product tiers. High upgrade rates indicate successful product development, effective value proposition, and customer growth within the product ecosystem. High downgrade rates can signal issues with higher-tier features, pricing, or customer fit, potentially leading to revenue loss. Monitoring both helps understand customer journey and product-market fit.
*/

{{ config( materialized='table' ) }}

with user_history as (
    select
        user_id,
        transaction_date,
        subscription_event_type
    from {{ ref('dim_user_subscriptions_history') }}
    where subscription_event_type in ('Upgrade', 'Downgrade', 'New Acquisition', 'Renewal/Retention')
),

monthly_event_counts as (
    select
        date_trunc(transaction_date, month) as month_start_date,
        count(distinct case when subscription_event_type = 'Upgrade' then user_id end) as monthly_upgrades_count,
        count(distinct case when subscription_event_type = 'Downgrade' then user_id end) as monthly_downgrades_count,
        -- Total users who had an event (upgrade, downgrade, new acquisition, renewal/retention) in the month
        -- This serves as the base for the rate calculation.
        count(distinct user_id) as monthly_total_users_with_events
    from user_history
    group by 1
    order by 1
)

select
    month_start_date,
    monthly_upgrades_count,
    monthly_downgrades_count,
    monthly_total_users_with_events,
    -- Calculate upgrade rate, handling division by zero
    case
        when monthly_total_users_with_events > 0
        then cast(monthly_upgrades_count as numeric) / monthly_total_users_with_events
        else 0
    end as monthly_upgrade_rate,
    -- Calculate downgrade rate, handling division by zero
    case
        when monthly_total_users_with_events > 0
        then cast(monthly_downgrades_count as numeric) / monthly_total_users_with_events
        else 0
    end as monthly_downgrade_rate
from monthly_event_counts
