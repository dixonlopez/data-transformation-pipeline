/**********************************************************************
 *                                                                    *
 *                      Preliminary Analysis                          *
 *                 Overview of Key Table Characteristics             *
 *                                                                    *
 ***********************************************************************/

/*
    This analysis provides a high-level overview of the main features 
    of the tables being analyzed, including data types, sizes, and 
    potential areas for further investigation.
*/

----------------------------------------------------------------------------------------------------

/*

    I performed an analysis of the pay_acceptance data, examining the characteristics of each column. 
    The base model was iteratively refined based on these column-by-column findings

*/

{{ generate_model_data("base_globepay__pay_acceptance_report") }}

/*

    I performed an analysis of the pay_acceptance data, examining the characteristics of each column. 
    The base model was iteratively refined based on these column-by-column findings

*/

{{ generate_model_data("base_globepay__pay_chargeback_report") }}


/*
    Test: Verify if all amounts are positive
    Result: One amount is not positive, assumed to be an error because a payment should not be negative
*/
SELECT amount_charged_local_currency >= 0 AS is_positive, COUNT(*)
FROM {{ ref("base_globepay__pay_acceptance_report") }}
GROUP BY 1;

/*
    Test: Verify that all rows have an exchange_rate after extracting it from JSON
    Result: All rows have exchange_rate
*/
SELECT COUNT(*)
FROM {{ ref("base_globepay__pay_acceptance_report") }}
WHERE exchange_rate IS NULL;

/*
    Test: Verify that external_id is unique to determine if it can be used as the primary key
    Result: external_id is unique
*/
SELECT payment_external_id, COUNT(*)
FROM {{ ref("base_globepay__pay_acceptance_report") }}
GROUP BY 1
HAVING COUNT(*) > 1;

/*
    Test: Verify that external_id is unique to determine if it can be used as the primary key
    Result: external_id is unique
*/
SELECT payment_external_id, COUNT(*)
FROM {{ ref("base_globepay__pay_chargeback_report") }}
GROUP BY 1
HAVING COUNT(*) > 1;

/*
    Test: Verify that all transactions in the acceptance report also exist in the chargeback report to determine the required join type
    Result: All payments from one source are also in the other
*/
WITH
    acceptance_report AS (
        SELECT payment_external_id
        FROM {{ ref("base_globepay__pay_acceptance_report") }}
    ),
    chargeback_report AS (
        SELECT payment_external_id
        FROM {{ ref("base_globepay__pay_chargeback_report") }}
    )
SELECT
    -- Payments only in the acceptance table
    SUM(
        CASE
            WHEN acceptance_report.payment_external_id IS NOT NULL AND chargeback_report.payment_external_id IS NULL
            THEN 1
            ELSE 0
        END
    ) AS acceptance_only_count,

    -- Payments only in the chargeback table
    SUM(
        CASE
            WHEN chargeback_report.payment_external_id IS NOT NULL AND acceptance_report.payment_external_id IS NULL
            THEN 1
            ELSE 0
        END
    ) AS chargeback_only_count,

    -- Payments present in both tables
    SUM(
        CASE
            WHEN acceptance_report.payment_external_id IS NOT NULL AND chargeback_report.payment_external_id IS NOT NULL
            THEN 1
            ELSE 0
        END
    ) AS both_sources_count

FROM acceptance_report 
FULL OUTER JOIN chargeback_report ON acceptance_report.payment_external_id = chargeback_report.payment_external_id;
