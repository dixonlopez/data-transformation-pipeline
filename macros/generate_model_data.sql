{% macro generate_model_data(model) %}
    /**
    * Macro: generate_model_data
    * 
    * Description:
    * This macro generates summary statistics for all columns in a given model.
    * It retrieves the column names dynamically and calculates key metrics such as:
    * - Count of distinct values
    * - Minimum value (as text)
    * - Maximum value (as text)
    * - Minimum length of values (as text)
    * - Maximum length of values (as text)
    * 
    * Parameters:
    * - model (string): The name of the DBT model to analyze.
    * 
    * Returns:
    * - A SQL query that provides summary statistics for each column in the specified model.
    * 
    */
{% set cols = adapter.get_columns_in_relation(ref(model)) %}
{% set column_names = cols | map(attribute='name') | list %}

with models_data as (
    {% for column_name in column_names %}
        select
            '{{model}}' as model_name,  -- Model name being analyzed
            '{{column_name}}' as column_name,  -- Column name being analyzed
            count(distinct {{column_name}}) as count_distinct_values,  -- Number of distinct values in the column
            min({{column_name}}::text) as min_value,  -- Minimum value in the column (cast as text)
            max({{column_name}}::text) as max_value,  -- Maximum value in the column (cast as text)
            min(length({{column_name}}::text)) as min_length_value,  -- Minimum string length of values
            max(length({{column_name}}::text)) as max_length_value  -- Maximum string length of values
        from {{ ref(model) }}  -- Querying the provided model
        {%- if not loop.last %}
        union all
        {% endif -%}
    {% endfor %}
)

select * from models_data

{% endmacro %}
