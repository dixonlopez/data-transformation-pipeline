{% macro generate_model_data(model) %}

{% set cols = adapter.get_columns_in_relation(ref(model)) %}
{% set column_names = cols | map(attribute='name') | list %}

with models_data as (
    {% for column_name in column_names %}
        select
            '{{model}}' as model_name,
            '{{column_name}}' as column_name,
            count(distinct {{column_name}}) as count_distinct_values,
            min({{column_name}}::text) as min_value,
            max({{column_name}}::text) as max_value,
            min(length({{column_name}}::text)) as min_length_value,
            max(length({{column_name}}::text)) as max_length_value
        from {{ ref(model) }}
        {%- if not loop.last %}
        union all
        {% endif -%}
    {% endfor %}
)

select * from models_data

{% endmacro %}