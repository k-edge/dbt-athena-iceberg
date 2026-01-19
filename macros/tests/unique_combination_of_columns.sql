{% test unique_combination_of_columns(model, combination_of_columns, where=none) %}

with validation as (
    select
        {{ combination_of_columns | join(', ') }},
        count(*) as cnt
    from {{ model }}
    {% if where %}
    where {{ where }}
    {% endif %}
    group by {{ combination_of_columns | join(', ') }}
    having count(*) > 1
)

select *
from validation

{% endtest %}

