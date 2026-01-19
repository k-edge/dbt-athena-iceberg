{% test expect_equal_columns(model, left_column, right_column, where=none) %}

{#-
  Fails when left_column != right_column.
  Optional `where` lets you limit rows (e.g. "card_line_id is not null").
-#}

with validation as (
    select
        {{ left_column }} as left_value,
        {{ right_column }} as right_value
    from {{ model }}
    {% if where is not none %}
    where {{ where }}
    {% endif %}
)

select *
from validation
where left_value is not null
  and right_value is not null
  and left_value != right_value

{% endtest %}

