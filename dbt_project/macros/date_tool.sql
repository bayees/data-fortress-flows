{% macro date_to_int(date_column)  %}
coalesce(datepart('year', {{ date_column }}) * 10000 + datepart('month', {{ date_column }}) * 100 + datepart('day', {{ date_column }}), 0)
{% endmacro %}


{% macro time_to_int(date_column)  %}
coalesce(datepart('hour', {{ date_column }}) * 10000 + datepart('minute', {{ date_column }}) * 100 + datepart('second', {{ date_column }}), 0)
{% endmacro %}