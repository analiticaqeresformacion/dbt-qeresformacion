{% macro split_part(string_text, delimiter_text, part_number) %}
  {{ adapter_macro('split_part', string_text, delimiter_text, part_number) }}
{% endmacro %}


{% macro default__split_part(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
        )

{% endmacro %}


{% macro bigquery__split_part(string_text, delimiter_text, part_number) %}

    split(
        {{ string_text }},
        {{ delimiter_text }}
        )[offset({{ part_number - 1 }})]

{% endmacro %}