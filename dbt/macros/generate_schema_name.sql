{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if node.resource_type == 'test' -%}
        audit

    {%- elif custom_schema_name is none -%}
        {{ target.schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}
        
    {%- endif -%}

{%- endmacro %}