{% macro fnSplitInColumns(table_name,column_name,delimeter='-') %}
    {%- for i in range(9) -%}
            {%- if  loop.first -%}
                    select {{column_name}} ,
            {% endif -%}
        split_part({{column_name}},'-',{{i}}) as column{{i}}
            {%- if  loop.last %}
                from {{table_name}}
            {% endif %}
            {%- if not loop.last -%}
                    ,
            {% endif -%}
    {%- endfor %}
{% endmacro %}