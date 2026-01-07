{% macro surrogate_key_hash(cols=[], ts_cols=[], epoch_unit='second') %}
  {# Use like: 
    surrogate_key_hash(cols=['order_item_id', 'order_id'], ts_cols=['updated_at'])
  #}
  {# Note: epoch_unit can be 'second' or 'millisecond' #}

  {% set rendered = [] %}
  {% set total_cols = (cols | length) + (ts_cols | length) %}

  {# prepare expressions for hashing #}
  {% for col in cols %}
    {% do rendered.append(col) %}
  {% endfor %}

  {% for col in ts_cols %}
    {% if epoch_unit == 'millisecond' %}
      {% do rendered.append("date_part('epoch_millisecond', " ~ col ~ ")") %}
    {% else %}
      {% do rendered.append("date_part('epoch_second', " ~ col ~ ")") %}
    {% endif %}
  {% endfor %}

  {# single-column special case: return -1 #}
  {% if total_cols == 1 %}
    {% set only_col = (cols + ts_cols)[0] %}
    case
      when {{ only_col }} is null then -1
      else HASH({{ rendered[0] }})
    end

  {# multi-column: let HASH() handle nulls normally #}
  {% else %}
    HASH({{ rendered | join(', ') }})
  {% endif %}
{% endmacro %}