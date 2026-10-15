{#-
  Gộp các file nhỏ của một bảng Iceberg thành file lớn (thủ tục rewrite_data_files).
  Chỉ những nhóm có từ 5 file nhỏ trở lên mới được gộp, nên partition đã gọn sẽ được bỏ qua.
  Dùng: dbt run-operation compact_table --args '{table_name: bronze.events}'
-#}
{% macro compact_table(table_name) %}

    {% set sql %}
        call lakehouse.system.rewrite_data_files(table => '{{ table_name }}')
    {% endset %}

    {% set result = run_query(sql) %}
    {% for column in result.column_names %}
        {{ log(table_name ~ ": " ~ column ~ " = " ~ result.rows[0][loop.index0], info=True) }}
    {% endfor %}

{% endmacro %}
