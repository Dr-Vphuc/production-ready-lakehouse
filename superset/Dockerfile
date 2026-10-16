# Superset bản chính thức, thêm driver ClickHouse.
# Image gốc ("lean") cài Superset trong virtualenv /app/.venv bằng uv, chạy bằng user `superset`.
FROM apache/superset:6.1.0

USER root
# clickhouse-connect cung cấp dialect `clickhousedb://` cho SQLAlchemy.
# Superset 6.1 chỉ chấp nhận clickhouse-connect >=0.13, <1.0 nên ghim bản 0.x mới nhất.
RUN uv pip install --no-cache clickhouse-connect==0.15.1
USER superset
