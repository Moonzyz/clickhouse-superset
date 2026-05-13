FROM apache/superset:latest

USER root

RUN uv pip install clickhouse-connect --python /app/.venv/bin/python

USER superset