FROM apache/superset:latest

USER root
# Install into superset venv using system pip3 with target directory
RUN pip3 install --target /app/.venv/lib/python3.10/site-packages --no-cache-dir sqlalchemy-trino

USER superset
