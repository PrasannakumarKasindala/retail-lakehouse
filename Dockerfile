# Runs the local (delta-rs + dbt-duckdb) pipeline. The Spark jobs target a
# cluster and are not part of this image.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY transform ./transform

RUN pip install --no-cache-dir . && \
    useradd --create-home --uid 10001 rlh
ENV RLH_DBT_PROJECT_DIR=/app/transform
USER rlh

ENTRYPOINT ["rlh"]
CMD ["--help"]
