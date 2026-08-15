FROM apache/airflow:3.3.0

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

USER airflow