FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system --gid 10001 metropt \
    && useradd --system --uid 10001 --gid metropt --no-create-home metropt

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir .

USER 10001:10001

ENTRYPOINT ["metropt-ingest"]
