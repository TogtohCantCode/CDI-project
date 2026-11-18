# syntax=docker/dockerfile:1

FROM python:3.11.11-slim-bookworm AS builder
WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


FROM python:3.11.11-slim-bookworm AS runtime
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000 \
    GUNICORN_WORKERS=2 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=60

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/* \
    && rm -rf /wheels

RUN groupadd --gid 10001 appgroup \
    && useradd --uid 10001 --gid appgroup --shell /usr/sbin/nologin --create-home appuser

COPY --chown=appuser:appgroup . .

USER 10001:10001
EXPOSE 5000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/healthz/liveness', timeout=2)" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["sh", "-c", "exec gunicorn \
    --workers=${GUNICORN_WORKERS} \
    --threads=${GUNICORN_THREADS} \
    --worker-class=gthread \
    --worker-tmp-dir=/dev/shm \
    --timeout=${GUNICORN_TIMEOUT} \
    --graceful-timeout=30 \
    --keep-alive=5 \
    --access-logfile=- \
    --error-logfile=- \
    --bind=0.0.0.0:${PORT} \
    app:app"]