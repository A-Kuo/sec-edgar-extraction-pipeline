# Multi-stage build.
#
# Stage 1 compiles wheels (lxml, psycopg2 and scikit-learn all need a toolchain);
# stage 2 copies only the installed packages, so gcc and the header files never
# reach the published image. That is both a size win and a smaller attack
# surface — a compiler in a production container is a tool for whoever gets in.

# --------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Build-time only: dropped when this stage is discarded.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libxml2-dev \
        libxslt1-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# A virtualenv, rather than --user or the system site-packages, so stage 2 can
# take the whole dependency tree with a single COPY.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies before source: this layer is cached across every build that does
# not change requirements.txt, which is most of them.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS runtime

# Runtime shared libraries only — the -dev headers stay behind in the builder.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2 \
        libxslt1.1 \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root. The container has no reason to write anywhere but /app, and running
# as root would mean a container escape starts with root on the host namespace.
RUN groupadd --system --gid 1000 appuser \
    && useradd --system --uid 1000 --gid appuser --create-home appuser

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    MODEL_REGISTRY_ROOT=/app/models

WORKDIR /app

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser api/ ./api/
COPY --chown=appuser:appuser dags/ ./dags/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser migrations/ ./migrations/
COPY --chown=appuser:appuser alembic.ini pyproject.toml ./

# The registry is a mount point in production; create it so a container started
# without a volume still has somewhere to look rather than erroring on startup.
RUN mkdir -p /app/models && chown appuser:appuser /app/models

USER appuser

EXPOSE 8000

# Hits the real /health endpoint, which probes both PostgreSQL and Redis, so an
# orchestrator restarts the container when its dependencies are unreachable
# rather than leaving it up and failing every request.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
