# Stage 1: Build Python packages
FROM public.ecr.aws/docker/library/python:3.12-slim-bookworm AS packages-builder
LABEL stage="packages-builder"

# IMPORTANT: Declare TARGETARCH for multi-platform builds
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    apt-transport-https \
    gnupg \
    ca-certificates \
    build-essential \
    libffi-dev \
    freetds-dev \
    freetds-bin \
    libpq-dev \
    postgresql-client && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto with architecture detection
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        curl -fSsL https://corretto.aws/downloads/latest/amazon-corretto-17-aarch64-linux-jdk.deb -o corretto.deb; \
    else \
        curl -fSsL https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.deb -o corretto.deb; \
    fi && \
    dpkg -i corretto.deb && \
    rm corretto.deb

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH

# Poetry setup
ENV POETRY_VERSION="2.1.3" \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR="/tmp/poetry_cache"

ENV PATH="${POETRY_HOME}/bin:${PATH}"

RUN curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION}

WORKDIR /usr/src/app

COPY pyproject.toml poetry.lock ./

RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --only main --no-root --quiet || \
    poetry install --only main --no-root --quiet || \
    poetry install --only main --no-root --quiet

# Clean up to reduce layer size
RUN rm -rf "${POETRY_CACHE_DIR}" && \
    rm -rf /root/.cache/pip && \
    find /usr/local/lib/python3.12/site-packages/ -name "*.dist-info" -exec rm -rf {} + || true && \
    find /usr/local/lib/python3.12/site-packages/ -name "__pycache__" -exec rm -rf {} + || true

# Add AWS SSO support JARs after installation
RUN cd /usr/local/lib/python3.12/site-packages/amazon_kclpy/jars && \
    curl --retry 5 --retry-delay 2 --retry-max-time 120 -fSsL -o sso-2.25.64.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/sso/2.25.64/sso-2.25.64.jar" || true && \
    curl --retry 5 --retry-delay 2 --retry-max-time 120 -fSsL -o ssooidc-2.25.64.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/ssooidc/2.25.64/ssooidc-2.25.64.jar" || true

# Stage 2: Application Runtime
FROM public.ecr.aws/docker/library/python:3.12-slim-bookworm AS application-runtime
LABEL stage="application-runtime"

# Declare TARGETARCH for this stage too
ARG TARGETARCH

RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    java-common \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto with architecture detection
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        curl -fSsL https://corretto.aws/downloads/latest/amazon-corretto-17-aarch64-linux-jdk.deb -o corretto.deb; \
    else \
        curl -fSsL https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.deb -o corretto.deb; \
    fi && \
    dpkg -i corretto.deb && \
    rm corretto.deb && \
    rm -rf /usr/lib/jvm/java-17-amazon-corretto/lib/src.zip \
           /usr/lib/jvm/java-17-amazon-corretto/man \
           /usr/lib/jvm/java-17-amazon-corretto/demo \
           /usr/lib/jvm/java-17-amazon-corretto/sample \
           /usr/lib/jvm/java-17-amazon-corretto/jmods \
           /usr/lib/jvm/java-17-amazon-corretto/include

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV JAVA_OPTS="-XX:+UseContainerSupport -Xms768m -Xmx768m -XX:MaxMetaspaceSize=128m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"

# Build arguments for AWS configuration
ARG PGSQL_CREDENTIALS_SECRET_NAME
ARG REDIS_CREDENTIALS_SECRET_NAME
ARG AWS_REGION
ARG TX_TYPE

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/usr/src/app:${PYTHONPATH} \
    PGSQL_CREDENTIALS_SECRET_NAME=$PGSQL_CREDENTIALS_SECRET_NAME \
    REDIS_CREDENTIALS_SECRET_NAME=$REDIS_CREDENTIALS_SECRET_NAME \
    AWS_REGION=$AWS_REGION \
    TX_TYPE=$TX_TYPE \
    OTEL_RESOURCE_ATTRIBUTES=service.name="tx-asset-cycle-vlx"

# Create non-root user
RUN useradd --system --create-home --user-group --shell /bin/false appuser

WORKDIR /usr/src/app

# Copy Python packages from builder stage
COPY --from=packages-builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=packages-builder /usr/local/bin/ /usr/local/bin/

# Copy application code
COPY --chown=appuser:appuser ./main.py ./
COPY --chown=appuser:appuser ./config ./config
COPY --chown=appuser:appuser ./models ./models
COPY --chown=appuser:appuser ./processing ./processing
COPY --chown=appuser:appuser ./utils ./utils

# Fix Python symlinks
RUN ln -sf /usr/local/bin/python3.12 /usr/bin/python3 && \
    ln -sf /usr/local/bin/pip3.12 /usr/bin/pip && \
    ln -sf /usr/local/bin/pip3.12 /usr/bin/pip3

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "/usr/src/app/utils/healthcheck.py"]

# Entry point
ENTRYPOINT ["python", "utils/kclpy_helper.py"]