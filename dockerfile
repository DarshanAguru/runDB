# HArdened image
FROM python:3.11-slim-bookworm

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RUNDB_HOST=0.0.0.0 \
    RUNDB_PORT=7379 \
    RUNDB_AOF_FILE=/data/run-master.aof

# Network utils for health check
RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /data

# Create a restricted system group and user (10001) with no shell access and no home directory
RUN groupadd -g 10001 rundb && \
    useradd -u 10001 -g 10001 -M -s /sbin/nologin -c "RunDB Service User" rundb

# Copy project files and ensure they are owned by our non-root user
COPY --chown=rundb:rundb . .

# Adjust permissions so that /data is writable and app files are readable
RUN chmod 755 /app && \
    chown -R rundb:rundb /data && \
    chmod 700 /data

# Switch to the non-root system user
USER 10001

# Expose the default port
EXPOSE 7379

# Add healthcheck to verify database availability
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD nc -z localhost 7379 || exit 1

# Start the RunDB service
CMD ["python", "main.py"]
