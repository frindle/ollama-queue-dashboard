# ollama-queue-dashboard -- the QUEUE (daemon + API + dashboard) only.
# Ollama and the models stay on their own hosts; this container reaches them
# over HTTP (configured in config/servers.json / the Settings page).
FROM python:3.12-slim

# bash is used by the entrypoint (wait -n) and by job verify commands.
RUN apt-get update && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/*

# Code path (baked into the image). State + logs live under $HOME (a volume);
# the servers file lives at $OLLAMA_QUEUE_SERVERS (a volume) -- see entrypoint.
ENV HOME=/data \
    OLLAMA_QUEUE_SERVERS=/config/servers.json \
    QUEUE_API_PORT=7684 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY bin/ /app/bin/
COPY config/ /app/config/
COPY docker/entrypoint.sh /app/entrypoint.sh

# Pure stdlib -- nothing to install. (See requirements.txt for the optional
# research-only extras, deliberately not installed here to keep the image slim.)
RUN chmod +x /app/entrypoint.sh /app/bin/*.py \
    && mkdir -p /data/bin /config

EXPOSE 7684

# Optional shared-secret gate for the API when there's no auth proxy in front:
#   docker run -e QUEUE_API_TOKEN=... ...
# Unset => open (assumes a trusted network / an auth proxy).
ENTRYPOINT ["/app/entrypoint.sh"]
