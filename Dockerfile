# ollama-queue-dashboard -- the QUEUE (daemon + API + dashboard) + the coding
# feedback loop. LLM, image, and video work all run on their own hosts/services;
# this container reaches them over HTTP (config/servers.json / the Settings page).
#
# The ONE thing that DOES execute in-container is the coding-dispatch feedback
# loop (scaffolding / gates / reviews / verify), so the image carries the minimal
# toolchain that loop needs. Each package below is justified; nothing else is
# added, to keep the image slim.
FROM python:3.12-slim

# Toolchain for the in-container coding feedback loop:
#   bash  -- the entrypoint (wait -n) and shell-form job verify commands.
#   git   -- worktree create/reset, baseline HEAD + diff reads that the gate and
#            signoff depend on; the dispatch worktree isolation is pure git.
#   ca-certificates -- HTTPS to the backend services and to git remotes.
#   nodejs + npm -- TS/JS verifies (tsx / prisma-class projects run `npm ci`,
#            `prisma generate`, `tsc`, and the project's own test command at
#            verify time). Node is the only non-Python runtime the verify step
#            needs; Python 3 is already in the base image (pip verifies use it).
# python3 comes from the base image. A repo's OWN deps are NOT baked here -- they
# install at verify time (npm ci / prisma generate / pip install) via the
# existing env-parity bootstrap, against the mounted target repo.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash git ca-certificates nodejs npm \
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
