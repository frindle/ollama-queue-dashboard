# Running on Unraid

The queue container runs happily on Unraid's Docker. It is a **universal
dispatcher**: LLM (Ollama), image, and video work all run on their own
hosts/services, and this container just reaches those over HTTP. The one thing
that runs *in* the container is the coding feedback loop (verify/gate/review) —
see "Coding feedback loop: mounting target repos" below. Register each backend
(type + URL) on the Settings page; see `docs/BACKENDS.md` for the non-Ollama
backend HTTP contract.

## Option A — Add a Container (Docker tab)

1. **Docker** tab → **Add Container**.
2. Fill in:
   - **Name:** `ollama-queue`
   - **Repository:** your built/pushed image, e.g. `ghcr.io/YOURNAME/ollama-queue-dashboard:latest`
     (build it with `docker build -t ...` and push, or build on Unraid).
   - **Network Type:** `Bridge` (or `Host` — see networking below).
3. **Add Port:**
   - Container Port `7684` → Host Port `7684` (TCP).
4. **Add Path (volume):**
   - `/config` → `/mnt/user/appdata/ollama-queue/config`  (holds `servers.json`)
   - `/data`   → `/mnt/user/appdata/ollama-queue/data`    (queue state + logs)
5. **Add Variable (optional but recommended):**
   - `QUEUE_API_TOKEN` → a long random string. Every request must then send
     `Authorization: Bearer <token>`.
6. **Apply.** Open `http://<unraid-ip>:7684`, go to **Settings**, and add your
   Ollama hosts (or pre-place a `servers.json` in the config path — the container
   seeds one from the example on first run).

## Option B — docker compose (Compose Manager plugin)

Drop this repo's `docker-compose.yml` into a Compose stack and set
`QUEUE_API_TOKEN` in the stack's environment. It maps `7684`, mounts `./config`
and a `queue-data` volume, and adds `host.docker.internal`.

## Option C — Community-Applications-style template

Save as `ollama-queue-dashboard.xml` and import via **Docker → Add Container →
Template**, or drop it in `/boot/config/plugins/dockerMan/templates-user/`.
Replace the `Repository` with your image.

```xml
<?xml version="1.0"?>
<Container version="2">
  <Name>ollama-queue-dashboard</Name>
  <Repository>ghcr.io/YOURNAME/ollama-queue-dashboard:latest</Repository>
  <Registry>https://github.com/YOURNAME/ollama-queue-dashboard</Registry>
  <Network>bridge</Network>
  <Privileged>false</Privileged>
  <Support>https://github.com/YOURNAME/ollama-queue-dashboard</Support>
  <Overview>Job queue + dashboard for Ollama, plus an OpenAI-compatible /v1 LLM
    endpoint. Only the queue runs here; Ollama stays on its own hosts and is
    reached over HTTP (configured in config/servers.json or the Settings page).</Overview>
  <Category>AI: Productivity: Tools:</Category>
  <WebUI>http://[IP]:[PORT:7684]/</WebUI>
  <ExtraParams>--add-host host.docker.internal:host-gateway</ExtraParams>
  <Config Name="WebUI" Target="7684" Default="7684" Mode="tcp" Type="Port" Display="always" Required="true">7684</Config>
  <Config Name="Config (servers.json)" Target="/config" Default="/mnt/user/appdata/ollama-queue/config" Mode="rw" Type="Path" Display="always" Required="true"/>
  <Config Name="Data (state + logs)" Target="/data" Default="/mnt/user/appdata/ollama-queue/data" Mode="rw" Type="Path" Display="always" Required="true"/>
  <Config Name="API token" Target="QUEUE_API_TOKEN" Default="" Mode="" Type="Variable" Display="always" Required="false" Mask="true"/>
</Container>
```

## Networking — how the container reaches Ollama

The container calls the `url` of each server in `servers.json`. Make sure Ollama
is bound to a reachable interface (`OLLAMA_HOST=0.0.0.0:11434`), then:

- **Ollama on another LAN box** (most common): use its LAN URL directly, e.g.
  `http://192.168.1.50:11434`. No special Docker settings needed.
- **Ollama on the Unraid host itself:** with `--add-host
  host.docker.internal:host-gateway` (in the template above), use
  `http://host.docker.internal:11434`.
- **Host networking:** set **Network Type: Host** and drop the port mapping; then
  `http://127.0.0.1:11434` reaches a host-local Ollama and `:7684` is served on
  the host directly.

## Plain `docker run` on Unraid

```bash
docker run -d --name ollama-queue \
  -p 7684:7684 \
  -v /mnt/user/appdata/ollama-queue/config:/config \
  -v /mnt/user/appdata/ollama-queue/data:/data \
  --add-host host.docker.internal:host-gateway \
  -e QUEUE_API_TOKEN=changeme \
  --restart unless-stopped \
  ghcr.io/YOURNAME/ollama-queue-dashboard:latest
```

## Coding feedback loop: mounting target repos

The coding-dispatch loop (scaffolding / gates / reviews / verify) runs **inside**
this container, so any repo it verifies must be mounted:

- **Add Path (volume):** `/repos` → `/mnt/user/appdata/dispatch-repos` (**rw** —
  the queue creates a throwaway git worktree per job and writes install
  artifacts). Then enqueue with `--repo /repos/<name>` (or `--cwd`).
- The image ships the toolchain the loop itself needs: `git`, `python3`,
  `nodejs`, `npm`. A repo's **own** dependencies are **not** baked in — they
  install at verify time (`npm ci`, `prisma generate`, `pip install ...`) via the
  existing env-parity bootstrap, against the mounted repo. Give the container
  outbound network access so those installs can reach npm / PyPI.

## Backend services (image / video / ComfyUI)

Non-Ollama jobs are dispatched to HTTP services over the network — nothing extra
is mounted or installed in this container for them. Register each on the Settings
page (name, type `comfyui|img2vid|image`, URL). The queue POSTs the job and polls
for a result URL; the service (e.g. the Studio pet-portrait backend) does the GPU
work. Contract: `docs/BACKENDS.md`.

## Notes

- **Persistence:** everything durable is under `/config` (your backend registry)
  and `/data` (queue state + logs). Back those two paths up; the image is
  disposable.
- **Updates:** pull a new image and recreate the container — state/config survive
  in the mounted paths.
