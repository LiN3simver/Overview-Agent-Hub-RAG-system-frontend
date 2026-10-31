# Deployment (Docker Compose + git push)

[繁體中文](README.md) | **English**

Run from Git Bash on your local machine. It `git push`es the chosen commit to the server over SSH,
and the server-side `apply.sh` decides — based on which files changed — whether to rebuild the image, just restart, or do nothing.

```bash
bash deploy/deploy.sh bootstrap   # first time: create the repo on the server and add the local remote (idempotent)
bash deploy/deploy.sh plan        # preview what will be sent and what the server will do (read-only)
bash deploy/deploy.sh deploy      # deploy; rolls back automatically if the health check fails
bash deploy/deploy.sh status      # current version and health on the server
bash deploy/deploy.sh rollback    # go back to the previous healthy version
bash deploy/deploy.sh check       # compare the agentkit versions loaded by the Hub and each agent (read-only)
```

## Configuration

Deployment targets and rules live in [`target.conf`](target.conf):

| Variable | Description |
|---|---|
| `T_HOST` / `T_REMOTE` | SSH target and directory on the server (defaults are placeholders — change them to your own) |
| `T_IMAGE_RE` | Changes matching these paths rebuild the image |
| `T_COMPOSE_RE` | Changes matching these paths re-run `docker compose up` |
| `T_HEALTH_CMD` | Post-deploy health check (by default the Hub home page must return 200) |
| `T_IMAGE_KEEP` | How many old images to keep for rollback |
| `T_DEPENDENT_DIRS` | Other agent projects to offer rebuilding when `agentkit/` changes |

## Design Notes

- **Only version-controlled files are sent**: `.env`, `data/`, and `.venv/` are in `.gitignore` and are never uploaded or overwritten.
- **Deploy a commit, not the working tree**: every version on the server is traceable; deployment aborts by default if there are uncommitted changes.
- **Public port guard**: before deploying, compares "ports currently running" with "ports this deploy will use" and aborts before touching containers if they differ.
- **Fast rollback**: the running image is tagged with its commit before every build, so rollback needs no rebuild.
- **agentkit coupling**: other agents' images bundle an agentkit snapshot; changing `agentkit/` and deploying only the Hub
  causes a version mismatch, so `deploy` prompts to rebuild them too, and `check` verifies that all services load the same version.
