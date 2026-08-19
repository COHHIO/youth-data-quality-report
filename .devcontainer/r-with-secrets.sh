#!/usr/bin/env bash
# Launch R with Bitwarden Secrets Manager secrets injected as environment
# variables. `bws run` fetches the project's secrets and exec's R with each
# secret key/value already present in the process environment, so they are
# available via Sys.getenv() in the R terminal (incl. when you run
# golem::run_dev()). Values live only in the R process — never written to disk.
#
# This is wired in as the VS Code R extension's terminal program
# (r.rterm.linux), so every R terminal gets the secrets automatically.
#
# Requires BWS_ACCESS_TOKEN and BWS_PROJECT_ID in the environment (loaded from
# .devcontainer/.env via the container's --env-file run arg).

if [ -n "${BWS_ACCESS_TOKEN}" ] && command -v bws >/dev/null 2>&1; then
  # --project-id scopes the injected secrets to this project. If your machine
  # account token only has access to the one project you can drop the flag.
  exec bws run --project-id "${BWS_PROJECT_ID}" -- R "$@"
fi

echo "r-with-secrets.sh: BWS unavailable (no token or bws not installed); starting R without injected secrets." >&2
exec R "$@"
