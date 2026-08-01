#!/usr/bin/env bash
set -euo pipefail

for command in curl jq git; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 2
  }
done

: "${COOLIFY_API_TOKEN:?Set COOLIFY_API_TOKEN from Coolify Keys & Tokens}"
: "${SYMPHONY_BENCH_EXECUTOR_TOKEN:?Set the executor token used in bench .env}"

if [[ -z "${COOLIFY_API_BASE:-}" ]]; then
  coolify_ssh_host="${COOLIFY_SSH_HOST:-prod_eng@esh-kulichevskiy-2.adjust.dev}"
  coolify_tunnel_port="${COOLIFY_TUNNEL_PORT:-18080}"
  api_base="http://127.0.0.1:$coolify_tunnel_port/api/v1"
  ssh -o BatchMode=yes -o ExitOnForwardFailure=yes -N \
    -L "127.0.0.1:$coolify_tunnel_port:127.0.0.1:8000" \
    "$coolify_ssh_host" &
  tunnel_pid=$!
  cleanup() {
    kill "$tunnel_pid" 2>/dev/null || true
    wait "$tunnel_pid" 2>/dev/null || true
  }
  trap cleanup EXIT
  tunnel_ready=false
  for _ in $(seq 1 20); do
    if curl --fail --silent "$api_base/health" >/dev/null; then
      tunnel_ready=true
      break
    fi
    sleep 0.25
  done
  if ! kill -0 "$tunnel_pid" 2>/dev/null; then
    echo "Coolify SSH tunnel failed" >&2
    exit 2
  fi
  if [[ "$tunnel_ready" != "true" ]]; then
    echo "Coolify API did not become ready through the SSH tunnel" >&2
    exit 2
  fi
else
  api_base="$COOLIFY_API_BASE"
  if [[ "$api_base" == http://* && "$api_base" != http://127.0.0.1:* ]]; then
    echo "refusing to send COOLIFY_API_TOKEN over non-loopback HTTP" >&2
    exit 2
  fi
fi
project_name="${COOLIFY_BENCH_PROJECT:-Symphony Bench}"
application_name="${COOLIFY_BENCH_APPLICATION:-symphony-bench}"
repository="${COOLIFY_BENCH_REPOSITORY:-https://github.com/kulichevskiy/symphony}"
branch="${COOLIFY_BENCH_BRANCH:-$(git branch --show-current)}"

api() {
  method="$1"
  path="$2"
  body="${3:-}"
  args=(
    --fail-with-body --silent --show-error
    --request "$method"
    --header "Authorization: Bearer $COOLIFY_API_TOKEN"
    --header "Content-Type: application/json"
  )
  if [[ -n "$body" ]]; then
    curl "${args[@]}" --data-binary @- "$api_base$path" <<<"$body"
    return
  fi
  curl "${args[@]}" "$api_base$path"
}

projects="$(api GET /projects)"
project_uuid="$(jq -r --arg name "$project_name" '.[] | select(.name == $name) | .uuid' <<<"$projects" | head -1)"
if [[ -z "$project_uuid" ]]; then
  project_uuid="$(api POST /projects "$(jq -n --arg name "$project_name" '{name: $name, description: "Isolated Symphony E2E verification kit"}')" | jq -r .uuid)"
  echo "created Coolify project $project_name ($project_uuid)"
fi

servers="$(api GET /servers)"
if [[ -n "${COOLIFY_SERVER_UUID:-}" ]]; then
  server_uuid="$COOLIFY_SERVER_UUID"
else
  usable_count="$(jq '[.[] | select(.settings.is_usable == true)] | length' <<<"$servers")"
  if [[ "$usable_count" != "1" ]]; then
    echo "set COOLIFY_SERVER_UUID: found $usable_count usable servers" >&2
    exit 2
  fi
  server_uuid="$(jq -r '.[] | select(.settings.is_usable == true) | .uuid' <<<"$servers")"
fi

applications="$(api GET /applications)"
application_uuid="$(jq -r --arg name "$application_name" '.[] | select(.name == $name) | .uuid' <<<"$applications" | head -1)"
common="$(jq -n \
  --arg project "$project_uuid" \
  --arg server "$server_uuid" \
  --arg repository "$repository" \
  --arg branch "$branch" \
  --arg name "$application_name" \
  '{
    project_uuid: $project,
    server_uuid: $server,
    environment_name: "production",
    git_repository: $repository,
    git_branch: $branch,
    build_pack: "dockercompose",
    docker_compose_location: "/docker-compose.bench.coolify.yml",
    ports_exposes: "80",
    name: $name,
    description: "Isolated Symphony E2E verification kit",
    is_auto_deploy_enabled: false,
    autogenerate_domain: true,
    connect_to_docker_network: false
  }')"

if [[ -z "$application_uuid" ]]; then
  application_uuid="$(api POST /applications/public "$(jq '. + {instant_deploy: false}' <<<"$common")" | jq -r .uuid)"
  echo "created Coolify application $application_name ($application_uuid)"
else
  api PATCH "/applications/$application_uuid" "$common" >/dev/null
  echo "updated Coolify application $application_name ($application_uuid)"
fi

envs="$(api GET "/applications/$application_uuid/envs")"
executor_env="$(jq -n \
  --arg key "SYMPHONY_BENCH_EXECUTOR_TOKEN" \
  --arg value "$SYMPHONY_BENCH_EXECUTOR_TOKEN" \
  '{key: $key, value: $value, is_preview: false, is_literal: true, is_multiline: false}')"
if jq -e '.[] | select(.key == "SYMPHONY_BENCH_EXECUTOR_TOKEN")' <<<"$envs" >/dev/null; then
  api PATCH "/applications/$application_uuid/envs" "$executor_env" >/dev/null
else
  api POST "/applications/$application_uuid/envs" "$executor_env" >/dev/null
fi

application="$(api GET "/applications/$application_uuid")"
domain="$(jq -r '.fqdn // .domains // "domain pending"' <<<"$application")"
if [[ "${COOLIFY_PREPARE_ONLY:-0}" == "1" ]]; then
  echo "prepared: $domain"
  exit 0
fi

deployment="$(api GET "/deploy?uuid=$application_uuid&force=true")"
deployment_uuid="$(jq -r '.deployments[0].deployment_uuid' <<<"$deployment")"
echo "deployment queued: $deployment_uuid"

for _ in $(seq 1 180); do
  state="$(api GET "/deployments/$deployment_uuid")"
  status="$(jq -r .status <<<"$state")"
  case "$status" in
    finished)
      application="$(api GET "/applications/$application_uuid")"
      echo "deployed: $(jq -r '.fqdn // .domains // "domain pending"' <<<"$application")"
      exit 0
      ;;
    failed|cancelled|cancelled-by-user)
      echo "deployment $status" >&2
      jq -r '.logs // empty' <<<"$state" >&2
      exit 1
      ;;
  esac
  sleep 5
done

echo "deployment did not finish within 15 minutes" >&2
exit 1
