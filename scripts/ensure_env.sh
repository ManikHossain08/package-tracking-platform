#!/usr/bin/env bash
# Creates .env from .env.example, or appends keys that .env is missing. Never overwrites a value.
# Variables named *_PASSWORD get a random value.
set -euo pipefail
[[ -f .env ]] || { touch .env; chmod 600 .env; }
added=0
while IFS= read -r line; do
  [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]] || continue
  key="${BASH_REMATCH[1]}"; value="${BASH_REMATCH[2]}"
  grep -q "^${key}=" .env && continue
  [[ "$key" == *_PASSWORD ]] && value="$(openssl rand -hex 24)"
  printf '%s=%s\n' "$key" "$value" >> .env
  added=$((added + 1))
done < .env.example
echo "env: .env ready ($added key(s) added)"
