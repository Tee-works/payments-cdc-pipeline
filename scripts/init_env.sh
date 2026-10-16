#!/usr/bin/env bash
# Create .env from .env.example, with a fresh random value for every change-me.
# Leaves an existing .env alone, so it is safe to run on every make.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && exit 0
while IFS= read -r line || [ -n "$line" ]; do
  if [[ "$line" == *=change-me ]]; then
    echo "${line%change-me}$(openssl rand -hex 16)"
  else
    echo "$line"
  fi
done < .env.example > .env
echo "Wrote .env with generated passwords" >&2
