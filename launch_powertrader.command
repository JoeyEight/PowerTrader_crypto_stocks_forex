#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$HOME/PowerTrader_AI"
cd "$PROJECT_DIR" || exit 1

# Ensure runtime directories exist.
mkdir -p \
  "$PROJECT_DIR/hub_data/logs" \
  "$PROJECT_DIR/hub_data/.mplconfig" \
  "$PROJECT_DIR/hub_data/autofix/tickets" \
  "$PROJECT_DIR/hub_data/autofix/patches"

# Resolve settings and runtime paths for hub + children.
export POWERTRADER_PROJECT_DIR="$PROJECT_DIR"
export POWERTRADER_GUI_SETTINGS="$PROJECT_DIR/gui_settings.json"
export POWERTRADER_HUB_DIR="$PROJECT_DIR/hub_data"
export MPLCONFIGDIR="$PROJECT_DIR/hub_data/.mplconfig"

# Keep package imports stable when launching from Finder/Terminal.
export PYTHONPATH="$PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"

# Optional: load OpenAI key used by runtime/pt_autofix.py.
# Priority: existing env var, then project key file.
if [[ -z "${OPENAI_API_KEY:-}" && -f "$PROJECT_DIR/keys/openai_api_key.txt" ]]; then
  export OPENAI_API_KEY="$(tr -d '\r\n' < "$PROJECT_DIR/keys/openai_api_key.txt")"
fi

PY_BIN="$PROJECT_DIR/venv/bin/python3"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3)"
fi

exec "$PY_BIN" -m ui.pt_hub
