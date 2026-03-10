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

VENV_DIR="$PROJECT_DIR/venv"
PY_BIN="$VENV_DIR/bin/python3"

# Keep startup deterministic: always use project venv.
if [[ ! -x "$PY_BIN" ]]; then
  echo "[launch] venv not found; creating at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

# If core deps are missing/corrupt, bootstrap from requirements.
if ! "$PY_BIN" -c "import matplotlib" >/dev/null 2>&1; then
  echo "[launch] installing dependencies from requirements.txt"
  "$PY_BIN" -m pip install --upgrade pip setuptools wheel
  "$PY_BIN" -m pip install -r "$PROJECT_DIR/requirements.txt"
fi

exec "$PY_BIN" -m ui.pt_hub
