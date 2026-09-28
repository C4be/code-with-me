#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

case "$(uname -s)" in
  Darwin) DEFAULT_BUNDLES="dmg" ;;
  Linux) DEFAULT_BUNDLES="appimage,deb" ;;
  *) echo "Этот скрипт предназначен для macOS и Linux. На Windows запустите scripts/build-desktop.ps1." >&2; exit 2 ;;
esac

BUNDLES="${1:-$DEFAULT_BUNDLES}"

# Shell scripts don't always inherit Node's PATH when it is managed by nvm/fnm.
for node_bin in "$HOME"/.nvm/versions/node/*/bin "$HOME"/.fnm/node-versions/*/installation/bin; do
  if [[ -x "$node_bin/node" ]]; then PATH="$node_bin:$PATH"; fi
done
if [[ -x "$HOME/.volta/bin/node" ]]; then PATH="$HOME/.volta/bin:$PATH"; fi

# Codex's bundled runtime is a convenient fallback on development machines.
CODEX_NODE="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin"
CODEX_TOOLS="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/bin/fallback"
if ! command -v node >/dev/null 2>&1 && [[ -x "$CODEX_NODE/node" ]]; then
  PATH="$CODEX_NODE:$PATH"
fi
for pnpm_bin in "$HOME/Library/pnpm" "$HOME/.local/share/pnpm" "$CODEX_TOOLS"; do
  if [[ -x "$pnpm_bin/pnpm" ]]; then PATH="$pnpm_bin:$PATH"; fi
done
export PATH

if ! command -v node >/dev/null 2>&1; then
  echo "Не найден Node.js. Установите Node.js LTS (например, командой 'brew install node'), затем откройте новый терминал и повторите сборку." >&2
  exit 1
fi
for tool in rustc cargo; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Не найдена команда '$tool'. Установите Rust stable, затем повторите сборку." >&2
    exit 1
  fi
done
if ! command -v pnpm >/dev/null 2>&1; then
  echo "Не найден pnpm. Установите pnpm 11.19 командой 'corepack enable && corepack prepare pnpm@11.19.0 --activate', затем повторите сборку." >&2
  exit 1
fi

echo "Установка JS-зависимостей..."
pnpm install --frozen-lockfile
echo "Сборка пакетов Tauri: $BUNDLES"
pnpm exec tauri build --bundles "$BUNDLES"

echo
echo "Сборка завершена. Пакеты находятся в:"
echo "  $ROOT_DIR/src-tauri/target/release/bundle"
