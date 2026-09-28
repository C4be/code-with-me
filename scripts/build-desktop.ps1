param(
    [string]$Bundles = "nsis"
)

$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

foreach ($Tool in @("node", "pnpm", "rustc", "cargo")) {
    if (-not (Get-Command $Tool -ErrorAction SilentlyContinue)) {
        throw "Не найдена команда '$Tool'. Установите Node.js, pnpm и Rust, затем повторите сборку."
    }
}

Write-Host "Установка JS-зависимостей..."
pnpm install --frozen-lockfile
if ($LASTEXITCODE -ne 0) { throw "Не удалось установить JS-зависимости (код $LASTEXITCODE)." }

Write-Host "Сборка пакетов Tauri: $Bundles"
pnpm exec tauri build --bundles $Bundles
if ($LASTEXITCODE -ne 0) { throw "Сборка не удалась (код $LASTEXITCODE)." }

Write-Host ""
Write-Host "Сборка завершена. Пакеты находятся в:"
Write-Host "  $RootDir\src-tauri\target\release\bundle"
