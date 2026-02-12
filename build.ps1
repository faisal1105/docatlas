# Build a portable Windows exe using PyInstaller
# Run in PowerShell: .\build.ps1

$ErrorActionPreference = "Stop"

# Ensure venv
if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

. .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
pip install pyinstaller

# Clean old build
if (Test-Path "dist") { Remove-Item -Recurse -Force "dist" }
if (Test-Path "build") { Remove-Item -Recurse -Force "build" }
if (Test-Path "docatlas.spec") { Remove-Item -Force "docatlas.spec" }

# Build
pyinstaller --noconsole --onefile docatlas.py

Write-Host "Build complete. EXE in .\\dist\\docatlas.exe"
