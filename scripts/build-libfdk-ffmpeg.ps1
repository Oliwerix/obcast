# Windows counterpart to build-libfdk-ffmpeg.sh — see that script's header
# for what/why. Compiling ffmpeg from source with libfdk_aac on Windows
# needs a MinGW/MSYS2 toolchain that isn't worth hand-rolling here when
# vcpkg's ffmpeg port already builds it with the fdk-aac feature via MSVC,
# matching how the rest of the client is built for this target
# (x86_64-pc-windows-msvc, see dist-workspace.toml).
param(
    [Parameter(Mandatory = $true)]
    [string]$InstallDir
)

$ErrorActionPreference = "Stop"

$VcpkgRoot = Join-Path $env:RUNNER_TEMP "vcpkg"
if (-not (Test-Path $VcpkgRoot)) {
    git clone --depth 1 https://github.com/microsoft/vcpkg.git $VcpkgRoot
}
& "$VcpkgRoot\bootstrap-vcpkg.bat"

& "$VcpkgRoot\vcpkg.exe" install "ffmpeg[fdk-aac]:x64-windows" --recurse

$Built = Join-Path $VcpkgRoot "installed\x64-windows\tools\ffmpeg\ffmpeg.exe"
if (-not (Test-Path $Built)) {
    throw "vcpkg did not produce ffmpeg.exe at the expected path: $Built"
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item $Built (Join-Path $InstallDir "ffmpeg.exe")

Write-Host "built: $InstallDir\ffmpeg.exe"
& (Join-Path $InstallDir "ffmpeg.exe") -hide_banner -encoders | Select-String -SimpleMatch "libfdk_aac"
