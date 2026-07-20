#!/usr/bin/env bash
# Windows counterpart to build-libfdk-ffmpeg.sh (see that script's header for
# what/why) — run inside an MSYS2 MinGW64 shell (msys2/setup-msys2 in CI).
#
# vcpkg's ffmpeg port was tried first and rejected: it builds the libav*
# libraries fine (fdk-aac feature included) but deliberately passes
# --disable-ffmpeg, so it never produces the ffmpeg.exe CLI binary
# encode.rs actually shells out to — only useful for linking ffmpeg into
# another C/C++ project, not for bundling the tool itself. MSYS2 also
# doesn't reliably package fdk-aac (same licensing reason it's excluded from
# most distros' official repos — see build-libfdk-ffmpeg.sh), so this builds
# both fdk-aac and ffmpeg from source with the MinGW toolchain, mirroring
# the Linux/macOS recipe.
set -euo pipefail

PREFIX="${1:?usage: build-libfdk-ffmpeg-windows.sh <install-prefix>}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FDK_AAC_VERSION=2.0.3
FFMPEG_VERSION=7.1

# --- fdk-aac ---
curl -fsSL "https://github.com/mstorsjo/fdk-aac/archive/refs/tags/v${FDK_AAC_VERSION}.tar.gz" -o "$WORK/fdk-aac.tar.gz"
tar -xf "$WORK/fdk-aac.tar.gz" -C "$WORK"
(
  cd "$WORK/fdk-aac-${FDK_AAC_VERSION}"
  autoreconf -fiv
  ./configure --prefix="$PREFIX" --disable-shared --enable-static
  make -j"$(nproc)"
  make install
)

# --- ffmpeg, linked against the fdk-aac just built ---
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig"
curl -fsSL "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" -o "$WORK/ffmpeg.tar.xz"
tar -xf "$WORK/ffmpeg.tar.xz" -C "$WORK"
(
  cd "$WORK/ffmpeg-${FFMPEG_VERSION}"
  ./configure \
    --prefix="$PREFIX" \
    --target-os=mingw32 \
    --arch=x86_64 \
    --enable-nonfree \
    --enable-libfdk-aac \
    --extra-libs=-lstdc++ \
    --disable-everything \
    --disable-doc \
    --disable-debug \
    --disable-ffplay \
    --disable-ffprobe \
    --enable-avcodec \
    --enable-avformat \
    --enable-avfilter \
    --enable-swresample \
    --enable-encoder=aac,libfdk_aac,pcm_s16le,pcm_f32le \
    --enable-decoder=aac,pcm_s16le,pcm_f32le \
    --enable-muxer=mpegts,segment \
    --enable-demuxer=mpegts,wav,pcm_s16le,pcm_f32le \
    --enable-protocol=file,pipe \
    --enable-filter=aresample,anullsrc,sine \
    --enable-parser=aac \
    --enable-indev=lavfi \
    --enable-bsf=aac_adtstoasc \
    --pkg-config-flags=--static
  make -j"$(nproc)"
  make install
)

echo "built: $PREFIX/bin/ffmpeg.exe"
"$PREFIX/bin/ffmpeg.exe" -hide_banner -encoders | grep -F libfdk_aac
