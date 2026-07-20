#!/usr/bin/env bash
# Builds a minimal ffmpeg with libfdk_aac (HE-AAC encode support) for Linux
# or macOS, bundled with the obcast-client release so the survival rung's
# HE-AAC path (crates/obcast-client/src/encode.rs) doesn't depend on the
# operator's own ffmpeg build having libfdk_aac — most distro/Homebrew
# ffmpeg packages exclude it for licensing reasons (see CLAUDE.md §8
# "packaging"). Windows uses vcpkg instead — see
# .github/workflows/build-libfdk-ffmpeg.yml.
#
# This is the exact recipe verified in that CLAUDE.md entry: a
# --disable-everything build enabling only what obcast-client's encode
# pipeline actually uses (aac/libfdk_aac/pcm encode, the segment muxer,
# lavfi for the --headless sine-test source). Deliberately not a general-
# purpose ffmpeg — smaller and much faster to build than ffmpeg-full, and
# there's no reason to ship codec/format surface obcast never invokes.
set -euo pipefail

PREFIX="${1:?usage: build-libfdk-ffmpeg.sh <install-prefix>}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FFMPEG_VERSION=7.1

case "$(uname -s)" in
  Linux)
    if ! pkg-config --exists fdk-aac 2>/dev/null; then
      echo "libfdk-aac dev package not found; install it first (e.g. 'pacman -S libfdk-aac' / 'apt install libfdk-aac-dev', or build from https://github.com/mstorsjo/fdk-aac if your distro excludes it too)." >&2
      exit 1
    fi
    ;;
  Darwin)
    if ! pkg-config --exists fdk-aac 2>/dev/null; then
      echo "libfdk-aac not found; run 'brew install fdk-aac' first." >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported platform for this script: $(uname -s) — see build-libfdk-ffmpeg.ps1 for Windows" >&2
    exit 1
    ;;
esac

curl -fsSL "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" -o "$WORK/ffmpeg.tar.xz"
tar -xf "$WORK/ffmpeg.tar.xz" -C "$WORK"
cd "$WORK/ffmpeg-${FFMPEG_VERSION}"

./configure \
  --prefix="$PREFIX" \
  --enable-nonfree \
  --enable-libfdk-aac \
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
  --enable-bsf=aac_adtstoasc

make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
make install

echo "built: $PREFIX/bin/ffmpeg"
"$PREFIX/bin/ffmpeg" -hide_banner -encoders | grep -F libfdk_aac
