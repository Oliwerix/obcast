# obcast-server container image. Only the server is packaged here — the
# encoder client is a GUI app meant to run at the OB site, not in a
# container. See CLAUDE.md §8 item 7.
#
# The server shells out to `ffmpeg` for playout decode (playout.rs) and
# waveform generation (waveform.rs), and drives hardware audio output via
# `cpal`/ALSA (playout.rs) — both are real runtime dependencies, not build
# tooling, so they're installed in the final image, not just the build stage.

FROM rust:1-slim-bookworm AS build
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libasound2-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN cargo build --release -p obcast-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libasound2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/release/obcast-server /usr/local/bin/obcast-server
COPY web/remote /app/web/remote

ENV OBCAST_DATA_DIR=/data
ENV OBCAST_LISTEN_ADDR=0.0.0.0:8080
ENV OBCAST_WEB_REMOTE_DIR=/app/web/remote
VOLUME ["/data"]
EXPOSE 8080

# Hardware playout needs a real ALSA device: run with `--device /dev/snd`
# (Linux host) or accept that playout will error/underrun without one —
# ingest, HLS origin, and the control API work fine regardless.
ENTRYPOINT ["obcast-server"]
