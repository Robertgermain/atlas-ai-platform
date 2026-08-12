# syntax=docker/dockerfile:1
#
# Shared, immutable Atlas backend image (Milestone 14 Slice 14A).
#
# One image runs all four long-running backend roles plus two one-shot
# administration commands, by overriding the container command only (the
# image's ENTRYPOINT -- a minimal init process, see below -- is identical
# for every role and is never overridden):
#
#   API              (default CMD): uvicorn atlas.main:app --host 0.0.0.0 --port 8000 --timeout-graceful-shutdown 10
#   Worker:                         python -m atlas.worker
#   Outbox relay:                   python -m atlas.outbox
#   Kafka consumer:                 python -m atlas.consumer
#   Migration (one-shot):           alembic upgrade head
#   Topic administration (one-shot): python -m atlas.outbox.topic_admin
#
# See docs/TECHNICAL_DESIGN.md ("Backend container image, Milestone 14 Slice
# 14A") for the full design rationale (shared-image decision, digest-pinning
# process, non-root/read-only-filesystem behavior, portable PID 1/init
# design, and verification evidence).

# ---------------------------------------------------------------------------
# Tini (minimal init, PID 1) -- fetched from its own upstream GitHub release
# and checksum-verified inline by BuildKit itself (`ADD --checksum=...`), not
# from a third-party-rebuilt image and not via an unverified `curl | sh`-style
# download. This is deliberately an image-level (Dockerfile ENTRYPOINT)
# mechanism rather than a Compose-only `docker run --init`/`init: true`
# setting: Compose's `--init` has no equivalent flag once the same image runs
# under `kind` or AWS EKS (a Kubernetes Pod spec has no "give my container an
# init process" field), so relying on it exclusively would leave the image
# without correct PID 1 signal semantics anywhere except Compose. Building
# the init boundary into the image itself makes it portable across Compose,
# `kind`, and EKS unchanged. Pin source: https://github.com/krallin/tini
# release v0.19.0; checksums below were independently re-verified locally
# (downloaded both architectures' binaries and recomputed their SHA-256
# digests) against the values GitHub publishes for this release, not merely
# copied from the release page. Static binaries: no libc/runtime dependency,
# nothing else installed (no apt, no curl/wget kept in the final image).
# ---------------------------------------------------------------------------
FROM scratch AS tini-amd64
ADD --checksum=sha256:c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee \
    --chmod=755 \
    https://github.com/krallin/tini/releases/download/v0.19.0/tini-static-amd64 /tini

FROM scratch AS tini-arm64
ADD --checksum=sha256:eae1d3aa50c48fb23b8cbdf4e369d0910dfc538566bfd09df89a774aa84a48b9 \
    --chmod=755 \
    https://github.com/krallin/tini/releases/download/v0.19.0/tini-static-arm64 /tini

# TARGETARCH is an automatic BuildKit platform arg ("amd64"/"arm64"); an
# explicit ARG declaration is required to use it in a stage name below.
ARG TARGETARCH
FROM tini-${TARGETARCH} AS tini

# ---------------------------------------------------------------------------
# `uv` is copied from its own pinned, digest-referenced image rather than
# installed via pip/curl, matching the upstream-recommended pattern. The
# version (0.11.8) matches the version already pinned for GitHub Actions in
# .github/workflows/ci.yml, so the same uv release builds and tests the
# project everywhere.
# ---------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:0.11.8@sha256:3b7b60a81d3c57ef471703e5c83fd4aaa33abcd403596fb22ab07db85ae91347 AS uv

# ---------------------------------------------------------------------------
# Builder stage: installs frozen runtime dependencies and the `atlas` package
# itself into a venv. Never shipped to the runtime stage directly -- only
# /app/.venv is copied out of it below.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS builder

COPY --from=uv /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Dependency layer first (cacheable independent of source changes). Excludes
# the [dependency-groups].dev group (Pytest, Ruff, mypy, httpx2) entirely --
# --no-install-project means only third-party dependencies are resolved here,
# not the local `atlas` package itself.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Project layer. README.md is required at this step because pyproject.toml
# declares `readme = "README.md"`, which hatchling (the configured build
# backend) reads while building/installing the local `atlas` package.
COPY README.md ./
COPY src ./src

# --no-editable installs `atlas` as a regular (non-editable) package into
# site-packages inside /app/.venv, so the runtime stage below needs to copy
# only /app/.venv -- not a separate copy of src/ -- to have a fully
# importable `atlas` package.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------------------
# Runtime stage: minimal filesystem, fixed non-root user, no uv/build tools,
# no dev dependencies, no source tree (atlas is fully installed into the
# copied venv), no tests/docs/.env/.git.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS runtime

# Populated at build time (see docs/TECHNICAL_DESIGN.md for the exact build
# command). GIT_SHA and BUILD_DATE both default to "unknown" only so that an
# accidental plain `docker build` without --build-arg still produces a valid,
# clearly-unlabeled image rather than failing -- real builds always pass both.
# BUILD_DATE must be derived from the source commit's own timestamp (e.g.
# `git show -s --format=%cI <sha>`), never the wall-clock time of the build,
# so that rebuilding the same commit reproduces the same label value instead
# of changing the image config (and therefore its digest) on every rebuild.
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.source="https://github.com/Robertgermain/atlas-ai-platform" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.title="atlas-backend" \
      org.opencontainers.image.description="Atlas AI Platform backend (API, worker, outbox relay, Kafka consumer, migrations, topic administration)" \
      org.opencontainers.image.created="${BUILD_DATE}"

# Fixed, documented non-root UID/GID (never auto-assigned) so the identity is
# stable across rebuilds and reviewable in one place.
RUN groupadd --system --gid 10001 atlas \
    && useradd --system --uid 10001 --gid atlas --no-create-home \
       --shell /usr/sbin/nologin atlas

WORKDIR /app

# Only the fully-built venv (third-party deps + the installed `atlas`
# package) and the Alembic migration assets are copied in. No src/, no
# tests/, no docs/, no .git, no .env, no uv binary, no dev dependencies, no
# build cache.
COPY --from=builder --chown=atlas:atlas /app/.venv /app/.venv
COPY --chown=atlas:atlas alembic ./alembic
COPY --chown=atlas:atlas alembic.ini ./alembic.ini
COPY --from=tini /tini /usr/bin/tini

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER atlas

EXPOSE 8000

# Tini is PID 1 for every role (ENTRYPOINT is never overridden); the actual
# application command runs as its direct child and still receives
# SIGTERM/SIGINT/etc. forwarded by Tini, so the worker's, outbox relay's,
# and Kafka consumer's own installed SIGINT/SIGTERM handlers still observe
# and act on the signal exactly as before -- Tini forwards, it does not
# swallow or translate signals. Tini also reaps any zombie child processes,
# which matters because `python -m atlas.worker`/`atlas.outbox`/`atlas.
# consumer` are not themselves PID 1 here and so do not need to (and do not)
# implement their own zombie-reaping. This is exec-form, not shell-form,
# both for ENTRYPOINT and CMD: no shell is ever interposed. This is still
# not a shell entrypoint wrapper -- Tini does no argument processing,
# environment templating, or multi-command orchestration; it only execs its
# argv (`--` separates Tini's own flags from the command to run) and
# forwards signals/reaps zombies. The other five roles are invoked by
# overriding CMD wholesale, exactly as before Tini was added -- ENTRYPOINT
# itself is fixed and identical for every role.
#
# This is the API's default command. `--timeout-graceful-shutdown 10`
# gives uvicorn's own graceful-shutdown window an explicit, documented
# upper bound (uvicorn's undocumented default is 5s if this flag is
# omitted) so the Compose `api` service's `stop_grace_period: 15s`
# (docker-compose.yml) has 5s of margin beyond uvicorn's own worst-case
# shutdown time, not merely whatever uvicorn happens to default to today.
# Other roles override the full command (ENTRYPOINT still applies, so Tini
# remains PID 1 for all of them):
#   docker run <image> python -m atlas.worker
#   docker run <image> python -m atlas.outbox
#   docker run <image> python -m atlas.consumer
#   docker run <image> alembic upgrade head
#   docker run <image> python -m atlas.outbox.topic_admin
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "atlas.main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "10"]
