#!/usr/bin/env bash
# Rebuild the mlm-dvae training image and push it to Artifact Registry.
#
# The Artifact Registry lives under the techgen314 project, which requires
# authenticating as luka@peltarion.com. This helper switches accounts, runs the
# multi-arch build, pushes the image, and then restores the previous gcloud
# account so subsequent commands keep using the usual king.com identity.
#
# Usage:
#   infra/build_and_push_docker.sh [image_repo_or_tag] [context] [-f dockerfile]
#
# Examples:
#   infra/build_and_push_docker.sh  # Build from repo root with base.Dockerfile
#   infra/build_and_push_docker.sh <image> <exp-dir> -f infra/experiment.Dockerfile
#
# Environment variables:
#   PLATFORM             Defaults to linux/amd64 (sufficient for CE / Vertex)
#   PELTARION_ACCOUNT    Defaults to luka@peltarion.com
#   DOCKER_EXTRA_ARGS    Extra flags appended to docker buildx (e.g. --build-arg ...)
#   DRY_RUN=1            Print commands without building/pushing
set -euo pipefail

IMAGE_ARG=${1:-us-docker.pkg.dev/techgen314/mlm-dvae/mlm-dvae}
BUILD_CONTEXT=${2:-.}
DOCKERFILE=""

# Parse additional arguments
shift 2 2>/dev/null || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--dockerfile)
            DOCKERFILE="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Default dockerfile based on context
if [[ -z "$DOCKERFILE" ]]; then
    if [[ -f "$BUILD_CONTEXT/code/marketlm/__init__.py" ]]; then
        # Looks like an experiment directory
        DOCKERFILE="infra/experiment.Dockerfile"
    else
        DOCKERFILE="infra/base.Dockerfile"
    fi
fi
PLATFORM=${PLATFORM:-linux/amd64}
PELTARION_ACCOUNT=${PELTARION_ACCOUNT:-luka@peltarion.com}
ORIGINAL_ACCOUNT="$(gcloud config get-value account 2>/dev/null || true)"

GIT_SHA=$(git rev-parse HEAD)
GIT_DIRTY=$(git status --porcelain | wc -l | tr -d " \n")
BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [[ "${GIT_DIRTY}" != "0" ]]; then
  echo "Warning: working tree has ${GIT_DIRTY} dirty file(s); image will be labeled git_dirty=${GIT_DIRTY}" >&2
fi

# Derive repo + tag
if [[ "${IMAGE_ARG}" == *:* ]]; then
  IMAGE_REPO=${IMAGE_ARG%:*}
  IMAGE_TAG=${IMAGE_ARG##*:}
else
  IMAGE_REPO=${IMAGE_ARG}
  IMAGE_TAG=latest
fi

IMAGE_LATEST="${IMAGE_REPO}:latest"
IMAGE_SHA="${IMAGE_REPO}:${GIT_SHA}"
IMAGE_EXTRA_TAG=""
if [[ "${IMAGE_TAG}" != "latest" ]]; then
  IMAGE_EXTRA_TAG="${IMAGE_REPO}:${IMAGE_TAG}"
fi

function restore_account {
  if [[ -n "${ORIGINAL_ACCOUNT}" ]]; then
    gcloud config set account "${ORIGINAL_ACCOUNT}" >/dev/null 2>&1 || true
  fi
}
trap restore_account EXIT

if [[ "${ORIGINAL_ACCOUNT}" != "${PELTARION_ACCOUNT}" ]]; then
  echo "Switching gcloud account ${ORIGINAL_ACCOUNT:-<unset>} -> ${PELTARION_ACCOUNT}"
  gcloud config set account "${PELTARION_ACCOUNT}" >/dev/null
fi

echo "Ensuring Artifact Registry helper for us-docker.pkg.dev"
gcloud auth configure-docker us-docker.pkg.dev --quiet >/dev/null

echo "Building ${IMAGE_REPO} (platform ${PLATFORM}); tags: ${IMAGE_LATEST}, ${IMAGE_SHA}"
echo "  Dockerfile: ${DOCKERFILE}"
echo "  Context: ${BUILD_CONTEXT}"

BUILD_CMD=(
  docker buildx build
  --platform "${PLATFORM}"
  -f "${DOCKERFILE}"
  -t "${IMAGE_LATEST}"
  -t "${IMAGE_SHA}"
)

if [[ -n "${IMAGE_EXTRA_TAG}" ]]; then
  BUILD_CMD+=( -t "${IMAGE_EXTRA_TAG}" )
fi

BUILD_CMD+=(
  --label "git_sha=${GIT_SHA}"
  --label "git_dirty=${GIT_DIRTY}"
  --label "build_time=${BUILD_TIME}"
)

if [[ -n "${DOCKER_EXTRA_ARGS:-}" ]]; then
  # Preserve quoting in DOCKER_EXTRA_ARGS while avoiding globbing
  read -r -a EXTRA_ARGS <<<"${DOCKER_EXTRA_ARGS}"
  BUILD_CMD+=( "${EXTRA_ARGS[@]}" )
fi

BUILD_CMD+=( --push "${BUILD_CONTEXT}" )

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  printf 'DRY_RUN=1 — skipping build. Would run:\n%s\n' "${BUILD_CMD[*]}"
  exit 0
fi

"${BUILD_CMD[@]}"

echo "Images pushed: ${IMAGE_LATEST} and ${IMAGE_SHA}"
