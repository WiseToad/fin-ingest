#!/usr/bin/env bash

TARGET="fin-ingest.tar.gz"

PROJECT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"

mkdir -p "${BUILD_DIR}"
tar --owner=root --group=root -czf "${BUILD_DIR}/${TARGET}" -C "${PROJECT_DIR}" --exclude __pycache__ -T "${PROJECT_DIR}/build.files"
