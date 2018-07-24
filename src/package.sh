#!/bin/bash
set -e

REPO=${REPO:-openebs}
docker build -t ${REPO}/istgt:test .
