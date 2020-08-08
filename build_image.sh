#!/bin/bash
# Copyright 2018-2020 The OpenEBS Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

pwd

# Determine the arch/os we're building for
ARCH=$(uname -m)

make clean
bash autogen.sh

if [ "${ARCH}" = "x86_64" ]; then
	./configure --enable-replication
elif [ "${ARCH}" = "aarch64" ]; then
	./configure --enable-replication --build=arm-linux
else
	echo "Unsupported architecture: ${ARCH}"
	exit 1
fi
make clean
make -j$(nproc)

# The images can be pushed to any docker/image registeries
# like docker hub, quay. The registries are specified in 
# the `build/push` script.
#
# The images of a project or company can then be grouped
# or hosted under a unique organization key like `openebs`
#
# Each component (container) will be pushed to a unique 
# repository under an organization. 
# Putting all this together, an unique uri for a given 
# image comprises of:
#   <registry url>/<image org>/<image repo>:<image-tag>
#
# IMAGE_ORG can be used to customize the organization 
# under which images should be pushed. 
# By default the organization name is `openebs`. 

if [ -z "${IMAGE_ORG}" ]; then
  IMAGE_ORG="openebs"
fi

# Specify the date of build
DBUILD_DATE=$(date +'%Y-%m-%dT%H:%M:%SZ')

# Specify the docker arg for repository url
if [ -z "${DBUILD_REPO_URL}" ]; then
  DBUILD_REPO_URL="https://github.com/openebs/istgt"
fi

# Specify the docker arg for website url
if [ -z "${DBUILD_SITE_URL}" ]; then
  DBUILD_SITE_URL="https://openebs.io"
fi

DBUILD_ARGS="--build-arg DBUILD_DATE=${DBUILD_DATE} --build-arg DBUILD_REPO_URL=${DBUILD_REPO_URL} --build-arg DBUILD_SITE_URL=${DBUILD_SITE_URL} --build-arg ARCH=${ARCH}"

if [ "${ARCH}" = "x86_64" ]; then
	REPO_NAME="$IMAGE_ORG/cstor-istgt"
	DOCKERFILE="Dockerfile"
elif [ "${ARCH}" = "aarch64" ]; then
	REPO_NAME="$IMAGE_ORG/cstor-istgt-arm64"
	DOCKERFILE="Dockerfile.arm64"
else
	echo "Unusable architecture: ${ARCH}"
	exit 1
fi

echo "Build image ${REPO_NAME}:ci with BUILD_DATE=${DBUILD_DATE}"

cp src/istgt ./docker
cp src/istgtcontrol ./docker
cp src/istgt.conf ./docker
cp src/istgtcontrol.conf ./docker

sudo docker version
sudo docker build --help

curl --fail https://raw.githubusercontent.com/openebs/charts/gh-pages/scripts/release/buildscripts/push > ./docker/push
chmod +x ./docker/push

cd docker && \
 sudo docker build -f ${DOCKERFILE} -t ${REPO_NAME}:ci ${DBUILD_ARGS} . && \
 DIMAGE=${REPO_NAME} ./push && \
 cd ..
if [ $? -ne 0 ]; then
 echo "Failed to run push script"
 exit 1
fi

rm -rf ./docker/istgt*

