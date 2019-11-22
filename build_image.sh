#!/bin/bash
set -e

pwd

# Determine the arch/os we're building for
ARCH=$(uname -m)
OS=$(uname)

make clean
bash autogen.sh
./configure --enable-replication
make clean
make

BUILD_DATE=$(date +'%Y%m%d%H%M%S')
if [ "${ARCH}" = "x86_64" ]; then
	REPO_NAME="openebs/cstor-istgt"
	DOCKERFILE="Dockerfile"
elif [ "${ARCH}" = "aarch64" ]; then
	REPO_NAME="openebs/cstor-istgt-arm64"
	DOCKERFILE="Dockerfile.arm64"
else
	echo "Unusable architecture: ${ARCH}"
	exit 1
fi

echo "Build image ${REPO_NAME}:ci with BUILD_DATE=${BUILD_DATE}"

cp src/istgt ./docker
cp src/istgtcontrol ./docker
cp src/istgt.conf ./docker
cp src/istgtcontrol.conf ./docker

sudo docker version
sudo docker build --help

cd docker && \
 sudo docker build -f ${DOCKERFILE} -t ${REPO_NAME}:ci --build-arg BUILD_DATE=${BUILD_DATE} . && \
 IMAGE_REPO=${REPO_NAME} ./push && \
 cd ..

rm -rf ./docker/istgt*

