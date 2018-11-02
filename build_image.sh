#!/bin/bash
set -e

pwd
make clean
sh autogen.sh
./configure --enable-replication
make clean
make

BUILD_DATE=$(date +'%Y%m%d%H%M%S')
REPO_NAME="openebs/cstor-istgt"

echo "Build image ${REPO_NAME}:ci with BUILD_DATE=${BUILD_DATE}"

cp src/istgt ./docker
cp src/istgtcontrol ./docker
cp src/istgt.conf ./docker
cp src/istgtcontrol.conf ./docker

sudo docker version
sudo docker build --help

cd docker && \
 sudo docker build -f Dockerfile -t ${REPO_NAME}:ci --build-arg BUILD_DATE=${BUILD_DATE} . && \
 IMAGE_REPO=${REPO_NAME} ./push && \
 cd ..

rm -rf ./docker/istgt*

