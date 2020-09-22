# Copyright 2020 The OpenEBS Authors. All rights reserved.
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

set -ex

apt-get update -qq
apt-get install --yes -qq gcc-6 g++-6 gdb wget dh-autoreconf
apt-get install --yes libssl-dev open-iscsi libjson-c-dev ioping jq net-tools
# use gcc-6 by default
unlink /usr/bin/gcc && ln -s /usr/bin/gcc-6 /usr/bin/gcc
unlink /usr/bin/g++ && ln -s /usr/bin/g++-6 /usr/bin/g++

cd istgt

# Determine the arch/os we're building for
ARCH=$(uname -m)

bash autogen.sh

if [ "${ARCH}" = "x86_64" ]; then
	./configure --enable-replication
elif [ "${ARCH}" = "aarch64" ]; then
	./configure --enable-replication --build=arm-linux
elif [ "${ARCH}" = "ppc64le" ]; then
	./configure --enable-replication --build=ppc-linux
else
	echo "Unsupported architecture: ${ARCH}"
	exit 1
fi
make clean
make -j4;

cp src/istgt ./docker
cp src/istgtcontrol ./docker
cp src/istgt.conf ./docker
cp src/istgtcontrol.conf ./docker
