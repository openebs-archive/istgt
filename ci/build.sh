#!/bin/bash
# Copyright 2019-2020 The OpenEBS Authors. All rights reserved.
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

# set -ex

sudo modprobe iscsi_tcp

# Setup installation before running the tests

sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update -qq
sudo apt-get install --yes -qq gcc-6 g++-6 gdb
sudo apt-get install automake libssl-dev open-iscsi libjson-c-dev ioping jq net-tools
# use gcc-6 by default
sudo unlink /usr/bin/gcc && sudo ln -s /usr/bin/gcc-6 /usr/bin/gcc
sudo unlink /usr/bin/g++ && sudo ln -s /usr/bin/g++-6 /usr/bin/g++

./autogen.sh
# we are running build two times. One is without replication code and
# another one is with replication code. The reason behind running build
# without replication code is to make sure that replication feature
# related code doesn't break the existing target code.
./configure CC="gcc-6" CXX="gcc-6"
make
make clean
ARCH=$(uname -m)
echo ARCH "$ARCH"
if [ "${ARCH}" = "x86_64" ]; then
    ./configure CC="gcc-6" CXX="gcc-6" --enable-debug --enable-replication
elif [ "${ARCH}" = "aarch64" ]; then
    ./configure CC="gcc-6" CXX="gcc-6" --enable-debug --enable-replication --build=arm-linux
fi

make