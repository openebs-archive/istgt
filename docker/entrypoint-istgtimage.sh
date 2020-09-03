#!/bin/sh

## Copyright © 2020 The OpenEBS Authors
## 
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
## 
##     http://www.apache.org/licenses/LICENSE-2.0
## 
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

set -o errexit
trap 'call_exit $LINE_NO' EXIT

call_exit()
{
echo "at call_exit.."
echo  "exit code:" $?
echo "reference: "  $0
}

if [ ! -f "/usr/local/etc/istgt/istgt.conf" ];then
	cp /usr/local/etc/bkpistgt/istgt.conf /usr/local/etc/istgt/
	sed -i -n '/LogicalUnit section/,$!p' /usr/local/etc/istgt/istgt.conf
fi
cp /usr/local/etc/bkpistgt/istgtcontrol.conf /usr/local/etc/istgt/
touch /usr/local/etc/istgt/auth.conf
touch /usr/local/etc/istgt/logfile
export externalIP=0.0.0.0
service rsyslog start

# Disabling coredumps by default in the shell where istgt runs
if [ -z "$ENABLE_COREDUMP" ]; then
	echo "Disabling dumping core"
	ulimit -c 0
else
	echo "Enabling coredumps"
	ulimit -c unlimited
	## /var/openebs is mounted as persistent directory on
	## host machine
	cd /var/openebs/cstor-target || exit
	mkdir -p core
	cd core
fi

ARCH=$(uname -m)
export LD_PRELOAD=/usr/lib/${ARCH}-linux-gnu/libjemalloc.so
exec /usr/local/bin/istgt

