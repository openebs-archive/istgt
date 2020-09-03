#!/bin/bash

## Copyright © 2018 The OpenEBS Authors
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

ISTGTCONTROL=$PWD/src/istgtcontrol

# if $1 is 0, snapcreate command should fail in all cases
run_snap_commands()
{
	echo "run_snap_commands: "$1
	sudo $ISTGTCONTROL snapcreate vol2 snapname1 0
	if [ $? -ne 1 ]; then
		echo "istgtcontrol snapcreate should fail due to wrong volname"
		exit 1
	fi

	for (( i = 1; i <= 5; i++ )) do
		sudo $ISTGTCONTROL snapcreate vol1 snapname1 0
		if [ $? -ne 1 ]; then
			echo "istgtcontrol snapcreate should fail"
			exit 1
		fi
	done
	for (( j = 2; j <= 5; j++ )) do
		for (( i = 1; i <= 10; i++ )) do
			sudo $ISTGTCONTROL snapcreate vol1 snapname1 1 $j
			if [ $? -ne 1 ]; then
				if [ $1 -eq 0 ]; then
					echo "istgtcontrol snapcreate should fail due to scenario"
					exit 1
				fi
			fi
		done
	done
	for (( i = 1; i <= 5; i++ )) do
		sudo $ISTGTCONTROL snapdestroy vol1 snapname1
		if [ $? -ne 0 ]; then
			echo "istgtcontrol snapdestroy failure"
			exit 1
		fi
	done
}

run_snap_commands $1
exit 0
