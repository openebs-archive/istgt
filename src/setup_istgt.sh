#!/bin/bash

run_istgt ()
{
	local volume_size

	if [ $# -eq 1 ]; then
		volume_size=$1
	else
		volume_size=10g
	fi

	ulimit -c unlimited
	rm -rf core
	mkdir -p /usr/local/etc/istgt
	mkdir -p /tmp/cstor
	cp istgt.conf istgtcontrol.conf /tmp/cstor/
	cp istgt.conf istgtcontrol.conf /usr/local/etc/istgt/
	cp istgt.full /usr/local/bin/istgt
	cp istgtcontrol.full /usr/local/bin/istgtcontrol
	ps -aux | grep "\./istgt" | grep -v grep | sudo kill -9 `awk '{print $2}'`
	./init.sh volname=vol1 portal=127.0.0.1 path=/tmp/cstor size=$volume_size externalIP=127.0.0.1 replication_factor=3 consistency_factor=2
}

parent_file=$( basename $0 )
setup_file=$( basename $BASH_SOURCE )
if [ $parent_file == $setup_file ]; then
	run_istgt $*
fi
