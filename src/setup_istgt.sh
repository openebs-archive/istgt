#!/bin/bash

run_istgt ()
{
	local volume_size
	local rf
	local cf

	if [ $# -eq 1 ]; then
		volume_size=$1
	else
		volume_size=10g
	fi

	[ ! -z $REPLICATION_FACTOR ] && rf=$REPLICATION_FACTOR || rf=3
	[ ! -z $CONSISTENCY_FACTOR ] && cf=$CONSISTENCY_FACTOR || cf=2

	ulimit -c unlimited
	rm -rf core
	mkdir -p /usr/local/etc/istgt
	mkdir -p /tmp/cstor
	cp istgt.conf istgtcontrol.conf /tmp/cstor/
	cp istgt.conf istgtcontrol.conf /usr/local/etc/istgt/
	cp istgt /usr/local/bin/istgt
	cp istgtcontrol /usr/local/bin/istgtcontrol
	ps -aux | grep "\./istgt" | grep -v grep | sudo kill -9 `awk '{print $2}'`
	./init.sh volname=vol1 portal=127.0.0.1 size=$volume_size externalIP=127.0.0.1 replication_factor=$rf consistency_factor=$cf
}

parent_file=$( basename $0 )
setup_file=$( basename $BASH_SOURCE )
if [ $parent_file == $setup_file ]; then
	run_istgt $*
fi
