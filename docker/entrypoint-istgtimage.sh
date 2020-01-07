#!/bin/sh

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
        ## Remove hardcoded value. Take it via ENV
	cd /var/openebs/sparse || exit
fi

ARCH=$(uname -m)
export LD_PRELOAD=/usr/lib/${ARCH}-linux-gnu/libjemalloc.so
exec /usr/local/bin/istgt

