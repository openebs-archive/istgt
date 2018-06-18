#!/bin/bash
set -x

_term() { 
	echo "Caught signal!" 
	kill -9 "$child"
}

trap _term SIGTERM SIGINT SIGKILL SIGSTOP

for ARGUMENT in "$@"
do

	KEY=$(echo $ARGUMENT | cut -f1 -d=)
	VALUE=$(echo $ARGUMENT | cut -f2 -d=)

	case "$KEY" in
		volname)		volname=${VALUE} ;;
		portal)			portal=${VALUE} ;;
		path)			path=${VALUE} ;;
		size)			size=${VALUE} ;;
		externalIP)		externalIP=${VALUE} ;;
		replication_factor)	replication_factor=${VALUE} ;;
		consistency_factor)	consistency_factor=${VALUE} ;;
		*)
	esac
done

ifconfig
#portal=`ifconfig | awk '/inet addr/{print substr($2,6)}' | grep -v 127.0.0.1`
CONF_FILE=/tmp/cstor/istgt.conf

if [ $volname == "" ]
then
	echo "volume name not passed"
	echo "Usage: init.sh volname=<volname> portal=<portal> path=<path> size=<size>"
	exit -1
fi

if [ $portal == "" ]
then
	echo "portal not passed"
	echo "Usage: init.sh volname=<volname> portal=<portal> path=<path> size=<size>"
	exit -1
fi

if [ $path == "" ]
then
	echo "Volume path not passed"
	echo "Usage: init.sh volname=<volname> portal=<portal> path=<path> size=<size>"
	exit -1
fi

if [ $size == "" ]
then
	echo "Size not passed"
	echo "Usage: init.sh volname=<volname> portal=<portal> path=<path> size=<size>"
	exit -1
fi

sed -i "s|TargetName.*|TargetName $volname|g" $CONF_FILE
sed -i "s|ReplicationFactor.*|ReplicationFactor $replication_factor|g" $CONF_FILE
sed -i "s|ConsistencyFactor.*|ConsistencyFactor $consistency_factor|g" $CONF_FILE
sed -i "s|TargetAlias.*|TargetAlias nicknamefor-$volname|g" $CONF_FILE
sed -i "s|Portal UC1.*|Portal UC1 $portal:3261|g" $CONF_FILE
sed -i "s|Portal DA1.*|Portal DA1 $portal:3260|g" $CONF_FILE
sed -i "s|Netmask IP.*|Netmask $portal\/8|g" $CONF_FILE
sed -i "s|LUN0 Storage.*|LUN0 Storage $path\/$volname $size 32k|g" $CONF_FILE

cp $CONF_FILE /usr/local/etc/istgt/

mkdir -p $path
touch $path/$volname
truncate -s $size $path/$volname
export externalIP=$externalIP
echo $external
service rsyslog start
#setting replica timeout to 20 seconds
/usr/local/bin/istgt -R 20 &
child=$!
echo "child PID from init script: "$child
wait
