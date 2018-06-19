#!/bin/bash

DIR=$PWD
SETUP_ISTGT=$DIR/src/setup_istgt.sh
REPLICATION_TEST=$DIR/src/replication_test
TEST_SNAPSHOT=$DIR/test_snapshot.sh
MEMPOOL_TEST=$DIR/src/mempool_test
ISTGT_INTEGRATION=$DIR/src/istgt_integration
ISCSIADM=iscsiadm
SETUP_PID=-1
device_name=""

CONTROLLER_IP="127.0.0.1"
CONTROLLER_PORT="6060"

source $SETUP_ISTGT

login_to_volume() {
	$ISCSIADM -m discovery -t st -p $1
	$ISCSIADM -m node -l
}

logout_of_volume() {
	$ISCSIADM -m node -u
	$ISCSIADM -m node -o delete
}

get_scsi_disk() {
	device_name=$($ISCSIADM -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
	i=0
	while [ -z $device_name ]; do
		sleep 5
		device_name=$($ISCSIADM -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
		i=`expr $i + 1`
		if [ $i -eq 10 ]; then
			echo "scsi disk not found";
			tail -20 /var/log/syslog
			exit;
		else
			continue;
		fi
	done
}

start_istgt() {
	cd $DIR/src
	run_istgt $* &
	SETUP_PID=$!
	echo $SETUP_PID
	sleep 5
	cd ..
}

stop_istgt() {
	if [ $SETUP_PID -ne -1 ]; then
		kill -9 $(list_descendants $SETUP_PID)
		kill -9 $SETUP_PID
	fi

}

run_mempool_test()
{
	$MEMPOOL_TEST
	[[ $? -ne 0 ]] && echo "mempool test failed" && tail -20 /var/log/syslog && exit 1
	return 0
}

run_istgt_integration()
{
	export externalIP=127.0.0.1
	echo $externalIP
	$ISTGT_INTEGRATION
	[[ $? -ne 0 ]] && echo "istgt integration test failed" && tail -20 /var/log/syslog && exit 1
	rm -f /tmp/test_vol*
	return 0
}

write_and_verify_data(){
	login_to_volume "$CONTROLLER_IP:3260"
	sleep 5
	get_scsi_disk
	if [ "$device_name"!="" ]; then
		mkfs.ext2 -F /dev/$device_name
		[[ $? -ne 0 ]] && echo "mkfs failed for $device_name" && tail -20 /var/log/syslog && exit 1

		mount /dev/$device_name /mnt/store
		[[ $? -ne 0 ]] && echo "mount for $device_name" && tail -20 /var/log/syslog && exit 1

		dd if=/dev/urandom of=file1 bs=4k count=10000
		hash1=$(md5sum file1 | awk '{print $1}')
		cp file1 /mnt/store
		hash2=$(md5sum /mnt/store/file1 | awk '{print $1}')
		if [ $hash1 == $hash2 ]; then echo "DI Test: PASSED"
		else
			rm file1
			echo "DI Test: FAILED";
			tail -20 /var/log/syslog
			exit 1
		fi
		rm file1

		umount /mnt/store
		logout_of_volume
		sleep 5
	else
		echo "Unable to detect iSCSI device, login failed";
		tail -20 /var/log/syslog
		exit 1
	fi
}

write_data()
{
	local offset=$1
	local len=$2
	local base=$3
	local output1=$4
	local output2=$5

	seek=$(( $offset / $base ))
	count=$(( $len / $base ))

	dd if=/dev/urandom | tr -dc 'a-zA-Z0-9'| head  -c $len  | \
	    tee >(dd of=$output1 conv=notrunc bs=$base count=$count seek=$seek oflag=direct) | \
	    (dd of=$output2 conv=notrunc bs=$base count=$count seek=$seek oflag=direct)
}

setup_test_env() {
	rm -f /tmp/test_vol*
	mkdir -p /mnt/store
	truncate -s 5G /tmp/test_vol1 /tmp/test_vol2 /tmp/test_vol3
	logout_of_volume

	start_istgt 5G
}

cleanup_test_env() {
	stop_istgt
	rm -rf /mnt/store
}

wait_for_pids()
{
	for p in "$@"; do
		wait $p
		status=$?
		if [ $status -ne 0 ] && [ $status -ne 127 ]; then
			tail -20 /var/log/syslog
			exit 1
		fi
	done
}

list_descendants ()
{
	local children=$(ps -o pid= --ppid "$1")

	for pid in $children
	do
		list_descendants "$pid"
	done

	echo "$children"
}

run_data_integrity_test() {
	local replica1_port="6161"
	local replica2_port="6162"
	local replica3_port="6163"
	local replica1_ip="127.0.0.1"
	local replica2_ip="127.0.0.1"
	local replica3_ip="127.0.0.1"

	setup_test_env
	$TEST_SNAPSHOT 0

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V "/tmp/test_vol1" &
	replica1_pid=$!
	$TEST_SNAPSHOT 0

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica2_ip" -P "$replica2_port" -V "/tmp/test_vol2" &
	replica2_pid=$!
	$TEST_SNAPSHOT 0

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica3_ip" -P "$replica3_port" -V "/tmp/test_vol3" &
	replica3_pid=$!
	sleep 15

	$TEST_SNAPSHOT 1 &
	test_snapshot_pid=$!

	write_and_verify_data
	sleep 50
	wait_for_pids $test_snapshot_pid

	$TEST_SNAPSHOT 1

	pkill -9 -P $replica1_pid
	kill -SIGKILL $replica1_pid
	sleep 5
	write_and_verify_data

	#sleep is required for more than 60 seconds, as status messages are sent every 60 seconds
	sleep 65
	ps -auxwww
	ps -o pid,ppid,command
	$TEST_SNAPSHOT 0

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V "/tmp/test_vol1" &
	replica1_pid=$!
	sleep 5
	write_and_verify_data
	$TEST_SNAPSHOT 1

	sleep 65
	ps -auxwww
	ps -o pid,ppid,command
	$TEST_SNAPSHOT 1

	pkill -9 -P $replica1_pid

	# test replica IO timeout
	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V "/tmp/test_vol1" -n 500&
	replica1_pid1=$!
	sleep 5
	write_and_verify_data
	sleep 5
	write_and_verify_data
	wait $replica1_pid1
	if [ $? == 0 ]; then
		echo "Replica timeout failed"
		exit 1
	else
		echo "Replica timeout passed"
	fi

	pkill -9 -P $replica1_pid1
	pkill -9 -P $replica2_pid
	pkill -9 -P $replica3_pid

	kill -SIGKILL $replica1_pid
	kill -SIGKILL $replica1_pid1
	kill -SIGKILL $replica2_pid
	kill -SIGKILL $replica3_pid

	cleanup_test_env

	ps -auxwww
	ps -o pid,ppid,command

}

run_read_consistency_test ()
{
	local replica1_port="6161"
	local replica2_port="6162"
	local replica3_port="6163"
	local replica1_ip="127.0.0.1"
	local replica2_ip="127.0.0.1"
	local replica3_ip="127.0.0.1"
	local replica1_vdev="/tmp/test_vol1"
	local replica2_vdev="/tmp/test_vol2"
	local replica3_vdev="/tmp/test_vol3"
	local file_name="/root/data_file"
	local device_file="/root/device_file"
	local w_pid

	# Test to check if replication module is not initialized then
	# istgtcontrol should return an error
	export ReplicationDelay=40
	setup_test_env
	sleep 2
	istgtcontrol status >/dev/null  2>&1
	if [ $? -ne 0 ]; then
		echo "ISTGTCONTROL returned error as replication module not initialized"
	else
		echo "ISTGTCONTROL returned success .. something went wrong"
		stop_istgt
		exit 1
	fi

	unset ReplicationDelay

	sleep 60
	rm -rf $file_name $device_file

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V $replica1_vdev &
	replica1_pid=$!

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica2_ip" -P "$replica2_port" -V $replica2_vdev  &
	replica2_pid=$!

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica3_ip" -P "$replica3_port" -V $replica3_vdev &
	replica3_pid=$!
	sleep 5

	login_to_volume "$CONTROLLER_IP:3260"
	sleep 5

	get_scsi_disk
	if [ "$device_name" == "" ]; then
		echo "error happened while running read consistency test"
		kill -9 $replica1_pid $replica2_pid $replica3_pid
		return
	fi

	write_data 0 104857600 512 "/dev/$device_name" $file_name
	sync

	write_data 0 31457280 4096 "/dev/$device_name" $file_name &
	w_pid=$!
	sleep 1
	kill -9 $replica1_pid
	wait $w_pid
	sync

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V $replica1_vdev -d &
	replica1_pid=$!
	sleep 5
	write_data 39845888 31457280 4096 "/dev/$device_name" $file_name &
	w_pid=$!
	sleep 1
	kill -9 $replica2_pid
	wait $w_pid
	sync

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica2_ip" -P "$replica2_port" -V $replica2_vdev -d &
	replica2_pid=$!
	sleep 5
	write_data 71303168 31457280 4096 "/dev/$device_name" $file_name &
	w_pid=$!
	sleep 1
	kill -9 $replica3_pid
	wait $w_pid
	sync

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica3_ip" -P "$replica3_port" -V $replica3_vdev -d &
	replica3_pid=$!
	sleep 5

	dd if=/dev/$device_name of=$device_file bs=4096 iflag=direct oflag=direct count=25600
	diff $device_file $file_name >> /dev/null 2>&1
	if [ $? -ne 0 ]; then
		echo "read consistency test failed"
		tail -50 /var/log/syslog
	else
		echo "read consistency test passed"
	fi

	logout_of_volume
	kill -9 $replica1_pid $replica2_pid $replica3_pid
	rm -rf ${replica1_vdev}* ${replica2_vdev}* ${replica3_vdev}*
	rm -rf $file_name $device_file
	stop_istgt
}

run_replication_factor_test()
{
	local replica1_port="6161"
	local replica2_port="6162"
	local replica3_port="6163"
	local replica4_port="6164"
	local replica1_ip="127.0.0.1"
	local replica2_ip="127.0.0.1"
	local replica3_ip="127.0.0.1"
	local replica4_ip="127.0.0.1"
	local replica1_vdev="/tmp/test_vol1"
	local ret=0

	setup_test_env

	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica1_ip" -P "$replica1_port" -V $replica1_vdev &
	replica1_pid=$!
	sleep 2	#Replica will take some time to make successful connection to target

	# As long as we are not running any IOs we can use the same vdev file
	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica2_ip" -P "$replica2_port" -V $replica1_vdev  &
	replica2_pid=$!
	sleep 2

	# As long as we are not running any IOs we can use the same vdev file
	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica3_ip" -P "$replica3_port" -V $replica1_vdev &
	replica3_pid=$!
	sleep 2

	# As long as we are not running any IOs we can use the same vdev file
	$REPLICATION_TEST -i "$CONTROLLER_IP" -p "$CONTROLLER_PORT" -I "$replica4_ip" -P "$replica4_port" -V $replica1_vdev &
	replica4_pid=$!
	sleep 5

	wait $replica4_pid
	if [ $? == 0 ]; then
		echo "replica limit test failed"
		kill -9 $replica4_pid
		ret=1
	else
		echo "replica limit test passed"
	fi

	kill -9 $replica1_pid $replica2_pid $replica3_pid
	stop_istgt
	rm -rf ${replica1_vdev}*

	if [ $ret == 1 ]; then
		exit 1
	fi
}

run_data_integrity_test
run_mempool_test
run_istgt_integration
run_read_consistency_test
run_replication_factor_test

tail -20 /var/log/syslog

exit 0
