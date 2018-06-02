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

login_to_volume() {
	sudo $ISCSIADM -m discovery -t st -p $1
	sudo $ISCSIADM -m node -l
}

logout_of_volume() {
	sudo $ISCSIADM -m node -u
	sudo $ISCSIADM -m node -o delete
}

get_scsi_disk() {
	device_name=$(sudo $ISCSIADM -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
	i=0
	while [ -z $device_name ]; do
		sleep 5
		device_name=$(sudo $ISCSIADM -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
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
	sudo sh $SETUP_ISTGT &
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
	sudo rm -f /tmp/test_vol*
	return 0
}

write_and_verify_data(){
	login_to_volume "$CONTROLLER_IP:3260"
	sleep 5
	get_scsi_disk
	if [ "$device_name"!="" ]; then
		sudo mkfs.ext2 -F /dev/$device_name
		[[ $? -ne 0 ]] && echo "mkfs failed for $device_name" && tail -20 /var/log/syslog && exit 1

		sudo mount /dev/$device_name /mnt/store
		[[ $? -ne 0 ]] && echo "mount for $device_name" && tail -20 /var/log/syslog && exit 1

		sudo dd if=/dev/urandom of=file1 bs=4k count=10000
		hash1=$(sudo md5sum file1 | awk '{print $1}')
		sudo cp file1 /mnt/store
		hash2=$(sudo md5sum /mnt/store/file1 | awk '{print $1}')
		if [ $hash1 == $hash2 ]; then echo "DI Test: PASSED"
		else
			echo "DI Test: FAILED";
			tail -20 /var/log/syslog
			exit 1
		fi

		sudo umount /mnt/store
		logout_of_volume
		sleep 5
	else
		echo "Unable to detect iSCSI device, login failed";
		tail -20 /var/log/syslog
		exit 1
	fi
}

setup_test_env() {
	sudo rm -f /tmp/test_vol*
	sudo mkdir -p /mnt/store
	sudo truncate -s 20G /tmp/test_vol1 /tmp/test_vol2 /tmp/test_vol3
	logout_of_volume

	start_istgt
}

cleanup_test_env() {
	stop_istgt
	sudo rm -rf /mnt/store
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

	sudo $REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica1_ip" "$replica1_port" "/tmp/test_vol1" &
	replica1_pid=$!
	$TEST_SNAPSHOT 0

	sudo $REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica2_ip" "$replica2_port" "/tmp/test_vol2" &
	replica2_pid=$!
	$TEST_SNAPSHOT 0

	sudo $REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica3_ip" "$replica3_port" "/tmp/test_vol3" &
	replica3_pid=$!
	sleep 15

	$TEST_SNAPSHOT 1 &
	test_snapshot_pid=$!

	write_and_verify_data
	sleep 50
	wait_for_pids $test_snapshot_pid

	$TEST_SNAPSHOT 1

	sudo pkill -9 -P $replica1_pid
	sudo kill -SIGKILL $replica1_pid
	sleep 5
	write_and_verify_data

	#sleep is required for more than 60 seconds, as status messages are sent every 60 seconds
	sleep 65
	ps -auxwww
	ps -o pid,ppid,command
	$TEST_SNAPSHOT 0

	sudo $REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica1_ip" "$replica1_port" "/tmp/test_vol1" &
	replica1_pid=$!
	sleep 5
	write_and_verify_data
	$TEST_SNAPSHOT 1

	sleep 65
	ps -auxwww
	ps -o pid,ppid,command
	$TEST_SNAPSHOT 1

	sudo pkill -9 -P $replica1_pid
	sudo pkill -9 -P $replica2_pid
	sudo pkill -9 -P $replica3_pid

	sudo kill -SIGKILL $replica1_pid
	sudo kill -SIGKILL $replica2_pid
	sudo kill -SIGKILL $replica3_pid
	cleanup_test_env

	ps -auxwww
	ps -o pid,ppid,command

}

run_data_integrity_test
run_mempool_test
run_istgt_integration

tail -20 /var/log/syslog

exit 0
