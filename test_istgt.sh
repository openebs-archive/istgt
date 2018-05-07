#!/bin/bash

DIR=$PWD
SETUP_ISTGT=$DIR/src/setup_istgt.sh
REPLICATION_TEST=$DIR/src/replication_test
MEMPOOL_TEST=$DIR/src/mempool_test
ISCSIADM=iscsiadm
ISTGT_PID=-1
device_name=""

CONTROLLER_IP="127.0.0.1"
CONTROLLER_PORT="6060"

VOL_SIZE="1073741824"
BLOCKLEN="512"

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
			exit;
		else
			continue;
		fi
	done
}

start_istgt() {
	cd $DIR/src
	sh $SETUP_ISTGT &
	ISTGT_PID=$!
	sleep 5
	cd ..
}

stop_istgt() {
	if [ $ISTGT_PID -ne -1 ]; then
		kill -SIGKILL $ISTGT_PID
	fi

}

run_mempool_test()
{
	$MEMPOOL_TEST
	[[ $? -ne 0 ]] && echo "mempool test failed" && exit 1
	return 0
}

write_and_verify_data(){
	login_to_volume "$CONTROLLER_IP:3260"
	sleep 5
	get_scsi_disk
	if [ "$device_name"!="" ]; then
		mkfs.ext2 -F /dev/$device_name
		[[ $? -ne 0 ]] && echo "mkfs failed for $device_name" && exit 1

		mount /dev/$device_name /mnt/store
		[[ $? -ne 0 ]] && echo "mount for $device_name" && exit 1

		dd if=/dev/urandom of=file1 bs=4k count=10000
		hash1=$(md5sum file1 | awk '{print $1}')
		cp file1 /mnt/store
		hash2=$(md5sum /mnt/store/file1 | awk '{print $1}')
		if [ $hash1 == $hash2 ]; then echo "DI Test: PASSED"
		else
			echo "DI Test: FAILED"; exit 1
		fi

		umount /mnt/store
		logout_of_volume
		sleep 5
	else
		echo "Unable to detect iSCSI device, login failed"; exit 1
	fi
}

setup_test_env() {
	rm -f /tmp/test_vol*
	mkdir -p /mnt/store
	truncate -s 1G /tmp/test_vol1 /tmp/test_vol2 /tmp/test_vol3
	truncate -s 10M /tmp/hash_file1 /tmp/hash_file2 /tmp/hash_file3
	logout_of_volume

	start_istgt
}

cleanup_test_env() {
	stop_istgt
	rm -f /tmp/test_vol*
	rm -f /tmp/test_vol*
	rm -rf /mnt/store
}

run_data_integrity_test() {
	local replica1_port="6161"
	local replica2_port="6162"
	local replica3_port="6163"
	local replica1_ip="127.0.0.1"
	local replica2_ip="127.0.0.1"
	local replica3_ip="127.0.0.1"

	setup_test_env

	$REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica1_ip" "$replica1_port" "/tmp/test_vol1" "/tmp/hash_file1" "$VOL_SIZE" "$BLOCKLEN" "1" &
	replica1_pid=$!
	$REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica2_ip" "$replica2_port" "/tmp/test_vol2" "/tmp/hash_file2" "$VOL_SIZE" "$BLOCKLEN" "2" &
	replica2_pid=$!
	$REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica3_ip" "$replica3_port" "/tmp/test_vol3" "/tmp/hash_file3" "$VOL_SIZE" "$BLOCKLEN" "3" &
	replica3_pid=$!

	sleep 15
	write_and_verify_data

	kill -9 $replica1_pid
	sleep 5
	write_and_verify_data

	$REPLICATION_TEST "$CONTROLLER_IP" "$CONTROLLER_PORT" "$replica1_ip" "$replica1_port" "/tmp/test_vol1" "$VOL_SIZE" "$BLOCKLEN" "1" &
	replica1_pid=$!
	sleep 5
	write_and_verify_data

	kill -9 $replica1_pid $replica2_pid $replica3_pid
	cleanup_test_env
}

run_data_integrity_test
run_mempool_test
exit 0
