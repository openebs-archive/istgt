#!/bin/bash

CONTROLLER_IP="127.0.0.1"
CONTROLLER_PORT="6060"
REPLICA1_IP="127.0.0.1"
REPLICA2_IP="127.0.0.1"
REPLICA3_IP="127.0.0.1"
REPLICA1_PORT="6161"
REPLICA2_PORT="6162"
REPLICA3_PORT="6163"

login_to_volume() {
	sudo iscsiadm -m discovery -t st -p $1
	sudo iscsiadm -m node -l
}

logout_of_volume() {
	sudo iscsiadm -m node -u
	sudo iscsiadm -m node -o delete
}

get_scsi_disk() {
	device_name=$(sudo iscsiadm -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
	i=0
	while [ -z $device_name ]; do
		sleep 5
		device_name=$(sudo iscsiadm -m session -P 3 |grep -i "Attached scsi disk" | awk '{print $4}')
		i=`expr $i + 1`
		if [ $i -eq 10 ]; then
			echo "scsi disk not found";
			exit;
		else
			continue;
		fi
	done
}

run_data_integrity_test(){
	login_to_volume "$CONTROLLER_IP:3260"
	sleep 5
	get_scsi_disk
	if [ "$device_name"!="" ]; then
		sudo mkfs.ext2 -F /dev/$device_name

		sudo mount /dev/$device_name /mnt/store

		sudo dd if=/dev/urandom of=file1 bs=4k count=10000
		hash1=$(sudo md5sum file1 | awk '{print $1}')
		sudo cp file1 /mnt/store
		hash2=$(sudo md5sum /mnt/store/file1 | awk '{print $1}')
		if [ $hash1 == $hash2 ]; then echo "DI Test: PASSED"
		else
			echo "DI Test: FAILED"; exit 1
		fi

		sudo umount /mnt/store
		logout_of_volume
		sleep 5
	else
		echo "Unable to detect iSCSI device, login failed"; exit 1
	fi
}

prepare_test_env() {
	sudo rm -f /tmp/test_vol*
	sudo mkdir -p /mnt/store
	sudo truncate -s 20G /tmp/test_vol1 /tmp/test_vol2 /tmp/test_vol3
	logout_of_volume
}

prepare_test_env

cd src
sudo sh ./setup_istgt.sh &
controller_pid=$!
cd ..
sleep 15
sudo ./src/replication_test "$CONTROLLER_IP" "$CONTROLLER_PORT" "$REPLICA1_IP" "$REPLICA1_PORT" "/tmp/test_vol1" &
replica1_pid=$!
sudo ./src/replication_test "$CONTROLLER_IP" "$CONTROLLER_PORT" "$REPLICA2_IP" "$REPLICA2_PORT" "/tmp/test_vol2" &
replica2_pid=$!
sudo ./src/replication_test "$CONTROLLER_IP" "$CONTROLLER_PORT" "$REPLICA3_IP" "$REPLICA3_PORT" "/tmp/test_vol3" &
replica3_pid=$!

sleep 15

run_data_integrity_test
sudo kill -9 $replica3_pid
sleep 5
run_data_integrity_test
sudo ./src/replication_test "$CONTROLLER_IP" "$CONTROLLER_PORT" "$REPLICA3_IP" "$REPLICA3_PORT" "/tmp/test_vol3" &
replica3_pid=$!
sleep 5
run_data_integrity_test

sudo kill -9 $controller_pid $replica1_pid $replica2_pid $replica3_pid
ps -aux | grep -w replication_test | grep -v grep | awk '{print "kill -9 "$2}' | sudo sh
ps -aux | grep -w istgt | grep -v grep | awk '{print "kill -9 "$2}' | sudo sh

exit 0
