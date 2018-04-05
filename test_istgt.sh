
DIR=$PWD
SETUP_ISTGT=$DIR/src/setup_istgt.sh
SPARSE_FILE=$DIR/test_vol
REPLICATION_TEST=$DIR/src/replication_test
ISCSIADM=iscsiadm
ISTGT_PID=-1
REPLICATION_PID=-1

# Wait for iSCSI device node (scsi device) to be created
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

start_istgt() {
	cd $DIR/src
	sudo sh $SETUP_ISTGT &
	ISTGT_PID=$!
	sleep 5
	cd ..

	sudo truncate -s 20G $SPARSE_FILE

	sudo $REPLICATION_TEST 127.0.0.1 6060 127.0.0.1 6161 &
	REPLICATION_PID=$!

	sleep 15
}

stop_istgt() {
	if [ $ISTGT_PID -ne -1 ]; then
		sudo kill -SIGKILL $ISTGT_PID
	fi

	if [ $REPLICATION_PID -ne -1 ]; then
		sudo kill -SIGKILL $REPLICATION_PID
	fi

	sudo rm -rf $SPARSE_FILE
}

run_io_test()
{
	start_istgt

	sudo $ISCSIADM -m discovery -t st -p 127.0.0.1:3260

	sudo $ISCSIADM -m node -l

	sudo $ISCSIADM -m session -P 3

	get_scsi_disk

	echo $device_name

	if [ "$device_name" != "" ]; then
		sudo mkdir -p /mnt/store
		sudo mount /dev/$device_name /mnt/store

		sudo dd if=/dev/urandom of=/mnt/store/file1 bs=4k count=10000
		sudo dd if=/mnt/store/file1 of=/dev/zero bs=4k count=10000

		sudo umount /mnt/store
	fi

	sudo iscsiadm -m node -u

	sudo iscsiadm -m node -o delete

	stop_istgt
}

run_mempool_test()
{
	$DIR/src/mempool_test
	[[ $? -ne 0 ]] && echo "mempool test failed" && exit 1
	return 0
}

run_io_test
run_mempool_test
