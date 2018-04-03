
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

cd src
sudo sh ./setup_istgt.sh &

sleep 5

cd ..
sudo truncate -s 20G test_vol

sudo ./src/replication_test 127.0.0.1 6060 127.0.0.1 6161 &

sleep 15

sudo iscsiadm -m discovery -t st -p 127.0.0.1:3260

sudo iscsiadm -m node -l

sudo iscsiadm -m session -P 3

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

