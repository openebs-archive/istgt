sudo sh ./src/setup_istgt.sh &

sleep 5

sudo truncate -s 20G test_vol

sudo ./src/replication_test 127.0.0.1 6060 127.0.0.1 6161 &

sleep 5

sudo iscsiadm -m discovery -t st -p 127.0.0.1:3260

sudo iscsiadm -m node -l

