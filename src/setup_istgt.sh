ulimit -c unlimited
sudo rm -rf core
sudo mkdir -p /usr/local/etc/istgt
sudo mkdir -p /tmp/cstor
make clean
make
sudo cp istgt.conf istgtcontrol.conf /tmp/cstor/
sudo cp istgt.conf istgtcontrol.conf /usr/local/etc/istgt/
sudo cp istgt istgtcontrol /usr/local/bin/
ps -aux | grep "\./istgt" | grep -v grep | sudo kill -9 `awk '{print $2}'`
sudo ./init.sh volname=vol1 portal=127.0.0.1 path=/tmp/cstor size=10g externalIP=127.0.0.1 replication_factor=3 consistency_factor=2
