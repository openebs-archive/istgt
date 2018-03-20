ulimit -c unlimited
sudo rm -rf core
sudo mkdir -p /usr/local/etc/istgt
sudo mkdir -p /tmp/cstor
make
sudo cp istgt.conf istgtcontrol.conf /tmp/cstor/
sudo cp istgt.conf istgtcontrol.conf /usr/local/etc/istgt/
sudo cp istgt istgtcontrol /usr/local/bin/
ps -aux | grep "\./istgt" | grep -v grep | sudo kill -9 `awk '{print $2}'`
sudo ./init.sh volname=vol1 portal=192.168.1.16 path=/tmp/cstor size=10g externalIP=192.168.1.16
