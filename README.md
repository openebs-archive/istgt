### Instructions to check cstyle
```
Checkout replication branch
Do ./cstyle.pl <filename with path>
```

### Building
```bash
git checkout replication
sudo apt-get install libssl-dev docker.io
sudo apt-get install autoconf
./autogen.sh
./configure --enable-replication
make cstyle
make clean
make
```
### Run target in a container on host network
Copy istgt.conf file to /tmp/cstor
```bash
sudo docker run -d --network=host -v /tmp/cstor:/tmp/cstor openebs/istgt:test /bin/bash ./init.sh volname=vol1 portal=10.128.0.2 path=/tmp/cstor size=10g externalIP=10.128.0.2
```
### Run target as a binary on host network
Update volname, size, portal and externalIP in restart.sh
```bash
sh restart.sh
```
