[![Build Status](https://travis-ci.org/openebs/istgt.svg?branch=replication)](https://travis-ci.org/openebs/istgt)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fopenebs%2Fistgt.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fopenebs%2Fistgt?ref=badge_shield)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2738/badge)](https://bestpractices.coreinfrastructure.org/projects/2738)
[![Slack](https://img.shields.io/badge/chat!!!-slack-ff1493.svg?style=flat-square)](https://kubernetes.slack.com/messages/openebs)

### Instructions to check cstyle
```sh
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


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fopenebs%2Fistgt.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fopenebs%2Fistgt?ref=badge_large)
