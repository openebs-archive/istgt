#!/bin/bash

while sleep 30; do
    echo "=====[ $SECONDS seconds still running ]=====";
    ps -auxwww;
    netstat -napt;
    tail /var/log/syslog
    echo "============================================";
done
