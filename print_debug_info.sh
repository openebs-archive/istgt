#!/bin/bash

while sleep 60; do
    echo "=====[ $SECONDS seconds still running ]=====";
    ps -auxwww;
    netstat -napt;
    tail /var/log/syslog
    tail /tmp/istgt.log
    echo "============================================";
done
