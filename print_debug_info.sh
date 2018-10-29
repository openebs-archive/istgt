#!/bin/bash

while sleep 60; do
    echo "=====[ $SECONDS seconds still running ]=====";
    ps -auxwww;
    netstat -napt;
    tail -20 /var/log/syslog
    tail -20 /tmp/istgt.log
    tail -20 /tmp/integration_test.log
    echo "============================================";
done
