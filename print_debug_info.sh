#!/bin/bash

while sleep 30; do
    echo "=====[ $SECONDS seconds still running ]=====";
    ps -auxwww;
    netstat -nap;
    echo "============================================";
done
