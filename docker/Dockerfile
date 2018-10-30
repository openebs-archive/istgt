#
# This Dockerfile builds cstor istgt container running istgt from istgt base image
#

FROM ubuntu:16.04

ARG BUILD_DATE
LABEL org.label-schema.name="cstor"
LABEL org.label-schema.description="OpenEBS cstor"
LABEL org.label-schema.url="http://www.openebs.io/"
LABEL org.label-schema.vcs-url="https://github.com/openebs/cstor"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE

EXPOSE 3260 6060

RUN apt-get update && \
    apt-get install -y rsyslog net-tools iputils-ping libssl-dev libjson-c-dev libjemalloc-dev && \
    mkdir -p /usr/local/etc/bkpistgt /usr/local/etc/istgt && \
    touch /usr/local/etc/bkpistgt/auth.conf && \
    touch /usr/local/etc/bkpistgt/logfile && \
    chmod +x /usr/local/bin/entrypoint-istgtimage.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY istgt istgtcontrol /usr/local/bin/
COPY istgt.conf istgtcontrol.conf /usr/local/etc/bkpistgt/
COPY entrypoint-istgtimage.sh /usr/local/bin/

ENTRYPOINT entrypoint-istgtimage.sh
