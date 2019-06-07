#
# This Dockerfile builds cstor istgt container running istgt from istgt base image
#

FROM openebs/cstor-ubuntu:xenial-20190515
RUN apt-get update; exit 0
RUN apt-get -y install rsyslog
RUN apt-get -y install curl tcpdump dnsutils net-tools iputils-ping gdb
RUN apt-get -y install apt-utils libssl-dev libjson-c-dev libjemalloc-dev
RUN apt-get -y install apt-file && apt-file update

RUN mkdir -p /usr/local/etc/bkpistgt
RUN mkdir -p /usr/local/etc/istgt
COPY istgt istgtcontrol /usr/local/bin/
COPY istgt.conf istgtcontrol.conf /usr/local/etc/bkpistgt/
RUN touch /usr/local/etc/bkpistgt/auth.conf
RUN touch /usr/local/etc/bkpistgt/logfile

COPY entrypoint-istgtimage.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint-istgtimage.sh

ARG BUILD_DATE
LABEL org.label-schema.name="cstor"
LABEL org.label-schema.description="OpenEBS cstor"
LABEL org.label-schema.url="http://www.openebs.io/"
LABEL org.label-schema.vcs-url="https://github.com/openebs/cstor"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE

ENTRYPOINT entrypoint-istgtimage.sh
EXPOSE 3260 6060
