## Copyright © 2018-2020 The OpenEBS Authors
## 
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
## 
##     http://www.apache.org/licenses/LICENSE-2.0
## 
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

#
# This Dockerfile builds cstor istgt container running istgt from istgt base image
#

FROM openebs/cstor-ubuntu:bionic-20200219

RUN apt-get update && \
    apt-get install -y \
    rsyslog \
    curl \
    tcpdump \
    dnsutils \
    net-tools \
    iputils-ping \
    gdb \
    apt-utils \
    libssl-dev \
    libjson-c-dev \
    libjemalloc-dev \
    apt-file && apt-file update \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/etc/bkpistgt
RUN mkdir -p /usr/local/etc/istgt
COPY istgt istgtcontrol /usr/local/bin/
COPY istgt.conf istgtcontrol.conf /usr/local/etc/bkpistgt/
RUN touch /usr/local/etc/bkpistgt/auth.conf
RUN touch /usr/local/etc/bkpistgt/logfile

COPY entrypoint-istgtimage.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint-istgtimage.sh

ARG ARCH
ARG DBUILD_DATE
ARG DBUILD_REPO_URL
ARG DBUILD_SITE_URL

LABEL org.label-schema.name="istgt"
LABEL org.label-schema.description="OpenEBS istgt"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$DBUILD_DATE
LABEL org.label-schema.vcs-url=$DBUILD_REPO_URL
LABEL org.label-schema.url=$DBUILD_SITE_URL

ENTRYPOINT entrypoint-istgtimage.sh
EXPOSE 3260 6060
