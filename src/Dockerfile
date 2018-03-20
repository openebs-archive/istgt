FROM openebs/istgt_base:latest
COPY istgt istgtcontrol /usr/local/bin/
COPY istgt.conf istgtcontrol.conf /usr/local/etc/istgt/
RUN mkdir -p /usr/local/etc/istgt
RUN touch /usr/local/etc/istgt/auth.conf
RUN touch /usr/local/etc/istgt/logfile

COPY init.sh /
RUN chmod +x /init.sh
