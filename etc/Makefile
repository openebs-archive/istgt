# for etc

top_srcdir = ..
srcdir   = .

prefix   = /usr/local
exec_prefix = ${prefix}
bindir   = ${exec_prefix}/bin
sbindir  = ${exec_prefix}/sbin
sysconfdir = ${prefix}/etc
datarootdir = ${prefix}/share
datadir  = ${datarootdir}
libexecdir = ${exec_prefix}/libexec
mandir   = ${datarootdir}/man

configdir = /usr/local/etc/istgt
mediadir = /var/istgt

SED      = /bin/sed
INSTALL  = /usr/bin/install -c
MKDIR_P  = /bin/mkdir -p

source   = 
header   = 
document = 
sample   = auth.conf istgtcontrol.conf \
	istgt.large.conf.in istgt.conf.in \
	istgt.sh.in istgt_netbsd.sh.in istgt_linux.sh.in
rctemplate = istgt_linux.sh.in
rcdir    = /etc/init.d
rcfile   = $(rctemplate:.sh.in=.sh)

DISTDIR = $(top_srcdir)/`cat $(top_srcdir)/distdir`
DISTFILES = Makefile.in \
	$(header) $(source) \
	$(document) $(sample)

#########################################################################

.PHONY: all install install-dirs
all: istgt.large.conf istgt.conf $(rcfile)

istgt.large.conf:
	$(SED) -e 's:%%SYSCONFDIR%%:$(sysconfdir):g' \
		-e 's:%%CONFIGDIR%%:$(configdir):g' \
		-e 's:%%MEDIADIR%%:$(mediadir):g' < istgt.large.conf.in > $@
istgt.conf:
	$(SED) -e 's:%%SYSCONFDIR%%:$(sysconfdir):g' \
		-e 's:%%CONFIGDIR%%:$(configdir):g' \
		-e 's:%%MEDIADIR%%:$(mediadir):g' < istgt.conf.in > $@
$(rcfile):
	$(SED) -e 's:%%SYSCONFDIR%%:$(sysconfdir):g' \
		-e 's:%%CONFIGDIR%%:$(configdir):g' \
		-e 's:%%MEDIADIR%%:$(mediadir):g' \
		-e 's:%%BINDIR%%:$(bindir):g' \
		-e 's:%%SBINDIR%%:$(sbindir):g' < $(rctemplate) > $@

.PHONY: dist clean distclean depend
dist: $(DISTFILES)
	$(MKDIR_P) $(DISTDIR)/$(subdir)
	cd $(srcdir); thisdir=`pwd`; \
	for file in $(DISTFILES); do \
	    cp -p $$thisdir/$$file $(DISTDIR)/$(subdir); \
	done

install: install-dirs
	$(INSTALL) -m 0644 istgt.conf $(DESTDIR)$(sysconfdir)/istgt/istgt.conf.sample
	$(INSTALL) -m 0644 istgt.large.conf \
		$(DESTDIR)$(sysconfdir)/istgt/istgt.large.conf.sample
	$(INSTALL) -m 0600 auth.conf $(DESTDIR)$(sysconfdir)/istgt/auth.conf.sample
	$(INSTALL) -m 0600 istgtcontrol.conf \
		$(DESTDIR)$(sysconfdir)/istgt/istgtcontrol.conf.sample
#	$(INSTALL) -m 0555 istgt.sh $(DESTDIR)$(sysconfdir)/rc.d/istgt
	if [ "x$(rcfile)" != "x" -a -f "$(rcfile)" ]; then \
	    $(INSTALL) -m 0555 $(rcfile) $(DESTDIR)$(rcdir)/istgt; \
	fi

install-dirs:
	$(MKDIR_P) $(DESTDIR)$(sysconfdir)/istgt
	if [ -n "$(rcfile)" ] && [ -f "$(rcfile)" ]; then \
	    if [ -n "$(DESTDIR)$(rcdir)" ] && [ ! -d "$(DESTDIR)$(rcdir)" ]; then \
		$(MKDIR_P) $(DESTDIR)$(rcdir); \
	    fi \
	fi

clean:
	-rm -f istgt.large.conf istgt.conf $(rcfile)
	-rm -f a.out *.o *.core
	-rm -f *~

distclean: clean
	-rm -f Makefile config.status config.cache config.log config.h

depend:

#########################################################################
