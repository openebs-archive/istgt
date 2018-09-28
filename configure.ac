#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.62])
AC_INIT([istgt], [0.5])
AC_CONFIG_SRCDIR([src/istgt.c])
AC_CONFIG_HEADERS([src/config.h])

# Checks for programs.
AC_PROG_CC([clang gcc cc])
AC_PROG_INSTALL
AC_PROG_RANLIB
AC_PROG_SED
AC_PROG_MKDIR_P

# clagn with -O3
if test "$CC" = "clang" && test "$ac_test_CFLAGS" != "set"; then
  CFLAGS="-g -O3 -DNDEBUG"
fi

# host OS related
vboxinc="/usr/local/src/virtualbox/*/include"
vboxlib="/usr/local/lib/virtualbox"
AC_SUBST([rctemplate], [""])
AC_SUBST([rcdir], ['${prefix}/etc/rc.d'])
AC_SUBST([target_source_files], ['${istgt_source}'])
AC_SUBST([target_header_files], ['${istgt_header}'])

AC_CANONICAL_HOST
case "$host_os" in
*linux*)
	tmp="/usr/src/virtualbox/*/include"
	if test -d "`echo $tmp`"; then
	    vboxinc=$tmp
	fi
	vboxlib="/usr/lib/virtualbox"
	AC_SUBST([rctemplate], ["istgt_linux.sh.in"])
	AC_SUBST([rcdir], ['/etc/init.d'])
	AC_DEFINE([_GNU_SOURCE], 1, [Define if linux host])
	;;
*netbsd*)
	AC_SUBST([rctemplate], ["istgt_netbsd.sh.in"])
	AC_SUBST([rcdir], ['/etc/rc.d'])
	;;
*freebsd*)
	tmp="/usr/ports/emulators/virtualbox-ose/work/*/include"
	if test -d "`echo $tmp`"; then
	    vboxinc=$tmp
	else
	    tmp="/usr/ports/emulators/virtualbox-ose-legacy/work/*/include"
	    if test -d "`echo $tmp`"; then
		vboxinc=$tmp
	    fi
	fi
	vboxlib="/usr/local/lib/virtualbox"
	AC_SUBST([rctemplate], ["istgt.sh.in"])
	AC_SUBST([rcdir], ['${prefix}/etc/rc.d'])
	if test "$mandir" = '${datarootdir}/man'; then
	    AC_SUBST([mandir], ['${prefix}/man'])
	fi
	;;
esac

# Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_create])
AC_CHECK_LIB([crypto], [MD5_Update])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/ioctl.h sys/param.h sys/socket.h sys/time.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_STRUCT_ST_BLOCKS
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([ftruncate memset realpath socket strcasecmp strchr strncasecmp strspn strtol strtoull])

# check compatibility
AC_SYS_LARGEFILE
AC_CHECK_HEADERS([aio.h sched.h uuid.h sys/disk.h sys/disklabel.h])
AC_CHECK_HEADERS([pthread.h])
AC_CHECK_HEADERS([pthread_np.h], [], [],
[#if HAVE_PTHREAD_H
#include <pthread.h>
#endif
])
AC_CHECK_FUNCS([strlcpy arc4random srandomdev pthread_yield sched_yield])
AC_CHECK_FUNCS([pthread_set_name_np setproctitle])
AC_SUBST([MKDEP])
AC_PATH_PROG([MKDEP], ["mkdep"])
#if test -z "$MKDEP"; then
#   AC_PATH_PROG([MKDEP], ["makedepend"])
#fi

# paths
configdir="$ac_default_prefix/etc/istgt"
mediadir="/var/istgt"
if test "$prefix" != "NONE"; then
  configdir="$prefix/etc/istgt"
fi
AC_ARG_WITH([configdir],
  AS_HELP_STRING([--with-configdir=DIR], [(default PREFIX/etc/istgt)]),
  [case $withval in
    yes|no)
        ;;
    *)
        configdir="$withval"
        ;;
  esac])
AC_SUBST([configdir])
AC_ARG_WITH([mediadir],
  AS_HELP_STRING([--with-mediadir=DIR], [(default /var/istgt)]),
  [case $withval in
    yes|no)
        ;;
    *)
        mediadir="$withval"
        ;;
  esac])
AC_SUBST([mediadir])

# startup script directory
AC_ARG_WITH([rcdir],
  AS_HELP_STRING([--with-rcdir=DIR], [(default PREFIX/etc/rc.d)]),
  [case $withval in
    yes|no)
        ;;
    *)
        rcdir="$withval"
        ;;
  esac])
AC_SUBST([rcdir])

# test for -Wall
saved_CFLAGS="$CFLAGS"
AC_MSG_CHECKING([whether the C compiler accepts -Wall])
CFLAGS="$CFLAGS -Wall"
AC_TRY_COMPILE(,,
  AC_MSG_RESULT(yes); saved_CFLAGS="$CFLAGS",
  AC_MSG_RESULT(no))
CFLAGS="$saved_CFLAGS"

# test for -Wextra
saved_CFLAGS="$CFLAGS"
AC_MSG_CHECKING([whether the C compiler accepts -Wextra])
CFLAGS="$CFLAGS -Wextra"
AC_TRY_COMPILE(,,
  AC_MSG_RESULT(yes); saved_CFLAGS="$CFLAGS",
  AC_MSG_RESULT(no))
CFLAGS="$saved_CFLAGS"

# check enable features
AC_MSG_CHECKING([whether to dump packet data (tracedump mode)])
AC_ARG_ENABLE([tracedump],
  AS_HELP_STRING([--enable-tracedump], [enable tracedump(logging)]),
  AC_MSG_RESULT(yes); AC_DEFINE([TRACEDUMP], 1, [Define if enable tracedump]),
  AC_MSG_RESULT(no))
AC_SUBST([TRACEDUMP])

AC_MSG_CHECKING([whether to enable debug mode])
AC_ARG_ENABLE([debug],
  AS_HELP_STRING([--enable-debug], [enable debug]),
  AC_MSG_RESULT(yes); AC_DEFINE([DEBUG], 1, [Define if enable debug])
  CFLAGS="-DDEBUG -Werror -ggdb3 -fstack-check",
  AC_MSG_RESULT(no))
AC_SUBST([DEBUG])

AC_MSG_CHECKING([whether to enable replication mode])
AC_ARG_ENABLE([replication],
  AS_HELP_STRING([--enable-replication], [enable replication feature]),
  AC_MSG_RESULT(yes); AC_DEFINE([REPLICATION], 1, [Define if enable replication])
  LIBS="$LIBS -lm -ljson-c"
  AC_SUBST([target_source_files], ['${istgt_source} ${replication_source}'])
  AC_SUBST([target_header_files], ['${istgt_header} ${replication_header}'])
  AC_MSG_NOTICE([fetching zrepl_prot.h file...])
  AC_SUBST([replication_bin], ['istgt_integration replication_test mempool_test'])
  AS_IF([$( cp /tmp/zrepl_prot.h src/zrepl_prot.h )], , [AC_MSG_ERROR([failed to fetch zrepl_prot.h])]),
  AC_MSG_RESULT(no)
  AC_SUBST([replication_bin], ['']))
AC_SUBST([REPLICATION])

AC_MSG_CHECKING([whether to use symlink device])
AC_ARG_ENABLE([symlink-device],
  AS_HELP_STRING([--enable-symlink-device], [allow symlink device]),
  AC_MSG_RESULT(yes); AC_DEFINE([ALLOW_SYMLINK_DEVICE], 1, [Define if enable symlink-device]),
  AC_MSG_RESULT(no))
AC_SUBST([ALLOW_SYMLINK_DEVICE])

AC_MSG_CHECKING([which syslog facility to use])
AC_ARG_WITH([logfacility],
  AS_HELP_STRING([--with-logfacility], [syslog facility to log with (default "local7")]),
  [case $with_logfacility in
    daemon|auth|authpriv|local0|local1|local2|local3|local4|local5|local6|local7)
        logfacility=$with_logfacility
        ;;
    *)  AC_MSG_ERROR(["unsupported facility $with_logfacility"])
        ;;
  esac],
  [logfacility="local7"])
AC_DEFINE_UNQUOTED([DEFAULT_LOG_FACILITY], ["$logfacility"], [syslog facility])
AC_MSG_RESULT("$logfacility")

AC_MSG_CHECKING([which syslog priority to use])
AC_ARG_WITH([logpriority],
  AS_HELP_STRING([--with-logpriority], [syslog priority to log with (default "info")]),
  [case $with_logpriority in
    emerg|alert|crit|err|warning|notice|info|debug)
        logpriority=$with_logpriority
        ;;
    *)  AC_MSG_ERROR(["unsupported priority $with_logpriority"])
        ;;
  esac],
  [logpriority="info"])
AC_DEFINE_UNQUOTED([DEFAULT_LOG_PRIORITY], ["$logpriority"], [syslog priority])
AC_MSG_RESULT("$logpriority")

#AC_MSG_CHECKING([whether to use asynchronous io])
#AC_ARG_WITH([aio],
#  AS_HELP_STRING([--with-aio], [use aio (default "no")]),
#  [case $with_aio in
#    yes)
#        AC_DEFINE([USE_AIO], 1, [use aio])
#        AC_MSG_RESULT(yes)
#        ;;
#    *)
#        AC_MSG_RESULT(no)
#        ;;
#  esac],
#  AC_MSG_RESULT(no))
#AC_SUBST([USE_AIO])

# vbox support
vbox="no"
AC_MSG_CHECKING([VirtualBox Virtual Disk support])
AC_ARG_WITH([vbox],
  AS_HELP_STRING([--with-vbox=INC], [VirtualBox include]),
  [case $with_vbox in
     no)
         AC_MSG_RESULT([no])
         ;;
     yes|*)
         AC_MSG_RESULT([yes])
         vbox="yes"
         if test "$with_vbox" != "yes"; then
             vboxinc="$with_vbox"
         fi
         ;;
   esac],
  [AC_MSG_RESULT([no])])
AC_ARG_WITH([vboxlib],
  AS_HELP_STRING([--with-vboxlib=LIB], [VirtualBox lib]),
  [case $with_vboxlib in
     no)
         ;;
     yes|*)
         vbox="yes"
         if test "$with_vboxlib" != "yes"; then
             vboxlib="$with_vboxlib"
         fi
         ;;
   esac],
  [])
# vbox support yes?
if test "$vbox" = "yes"; then
    vboxinc=`echo $vboxinc`
    vboxlib=`echo $vboxlib`
    if test ! -d "$vboxinc"; then
        AC_MSG_ERROR([not directory: $vboxinc])
    fi
    if test ! -d "$vboxlib"; then
        AC_MSG_ERROR([not directory: $vboxlib])
    fi
    CPPFLAGS="$CPPFLAGS -I$vboxinc"
    LDFLAGS="$LDFLAGS -Wl,-rpath,$vboxlib"
    LIBS="$LIBS $vboxlib/VBoxDDU.so $vboxlib/VBoxRT.so"
    AC_DEFINE([USE_VBOXVD], 1, [Use vbox virtual disk support])
    # define vbox include version
    changequote(<<, >>)
    vbox_major=`$SED -e '/^VBOX_VERSION_MAJOR[ 	]*=/!d' -e 's/.*=[^0-9]*\([0-9]*\).*/\1/' $vboxinc/../Config.kmk`
    vbox_minor=`$SED -e '/^VBOX_VERSION_MINOR[ 	]*=/!d' -e 's/.*=[^0-9]*\([0-9]*\).*/\1/' $vboxinc/../Config.kmk`
    vbox_build=`$SED -e '/^VBOX_VERSION_BUILD[ 	]*=/!d' -e 's/.*=[^0-9]*\([0-9]*\).*/\1/' $vboxinc/../Config.kmk`
    changequote([, ])
    AC_DEFINE_UNQUOTED([ISTGT_VBOXINC_VERSION_MAJOR], [$vbox_major], [VBox include major])
    AC_DEFINE_UNQUOTED([ISTGT_VBOXINC_VERSION_MINOR], [$vbox_minor], [VBox include minor])
    AC_DEFINE_UNQUOTED([ISTGT_VBOXINC_VERSION_BUILD], [$vbox_build], [VBox include build])
fi

# check for PTHREAD_MUTEX_ADAPTIVE_NP
AC_MSG_CHECKING([for PTHREAD_MUTEX_ADAPTIVE_NP])
AC_TRY_COMPILE([
#include <pthread.h>
],
[
	return (PTHREAD_MUTEX_ADAPTIVE_NP);
],
[AC_MSG_RESULT([yes])
AC_DEFINE([HAVE_PTHREAD_MUTEX_ADAPTIVE_NP], [1], [Define 1 if you have the value.])],
[AC_MSG_RESULT([no])]
)

AC_CHECK_HEADERS([sys/types.h])
AC_CHECK_HEADERS([machine/atomic.h], [], [],
[#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
])
AC_CHECK_HEADERS([sys/atomic.h])

# for FeeeBSD
#AC_CHECK_FUNCS([atomic_store_rel_int atomic_load_acq_int])
#AC_CHECK_FUNCS([atomic_cmpset_ptr atomic_cmpset_64 atomic_cmpset_32])

# for NetBSD
#AC_CHECK_FUNCS([atomic_swap_uint atomic_or_uint_nv])
#AC_CHECK_FUNCS([atomic_cas_ptr atomic_cas_64 atomic_cas_32])

atomic="no"
AC_MSG_CHECKING([for atomic operations])
AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <machine/atomic.h>
],
[
	int x, y;
	atomic_store_rel_int(&x, 1);
	y = atomic_load_acq_int(&x);
],
[AC_DEFINE([USE_ATOMIC], [1], [Use atomic])
AC_DEFINE([HAVE_ATOMIC_STORE_REL_INT], [1], [Define 1 if you have the function.])
AC_DEFINE([HAVE_ATOMIC_LOAD_ACQ_INT], [1], [Define 1 if you have the function.])
atomic="yes"]
)
if test $atomic = "no"; then
AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <sys/atomic.h>
],
[
	unsigned int x, y;
	atomic_swap_uint(&x, 1);
	y = atomic_or_uint_nv(&x, 0);
],
[AC_DEFINE([USE_ATOMIC], [1], [Use atomic])
AC_DEFINE([HAVE_ATOMIC_SWAP_UINT], [1], [Define 1 if you have the function.])
AC_DEFINE([HAVE_ATOMIC_OR_UINT_NV], [1], [Define 1 if you have the function.])
atomic="yes"]
)
fi
AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <sys/atomic.h>
],
[
	membar_producer();
],
[AC_DEFINE([HAVE_MEMBAR_PRODUCER], [1], [Define 1 if you have the function.])]
)
AC_MSG_RESULT($atomic)

# for gcc builtin
#AC_CHECK_FUNCS([__sync_fetch_and_add __sync_val_compare_and_swap])
#AC_CHECK_FUNCS([__sync_synchronize])

gcc_atomic="no"
AC_MSG_CHECKING([for gcc builtin atomic operations])
AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
],
[
	unsigned int x, y;
	y = __sync_fetch_and_add(&x, 0);
	__sync_val_compare_and_swap(&x, y, 2);
],
[AC_DEFINE([USE_GCC_ATOMIC], [1], [Use gcc builtin atomic])
AC_DEFINE([HAVE_GCC_ATOMIC_FETCH_AND_ADD_UINT], [1], [Define 1 if you have the function.])
AC_DEFINE([HAVE_GCC_ATOMIC_VAL_COMPARE_AND_SWAP_UINT], [1], [Define 1 if you have the function.])
gcc_atomic="yes"]
)
AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
],
[
	__sync_synchronize();
],
[AC_DEFINE([HAVE_GCC_ATOMIC_SYNCHRONIZE], [1], [Define 1 if you have the function.])]
)
AC_MSG_RESULT($gcc_atomic)

AC_CONFIG_FILES([Makefile src/Makefile etc/Makefile doc/Makefile])
AC_CONFIG_FILES([src/build.h])
AC_OUTPUT
