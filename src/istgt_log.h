/*
 * Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#ifndef	ISTGT_LOG_H
#define	ISTGT_LOG_H

#include <stdint.h>
#include <stddef.h>
#include <syslog.h>

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

#ifndef ISTGT_LOG_FACILITY
#define	ISTGT_LOG_FACILITY LOG_LOCAL7
#endif
#ifndef ISTGT_LOG_PRIORITY
#define	ISTGT_LOG_PRIORITY LOG_NOTICE
#endif

#define	ISTGT_TRACE_ALL	(~0U)
#define	ISTGT_TRACE_NONE    0U
#define	ISTGT_TRACE_DEBUG   0x80000000U
#define	ISTGT_TRACE_NET	0x0000000fU
#define	ISTGT_TRACE_ISCSI   0x000000f0U
#define	ISTGT_TRACE_SCSI    0x00000f00U
#define	ISTGT_TRACE_LU	0x0000f000U
#define	ISTGT_TRACE_PQ	0x000f0000U
#define	ISTGT_TRACE_MEM	0x00f00000U
#define	ISTGT_TRACE_PROF	0x01000000U
#define	ISTGT_TRACE_PROFX	0x02000000U
#define	ISTGT_TRACE_CMD	0x04000000U

extern __thread char tinfo[50];

#ifdef	REPLICATION
#include "replication_log.h"

#define	ISTGT_LOG	REPLICA_LOG
#define	ISTGT_NOTICELOG	REPLICA_NOTICELOG
#define	ISTGT_ERRLOG	REPLICA_ERRLOG
#define	ISTGT_WARNLOG	REPLICA_WARNLOG

#define	ISTGT_TRACELOG(FLAG, fmt, ...)					\
	do {								\
		if (g_trace_flag & FLAG)				\
			fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,	\
				__func__, __LINE__, tinfo, ##__VA_ARGS__); \
	} while (0)
/* REPLICATION */
#else
#define	ISTGT_LOG(fmt, ...) syslog(LOG_NOTICE, 	 \
	"%-18.18s:%4d: %-20.20s: " \
	fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define	ISTGT_NOTICELOG(fmt, ...) syslog(LOG_NOTICE, \
	"%-18.18s:%4d: %-20.20s: " \
	fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define	ISTGT_ERRLOG(fmt, ...) syslog(LOG_ERR,  	 \
	"%-18.18s:%4d: %-20.20s: " \
	fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define	ISTGT_WARNLOG(fmt, ...) syslog(LOG_ERR, 	 \
	"%-18.18s:%4d: %-20.20s: " \
	fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)

#define	ISTGT_TRACELOG(FLAG, fmt, ...)  \
	do {								\
		if (g_trace_flag & FLAG)		\
			syslog(LOG_NOTICE, "%-18.18s:%4d: %-20.20s: " \
			fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__); \
	} while (0)
#endif

#ifdef TRACEDUMP
#define	ISTGT_TRACEDUMP(FLAG, LABEL, BUF, LEN)				\
	do {								\
		if (g_trace_flag & (FLAG)) {				\
			istgt_trace_dump((FLAG), (LABEL), (BUF), (LEN));\
		}							\
	} while (0)
#else
#define	ISTGT_TRACEDUMP(FLAG, LABEL, BUF, LEN)
#endif /* TRACEDUMP */

int istgt_set_log_facility(const char *facility);
int istgt_set_log_priority(const char *priority);
void istgt_log(
	const char *file, const int line,
	const char *func, const char *format, ...
) __attribute__((__format__(__printf__, 4, 5)));
void istgt_noticelog(
	const char *file, const int line,
	const char *func, const char *format, ...
) __attribute__((__format__(__printf__, 4, 5)));
void istgt_tracelog(
	const int flag, const char *file,
	const int line, const char *func, const char *format, ...
) __attribute__((__format__(__printf__, 5, 6)));
void istgt_errlog(
	const char *file, const int line,
	const char *func, const char *format, ...
) __attribute__((__format__(__printf__, 4, 5)));
void istgt_warnlog(
	const char *file, const int line,
	const char *func, const char *format, ...
) __attribute__((__format__(__printf__, 4, 5)));
void istgt_open_log(void);
void istgt_close_log(void);
void istgtcontrol_open_log(void);
void istgtcontrol_close_log(void);
void istgt_set_trace_flag(int flag);
void istgt_trace_dump(int flag, const char *label,
	const uint8_t *buf, size_t len);

extern int g_trace_flag;
extern int g_warn_flag;

#endif /* ISTGT_LOG_H */
