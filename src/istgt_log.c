/*
 * Copyright (C) 2008-2010 Daisuke Aoyama <aoyama@peach.ne.jp>.
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>

#include "istgt.h"
#include "istgt_log.h"
#include "istgt_misc.h"

// static int g_trace_flag = 0;
int g_trace_flag = 0;
int g_warn_flag = 1;
static int g_log_facility = ISTGT_LOG_FACILITY;
static int g_log_priority = ISTGT_LOG_PRIORITY;
__thread char  tinfo[50] =  {0};
int
istgt_set_log_facility(const char *facility)
{
	if (strcasecmp(facility, "daemon") == 0) {
		g_log_facility = LOG_DAEMON;
	} else if (strcasecmp(facility, "auth") == 0) {
		g_log_facility = LOG_AUTH;
	} else if (strcasecmp(facility, "authpriv") == 0) {
		g_log_facility = LOG_AUTHPRIV;
	} else if (strcasecmp(facility, "local1") == 0) {
		g_log_facility = LOG_LOCAL1;
	} else if (strcasecmp(facility, "local2") == 0) {
		g_log_facility = LOG_LOCAL2;
	} else if (strcasecmp(facility, "local3") == 0) {
		g_log_facility = LOG_LOCAL3;
	} else if (strcasecmp(facility, "local4") == 0) {
		g_log_facility = LOG_LOCAL4;
	} else if (strcasecmp(facility, "local5") == 0) {
		g_log_facility = LOG_LOCAL5;
	} else if (strcasecmp(facility, "local6") == 0) {
		g_log_facility = LOG_LOCAL6;
	} else if (strcasecmp(facility, "local7") == 0) {
		g_log_facility = LOG_LOCAL7;
	} else {
		g_log_facility = ISTGT_LOG_FACILITY;
		return (-1);
	}
	return (0);
}

int
istgt_set_log_priority(const char *priority)
{
	if (strcasecmp(priority, "emerg") == 0) {
		g_log_priority = LOG_EMERG;
	} else if (strcasecmp(priority, "alert") == 0) {
		g_log_priority = LOG_ALERT;
	} else if (strcasecmp(priority, "crit") == 0) {
		g_log_priority = LOG_CRIT;
	} else if (strcasecmp(priority, "err") == 0) {
		g_log_priority = LOG_ERR;
	} else if (strcasecmp(priority, "warning") == 0) {
		g_log_priority = LOG_WARNING;
	} else if (strcasecmp(priority, "notice") == 0) {
		g_log_priority = LOG_NOTICE;
	} else if (strcasecmp(priority, "info") == 0) {
		g_log_priority = LOG_INFO;
	} else if (strcasecmp(priority, "debug") == 0) {
		g_log_priority = LOG_DEBUG;
	} else {
		g_log_priority = ISTGT_LOG_PRIORITY;
		return (-1);
	}
	return (0);
}

void
istgt_log(const char *file, const int line, const char *func,
    const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	vsnprintf(buf, sizeof (buf), format, ap);
	if (file != NULL) {
		if (func != NULL) {
			fprintf(stderr, "%s:%4d:%s: %s", file, line, func, buf);
			syslog(g_log_priority, "%s:%4d:%s: %s", file, line,
			    func, buf);
		} else {
			fprintf(stderr, "%s:%4d: %s", file, line, buf);
			syslog(g_log_priority, "%s:%4d: %s", file, line, buf);
		}
	} else {
		fprintf(stderr, "%s", buf);
		syslog(g_log_priority, "%s", buf);
	}
	va_end(ap);
}

void
istgt_noticelog(const char *file, const int line, const char *func,
    const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	vsnprintf(buf, sizeof (buf), format, ap);
	if (file != NULL) {
		if (func != NULL) {
			fprintf(stderr, "%s:%4d:%s: %s", file, line,
			    func, buf);
			syslog(LOG_NOTICE, "%s:%4d:%s: %s", file, line,
				func, buf);
		} else {
			fprintf(stderr, "%s:%4d: %s", file, line, buf);
			syslog(LOG_NOTICE, "%s:%4d: %s", file, line, buf);
		}
	} else {
		fprintf(stderr, "%s", buf);
		syslog(LOG_NOTICE, "%s", buf);
	}
	va_end(ap);
}

void
istgt_tracelog(const int flag, const char *file, const int line,
	const char *func, const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	if (g_trace_flag & flag) {
		vsnprintf(buf, sizeof (buf), format, ap);
		if (func != NULL) {
			fprintf(stderr, "%s:%4d:%s: %s", file, line,
				func, buf);
			syslog(LOG_INFO, "%s:%4d:%s: %s", file, line,
				func, buf);
		} else {
			fprintf(stderr, "%s:%4d: %s", file, line, buf);
			syslog(LOG_INFO, "%s:%4d: %s", file, line, buf);
		}
	}
	va_end(ap);
}

void
istgt_errlog(const char *file, const int line, const char *func,
	const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	vsnprintf(buf, sizeof (buf), format, ap);
	if (func != NULL) {
		fprintf(stderr, "%s:%4d:%s: ***ERROR*** %s", file, line,
			func, buf);
		syslog(LOG_ERR, "%s:%4d:%s: ***ERROR*** %s", file, line,
			func, buf);
	} else {
		fprintf(stderr, "%s:%4d: ***ERROR*** %s", file, line, buf);
		syslog(LOG_ERR, "%s:%4d: ***ERROR*** %s", file, line, buf);
	}
	va_end(ap);
}

void
istgt_warnlog(const char *file, const int line, const char *func,
				const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	vsnprintf(buf, sizeof (buf), format, ap);
	if (func != NULL) {
		fprintf(stderr, "%s:%4d:%s: ***WARNING*** %s", file, line,
			func, buf);
		syslog(LOG_WARNING, "%s:%4d:%s: ***WARNING*** %s",
			file, line, func, buf);
	} else {
		fprintf(stderr, "%s:%4d: ***WARNING*** %s", file, line, buf);
		syslog(LOG_WARNING, "%s:%4d: ***WARNING*** %s", file, line,
			buf);
	}
	va_end(ap);
}

void
istgt_open_log(void)
{
	if (g_log_facility != 0) {
		openlog("istgt", LOG_PID, g_log_facility);
	} else {
		openlog("istgt", LOG_PID, ISTGT_LOG_FACILITY);
	}
}

void
istgt_close_log(void)
{
	closelog();
}

void
istgtcontrol_open_log(void)
{
	if (g_log_facility != 0) {
		openlog("istgtcontrol", LOG_PID, g_log_facility);
	} else {
		openlog("istgtcontrol", LOG_PID, ISTGT_LOG_FACILITY);
	}
}

void
istgtcontrol_close_log(void)
{
	closelog();
}

void
istgt_set_trace_flag(int flag)
{
	if (flag == ISTGT_TRACE_NONE) {
		g_trace_flag = 0;
	} else {
		g_trace_flag |= flag;
	}
}

void
istgt_trace_dump(int flag, const char *label, const uint8_t *buf, size_t len)
{
	if (g_trace_flag & flag) {
		istgt_fdump(stderr, label, buf, len);
	}
}
