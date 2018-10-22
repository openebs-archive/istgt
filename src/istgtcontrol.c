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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "build.h"

#include <stdint.h>
#include <inttypes.h>

#include <stdarg.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/types.h>

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_conf.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_md5.h"

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

//	#define TRACE_UCTL

#define	DEFAULT_UCTL_CONFIG BUILD_ETC_ISTGT "/istgtcontrol.conf"
#define	DEFAULT_UCTL_TIMEOUT 60
#define	DEFAULT_UCTL_PORT 3261
#define	DEFAULT_UCTL_HOST "localhost"
#define	DEFAULT_UCTL_LUN 0
#define	DEFAULT_UCTL_MTYPE "-"
#define	DEFAULT_UCTL_MFLAGS "ro"
#define	DEFAULT_UCTL_MSIZE "auto"

#define	MAX_LINEBUF 4096
#define	UCTL_CHAP_CHALLENGE_LEN 1024

typedef struct istgt_uctl_auth_t {
	char *user;
	char *secret;
	char *muser;
	char *msecret;

	uint8_t chap_id[1];
	uint8_t chap_mid[1];
	int chap_challenge_len;
	uint8_t chap_challenge[UCTL_CHAP_CHALLENGE_LEN];
	int chap_mchallenge_len;
	uint8_t chap_mchallenge[UCTL_CHAP_CHALLENGE_LEN];
} UCTL_AUTH;

typedef struct istgt_uctl_t {
	CONFIG *config;

	char *host;
	int port;

	int sock;
	char *iqn;
	int lun;
	int OperationalMode;
	int gottrace;
	int traceflag;
	int delayus;
	int setzero;
	char *mflags;
	char *mfile;
	char *msize;
	char *mtype;
	int persistopt;

	int family;
	char caddr[MAX_ADDRBUF];
	char saddr[MAX_ADDRBUF];

	UCTL_AUTH auth;

	int timeout;
	int req_auth_auto;
	int req_auth;
	int req_auth_mutual;

	int recvtmpsize;
	int recvtmpcnt;
	int recvtmpidx;
	int recvbufsize;
	int sendbufsize;
	int worksize;
	char recvtmp[MAX_LINEBUF];
	char recvbuf[MAX_LINEBUF];
	char sendbuf[MAX_LINEBUF];
	char work[MAX_LINEBUF];
	char *cmd;
	char *arg;
	int setopt;
	int setval;
	char **setargv;
	int setargcnt;
	int detail;
} UCTL;
typedef UCTL *UCTL_Ptr;


static void fatal(const char *format, ...)
__attribute__((__noreturn__, __format__(__printf__, 1, 2)));

static void
fatal(const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
	exit(EXIT_FAILURE);
}

typedef enum {
	UCTL_CMD_OK = 0,
	UCTL_CMD_ERR = 1,
	UCTL_CMD_EOF = 2,
	UCTL_CMD_QUIT = 3,
	UCTL_CMD_DISCON = 4,
	UCTL_CMD_REQAUTH = 5,
	UCTL_CMD_CHAPSEQ = 6,
} UCTL_CMD_STATUS;

//	#define ARGS_DELIM " \t\r\n"
#define	ARGS_DELIM " \t"

static int
uctl_readline(UCTL_Ptr uctl)
{
	ssize_t total;

	total = istgt_readline_socket(uctl->sock, uctl->recvbuf,
		uctl->recvbufsize, uctl->recvtmp, uctl->recvtmpsize,
		&uctl->recvtmpidx, &uctl->recvtmpcnt, uctl->timeout);
	if (total < 0) {
		return (UCTL_CMD_DISCON);
	}
	if (total == 0) {
		return (UCTL_CMD_EOF);
	}
	return (UCTL_CMD_OK);
}

static int
uctl_writeline(UCTL_Ptr uctl)
{
	ssize_t total;
	ssize_t expect;

	expect = strlen(uctl->sendbuf);
	total = istgt_writeline_socket(uctl->sock,
	    uctl->sendbuf, uctl->timeout);
	if (total < 0) {
		return (UCTL_CMD_DISCON);
	}
	if (total != expect) {
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

    static int uctl_snprintf(UCTL_Ptr uctl, const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));

static int
uctl_snprintf(UCTL_Ptr uctl, const char *format, ...)
{
	va_list ap;
	int rc;

	va_start(ap, format);
	rc = vsnprintf(uctl->sendbuf, uctl->sendbufsize, format, ap);
	va_end(ap);
	return (rc);
}

static char *
get_banner(UCTL_Ptr uctl)
{
	char *banner;
	int rc;

	rc = uctl_readline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (NULL);
	}
	banner = xstrdup(trim_string(uctl->recvbuf));
	return (banner);
}

static int
is_err_req_auth(UCTL_Ptr uctl __attribute__((__unused__)), char *s)
{
	const char *req_auth_string = "auth required";

#ifdef TRACE_UCTL
	printf("S=%s, Q=%s\n", s, req_auth_string);
#endif /* TRCAE_UCTL */
	if (strncasecmp(s, req_auth_string, strlen(req_auth_string)) == 0)
		return (1);
	return (0);
}

static int
is_err_chap_seq(UCTL_Ptr uctl __attribute__((__unused__)), char *s)
{
	const char *chap_seq_string = "CHAP sequence error";

#ifdef TRACE_UCTL
	printf("S=%s, Q=%s\n", s, chap_seq_string);
#endif /* TRCAE_UCTL */
	if (strncasecmp(s, chap_seq_string, strlen(chap_seq_string)) == 0)
		return (1);
	return (0);
}

static int
send_and_read_result(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	int rc;
	char *result;
	char *arg;

	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	rc = uctl_readline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	arg = trim_string(uctl->recvbuf);
	result = strsepq(&arg, delim);
	strupr(result);
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_quit(UCTL_Ptr uctl)
{
	/* send command */
	uctl_snprintf(uctl, "QUIT\n");

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_noop(UCTL_Ptr uctl)
{
	/* send command */
	uctl_snprintf(uctl, "NOOP\n");

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_version(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	char *version;
	char *extver;
	int rc;

	/* send command */
	uctl_snprintf(uctl, "VERSION\n");
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0) {
			break;
		}
		version = strsepq(&arg, delim);
		extver = strsepq(&arg, delim);
		printf("target version %s %s\n", version, extver);
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_unload(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "UNLOAD \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_load(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "LOAD \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_list(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	char *target;
	char *num;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "LIST \"%s\"\n", uctl->iqn);
	} else {
		uctl_snprintf(uctl, "LIST\n");
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			target = strsepq(&arg, delim);
			num = strsepq(&arg, delim);
			printf("%s %s\n", target, num);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_change(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->mfile == NULL ||
		uctl->mtype == NULL || uctl->mflags == NULL ||
		uctl->msize == NULL) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "CHANGE \"%s\" %d \"%s\" "
	    "\"%s\" \"%s\" \"%s\"\n",
	    uctl->iqn, uctl->lun, uctl->mtype,
	    uctl->mflags, uctl->mfile, uctl->msize);
	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_reset(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "RESET \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_clear(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "CLEAR \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_refresh(UCTL_Ptr uctl)
{
	/* send command */
	uctl_snprintf(uctl, "REFRESH \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_sync(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}

	uctl_snprintf(uctl, "SYNC \"%s\" %d\n", uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_persist(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
	    return (UCTL_CMD_ERR);
	}

	uctl_snprintf(uctl, "PERSIST \"%s\" %d %d\n", uctl->iqn,
	    uctl->lun, uctl->persistopt);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_start(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "START \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_stop(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->iqn == NULL || uctl->lun < 0) {
		return (UCTL_CMD_ERR);
	}
	uctl_snprintf(uctl, "STOP \"%s\" %d\n",
	    uctl->iqn, uctl->lun);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_mem(UCTL_Ptr uctl)
{
	/* Send Command */
	uctl_snprintf(uctl, "%s\n", "MEM");

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_log(UCTL_Ptr uctl)
{
	/* Send Command */
	printf("LOG %d %d %d\n", uctl->gottrace,
	uctl->traceflag, uctl->delayus);
	uctl_snprintf(uctl, "LOG %d %d %d\n", uctl->gottrace,
	uctl->traceflag, uctl->delayus);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_memdebug(UCTL_Ptr uctl)
{
	/* Send Command */
	uctl_snprintf(uctl, "%s\n", "MEMDEBUG");

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_modify(UCTL_Ptr uctl)
{
	/* send command */
	if (uctl->OperationalMode < 0) {
		return (UCTL_CMD_ERR);
	}

	/* Send Command */
	uctl_snprintf(uctl, "MODIFY %d\n",
	    uctl->OperationalMode);

	/* send and receive result */
	return (send_and_read_result(uctl));
}

static int
exec_status(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "STATUS \"%s\"\n", uctl->iqn);
	} else {
		uctl_snprintf(uctl, "STATUS\n");
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		printf("%s\n", arg);
	}

	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_info(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "INFO \"%s\"\n", uctl->iqn);
	} else {
		uctl_snprintf(uctl, "INFO\n");
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
static int
exec_dump(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	char *type;
	char *var;
	char *c_num;
	char *isid;
	char *tsih;
	char *cid;
	char *t_ip;
	char *i_ip;
	char *i_name;
	int flag = 0;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "DUMP %d \"%s\"\n",
		uctl->detail, uctl->iqn);
	} else {
		uctl_snprintf(uctl, "DUMP %d\n", uctl->detail);
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		type = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (strcmp(type, "LUN") == 0) {
			printf("--------------------\n");
			while ((var = strsepq(&arg, delim)) != NULL)
				printf("%s\n", var);
			flag = 1;
			continue;
		}	else if (strcmp(type, "CONN") == 0) {
			if (flag == 1) {
				printf("c#	ISID	TSIH	CID");
				printf("T_IP	I_IP	NAME");
				flag = 0;
			}
			c_num = strsepq(&arg, delim);
			isid = strsepq(&arg, delim);
			tsih = strsepq(&arg, delim);
			cid = strsepq(&arg, delim);
			t_ip = strsepq(&arg, delim);
			i_ip = strsepq(&arg, delim);
			i_name = strsepq(&arg, delim);
			printf("%-6s  %-16s %-4s %3s    %3s    %-15s %s\n",
			c_num, isid, tsih, cid, t_ip, i_ip, i_name);
		}	else if (strcmp(type, "TOTAL") == 0) {
			printf("--------------------\n");
			if (uctl->iqn != NULL)
				continue;
			printf("TOTAL:\n");
			while ((var = strsepq(&arg, delim)) != NULL)
				printf("%s\n", var);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
#ifdef REPLICATION
// exec_iostats writes IOSTATS over the wire and gets the
// iostats from istgt
static int
exec_iostats(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	uctl_snprintf(uctl, "IOSTATS\n");
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0) {
			break;
		}
		fprintf(stdout, "%s\n", arg);
	}
	if (strcmp(result, "OK") != 0) {
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
#endif

static int
exec_stats(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	/* send command */
	/*
	 * if (uctl->iqn != NULL) {
	 *	uctl_snprintf(uctl, "STATS IQN \"%s\"\n", uctl->iqn);
	 * } else if (uctl->lun != -1) {
	 *	uctl_snprintf(uctl, "STATS LU \"%d\"\n", uctl->lun);
	 * } else {
	 *	uctl_snprintf(uctl, "STATS ALL \n");
	 * }
	 */
	uctl_snprintf(uctl, "STATS %d\n", uctl->setzero);
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

#ifdef	REPLICATION
static int
exec_snap(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc = 0, wait_time, io_wait_time;
	char *name = uctl->setargv[0];
	char *snapname = uctl->setargv[1];
	if (uctl->setargcnt >= 3)
		io_wait_time = atoi(uctl->setargv[2]);
	else
		io_wait_time = 10;
	if (uctl->setargcnt >= 4)
		wait_time = atoi(uctl->setargv[3]);
	else
		wait_time = 30;

	uctl_snprintf(uctl, "%s \"%s\" \"%s\" \"%d\" \"%d\"\n",
	uctl->cmd, name, snapname, io_wait_time, wait_time);
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_mempool_stats(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc = 0;

	uctl_snprintf(uctl, "%s\n", uctl->cmd);
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_replica(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc = 0;
	char *volname;

	if (uctl->setargcnt >= 1)
		volname = uctl->setargv[0];
	else
		volname = NULL;

	if (volname)
		uctl_snprintf(uctl, "%s \"%s\" \n", uctl->cmd, volname);
	else
		uctl_snprintf(uctl, "%s\n", uctl->cmd);

	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
#endif

static int
exec_maxtime(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	if (uctl->iqn == NULL)
		uctl_snprintf(uctl, "MAXTIME ALL %d\n", uctl->setzero);
	else
		uctl_snprintf(uctl, "MAXTIME \"%s\" %d\n",
		uctl->iqn, uctl->setzero);
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_set(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc = 0, i;

	if (uctl->setopt != 15) {
		if (uctl->iqn == NULL)
			uctl_snprintf(uctl, "SET ALL %d %d %d\n",
			uctl->lun, uctl->setopt, uctl->setval);
		else
			uctl_snprintf(uctl,	"SET \"%s\" %d %d %d\n",
			uctl->iqn,	uctl->lun,
			uctl->setopt,	uctl->setval);
	}
	else
	{
		if (uctl->iqn == NULL)
			rc = snprintf(uctl->sendbuf, uctl->sendbufsize,
			"SET ALL %d %d %d", uctl->lun,
			uctl->setopt, uctl->setargcnt-1);
		else
			rc = snprintf(uctl->sendbuf, \
				uctl->sendbufsize, "SET \"%s\" %d %d %d",
				uctl->iqn, uctl->lun,
				uctl->setopt, uctl->setargcnt-1);

		if (rc >= uctl->sendbufsize)
			return (UCTL_CMD_ERR);

		for (i = 1;	i < uctl->setargcnt;	i++) {
			rc += snprintf(uctl->sendbuf+rc,
				uctl->sendbufsize-rc, " %s", uctl->setargv[i]);
			if (rc >= uctl->sendbufsize)
				return (UCTL_CMD_ERR);
		}
		rc += snprintf(uctl->sendbuf+rc, uctl->sendbufsize-rc, "\n");
		if (rc >= uctl->sendbufsize)
			return (UCTL_CMD_ERR);
	}

	printf("cmd: %s\n", uctl->sendbuf);

	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
static int
exec_rsv(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "RSV \"%s\"\n", uctl->iqn);
	} else {
		uctl_snprintf(uctl, "RSV \n");
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int
exec_que(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	int rc;

	/* send command */
	if (uctl->iqn != NULL) {
		uctl_snprintf(uctl, "QUE IQN \"%s\"\n", uctl->iqn);
	} else if (uctl->lun != -1) {
		uctl_snprintf(uctl, "QUE LU \"%d\"\n", uctl->lun);
	} else {
		uctl_snprintf(uctl, "QUE ALL \n");
	}
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive result */
	while (1) {
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, uctl->cmd) != 0)
			break;
		if (uctl->iqn != NULL) {
			printf("%s\n", arg);
		} else {
			printf("%s\n", arg);
		}
	}
	if (strcmp(result, "OK") != 0) {
		if (is_err_req_auth(uctl, arg))
			return (UCTL_CMD_REQAUTH);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}
typedef struct exec_table_t
{
	const char *name;
	int (*func) (UCTL_Ptr uctl);
	int req_argc;
	int req_target;
} EXEC_TABLE;

static EXEC_TABLE exec_table[] =
{
	{ "QUIT",	exec_quit,	0,	0 },
	{ "NOOP",	exec_noop,	0,	0 },
	{ "VERSION", exec_version,  0, 0 },
	{ "LIST",	exec_list,	0,	0 },
	{ "UNLOAD",  exec_unload,   0, 1 },
	{ "LOAD",	exec_load,	0,	1 },
	{ "CHANGE",  exec_change,   1, 1 },
	{ "RESET",   exec_reset,    0, 1 },
	{ "CLEAR",   exec_clear,    0, 1 },
	{ "SYNC",   exec_sync,  0, 1 },
	{ "PERSIST", exec_persist, 1, 1},
	{ "REFRESH", exec_refresh, 0, 0 },
	{ "START",  exec_start, 0, 1 },
	{ "STOP",    exec_stop, 0, 1 },
	{"MODIFY", exec_modify, 0, 0 },
	{ "STATUS", exec_status, 0, 0},
	{ "INFO",	exec_info,	0,	0 },
	{ "DUMP",	exec_dump,	0,	0 },
	{"MEM", exec_mem, 0, 0 },
	{"MEMDEBUG", exec_memdebug, 0, 0 },
	{"LOG", exec_log, 0, 0 },
	{"RSV", exec_rsv, 0, 0 },
	{"QUE", exec_que, 0, 0 },
	{"STATS", exec_stats, 0, 0 },
#ifdef REPLICATION
	{"IOSTATS", exec_iostats, 0, 0 },
	{"MEMPOOL", exec_mempool_stats, 0, 0 },
#endif
	{"SET", exec_set, 0, 1},
	{"MAXTIME", exec_maxtime, 0, 0},
#ifdef	REPLICATION
	{"SNAPCREATE", exec_snap, 2, 0},
	{"SNAPDESTROY", exec_snap, 2, 0},
	{"REPLICA", exec_replica, 0, 0},
#endif
	{ NULL,	NULL,	0,	0 },
};

static int
do_auth(UCTL_Ptr uctl)
{
	uint8_t uctlmd5[ISTGT_MD5DIGEST_LEN];
	uint8_t resmd5[ISTGT_MD5DIGEST_LEN];
	ISTGT_MD5CTX md5ctx;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *result;
	char *label;
	char *chap_i;
	char *chap_c;
	char *chap_n;
	char *chap_r;
	char *hexmd5;
	char *hexchallenge;
	char *workp;
	int worksize;
	int algorithm = 5; /* CHAP with MD5 */
	int rc;

#ifdef TRACE_UCTL
	printf("do_auth: user=%s, secret=%s, muser=%s, msecret=%s\n",
	    uctl->auth.user,
	    uctl->auth.secret,
	    uctl->auth.muser,
	    uctl->auth.msecret);
#endif /* TRACE_UCTL */

	/* send algorithm CHAP_A */
	uctl_snprintf(uctl, "AUTH CHAP_A %d\n",
	    algorithm);
	rc = uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* receive CHAP_IC */
	rc = uctl_readline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	arg = trim_string(uctl->recvbuf);
	result = strsepq(&arg, delim);
	strupr(result);
	if (strcmp(result, "AUTH") != 0) {
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}

	label = strsepq(&arg, delim);
	chap_i = strsepq(&arg, delim);
	chap_c = strsepq(&arg, delim);
	if (label == NULL || chap_i == NULL || chap_c == NULL) {
		fprintf(stderr, "CHAP sequence error\n");
		return (UCTL_CMD_ERR);
	}
	if (strcasecmp(label, "CHAP_IC") != 0) {
		fprintf(stderr, "CHAP sequence error\n");
		return (UCTL_CMD_ERR);
	}

	/* Identifier */
	uctl->auth.chap_id[0] = (uint8_t) strtol(chap_i, NULL, 10);
	/* Challenge Value */
	rc = istgt_hex2bin(uctl->auth.chap_challenge,
	    UCTL_CHAP_CHALLENGE_LEN,
	    chap_c);
	if (rc < 0) {
		fprintf(stderr, "challenge format error\n");
		return (UCTL_CMD_ERR);
	}
	uctl->auth.chap_challenge_len = rc;

	if (uctl->auth.user == NULL || uctl->auth.secret == NULL) {
		fprintf(stderr, "ERROR auth user or secret is missing\n");
		return (UCTL_CMD_ERR);
	}

	istgt_md5init(&md5ctx);
	/* Identifier */
	istgt_md5update(&md5ctx, uctl->auth.chap_id, 1);
	/* followed by secret */
	istgt_md5update(&md5ctx, uctl->auth.secret,
	    strlen(uctl->auth.secret));
	/* followed by Challenge Value */
	istgt_md5update(&md5ctx, uctl->auth.chap_challenge,
	    uctl->auth.chap_challenge_len);
	/* uctlmd5 is Response Value */
	istgt_md5final(uctlmd5, &md5ctx);

	workp = uctl->work;
	worksize = uctl->worksize;

	istgt_bin2hex(workp, worksize,
	    uctlmd5, ISTGT_MD5DIGEST_LEN);
	hexmd5 = workp;
	worksize -= strlen(hexmd5) + 1;
	workp += strlen(hexmd5) + 1;

	/* mutual CHAP? */
	if (uctl->req_auth_mutual) {
		/* Identifier is one octet */
		istgt_gen_random(uctl->auth.chap_mid, 1);
		/* Challenge Value is a variable stream of octets */
		/* (binary length MUST not exceed 1024 bytes) */
		uctl->auth.chap_mchallenge_len = UCTL_CHAP_CHALLENGE_LEN;
		istgt_gen_random(uctl->auth.chap_mchallenge,
		    uctl->auth.chap_mchallenge_len);

		istgt_bin2hex(workp, worksize,
		    uctl->auth.chap_mchallenge,
		    uctl->auth.chap_mchallenge_len);
		hexchallenge = workp;
		worksize -= strlen(hexchallenge) + 1;
		workp += strlen(hexchallenge) + 1;

		/* send CHAP_NR with CHAP_IC */
		uctl_snprintf(uctl, "AUTH CHAP_NR %s %s %d %s\n",
		    uctl->auth.user, hexmd5,
		    (int) uctl->auth.chap_mid[0], hexchallenge);
		rc = uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}

		/* receive CHAP_NR */
		rc = uctl_readline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		arg = trim_string(uctl->recvbuf);
		result = strsepq(&arg, delim);
		strupr(result);
		if (strcmp(result, "AUTH") != 0) {
			fprintf(stderr, "ERROR %s\n", arg);
			return (UCTL_CMD_ERR);
		}

		label = strsepq(&arg, delim);
		chap_n = strsepq(&arg, delim);
		chap_r = strsepq(&arg, delim);
		if (label == NULL || chap_n == NULL || chap_r == NULL) {
			fprintf(stderr, "CHAP sequence error\n");
			return (UCTL_CMD_ERR);
		}
		if (strcasecmp(label, "CHAP_NR") != 0) {
			fprintf(stderr, "CHAP sequence error\n");
			return (UCTL_CMD_ERR);
		}

		rc = istgt_hex2bin(resmd5, ISTGT_MD5DIGEST_LEN, chap_r);
		if (rc < 0 || rc != ISTGT_MD5DIGEST_LEN) {
			fprintf(stderr, "response format error\n");
			return (UCTL_CMD_ERR);
		}

		if (uctl->auth.muser == NULL || uctl->auth.msecret == NULL) {
			fprintf(stderr, \
				"ERROR auth user or secret is missing\n");
			return (UCTL_CMD_ERR);
		}

		istgt_md5init(&md5ctx);
		/* Identifier */
		istgt_md5update(&md5ctx, uctl->auth.chap_mid, 1);
		/* followed by secret */
		istgt_md5update(&md5ctx, uctl->auth.msecret,
		    strlen(uctl->auth.msecret));
		/* followed by Challenge Value */
		istgt_md5update(&md5ctx, uctl->auth.chap_mchallenge,
		    uctl->auth.chap_mchallenge_len);
		/* uctlmd5 is expecting Response Value */
		istgt_md5final(uctlmd5, &md5ctx);

		/* compare MD5 digest */
		if (memcmp(uctlmd5, resmd5, ISTGT_MD5DIGEST_LEN) != 0) {
			/* not match */
			fprintf(stderr, \
				"ERROR auth user or secret is missing\n");
			/* discard result line */
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			arg = trim_string(uctl->recvbuf);
			result = strsepq(&arg, delim);
			strupr(result);
			if (strcmp(result, "OK") != 0) {
				fprintf(stderr, "ERROR %s\n", arg);
				return (UCTL_CMD_ERR);
			}
			/* final with ERR */
			return (UCTL_CMD_ERR);
		}
	} else {
		/* not mutual */
		/* send CHAP_NR */
		uctl_snprintf(uctl, "AUTH CHAP_NR %s %s\n",
		    uctl->auth.user, hexmd5);
		rc = uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
	}

	/* receive result */
	rc = uctl_readline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	arg = trim_string(uctl->recvbuf);
	result = strsepq(&arg, delim);
	strupr(result);
	if (strcmp(result, "OK") != 0) {
		if (is_err_chap_seq(uctl, arg))
			return (UCTL_CMD_CHAPSEQ);
		fprintf(stderr, "ERROR %s\n", arg);
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static char *
uctl_get_nmval(CF_SECTION *sp, const char *key, int idx1, int idx2)
{
	CF_ITEM *ip;
	CF_VALUE *vp;
	int i;

	ip = istgt_find_cf_nitem(sp, key, idx1);
	if (ip == NULL)
		return (NULL);
	vp = ip->val;
	if (vp == NULL)
		return (NULL);
	for (i = 0; vp != NULL; vp = vp->next) {
		if (i == idx2)
			return (vp->value);
		i++;
	}
	return (NULL);
}

static char *
uctl_get_nval(CF_SECTION *sp, const char *key, int idx)
{
	CF_ITEM *ip;
	CF_VALUE *vp;

	ip = istgt_find_cf_nitem(sp, key, idx);
	if (ip == NULL)
		return (NULL);
	vp = ip->val;
	if (vp == NULL)
		return (NULL);
	return (vp->value);
}

static char *
uctl_get_val(CF_SECTION *sp, const char *key)
{
	return (uctl_get_nval(sp, key, 0));
}

static int
uctl_get_nintval(CF_SECTION *sp, const char *key, int idx)
{
	const char *v;
	int value;

	v = uctl_get_nval(sp, key, idx);
	if (v == NULL)
		return (-1);
	value = (int)strtol(v, NULL, 10);
	return (value);
}

static int
uctl_get_intval(CF_SECTION *sp, const char *key)
{
	return (uctl_get_nintval(sp, key, 0));
}

static int
uctl_init(UCTL_Ptr uctl)
{
	CF_SECTION *sp;
	const char *val;
	const char *user, *muser;
	const char *secret, *msecret;
	int timeout;
	int port;
	int lun;
	int i;

	sp = istgt_find_cf_section(uctl->config, "Global");
	if (sp == NULL) {
		fprintf(stderr, "find_cf_section failed()\n");
		return (-1);
	}

	val = uctl_get_val(sp, "Comment");
	if (val != NULL) {
		/* nothing */
#ifdef TRACE_UCTL
		printf("Comment %s\n", val);
#endif /* TRACE_UCTL */
	}

	val = uctl_get_val(sp, "Host");
	if (val == NULL) {
		val = DEFAULT_UCTL_HOST;
	}
	uctl->host = xstrdup(val);
#ifdef TRACE_UCTL
	printf("Host %s\n", uctl->host);
#endif /* TRACE_UCTL */

	port = uctl_get_intval(sp, "Port");
	if (port < 0) {
		port = DEFAULT_UCTL_PORT;
	}
	uctl->port = port;
#ifdef TRACE_UCTL
	printf("Port %d\n", uctl->port);
#endif /* TRACE_UCTL */

	val = uctl_get_val(sp, "TargetName");
	if (val == NULL) {
		val = NULL;
	}
	uctl->iqn = xstrdup(val);
#ifdef TRACE_UCTL
	printf("TargetName %s\n", uctl->iqn);
#endif /* TRACE_UCTL */

	lun = uctl_get_intval(sp, "Lun");
	if (lun < 0) {
		lun = DEFAULT_UCTL_LUN;
	}
	uctl->lun = lun;
#ifdef TRACE_UCTL
	printf("Lun %d\n", uctl->lun);
#endif /* TRACE_UCTL */

	val = uctl_get_val(sp, "Flags");
	if (val == NULL) {
		val = DEFAULT_UCTL_MFLAGS;
	}
	uctl->mflags = xstrdup(val);
#ifdef TRACE_UCTL
	printf("Flags %s\n", uctl->mflags);
#endif /* TRACE_UCTL */

	val = uctl_get_val(sp, "Size");
	if (val == NULL) {
		val = DEFAULT_UCTL_MSIZE;
	}
	uctl->msize = xstrdup(val);
#ifdef TRACE_UCTL
	printf("Size %s\n", uctl->msize);
#endif /* TRACE_UCTL */

	timeout = uctl_get_intval(sp, "Timeout");
	if (timeout < 0) {
		timeout = DEFAULT_UCTL_TIMEOUT;
	}
	uctl->timeout = timeout;
#ifdef TRACE_UCTL
	printf("Timeout %d\n", uctl->timeout);
#endif /* TRACE_UCTL */

	val = uctl_get_val(sp, "AuthMethod");
	if (val == NULL) {
		uctl->req_auth_auto = 0;
		uctl->req_auth = 0;
	} else {
		uctl->req_auth_auto = 0;
		for (i = 0; ; i++) {
			val = uctl_get_nmval(sp, "AuthMethod", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "CHAP") == 0) {
				uctl->req_auth = 1;
			} else if (strcasecmp(val, "Mutual") == 0) {
				uctl->req_auth_mutual = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				uctl->req_auth_auto = 1;
				uctl->req_auth = 0;
				uctl->req_auth_mutual = 0;
			} else if (strcasecmp(val, "None") == 0) {
				uctl->req_auth = 0;
				uctl->req_auth_mutual = 0;
			} else {
				fprintf(stderr, "unknown auth\n");
				return (-1);
			}
		}
		if (uctl->req_auth_mutual && !uctl->req_auth) {
			fprintf(stderr, "Mutual but not CHAP\n");
			return (-1);
		}
	}
#ifdef TRACE_UCTL
	if (uctl->req_auth == 0) {
		printf("AuthMethod Auto\n");
	} else {
		printf("AuthMethod %s %s\n",
		    uctl->req_auth ? "CHAP" : "",
		    uctl->req_auth_mutual ? "Mutual" : "");
	}
#endif /* TRACE_UCTL */

	val = uctl_get_nval(sp, "Auth", 0);
	if (val == NULL) {
		user = secret = muser = msecret = NULL;
	} else {
		user = uctl_get_nmval(sp, "Auth", 0, 0);
		secret = uctl_get_nmval(sp, "Auth", 0, 1);
		muser = uctl_get_nmval(sp, "Auth", 0, 2);
		msecret = uctl_get_nmval(sp, "Auth", 0, 3);
	}
	uctl->auth.user = xstrdup(user);
	uctl->auth.secret = xstrdup(secret);
	uctl->auth.muser = xstrdup(muser);
	uctl->auth.msecret = xstrdup(msecret);
#ifdef TRACE_UCTL
	printf("user=%s, secret=%s, muser=%s, msecret=%s\n",
	    user, secret, muser, msecret);
#endif /* TRACE_UCTL */

	return (0);
}

static void
usage(void)
{
	printf("istgtcotrol [options] <command> [<file>]\n");
	printf("options:\n");
	printf("default may be changed by configuration file\n");
	printf(" -c config  config file (default %s)\n", DEFAULT_UCTL_CONFIG);
	printf(" -h host    target host name or IP (default %s)\n",
	DEFAULT_UCTL_HOST);
	printf(" -p port    port number (default %d)\n", DEFAULT_UCTL_PORT);
	printf(" -t target  target iqn\n");
	printf(" -l lun     target lun (default %d)\n", DEFAULT_UCTL_LUN);
	printf(" -f flags   media flags (default %s)\n", DEFAULT_UCTL_MFLAGS);
	printf(" -s size    media size (default %s)\n", DEFAULT_UCTL_MSIZE);
	printf(" -q         quiet mode\n");
	printf(" -v         verbose mode\n");
	printf(" -A method  authentication method (CHAP/Mutual CHAP/Auto)\n");
	printf(" -U user    auth user\n");
	printf(" -S secret  auth secret\n");
	printf(" -M muser   mutual auth user\n");
	printf(" -R msecret mutual auth secret\n");
	printf(" -H         show this usage\n");
	printf(" -V         show version\n");
	printf(" -F         Fake Operational Mode\n");
	printf(" -N	    Normal Operational Mode\n");
	printf("command:\n");
	printf(" noop       no operation\n");
	printf(" version    show target version\n");
	printf(" list       list all or specified target\n");
	printf(" load       load media to specified unit\n");
	printf(" unload     unload media from specified unit\n");
	printf(" change     change media with <file> at specified unit\n");
	printf(" sync       sync persistent reservation to zap\n");
	printf(" persist    turn persist <on=1> or <off=0> \n");
	printf(" reset      reset specified lun of target\n");
	printf(" clear      clear the persistent reservation \
	of a specified lun\n");
	printf(" refresh    refresh to reload the lun configuration\n");
	printf(" start      open the lun device\n");
	printf(" stop	    close the lun device\n");
	printf(" modify     Modify all the lun devices to Fake/Normal\n");
	printf(" status     get the status of all or specified lun device \n");
	printf(" info       show connections of target\n");
	printf(" iostats    displays iostats of volume\n");
	printf(" maxtime    list the IOs which took maximum time to process\n");
#ifdef	REPLICATION
	printf(" replica    list replica and its stats\n");
	printf(" mempool    get mempool details\n");
#endif
	printf(" set        set values for variables:\n");
	printf("            Syntax: istgtcontrol -t <iqn(ALL to set globally)> \
	set <variable number> <value>\n");
	printf("            Variables :\n");
	printf("            1\tsend_abrt_resp(Yes->1 No->0)\n");
	printf("            2\tabort_result_queue(Yes->1 No->0)\n");
	printf("            3\ttwait_inflights(Yes->1 No->0)\n");
	printf("            4\tmax_unmap_sectors\n");
	printf("	    5\tclear_resv(Yes->1 No->0)\n");
	printf("	    6\tATS(Disable->0 Enable->1)\n");
	printf("	    7\tXCOPY(Disable->0 Enable->1)\n");
	printf("            11\tabort_release(Yes->1 No->0)\n");
}

int
main(int argc, char *argv[])
{
	const char *config_file = DEFAULT_UCTL_CONFIG;
	CONFIG *config;
	UCTL xuctl, *uctl;
	struct sigaction sigact, sigoldact;
	int (*func) (UCTL_Ptr);
	int port = -1;
	int lun = -1;
	const char *host = NULL;
	const char *mflags = NULL;
	const char *mfile = NULL;
	const char *persistopt = NULL;
	const char *msize = NULL;
	const char *mtype = DEFAULT_UCTL_MTYPE;
	char *target = NULL;
	char *user = NULL;
	char *secret = NULL;
	char *muser = NULL;
	char *msecret = NULL;
	char *cmd = NULL;
	char *banner;
	long l;
	int detail = 0;
	int OperationalMode = -1;
	int traceflag = ISTGT_TRACE_NONE;
	int gottrace = 0;
	int delayus = -2;
	int setzero = 0;
	int exec_result;
	int req_argc;
	int req_target;
	int quiet = 0;
	int verbose = 0;
	int req_auth = -1;
	int ch;
	int sock;
	int rc;
	int i;

#ifdef HAVE_SETPROCTITLE
	setproctitle("version %s (%s)",
	    ISTGT_VERSION, ISTGT_EXTRA_VERSION);
#endif

	memset(&xuctl, 0, sizeof (xuctl));
	uctl = &xuctl;

	while ((ch = getopt(argc, argv, \
		"c:h:p:t:l:f:s:qvaA:U:S:M:R:T:L:zFHNV")) != -1) {
		switch (ch) {
		case 'c':
			config_file = optarg;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			l = strtol(optarg, NULL, 10);
			if (l < 0 || l > 65535) {
				fatal("invalid port %s\n", optarg);
			}
			port = (int) l;
			break;
		case 't':
			target = optarg;
			break;
		case 'l':
			l = strtol(optarg, NULL, 10);
			if (l < 0 || l > 0x3fff) {
				fatal("invalid lun %s\n", optarg);
			}
			lun = (int) l;
			break;
		case 'f':
			mflags = optarg;
			break;
		case 's':
			msize = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'a':
			detail = 1;
			break;
		case 'A':
			if (strcasecmp(optarg, "CHAP") == 0) {
				req_auth = 1;
			} else if (strcasecmp(optarg, "Mutual") == 0 ||
			strcasecmp(optarg, "Mutual CHAP") == 0 ||
			strcasecmp(optarg, "CHAP Mutual") == 0) {
				req_auth = 2;
			} else if (strcasecmp(optarg, "Auto") == 0) {
				req_auth = 0;
			} else {
				usage();
				exit(EXIT_SUCCESS);
			}
			break;
		case 'U':
			user = optarg;
			break;
		case 'S':
			secret = optarg;
#ifndef HAVE_SETPROCTITLE
			secret = xstrdup(optarg);
			memset(optarg, 'x', strlen(optarg));
#endif
			break;
		case 'M':
			muser = optarg;
			break;
		case 'R':
			msecret = optarg;
#ifndef HAVE_SETPROCTITLE
			msecret = xstrdup(optarg);
			memset(optarg, 'x', strlen(optarg));
#endif
			break;
		case 'F':
			OperationalMode = 1;
			break;
		case 'N':
			OperationalMode = 0;
			break;
		case 'T':
			gottrace = 1;
			if (strcasecmp(optarg, "NET") == 0) {
				traceflag |= ISTGT_TRACE_NET;
			} else if (strcasecmp(optarg, "ISCSI") == 0) {
				traceflag |= ISTGT_TRACE_ISCSI;
			} else if (strcasecmp(optarg, "SCSI") == 0) {
				traceflag |= ISTGT_TRACE_SCSI;
			} else if (strcasecmp(optarg, "LU") == 0) {
				traceflag |= ISTGT_TRACE_LU;
			} else if (strcasecmp(optarg, "PQ") == 0) {
				traceflag |= ISTGT_TRACE_PQ;
			} else if (strcasecmp(optarg, "ALL") == 0) {
				traceflag |= ISTGT_TRACE_ALL;
			} else if (strcasecmp(optarg, "MEM") == 0) {
				traceflag |= ISTGT_TRACE_MEM;
			} else if (strcasecmp(optarg, "PROF") == 0) {
				traceflag |= ISTGT_TRACE_PROF;
			} else if (strcasecmp(optarg, "PROFX") == 0) {
				traceflag |= ISTGT_TRACE_PROFX;
			} else if (strcasecmp(optarg, "CMD") == 0) {
				traceflag |= ISTGT_TRACE_CMD;
			} else if (strcasecmp(optarg, "NONE") == 0) {
				traceflag = ISTGT_TRACE_NONE;
			} else {
				gottrace = 0;
			}
			break;
		case 'L':
			l = strtol(optarg, NULL, 10);
			if (l > 999000)
				l = 999000;
			else if (l < -1)
				l = -2;
			delayus = (int) l;
			break;
		case 'z':
			setzero = 1;
			break;
		case 'V':
			printf("istgtcontrol version %s (%s)\n",
			    ISTGT_VERSION, ISTGT_EXTRA_VERSION);
			exit(EXIT_SUCCESS);
		case 'H':
		default:
			usage();
			exit(EXIT_SUCCESS);
		}
	}
	argc -= optind;
	argv += optind;

	/* read config files */
	config = istgt_allocate_config();
	rc = istgt_read_config(config, config_file);
	if (rc < 0) {
		fprintf(stderr, "config error\n");
		exit(EXIT_FAILURE);
	}
	if (config->section == NULL) {
		fprintf(stderr, "empty config\n");
		istgt_free_config(config);
		exit(EXIT_FAILURE);
	}
	uctl->config = config;
		// istgt_print_config(config);

	istgtcontrol_open_log();

	/* take specified command */
	if (argc < 1) {
	error_usage_return:
		istgt_free_config(config);
		usage();
		exit(EXIT_FAILURE);
	}
	cmd = strupr(xstrdup(argv[0]));
	argc--;
	argv++;

	/* get function pointer and parameters for specified command */
	func = NULL;
	req_argc = -1;
	req_target = -1;
	for (i = 0; exec_table[i].name != NULL;
		i++) {
		if (cmd[0] == exec_table[i].name[0] &&
			strcmp(cmd, exec_table[i].name) == 0) {
			func = exec_table[i].func;
			req_argc = exec_table[i].req_argc;
			req_target = exec_table[i].req_target;
			break;
		}
	}
	if (func == NULL) {
		istgt_free_config(config);
		fatal("unknown command %s\n", cmd);
	}

	/* patrameter check */
	if (argc < req_argc) {
		goto error_usage_return;
	}
#if 0
	if (req_target) {
		if (target == NULL) {
			goto error_usage_return;
		}
	}
#endif

	/* take args */
	if (strcmp(cmd, "CHANGE") == 0) {
		/* change require file */
		mfile = argv[0];
	}

	if (strcmp(cmd, "PERSIST") == 0) {
			/* persist requires option 0 or 1 */
				persistopt = argv[0];
			}
	if (strcmp(cmd, "SET") == 0) {
		uctl->setopt = atoi(argv[0]);
		uctl->setval = atoi(argv[1]);
		uctl->setargv = argv;
		uctl->setargcnt = argc;
	}

	if ((strcmp(cmd, "SNAPCREATE") == 0) ||
	(strcmp(cmd, "SNAPDESTROY") == 0) ||
	    (strcmp(cmd, "REPLICA") == 0)) {
		uctl->setargv = argv;
		uctl->setargcnt = argc;
	}

	/* build parameters */
	rc = uctl_init(uctl);
	if (rc < 0) {
		fprintf(stderr, "uctl_init() failed\n");
		istgt_free_config(config);
		exit(EXIT_FAILURE);
	}
	uctl->recvtmpcnt = 0;
	uctl->recvtmpidx = 0;
	uctl->recvtmpsize = sizeof (uctl)->recvtmp;
	uctl->recvbufsize = sizeof (uctl)->recvbuf;
	uctl->sendbufsize = sizeof (uctl)->sendbuf;
	uctl->worksize = sizeof (uctl)->work;
	uctl->detail = detail;

	/* override by command line */
	if (user != NULL) {
		xfree(uctl->auth.user);
		uctl->auth.user = xstrdup(user);
	}
	if (secret != NULL) {
		xfree(uctl->auth.secret);
		uctl->auth.secret = xstrdup(secret);
	}
	if (muser != NULL) {
		xfree(uctl->auth.muser);
		uctl->auth.muser = xstrdup(muser);
	}
	if (msecret != NULL) {
		xfree(uctl->auth.msecret);
		uctl->auth.msecret = xstrdup(msecret);
	}
	if (req_target) {
		if (uctl->iqn == NULL && target == NULL) {
			goto error_usage_return;
		}
	}
	if (req_auth >= 0) {
		uctl->req_auth_auto = 1;
		uctl->req_auth = 0;
		uctl->req_auth_mutual = 0;
		if (req_auth > 1) {
			uctl->req_auth_auto = 0;
			uctl->req_auth = 1;
			uctl->req_auth_mutual = 1;
		} else if (req_auth > 0) {
			uctl->req_auth_auto = 0;
			uctl->req_auth = 1;
		}
	}
#ifdef TRACE_UCTL
	printf("auto=%d, auth=%d, mutual=%d\n",
	    uctl->req_auth_auto, uctl->req_auth, uctl->req_auth_mutual);
#endif /* TRACE_UCTL */

	if (host != NULL) {
		xfree(uctl->host);
		uctl->host = xstrdup(host);
	}
	if (port >= 0) {
		uctl->port = port;
	}
	if (target != NULL) {
		xfree(uctl->iqn);
		if (strcasecmp(target, "ALL") == 0) {
			uctl->iqn = NULL;
		} else {
			uctl->iqn = escape_string(target);
		}
	}
	if (lun >= 0) {
		uctl->lun = lun;
	}

	if (OperationalMode < 0) {
		uctl->OperationalMode = DEFAULT_OPERATIONAL_MODE;
	} else {
		uctl->OperationalMode = OperationalMode;
	}
	uctl->gottrace = gottrace;
	uctl->traceflag = traceflag;
	uctl->delayus = delayus;
	uctl->setzero = setzero;
	if (mflags != NULL) {
		xfree(uctl->mflags);
		uctl->mflags = escape_string(mflags);
	}
	uctl->mfile = escape_string(mfile);
	if (msize != NULL) {
		xfree(uctl->msize);
		uctl->msize = escape_string(msize);
	}
	uctl->mtype = escape_string(mtype);
	uctl->cmd = escape_string(cmd);
		if (persistopt != NULL)
			uctl->persistopt = (persistopt[0] == '1' ? 1 : 0);

	/* show setting */
#define	NULLP(S) ((S) == NULL ? "NULL" : (S))
	if (verbose) {
		printf("iqn=%s, lun=%d\n", NULLP(uctl->iqn), uctl->lun);
		printf("media file=%s, flags=%s, size=%s\n",
		    NULLP(uctl->mfile), \
		    NULLP(uctl->mflags), NULLP(uctl->msize));
	}

	/* set signals */
	memset(&sigact, 0, sizeof (sigact));
	memset(&sigoldact, 0, sizeof (sigoldact));
	sigact.sa_handler = SIG_IGN;
	sigemptyset(&sigact.sa_mask);
	if (sigaction(SIGPIPE, &sigact, &sigoldact) != 0) {
		istgt_free_config(config);
		fatal("sigaction() failed");
	}

	/* connect to target */
	if (verbose) {
		printf("connect to %s else to %s:%d\n", ISTGT_UCTL_UNXPATH,
		    uctl->host, uctl->port);
	}
	sock = istgt_connect_unx(ISTGT_UCTL_UNXPATH);
	if (sock < 0) {
		printf("istgt_connect(%s) failed\n", ISTGT_UCTL_UNXPATH);
		sock = istgt_connect(uctl->host, uctl->port);
	}
	if (sock < 0) {
		istgt_free_config(config);
		fatal("istgt_connect(%s:%d) failed\n", uctl->host, uctl->port);
	}
	uctl->sock = sock;

	/* get target banner (ready to send) */
	banner = get_banner(uctl);
	if (banner == NULL) {
		close(uctl->sock);
		istgt_free_config(config);
		fatal("get_banner() failed\n");
	}
	if (verbose) {
		printf("target banner \"%s\"\n", banner);
	}

	/* authentication */
	retry_auth:
	if (uctl->req_auth) {
		rc = do_auth(uctl);
		if (rc != UCTL_CMD_OK) {
			if (rc == UCTL_CMD_REQAUTH || rc == UCTL_CMD_CHAPSEQ) {
			retry_auth_auto:
				/* Auth negotiation */
				if (uctl->req_auth == 0) {
#ifdef TRCAE_UCTL
					printf("Auto negotiation CHAP\n");
#endif /* TRCAE_UCTL */
					uctl->req_auth = 1;
					goto retry_auth;
				} else if (uctl->req_auth_mutual == 0) {
#ifdef TRCAE_UCTL
				printf("Auto negotiation Mutual CHAP\n");
#endif /* TRCAE_UCTL */
					uctl->req_auth_mutual = 1;
					goto retry_auth;
				}
			}
			if (!quiet) {
				printf("AUTH failed\n");
			}
			exec_result = rc;
			goto disconnect;
		}
	}

	/* send specified command */
	rc = func(uctl);
	exec_result = rc;
	if (rc != UCTL_CMD_OK) {
		if (rc == UCTL_CMD_REQAUTH|| rc == UCTL_CMD_CHAPSEQ) {
			goto retry_auth_auto;
		}
		if (!quiet) {
			printf("ABORT %s command\n", uctl->cmd);
		}
	} else {
		if (!quiet) {
			printf("DONE %s command\n", uctl->cmd);
		}
	}

	/* disconnect from target */
	disconnect:
	rc = exec_quit(uctl);
	if (rc != UCTL_CMD_OK) {
		fprintf(stderr, "QUIT failed\n");
		/* error but continue */
	}

	/* cleanup */
	close(sock);
	xfree(uctl->host);
	xfree(uctl->iqn);
	xfree(uctl->mflags);
	xfree(uctl->mfile);
	xfree(uctl->msize);
	xfree(uctl->mtype);
	xfree(uctl->cmd);
	xfree(banner);
	xfree(cmd);
	istgt_free_config(config);
	istgtcontrol_close_log();

	/* return value as execution result */
	if (exec_result != UCTL_CMD_OK) {
		exit(EXIT_FAILURE);
	}
	return (EXIT_SUCCESS);
}
