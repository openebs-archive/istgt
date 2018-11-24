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

#include <inttypes.h>
#include <stdint.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <unistd.h>
#include <sys/param.h>

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_md5.h"
#include "istgt_lu.h"
#include "istgt_iscsi.h"
#include "istgt_proto.h"

#ifdef	REPLICATION
#include <json-c/json_object.h>
#include "istgt_integration.h"
#include <replication_misc.h>
#endif

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

#define	TIMEOUT_RW 60
#define	MAX_LINEBUF 4096
// #define	MAX_LINEBUF 8192

#ifdef	REPLICATION
extern int replication_initialized;
#endif

typedef struct istgt_uctl_t {
	int id;

	ISTGT_Ptr istgt;
	PORTAL portal;
	int sock;
	pthread_t thread;

	int family;
	char caddr[MAX_ADDRBUF];
	char saddr[MAX_ADDRBUF];
	uint32_t iport;
	uint32_t iaddr;

	ISTGT_CHAP_AUTH auth;
	int authenticated;

	int timeout;
	int auth_group;
	int no_auth;
	int req_auth;
	int req_mutual;

	char *mediadirectory;

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
} UCTL;
typedef UCTL *UCTL_Ptr;

typedef enum {
	UCTL_CMD_OK = 0,
	UCTL_CMD_ERR = 1,
	UCTL_CMD_EOF = 2,
	UCTL_CMD_QUIT = 3,
	UCTL_CMD_DISCON = 4,
} UCTL_CMD_STATUS;

#define	ARGS_DELIM " \t"

static int
istgt_uctl_readline(UCTL_Ptr uctl)
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
istgt_uctl_writeline(UCTL_Ptr uctl)
{
	ssize_t total;
	ssize_t expect;

	expect = strlen(uctl->sendbuf);
	total = istgt_writeline_socket(uctl->sock, uctl->sendbuf,
	    uctl->timeout);
	if (total < 0) {
		return (UCTL_CMD_DISCON);
	}
	if (total != expect) {
		return (UCTL_CMD_ERR);
	}
	return (UCTL_CMD_OK);
}

static int istgt_uctl_snprintf(UCTL_Ptr uctl, const char *format, ...)
    __attribute__((__format__(__printf__, 2, 3)));

static int
istgt_uctl_snprintf(UCTL_Ptr uctl, const char *format, ...)
{
	va_list ap;
	int rc;

	va_start(ap, format);
	rc = vsnprintf(uctl->sendbuf, uctl->sendbufsize, format, ap);
	va_end(ap);
	return (rc);
}

static int
istgt_uctl_get_media_present(ISTGT_LU_Ptr lu __attribute__((__unused__)),
    int lun __attribute__((__unused__)))
{
	return (0);
}

static int
istgt_uctl_get_media_lock(ISTGT_LU_Ptr lu __attribute__((__unused__)),
    int lun __attribute__((__unused__)))
{
	return (0);
}

static int
istgt_uctl_get_authinfo(UCTL_Ptr uctl, const char *authuser)
{
	char *authfile = NULL;
	int ag_tag;
	int rc;

	ag_tag = uctl->auth_group;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ag_tag=%d\n", ag_tag);

	MTX_LOCK(&uctl->istgt->mutex);
	authfile = xstrdup(uctl->istgt->authfile);
	MTX_UNLOCK(&uctl->istgt->mutex);

	rc = istgt_chap_get_authinfo(&uctl->auth, authfile, authuser, ag_tag);
	if (rc < 0) {
		ISTGT_ERRLOG("chap_get_authinfo() failed\n");
		xfree(authfile);
		return (-1);
	}
	xfree(authfile);
	return (0);
}

static int
istgt_uctl_cmd_auth(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *label;
	char *chap_a;
	char *chap_i;
	char *chap_c;
	char *chap_n;
	char *chap_r;
	int rc;

	arg = uctl->arg;
	label = strsepq(&arg, delim);

	if (label == NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (strcasecmp(label, "CHAP_A") == 0) {
		if (uctl->auth.chap_phase != ISTGT_CHAP_PHASE_WAIT_A) {
			istgt_uctl_snprintf(uctl, "ERR CHAP sequence error\n");
		error_return:
			uctl->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_A;
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			return (UCTL_CMD_ERR);
		}

		chap_a = strsepq(&arg, delim);
		if (chap_a == NULL || strcasecmp(chap_a, "5") != 0) {
			istgt_uctl_snprintf(uctl, "ERR invalid algorithm\n");
			goto error_return;
		}

		/* Identifier is one octet */
		istgt_gen_random(uctl->auth.chap_id, 1);
		/* Challenge Value is a variable stream of octets */
		/* (binary length MUST not exceed 1024 bytes) */
		uctl->auth.chap_challenge_len = ISTGT_CHAP_CHALLENGE_LEN;
		istgt_gen_random(uctl->auth.chap_challenge,
		    uctl->auth.chap_challenge_len);

		istgt_bin2hex(uctl->work, uctl->worksize,
		    uctl->auth.chap_challenge,
		    uctl->auth.chap_challenge_len);

		istgt_uctl_snprintf(uctl, "%s CHAP_IC %d %s\n",
		    uctl->cmd, (int) uctl->auth.chap_id[0],
		    uctl->work);

		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		uctl->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_NR;
		/* 3-way handshake */
		return (UCTL_CMD_OK);
	} else if (strcasecmp(label, "CHAP_NR") == 0) {
		uint8_t resmd5[ISTGT_MD5DIGEST_LEN];
		uint8_t tgtmd5[ISTGT_MD5DIGEST_LEN];
		ISTGT_MD5CTX md5ctx;

		if (uctl->auth.chap_phase != ISTGT_CHAP_PHASE_WAIT_NR) {
			istgt_uctl_snprintf(uctl, "ERR CHAP sequence error\n");
			goto error_return;
		}

		chap_n = strsepq(&arg, delim);
		chap_r = strsepq(&arg, delim);
		if (chap_n == NULL || chap_r == NULL) {
			istgt_uctl_snprintf(uctl, "ERR no response\n");
			goto error_return;
		}
		// ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "N=%s, R=%s\n",
		//    chap_n, chap_r);

		rc = istgt_hex2bin(resmd5, ISTGT_MD5DIGEST_LEN, chap_r);
		if (rc < 0 || rc != ISTGT_MD5DIGEST_LEN) {
			istgt_uctl_snprintf(uctl,
			    "ERR response format error\n");
			goto error_return;
		}

		rc = istgt_uctl_get_authinfo(uctl, chap_n);
		if (rc < 0) {
			ISTGT_ERRLOG("auth failed (user %.64s)\n", chap_n);
			istgt_uctl_snprintf(uctl,
			    "ERR auth user or secret is missing\n");
			goto error_return;
		}
		if (uctl->auth.user == NULL || uctl->auth.secret == NULL) {
			ISTGT_ERRLOG("auth failed (user %.64s)\n", chap_n);
			istgt_uctl_snprintf(uctl,
			    "ERR auth user or secret is missing\n");
			goto error_return;
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
		/* tgtmd5 is expecting Response Value */
		istgt_md5final(tgtmd5, &md5ctx);

		/* compare MD5 digest */
		if (memcmp(tgtmd5, resmd5, ISTGT_MD5DIGEST_LEN) != 0) {
			/* not match */
			ISTGT_ERRLOG("auth failed (user %.64s)\n", chap_n);
			istgt_uctl_snprintf(uctl,
			    "ERR auth user or secret is missing\n");
			goto error_return;
		}
		/* OK client's secret */
		uctl->authenticated = 1;

		/* mutual CHAP? */
		chap_i = strsepq(&arg, delim);
		chap_c = strsepq(&arg, delim);
		if (chap_i != NULL && chap_c != NULL) {
			/* Identifier */
			uctl->auth.chap_mid[0] =
			    (uint8_t) strtol(chap_i, NULL, 10);
			/* Challenge Value */
			rc = istgt_hex2bin(uctl->auth.chap_mchallenge,
			    ISTGT_CHAP_CHALLENGE_LEN, chap_c);
			if (rc < 0) {
				istgt_uctl_snprintf(uctl,
				    "ERR challenge format error\n");
				goto error_return;
			}
			uctl->auth.chap_mchallenge_len = rc;

			if (uctl->auth.muser == NULL ||
			    uctl->auth.msecret == NULL) {
				ISTGT_ERRLOG("auth failed (user %.64s)\n",
				    chap_n);
				istgt_uctl_snprintf(uctl,
				    "ERR auth user or secret is missing\n");
				goto error_return;
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
			/* tgtmd5 is Response Value */
			istgt_md5final(tgtmd5, &md5ctx);

			istgt_bin2hex(uctl->work, uctl->worksize,
			    tgtmd5, ISTGT_MD5DIGEST_LEN);

			/* send NR for mutual CHAP */
			istgt_uctl_snprintf(uctl, "%s CHAP_NR \"%s\" %s\n",
			    uctl->cmd,
			    uctl->auth.muser,
			    uctl->work);
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
		} else {
			/* not mutual */
			if (uctl->req_mutual) {
				ISTGT_ERRLOG("required mutual CHAP\n");
				istgt_uctl_snprintf(uctl,
				    "ERR CHAP sequence error\n");
				goto error_return;
			}
		}

		uctl->auth.chap_phase = ISTGT_CHAP_PHASE_END;
	} else {
		istgt_uctl_snprintf(uctl, "ERR CHAP sequence error\n");
		goto error_return;
	}

	/* auth succeeded (but mutual may fail) */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_quit(UCTL_Ptr uctl)
{
	int rc;

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_QUIT);
}

static int
istgt_uctl_cmd_noop(UCTL_Ptr uctl)
{
	int rc;

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

extern char istgtvers[80];
static int
istgt_uctl_cmd_version(UCTL_Ptr uctl)
{
	int rc;

	istgt_uctl_snprintf(uctl, "%s %s\n", uctl->cmd, istgtvers);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	/* version succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_sync(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;
	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}

	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}

	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}
	if (lu->type == ISTGT_LU_TYPE_DISK) {
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_disk_sync_reservation(lu, lun_i);
		MTX_UNLOCK(&lu->mutex);
		if (rc < 0)
			istgt_uctl_snprintf(uctl,
			    "ERR in sync cmd execution\n");
	} else {
		istgt_uctl_snprintf(uctl, "ERR sync_rsv \n");
		rc = -1;
	}

	if (rc < 0) {
		error_return:
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			return (UCTL_CMD_ERR);
	}
	/* logging event */
	ISTGT_NOTICELOG("Unit Rsv_Persist %s lun%d\n", iqn, lun_i);
	/* Persist succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

#ifdef	REPLICATION
#define	CHECK_ARG_AND_GOTO_ERROR { \
	if (arg == NULL) { \
		istgt_uctl_snprintf(uctl, "ERR invalid param\n"); \
		goto error_return; \
	} \
}

static int
istgt_uctl_cmd_snap(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu = NULL;
	ISTGT_LU_DISK *spec = NULL;
	const char *delim = ARGS_DELIM;
	char *volname, *snapname;
	int rc = 0, ret = UCTL_CMD_ERR, io_wait_time, wait_time;
	char *arg;
	bool r;
	arg = uctl->arg;

	CHECK_ARG_AND_GOTO_ERROR;
	volname = strsepq(&arg, delim);

	CHECK_ARG_AND_GOTO_ERROR;
	snapname = strsepq(&arg, delim);

	CHECK_ARG_AND_GOTO_ERROR;
	io_wait_time = atoi(strsepq(&arg, delim));

	CHECK_ARG_AND_GOTO_ERROR;
	wait_time = atoi(strsepq(&arg, delim));

	lu = istgt_lu_find_target_by_volname(uctl->istgt, volname);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	spec = lu->lun[0].spec;
	if (strcmp(uctl->cmd, "SNAPCREATE") == 0)
		r = istgt_lu_create_snapshot(spec, snapname, io_wait_time,
		    wait_time);
	else
		r = istgt_lu_destroy_snapshot(spec, snapname);
	if (r == true) {
		istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
		ret = UCTL_CMD_OK;
	}
	else
		istgt_uctl_snprintf(uctl, "ERR failed %s\n", uctl->cmd);
error_return:
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (ret);
}

static int
istgt_uctl_cmd_mempoolstats(UCTL_Ptr uctl)
{
	int rc = 0;
	char *response = NULL;

	istgt_lu_mempool_stats(&response);
	istgt_uctl_snprintf(uctl, "%s  %s\n", uctl->cmd, response);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		if (response)
			free(response);
		return (rc);
	}

	if (response)
		free(response);

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_replica_stats(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	int rc = 0;
	char *arg;
	char *response = NULL;
	char *volname = NULL;
	arg = uctl->arg;

	if (arg)
		volname = strsepq(&arg, delim);

	istgt_lu_replica_stats(volname, &response);
	istgt_uctl_snprintf(uctl, "%s  %s\n", uctl->cmd, response);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		if (response)
			free(response);
		return (rc);
	}

	if (response)
		free(response);

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}
#endif

static int
istgt_uctl_cmd_set(UCTL_Ptr uctl)
{
	ISTGT_Ptr istgt = uctl->istgt;
	ISTGT_LU_Ptr lu = NULL;
	ISTGT_LU_DISK *spec = NULL, *spec1 = NULL;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int i = 0;
	int rc = 0;
	uint8_t val1, val2;

	int setopt = 0;
	int setval = 0, j, tot = 0;
	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);
	setopt = atoi(strsepq(&arg, delim));
	setval = atoi(strsepq(&arg, delim));
	ISTGT_LOG("%s %s %d %d %s\n", iqn, lun, setopt, setval, arg);

	if (iqn == NULL || (arg != NULL && setopt != 15)) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		goto error_return;
	}

	if (setopt == 0) {
		istgt_uctl_snprintf(uctl, "ERR no setopt\n");
		goto error_return;
	}
	if (setval < 0) {
		istgt_uctl_snprintf(uctl,
		    "ERR invalid parameter value %d for opt %d\n",
		    setval, setopt);
		goto error_return;
	}
	if (strcmp(iqn, "ALL")) {
		lu = istgt_lu_find_target(uctl->istgt, iqn);
		if (lu == NULL) {
			istgt_uctl_snprintf(uctl, "ERR no target\n");
			goto error_return;
		}
		spec = lu->lun[0].spec;
	}

	switch (setopt) {
	case 1:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		ISTGT_LOG("send_abrt_resp %d->%d\n", send_abrt_resp, setval);
		send_abrt_resp = setval;
		break;
	case 2:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		ISTGT_LOG("abort_result_queue %d->%d\n",
		    abort_result_queue, setval);
		abort_result_queue = setval;
		break;
	case 3:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		ISTGT_LOG("wait_inflights %d->%d\n", wait_inflights, setval);
		wait_inflights = setval;
		break;
	case 4:
		if (setval < 1) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->max_unmap_sectors = setval;
			}
			ISTGT_LOG("ALL->max_unmap_sectors ->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->max_unmap_sectors %d->%d\n",
		    iqn, spec->max_unmap_sectors, setval);
		spec->max_unmap_sectors = setval;
		break;
	case 5:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		ISTGT_LOG("clear_resv %d->%d\n", clear_resv, setval);
		clear_resv = setval;
		break;
	case 6:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->ats = setval;
			}
			ISTGT_LOG("ALL->ats ->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->ats %d->%d\n", iqn, spec->ats, setval);
		spec->ats = setval;
		break;
	case 7:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->xcopy = setval;
			}
			ISTGT_LOG("ALL->xcopy ->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->xcopy %d->%d\n", iqn, spec->xcopy, setval);
		spec->xcopy = setval;
		break;
	case 8:
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				istgt->logical_unit[i]->limit_q_size = setval;
			}
			ISTGT_LOG("ALL->limit_q_size ->%d\n", setval);
			break;
		} else {
			ISTGT_LOG("%s->limit_q_size %d->%d\n",
			    iqn, lu->limit_q_size, setval);
			lu->limit_q_size = setval;
		}
		break;
	case 9:
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->delay_reserve = setval;
			}
			ISTGT_LOG("ALL->delay_reserve ->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->delay_reserve %d->%d\n",
		    iqn, spec->delay_reserve, setval);
		spec->delay_reserve = setval;
		break;
	case 10:
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->delay_release = setval;
			}
			ISTGT_LOG("ALL->delay_release ->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->delay_release %d->%d\n",
		    iqn, spec->delay_release, setval);
		spec->delay_release = setval;
		break;
	case 11:
		if (setval > 1 || setval < 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		ISTGT_LOG("abort_release %d->%d\n", abort_release, setval);
		abort_release = setval;
		break;
	case 12:
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->error_inject = setval;
			}
			ISTGT_LOG("ALL->error_inject->0x%x\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->error_inject 0x%x->0x%x\n",
		    iqn, spec->error_inject, setval);
		spec->error_inject = setval;
		break;
	case 13:
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->inject_cnt = setval;
			}
			ISTGT_LOG("ALL->inject_cnt->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->inject_cnt %d->%d\n",
		    iqn, spec->inject_cnt, setval);
		spec->inject_cnt = setval;
		break;
	case 14:
		if (setval > 1) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		if (strcmp(iqn, "ALL") == 0) {
			for (i = 1; i <= istgt->nlogical_unit; i++) {
				spec = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				if (!spec)
					goto spec_error;
				spec->exit_lu_worker = setval;
			}
			ISTGT_LOG("ALL->exit_lu_worker->%d\n", setval);
			break;
		}
		if (!spec)
			goto spec_error;
		ISTGT_LOG("%s->exit_lu_worker %d->%d\n",
		    iqn, spec->exit_lu_worker, setval);
		spec->exit_lu_worker = setval;
		break;
	case 15:
		if (setval < 2 || setval%2 != 0) {
			istgt_uctl_snprintf(uctl,
			    "ERR invalid parameter value %d\n", setval);
			goto error_return;
		}
		if (!spec)
			goto spec_error;
		spec->percent_count = 0;
		sleep(1);
		if (strcmp(iqn, "ALL") == 0) {
			spec = (ISTGT_LU_DISK *)
			    istgt->logical_unit[1]->lun[0].spec;
			if (!spec)
				goto spec_error;
			tot = 0;
			for (j = 0; j < setval; j += 2) {
				val1 = atoi(strsepq(&arg, delim));
				val2 = atoi(strsepq(&arg, delim));
				tot += val1;
				spec->percent_val[j>>1] = val1;
				spec->percent_latency[j>>1] = val2;
				ISTGT_LOG("%d %d\n", val1, val2);
			}

			if (tot != 100) {
				istgt_uctl_snprintf(uctl,
				    "ERR in tot %d percentage\n", tot);
				goto error_return;
			}
			spec->percent_count = (j>>1);

			for (i = 2; i <= istgt->nlogical_unit; i++) {
				spec1 = (ISTGT_LU_DISK *)
				    istgt->logical_unit[i]->lun[0].spec;
				for (j = 0; j < setval; j += 2) {
					spec1->percent_val[j>>1] =
					    spec->percent_val[j>>1];
					spec1->percent_latency[j>>1] =
					    spec->percent_latency[j>>1];
				}
				spec1->percent_count = (j>>1);
			}
			break;
		}
		tot = 0;
		for (j = 0; j < setval; j += 2) {
			val1 = atoi(strsepq(&arg, delim));
			val2 = atoi(strsepq(&arg, delim));
			tot += val1;
			spec->percent_val[j>>1] = val1;
			spec->percent_latency[j>>1] = val2;
			ISTGT_LOG("%d %d\n", val1, val2);
		}

		if (tot != 100) {
			istgt_uctl_snprintf(uctl,
			    "ERR in tot %d percentage\n", tot);
			goto error_return;
		}

		spec->percent_count = (j>>1);

		break;
	default:
		istgt_uctl_snprintf(uctl, "ERR invalid option %d\n", setopt);
		goto error_return;
	}
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);

spec_error:
	istgt_uctl_snprintf(uctl, "ERR spec is NULL\n");

error_return:
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_ERR);
}

static int
istgt_uctl_cmd_persist(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	char *persistopt;
	int lun_i;
	int rc;
	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);
	persistopt = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}

	if (persistopt == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no persistopt\n");
		goto error_return;
	}

	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}

	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}
	if (lu->type == ISTGT_LU_TYPE_DISK) {
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_disk_persist_reservation(lu, lun_i, persistopt);
		MTX_UNLOCK(&lu->mutex);
		if (rc < 0)
			istgt_uctl_snprintf(uctl,
			    "ERR in persist cmd execution\n");
	} else {
		istgt_uctl_snprintf(uctl, "ERR clear_rsv \n");
		rc = -1;
	}

	if (rc < 0) {
		error_return:
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			return (UCTL_CMD_ERR);
	}
	/* logging event */
	ISTGT_NOTICELOG("Unit Rsv_Persist %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);
	/* Persist succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}


static int
istgt_uctl_cmd_list(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	char *mflags;
	char *mfile;
	char *msize;
	char *mtype;
	char *workp;
	int lun_i;
	int worksize;
	int present;
	int lock;
	int rc;
	int i;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (iqn == NULL) {
		/* all targets */
		MTX_LOCK(&uctl->istgt->mutex);
		for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
			lu = uctl->istgt->logical_unit[i];
			if (lu == NULL)
				continue;
			istgt_uctl_snprintf(uctl,
			    "%s %s LU%d\n", uctl->cmd, lu->name, lu->num);
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				MTX_UNLOCK(&uctl->istgt->mutex);
				return (rc);
			}
		}
		MTX_UNLOCK(&uctl->istgt->mutex);
	} else {
		/* specified target */
		MTX_LOCK(&uctl->istgt->mutex);
		if (lun == NULL) {
			lun_i = 0;
		} else {
			lun_i = (int) strtol(lun, NULL, 10);
		}
		lu = istgt_lu_find_target(uctl->istgt, iqn);
		if (lu == NULL) {
			MTX_UNLOCK(&uctl->istgt->mutex);
			istgt_uctl_snprintf(uctl, "ERR no target\n");
		error_return:
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			return (UCTL_CMD_ERR);
		}
		if (lun_i < 0 || lun_i >= lu->maxlun) {
			MTX_UNLOCK(&uctl->istgt->mutex);
			istgt_uctl_snprintf(uctl, "ERR no target\n");
			goto error_return;
		}
		llp = &lu->lun[lun_i];

		worksize = uctl->worksize;
		workp = uctl->work;

		switch (llp->type) {
		case ISTGT_LU_LUN_TYPE_REMOVABLE:
			mflags = istgt_lu_get_media_flags_string(
			    llp->u.removable.flags, workp, worksize);
			worksize -= strlen(mflags) + 1;
			workp += strlen(mflags) + 1;
			present = istgt_uctl_get_media_present(lu, lun_i);
			lock = istgt_uctl_get_media_lock(lu, lun_i);
			mfile = llp->u.removable.file;
			if (llp->u.removable.flags &
			    ISTGT_LU_FLAG_MEDIA_AUTOSIZE) {
				snprintf(workp, worksize, "auto");
			} else {
				snprintf(workp, worksize, "%"PRIu64,
				    llp->u.removable.size);
			}
			msize = workp;
			worksize -= strlen(msize) + 1;
			workp += strlen(msize) + 1;
			snprintf(workp, worksize, "-");
			mtype = workp;
			worksize -= strlen(msize) + 1;
			workp += strlen(msize) + 1;

			istgt_uctl_snprintf(uctl,
			    "%s lun%u %s %s %s %s %s \"%s\" %s\n",
			    uctl->cmd, lun_i,
			    "removable",
			    (present ? "present" : "absent"),
			    (lock ? "lock" : "unlock"),
			    mtype, mflags, mfile, msize);
			rc = istgt_uctl_writeline(uctl);
			break;
		case ISTGT_LU_LUN_TYPE_STORAGE:
			mfile = llp->u.storage.file;
			snprintf(workp, worksize, "%"PRIu64,
			    llp->u.storage.size);
			msize = workp;
			worksize -= strlen(msize) + 1;
			workp += strlen(msize) + 1;

			istgt_uctl_snprintf(uctl, "%s lun%u %s \"%s\" %s\n",
			    uctl->cmd, lun_i,
			    "storage",
			    mfile, msize);
			rc = istgt_uctl_writeline(uctl);
			break;
		case ISTGT_LU_LUN_TYPE_DEVICE:
			mfile = llp->u.device.file;

			istgt_uctl_snprintf(uctl, "%s lun%u %s \"%s\"\n",
			    uctl->cmd, lun_i,
			    "device",
			    mfile);
			rc = istgt_uctl_writeline(uctl);
			break;
		case ISTGT_LU_LUN_TYPE_SLOT:
		default:
			MTX_UNLOCK(&uctl->istgt->mutex);
			istgt_uctl_snprintf(uctl, "ERR unsupport LUN type\n");
			goto error_return;
		}

		if (rc != UCTL_CMD_OK) {
			MTX_UNLOCK(&uctl->istgt->mutex);
			return (rc);
		}
		MTX_UNLOCK(&uctl->istgt->mutex);
	}

	/* list succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_unload(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type != ISTGT_LU_LUN_TYPE_REMOVABLE) {
		istgt_uctl_snprintf(uctl, "ERR not removable\n");
		goto error_return;
	}

	rc = 0;

#if 0	/* unload media from lun */
	switch (lu->type) {
	case ISTGT_LU_TYPE_DVD:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_dvd_unload_media(lu->lun[lun_i].spec);
		MTX_UNLOCK(&lu->mutex);
		break;
	case ISTGT_LU_TYPE_TAPE:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_tape_unload_media(lu->lun[lun_i].spec);
		MTX_UNLOCK(&lu->mutex);
		break;
	default:
		rc = -1;
	}
#endif

	if (rc < 0) {
		istgt_uctl_snprintf(uctl, "ERR unload\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Media Unload %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);

	/* unload succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_load(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type != ISTGT_LU_LUN_TYPE_REMOVABLE) {
		istgt_uctl_snprintf(uctl, "ERR not removable\n");
		goto error_return;
	}

	rc = -1;

#if 0	/* load media to lun */
	switch (lu->type) {
	case ISTGT_LU_TYPE_DVD:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_dvd_load_media(lu->lun[lun_i].spec);
		MTX_UNLOCK(&lu->mutex);
		break;
	case ISTGT_LU_TYPE_TAPE:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_tape_load_media(lu->lun[lun_i].spec);
		MTX_UNLOCK(&lu->mutex);
		break;
	default:
		rc = -1;
	}
#endif

	if (rc < 0) {
		istgt_uctl_snprintf(uctl, "ERR load\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Media Load %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);

	/* load succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_change(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char empty_flags[] = "ro";
	char empty_size[] = "0";
	char *arg;
	char *iqn;
	char *lun;
	char *type;
	char *flags;
	char *file;
	char *size;
	char *safedir;
	char *fullpath;
	char *abspath;
	int lun_i;
	int len;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	type = strsepq(&arg, delim);
	flags = strsepq(&arg, delim);
	file = strsepq(&arg, delim);
	size = strsepq(&arg, delim);

	if (iqn == NULL || lun == NULL || type == NULL || flags == NULL ||
	    file == NULL || size == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type != ISTGT_LU_LUN_TYPE_REMOVABLE) {
		istgt_uctl_snprintf(uctl, "ERR not removable\n");
		goto error_return;
	}

	/* make safe directory (start '/', end '/') */
	len = 1 + strlen(uctl->mediadirectory) + 1 + 1;
	safedir = xmalloc(len);
	if (uctl->mediadirectory[0] != '/') {
		ISTGT_WARNLOG("MediaDirectory is not starting with '/'\n");
		snprintf(safedir, len, "/%s", uctl->mediadirectory);
	} else {
		snprintf(safedir, len, "%s", uctl->mediadirectory);
	}
	if (strlen(safedir) > 1 && safedir[strlen(safedir) - 1] != '/') {
		safedir[strlen(safedir) + 1] = '\0';
		safedir[strlen(safedir)] = '/';
	}

	/* check abspath in mediadirectory? */
	len = strlen(safedir) + strlen(file) + 1;
	fullpath = xmalloc(len);
	if (file[0] != '/') {
		snprintf(fullpath, len, "%s%s", safedir, file);
	} else {
		snprintf(fullpath, len, "%s", file);
	}
#ifdef PATH_MAX
	abspath = xmalloc(len + PATH_MAX);
	file = realpath(fullpath, abspath);
#else

#if 0
	{
		long path_max;
		path_max = pathconf(fullpath, _PC_PATH_MAX);
		if (path_max != -1L) {
			abspath = xmalloc(path_max);
			file = realpath(fullpath, abspath);
		}
	}
#endif

	file = abspath = realpath(fullpath, NULL);
#endif /* PATH_MAX */
	if (file == NULL) {
		ISTGT_ERRLOG("realpath(%s) failed\n", fullpath);
	internal_error:
		xfree(safedir);
		xfree(fullpath);
		xfree(abspath);
		istgt_uctl_snprintf(uctl, "ERR %s internal error\n", uctl->cmd);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (strcasecmp(file, "/dev/null") == 0) {
		/* OK, empty slot */
		flags = empty_flags;
		size = empty_size;
	} else if (strncasecmp(file, safedir, strlen(safedir)) != 0) {
		ISTGT_ERRLOG("Realpath(%s) is not within MediaDirectory(%s)\n",
		    file, safedir);
		goto internal_error;
	}

	rc = -1;

#if 0	/* unload and load media from lun */
	switch (lu->type) {
	case ISTGT_LU_TYPE_DVD:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_dvd_change_media(lu->lun[lun_i].spec,
		    type, flags, file, size);
		MTX_UNLOCK(&lu->mutex);
		break;
	case ISTGT_LU_TYPE_TAPE:
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_tape_change_media(lu->lun[lun_i].spec,
		    type, flags, file, size);
		MTX_UNLOCK(&lu->mutex);
		break;
	default:
		rc = -1;
	}
#endif

	if (rc < 0) {
		xfree(safedir);
		xfree(fullpath);
		xfree(abspath);
		istgt_uctl_snprintf(uctl, "ERR change\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Media Change \"%s %s %s %s\" on %s lun%d from %s\n",
	    type, flags, file, size, iqn, lun_i, uctl->caddr);

	xfree(safedir);
	xfree(fullpath);
	xfree(abspath);

	/* change succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_reset(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc = 0, cleared = 0;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}

	/* reset lun */
	switch (lu->type) {
	case ISTGT_LU_TYPE_DISK:
		MTX_LOCK(&lu->mutex);
		cleared = istgt_lu_disk_reset(lu, lun_i, 0);
		MTX_UNLOCK(&lu->mutex);
		break;
	case ISTGT_LU_TYPE_DVD:
	case ISTGT_LU_TYPE_TAPE:
	case ISTGT_LU_TYPE_NONE:
	case ISTGT_LU_TYPE_PASS:
		cleared = -1;
		break;
	default:
		cleared = -1;
	}

	if (cleared < 0) {
		istgt_uctl_snprintf(uctl, "ERR reset\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Unit Reset %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);

	/* reset succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_clear(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}

	if (lu->type == ISTGT_LU_TYPE_DISK) {
		MTX_LOCK(&lu->mutex);
		rc = istgt_lu_disk_clear_reservation(lu, lun_i);
		MTX_UNLOCK(&lu->mutex);
		if (rc < 0)
			istgt_uctl_snprintf(uctl,
			    "ERR in clear cmd execution\n");
	} else {
		istgt_uctl_snprintf(uctl, "ERR clear_rsv \n");
		rc = -1;
	}

	if (rc < 0) {
		error_return:
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
		return (UCTL_CMD_ERR);
	}
	/* logging event */
	ISTGT_NOTICELOG("Unit Rsv_Clear %s lun%d\n",
	    iqn, lun_i);

	/* Clear succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}


static int
istgt_uctl_cmd_refresh(UCTL_Ptr uctl)
{
	int rc;
	int inconfig = 0;

	MTX_LOCK(&uctl->istgt->state_mutex);
	if (uctl->istgt->inconfig == 1)
		inconfig = 1;
	else
		uctl->istgt->inconfig = 1;
	MTX_UNLOCK(&uctl->istgt->state_mutex);
	if (inconfig == 1) {
		ISTGT_LOG("Previous istgtcontrol refresh/load is"
		    "still running..n");
		istgt_uctl_snprintf(uctl,
		    "Error Previous Refresh command still running\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	rc = istgt_reload(uctl->istgt);

	MTX_LOCK(&uctl->istgt->state_mutex);
	uctl->istgt->inconfig = 0;
	MTX_UNLOCK(&uctl->istgt->state_mutex);

	if (rc < 0) {
		istgt_uctl_snprintf(uctl, "ERR Refresh\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* Refresh succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_start(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}

	/* start lun */
	rc = istgt_lu_disk_start(lu, lun_i);
	if (rc < 0) {
		istgt_uctl_snprintf(uctl, "ERR start\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Unit Start %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);

	/* stop succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_stop(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_LUN_Ptr llp;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (iqn == NULL || arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}
	lu = istgt_lu_find_target(uctl->istgt, iqn);
	if (lu == NULL) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
	error_return:
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lun_i < 0 || lun_i >= lu->maxlun) {
		istgt_uctl_snprintf(uctl, "ERR no target\n");
		goto error_return;
	}
	llp = &lu->lun[lun_i];
	if (llp->type == ISTGT_LU_LUN_TYPE_NONE) {
		istgt_uctl_snprintf(uctl, "ERR no LUN\n");
		goto error_return;
	}

	/* stop lun */
	rc = istgt_lu_disk_stop(lu, lun_i);
	if (rc < 0) {
		istgt_uctl_snprintf(uctl, "ERR stop\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	/* logging event */
	ISTGT_NOTICELOG("Unit Stop %s lun%d from %s\n",
	    iqn, lun_i, uctl->caddr);

	/* stop succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_modify(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	char *OperationalMode;
	int dofake;
	int rc;
	int inmodify = 0;
	arg = uctl->arg;
	OperationalMode = strsepq(&arg, delim);

	if (OperationalMode == NULL) {
		dofake = 0;
	} else {
		dofake = (int) strtol(OperationalMode, NULL, 10);
	}

	if (dofake < 0 || arg != NULL) {
		ISTGT_LOG("modify %d returning.. ERR invalid parameters\n",
		    dofake);
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	MTX_LOCK(&uctl->istgt->state_mutex);
	if (uctl->istgt->inmodify == 1)
		inmodify = 1;
	else
		uctl->istgt->inmodify = 1;
	MTX_UNLOCK(&uctl->istgt->state_mutex);
	if (inmodify == 1) {
		ISTGT_LOG("istgtcontrol modify is still running..");
		return (UCTL_CMD_ERR);
	}

	/* Modify all the lun */
	rc = istgt_lu_disk_modify(uctl->istgt, dofake);

	MTX_LOCK(&uctl->istgt->state_mutex);
	uctl->istgt->inmodify = 0;
	MTX_UNLOCK(&uctl->istgt->state_mutex);

	if (rc < 0) {
		ISTGT_LOG("modify %d returning.. ERR stop\n", dofake);
		istgt_uctl_snprintf(uctl, "ERR stop\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	uctl->istgt->OperationalMode = dofake;

	/* Modify succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_mem(UCTL_Ptr uctl)
{
	int rc;
	char memBuf[4096];
	int mlen = 4090;

	rc = poolprint(memBuf, mlen);

	/* succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

extern int memdebug;
static int
istgt_uctl_cmd_memdebug(UCTL_Ptr uctl)
{
	int rc;

	if (memdebug == 1) {
		memdebug = 0;
		istgt_uctl_snprintf(uctl, "OK debug-OFF %s\n", uctl->cmd);
	} else {
		memdebug = 1;
		istgt_uctl_snprintf(uctl, "OK debug-ON  %s\n", uctl->cmd);
	}

	/* succeeded */
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}
extern int g_logtimes;
extern uint64_t g_logdelayns;

static int
istgt_uctl_cmd_log(UCTL_Ptr uctl)
{
	int rc;
	int changetrace = 0;
	int newtrace = 0;
	int newdelay = 0;
	int oldtrace = g_trace_flag;
	int oldlogtimes = g_logtimes;
	uint64_t olddelayms = g_logdelayns;

	const char *delim = ARGS_DELIM;
	char *arg;
	char *gottracestr;
	char *tracestr;
	char *delaystr;

	arg = uctl->arg;
	gottracestr = strsepq(&arg, delim);
	tracestr = strsepq(&arg, delim);
	delaystr = strsepq(&arg, delim);

	if (gottracestr == NULL)
		changetrace = 0;
	else
		changetrace = (int) strtol(gottracestr, NULL, 10);

	if (tracestr == NULL)
		newtrace = 0;
	else
		newtrace = (int) strtol(tracestr, NULL, 10);

	if (delaystr == NULL)
		newdelay = 0;
	else
		newdelay = (int) strtol(delaystr, NULL, 10);

	if (changetrace == 1)
		g_trace_flag = newtrace;

	if (newdelay == -2) {
		// leave it untouched
	} else if (newdelay == -1) {
		g_logtimes = 0;
	} else {
		g_logtimes = 1;
		g_logdelayns = newdelay * 1000;
	}
	if (g_logdelayns < 1)
		g_logdelayns = 0;

	ISTGT_LOG("cmd_log[%s] trace:%x->%x  delayedlog:%d->%d  us:%ld->%ld\n",
	    uctl->cmd, oldtrace, g_trace_flag,
	    oldlogtimes, g_logtimes,
	    olddelayms > 0 ? olddelayms/1000: 0,
	    g_logdelayns > 0 ? g_logdelayns/1000: 0);

	istgt_uctl_snprintf(uctl,
	    "OK cmd_log[%s] trace:%x->%x  "
	    "delayedlog:%d->%d  ms:%ld->%ld\n",
	    uctl->cmd, oldtrace, g_trace_flag,
	    oldlogtimes, g_logtimes,
	    olddelayms > 0 ? olddelayms/1000000: 0,
	    g_logdelayns > 0 ? g_logdelayns/1000000: 0);

	/* succeeded */
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_info(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	CONN_Ptr conn;
	SESS_Ptr sess;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	int ncount;
	int rc;
	int i, j, k;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);

	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	ncount = 0;
	MTX_LOCK(&uctl->istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (iqn != NULL && strcasecmp(iqn, lu->name) != 0)
			continue;

		istgt_lock_gconns();
		MTX_LOCK(&lu->mutex);
		for (j = 1; j < MAX_LU_TSIH; j++) {
			if (lu->tsih[j].initiator_port != NULL &&
			    lu->tsih[j].tsih != 0) {
				conn = istgt_find_conn(
				    lu->tsih[j].initiator_port,
				    lu->name, lu->tsih[j].tsih);
				if (conn == NULL || conn->sess == NULL)
					continue;

				sess = conn->sess;
				MTX_LOCK(&sess->mutex);
				for (k = 0; k < sess->connections; k++) {
					conn = sess->conns[k];
					if (conn == NULL)
						continue;

					istgt_uctl_snprintf(uctl,
					    "%s w#%d: Login from %s (%s) on %s"
					    " LU%d LU_Online=%s (%s:%s,%d),"
					    " ISID=%"PRIx64", TSIH=%u, CID=%u,"
					    " HeaderDigest=%s, DataDigest=%s,"
					    " MaxConnections=%u,"
					    " FirstBurstLength=%u,"
					    " MaxBurstLength=%u,"
					    " MaxRecvDataSegmentLength=%u,"
					    " InitialR2T=%s, ImmediateData=%s,"
					    " PendingPDUs=%d\n",
					    uctl->cmd,
					    conn->id,
					    conn->initiator_name,
					    conn->initiator_addr,
					    conn->target_name, lu->num,
					    (lu->online ? "Yes" : "No"),
					    conn->portal.host,
					    conn->portal.port,
					    conn->portal.tag,
					    conn->sess->isid, conn->sess->tsih,
					    conn->cid,
					    (conn->header_digest ?
							    "on" : "off"),
					    (conn->data_digest ? "on" : "off"),
					    conn->sess->MaxConnections,
					    conn->sess->FirstBurstLength,
					    conn->sess->MaxBurstLength,
					    conn->MaxRecvDataSegmentLength,
					    (conn->sess->initial_r2t ?
							    "Yes" : "No"),
					    (conn->sess->immediate_data ?
							    "Yes" : "No"),
					    conn->pending_pdus.num);
					rc = istgt_uctl_writeline(uctl);
					if (rc != UCTL_CMD_OK) {
						MTX_UNLOCK(&sess->mutex);
						MTX_UNLOCK(&lu->mutex);
						istgt_unlock_gconns();
						MTX_UNLOCK(&uctl->istgt->mutex);
						return (rc);
					}
					ncount++;
				}
				MTX_UNLOCK(&sess->mutex);
			}
		}
		MTX_UNLOCK(&lu->mutex);
		istgt_unlock_gconns();
	}
	MTX_UNLOCK(&uctl->istgt->mutex);
	if (ncount == 0) {
		istgt_uctl_snprintf(uctl, "%s no login\n", uctl->cmd);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
	}

	/* info succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}
static int
istgt_uctl_cmd_status(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	char *lun;
	int lun_i;
	int status;
	int i;
	int rc;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	lun = strsepq(&arg, delim);

	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	if (lun == NULL) {
		lun_i = 0;
	} else {
		lun_i = (int) strtol(lun, NULL, 10);
	}

	/* Print Operational Mode */
	istgt_uctl_snprintf(uctl, "%s Running:%s\n", uctl->cmd,
	    (uctl->istgt->OperationalMode)? "FAKE MODE OF OPERATION" :
			    "NORMAL MODE OF OPERATION");
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	MTX_LOCK(&uctl->istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (iqn != NULL && strcasecmp(iqn, lu->name) != 0)
			continue;
		/* Fetch status */
		status = istgt_lu_disk_status(lu, lun_i);
		if (status < 0) {
			istgt_uctl_snprintf(uctl, "ERR status\n");
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				MTX_UNLOCK(&uctl->istgt->mutex);
				return (rc);
			}
			MTX_UNLOCK(&uctl->istgt->mutex);
			return (UCTL_CMD_ERR);
		}
		/* Print Status */
		istgt_uctl_snprintf(uctl, "%s %s %s\n",
		    uctl->cmd, lu->name,
		    (status == ISTGT_LUN_BUSY) ?  "FAKE" : "REAL");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			MTX_UNLOCK(&uctl->istgt->mutex);
			return (rc);
		}
	}
	MTX_UNLOCK(&uctl->istgt->mutex);

	/* status succeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_maxtime(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_DISK *spec;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	int rc;
	int i;
	char *zero = NULL;
	int setzero = 0;
	int ind = 0;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);
	if (arg)
		zero = strsepq(&arg, delim);
	if (zero != NULL) {
		i = (int) strtol(zero, NULL, 10);
		if (i == 1)
			setzero = 1;
	}
	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	for (i = 1; i <= uctl->istgt->nlogical_unit; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if ((iqn != NULL && strcmp(iqn, lu->name) != 0) &&
		    (strcmp(iqn, "ALL") != 0))
			continue;

		spec = (ISTGT_LU_DISK *)lu->lun[0].spec;
		if (setzero == 1) {
			for (ind = 0; ind < 10; ind++) {
				spec->IO_size[ind].write.total_time.tv_sec = 0;
				spec->IO_size[ind].write.total_time.tv_nsec = 0;
				spec->IO_size[ind].read.total_time.tv_sec = 0;
				spec->IO_size[ind].read.total_time.tv_nsec = 0;
				spec->IO_size[ind]
				    .cmp_n_write.total_time.tv_sec = 0;
				spec->IO_size[ind]
				    .cmp_n_write.total_time.tv_nsec = 0;
				spec->IO_size[ind].unmp.total_time.tv_sec = 0;
				spec->IO_size[ind].unmp.total_time.tv_nsec = 0;
				spec->IO_size[ind]
				    .write_same.total_time.tv_sec = 0;
				spec->IO_size[ind]
				    .write_same.total_time.tv_nsec = 0;
			}
			istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK) {
				return (rc);
			}
			return (UCTL_CMD_OK);
		}
		istgt_uctl_snprintf(uctl, "%s LU%d %s\n",
		    uctl->cmd, lu->num, lu->name);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		for (ind = 0; ind < 10; ind++) {
			if (spec->IO_size[ind].write.total_time.tv_sec != 0 ||
			    spec->IO_size[ind].write.total_time.tv_nsec != 0) {
				istgt_uctl_snprintf(uctl,
				    "%s WR       |%10lu + %4lu| %ld.%9.9ld"
				    " [%c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld]\n",
				    uctl->cmd, spec->IO_size[ind].write.lba,
				    spec->IO_size[ind].write.lblen,
				    spec->IO_size[ind].write.total_time.tv_sec,
				    spec->IO_size[ind].write.total_time.tv_nsec,
				    spec->IO_size[ind].write.caller[1] ?
					    spec->IO_size[ind].write.caller[1] :
					    '9',
				    spec->IO_size[ind].write.tdiff[1].tv_sec,
				    spec->IO_size[ind].write.tdiff[1].tv_nsec,
				    spec->IO_size[ind].write.caller[2] ?
					    spec->IO_size[ind].write.caller[2] :
					    '9',
				    spec->IO_size[ind].write.tdiff[2].tv_sec,
				    spec->IO_size[ind].write.tdiff[2].tv_nsec,
				    spec->IO_size[ind].write.caller[3] ?
					    spec->IO_size[ind].write.caller[3] :
					    '9',
				    spec->IO_size[ind].write.tdiff[3].tv_sec,
				    spec->IO_size[ind].write.tdiff[3].tv_nsec,
				    spec->IO_size[ind].write.caller[4] ?
					    spec->IO_size[ind].write.caller[4] :
					    '9',
				    spec->IO_size[ind].write.tdiff[4].tv_sec,
				    spec->IO_size[ind].write.tdiff[4].tv_nsec,
				    spec->IO_size[ind].write.caller[5] ?
					    spec->IO_size[ind].write.caller[5] :
					    '9',
				    spec->IO_size[ind].write.tdiff[5].tv_sec,
				    spec->IO_size[ind].write.tdiff[5].tv_nsec,
				    spec->IO_size[ind].write.caller[6] ?
					    spec->IO_size[ind].write.caller[6] :
					    '9',
				    spec->IO_size[ind].write.tdiff[6].tv_sec,
				    spec->IO_size[ind].write.tdiff[6].tv_nsec,
				    spec->IO_size[ind].write.caller[7] ?
					    spec->IO_size[ind].write.caller[7] :
					    '9',
				    spec->IO_size[ind].write.tdiff[7].tv_sec,
				    spec->IO_size[ind].write.tdiff[7].tv_nsec,
				    spec->IO_size[ind].write.caller[8] ?
					    spec->IO_size[ind].write.caller[8] :
					    '9',
				    spec->IO_size[ind].write.tdiff[8].tv_sec,
				    spec->IO_size[ind].write.tdiff[8].tv_nsec
				);
				rc = istgt_uctl_writeline(uctl);
				if (rc != UCTL_CMD_OK) {
					return (rc);
				}
			}
		}
		for (ind = 0; ind < 10; ind++) {
			if (spec->IO_size[ind].read.total_time.tv_sec != 0 ||
			    spec->IO_size[ind].read.total_time.tv_nsec != 0) {
				istgt_uctl_snprintf(uctl,
				    "%s RD       |%10lu + %4lu| %ld.%9.9ld"
				    " [%c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld]\n",
				    uctl->cmd, spec->IO_size[ind].read.lba,
				    spec->IO_size[ind].read.lblen,
				    spec->IO_size[ind].read.total_time.tv_sec,
				    spec->IO_size[ind].read.total_time.tv_nsec,
				    spec->IO_size[ind].read.caller[1] ?
					    spec->IO_size[ind].read.caller[1] :
					    '9',
				    spec->IO_size[ind].read.tdiff[1].tv_sec,
				    spec->IO_size[ind].read.tdiff[1].tv_nsec,
				    spec->IO_size[ind].read.caller[2] ?
					    spec->IO_size[ind].read.caller[2] :
					    '9',
				    spec->IO_size[ind].read.tdiff[2].tv_sec,
				    spec->IO_size[ind].read.tdiff[2].tv_nsec,
				    spec->IO_size[ind].read.caller[3] ?
					    spec->IO_size[ind].read.caller[3] :
					    '9',
				    spec->IO_size[ind].read.tdiff[3].tv_sec,
				    spec->IO_size[ind].read.tdiff[3].tv_nsec,
				    spec->IO_size[ind].read.caller[4] ?
					    spec->IO_size[ind].read.caller[4] :
					    '9',
				    spec->IO_size[ind].read.tdiff[4].tv_sec,
				    spec->IO_size[ind].read.tdiff[4].tv_nsec,
				    spec->IO_size[ind].read.caller[5] ?
					    spec->IO_size[ind].read.caller[5] :
					    '9',
				    spec->IO_size[ind].read.tdiff[5].tv_sec,
				    spec->IO_size[ind].read.tdiff[5].tv_nsec,
				    spec->IO_size[ind].read.caller[6] ?
					    spec->IO_size[ind].read.caller[6] :
					    '9',
				    spec->IO_size[ind].read.tdiff[6].tv_sec,
				    spec->IO_size[ind].read.tdiff[6].tv_nsec,
				    spec->IO_size[ind].read.caller[7] ?
					    spec->IO_size[ind].read.caller[7] :
					    '9',
				    spec->IO_size[ind].read.tdiff[7].tv_sec,
				    spec->IO_size[ind].read.tdiff[7].tv_nsec,
				    spec->IO_size[ind].read.caller[8] ?
					    spec->IO_size[ind].read.caller[8] :
					    '9',
				    spec->IO_size[ind].read.tdiff[8].tv_sec,
				    spec->IO_size[ind].read.tdiff[8].tv_nsec
				);
				rc = istgt_uctl_writeline(uctl);
				if (rc != UCTL_CMD_OK) {
					return (rc);
				}
			}
		}
		for (ind = 0; ind < 10; ind++) {
			if (spec->IO_size[ind]
				    .cmp_n_write.total_time.tv_sec != 0 ||
			    spec->IO_size[ind].cmp_n_write.total_time.tv_nsec
				    != 0) {
				istgt_uctl_snprintf(uctl,
				    "%s CMP_n_WR |%10lu + %4lu| %ld.%9.9ld"
				    " [%c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld]\n",
				    uctl->cmd,
				    spec->IO_size[ind].cmp_n_write.lba,
				    spec->IO_size[ind].cmp_n_write.lblen,
				    spec->IO_size[ind]
					    .cmp_n_write.total_time.tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.total_time.tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[1] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[1] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[1].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[1].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[2] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[2] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[2].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[2].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[3] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[3] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[3].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[3].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[4] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[4] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[4].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[4].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[5] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[5] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[5].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[5].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[6] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[6] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[6].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[6].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[7] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[7] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[7].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[7].tv_nsec,
				    spec->IO_size[ind].cmp_n_write.caller[8] ?
					    spec->IO_size[ind]
						    .cmp_n_write.caller[8] :
					    '9',
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[8].tv_sec,
				    spec->IO_size[ind]
					    .cmp_n_write.tdiff[8].tv_nsec
				);
				rc = istgt_uctl_writeline(uctl);
				if (rc != UCTL_CMD_OK) {
					return (rc);
				}
			}
		}
		for (ind = 0; ind < 10; ind++) {
			if (spec->IO_size[ind].unmp.total_time.tv_sec != 0 ||
			    spec->IO_size[ind].unmp.total_time.tv_nsec != 0) {
				istgt_uctl_snprintf(uctl,
				    "%s UNMP     |%10lu + %4lu| %ld.%9.9ld"
				    " [%c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld]\n",
				    uctl->cmd, spec->IO_size[ind].unmp.lba,
				    spec->IO_size[ind].unmp.lblen,
				    spec->IO_size[ind].unmp.total_time.tv_sec,
				    spec->IO_size[ind].unmp.total_time.tv_nsec,
				    spec->IO_size[ind].unmp.caller[1] ?
					    spec->IO_size[ind].unmp.caller[1] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[1].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[1].tv_nsec,
				    spec->IO_size[ind].unmp.caller[2] ?
					    spec->IO_size[ind].unmp.caller[2] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[2].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[2].tv_nsec,
				    spec->IO_size[ind].unmp.caller[3] ?
					    spec->IO_size[ind].unmp.caller[3] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[3].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[3].tv_nsec,
				    spec->IO_size[ind].unmp.caller[4] ?
					    spec->IO_size[ind].unmp.caller[4] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[4].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[4].tv_nsec,
				    spec->IO_size[ind].unmp.caller[5] ?
					    spec->IO_size[ind].unmp.caller[5] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[5].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[5].tv_nsec,
				    spec->IO_size[ind].unmp.caller[6] ?
					    spec->IO_size[ind].unmp.caller[6] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[6].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[6].tv_nsec,
				    spec->IO_size[ind].unmp.caller[7] ?
					    spec->IO_size[ind].unmp.caller[7] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[7].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[7].tv_nsec,
				    spec->IO_size[ind].unmp.caller[8] ?
					    spec->IO_size[ind].unmp.caller[8] :
					    '9',
				    spec->IO_size[ind].unmp.tdiff[8].tv_sec,
				    spec->IO_size[ind].unmp.tdiff[8].tv_nsec
				);
				rc = istgt_uctl_writeline(uctl);
				if (rc != UCTL_CMD_OK) {
					return (rc);
				}
			}
		}
		for (ind = 0; ind < 10; ind++) {
			if (spec->IO_size[ind]
				    .write_same.total_time.tv_sec != 0 ||
			    spec->IO_size[ind]
				    .write_same.total_time.tv_nsec != 0) {
				istgt_uctl_snprintf(uctl,
				    "%s WR_SAME  |%10lu + %4lu| %ld.%9.9ld"
				    " [%c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld"
				    " %c:%ld.%9.9ld %c:%ld.%9.9ld]\n",
				    uctl->cmd,
				    spec->IO_size[ind].write_same.lba,
				    spec->IO_size[ind].write_same.lblen,
				    spec->IO_size[ind]
					    .write_same.total_time.tv_sec,
				    spec->IO_size[ind]
					    .write_same.total_time.tv_nsec,
				    spec->IO_size[ind].write_same.caller[1] ?
					    spec->IO_size[ind]
						    .write_same.caller[1] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[1].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[1].tv_nsec,
				    spec->IO_size[ind].write_same.caller[2] ?
					    spec->IO_size[ind]
						    .write_same.caller[2] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[2].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[2].tv_nsec,
				    spec->IO_size[ind].write_same.caller[3] ?
					    spec->IO_size[ind]
						    .write_same.caller[3] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[3].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[3].tv_nsec,
				    spec->IO_size[ind].write_same.caller[4] ?
					    spec->IO_size[ind]
						    .write_same.caller[4] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[4].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[4].tv_nsec,
				    spec->IO_size[ind].write_same.caller[5] ?
					    spec->IO_size[ind]
						    .write_same.caller[5] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[5].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[5].tv_nsec,
				    spec->IO_size[ind].write_same.caller[6] ?
					    spec->IO_size[ind]
						    .write_same.caller[6] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[6].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[6].tv_nsec,
				    spec->IO_size[ind].write_same.caller[7] ?
					    spec->IO_size[ind]
						    .write_same.caller[7] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[7].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[7].tv_nsec,
				    spec->IO_size[ind].write_same.caller[8] ?
					    spec->IO_size[ind]
						    .write_same.caller[8] :
					    '9',
				    spec->IO_size[ind]
					    .write_same.tdiff[8].tv_sec,
				    spec->IO_size[ind]
					    .write_same.tdiff[8].tv_nsec
				);
				rc = istgt_uctl_writeline(uctl);
				if (rc != UCTL_CMD_OK) {
					return (rc);
				}
			}
		}
	}

	/* info succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}
static int
istgt_uctl_cmd_dump(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_DISK *spec;
	CONN_Ptr conn;
	SESS_Ptr sess;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	int ncount = 0;
	int lu_num = 0;
	int rc;
	int i, j, k, t;
	int x = 0;
	int detail = 0;
	PORTAL_GROUP *pgp;
	char temp[2048];
	char size[100];
	char *c_size = size;
	char *bp = temp;
	int count = 0;
	int rem = 2048, ln = 0;
	uint64_t temp_s = 0, temp_s2 = 0;

	arg = uctl->arg;
	detail = atoi(strsepq(&arg, delim));
	iqn = strsepq(&arg, delim);

	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}

	MTX_LOCK(&uctl->istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (iqn != NULL && strcasecmp(iqn, lu->name) != 0)
			continue;
		lu_num++;
		count = 0;

		istgt_lock_gconns();
		MTX_LOCK(&lu->mutex);
		/* limit host string to 2048 characters */
		for (j = 0; j < lu->maxmap && rem; j++) {
			pgp = istgt_lu_find_portalgroup(uctl->istgt,
			    lu->map[j].pg_tag);
			if (pgp != NULL) {
				for (x = 0; x < pgp->nportals && rem; x++) {
					ln = snprintf(bp, rem, " IP%d:%s  ",
					    x+1, pgp->portals[x]->host);
					if (ln < 0)
						ln = 0;
					else if (ln > rem)
						ln = rem;
					rem -= ln;
					bp += ln;
					*bp = '\0';
				}
			}
		}
		bp = temp;
		spec = (ISTGT_LU_DISK *)lu->lun[0].spec;
		temp_s = spec->size;
		do {
			if (temp_s/1024 == 0)
				break;
			else {
				count++;
				temp_s2 = temp_s % 1024;
				temp_s /= 1024;
			}
		} while (1);
		switch (count) {
			case 0: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'B'); break;
			case 1: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'K'); break;
			case 2: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'M'); break;
			case 3: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'G'); break;
			case 4: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'T'); break;
			case 5: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'P'); break;
			case 6: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'E'); break;
			case 7: snprintf(c_size, 100, "%lu.%lu%c",
			    temp_s, temp_s2, 'Z'); break;
		}
		if (detail == 1)
			istgt_uctl_snprintf(uctl,
			    "%s LUN LU%d %s Luworkers:%d Qdepth:%d Size:%s"
			    " Blocklength:%lu PhysRecordLength:%d Unmap:%s"
			    " Wzero:%s ATS:%s XCOPY:%s %s CONNECTIONS:%d\n",
			    uctl->cmd, lu->num, lu->name, lu->luworkers,
			    lu->queue_depth, c_size,
			    spec->blocklen, lu->recordsize,
			    (spec->unmap == 1) ? "Enabled":"Disabled",
			    (spec->wzero == 1) ? "Enabled":"Disabled",
			    (spec->ats == 1) ? "Enabled":"Disabled",
			    (spec->xcopy == 1) ? "Enabled":"Disabled",
			    temp, lu->conns);
		else
			istgt_uctl_snprintf(uctl,
			    "%s LUN LU%d %s %s CONNECTIONS:%d\n",
			    uctl->cmd, lu->num, lu->name, temp, lu->conns);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			MTX_UNLOCK(&lu->mutex);
			istgt_unlock_gconns();
			MTX_UNLOCK(&uctl->istgt->mutex);
			return (rc);
		}

		for (j = 1; j < MAX_LU_TSIH; j++) {
			if (lu->tsih[j].initiator_port != NULL &&
			    lu->tsih[j].tsih != 0) {
				conn = istgt_find_conn(
				    lu->tsih[j].initiator_port,
				    lu->name, lu->tsih[j].tsih);
				if (conn == NULL || conn->sess == NULL)
					continue;

				sess = conn->sess;
				MTX_LOCK(&sess->mutex);
				for (k = 0; k < sess->connections; k++) {
					conn = sess->conns[k];
					if (conn == NULL)
						continue;

					for (t = 0; t < lu->maxmap; t++) {
						pgp = istgt_lu_find_portalgroup(
						    uctl->istgt,
						    lu->map[t].pg_tag);
						if (pgp == NULL)
							continue;

						for (x = 0; x < pgp->nportals;
						    x++) {
							if (strcmp(pgp
							    ->portals[x]->host,
							    conn->target_addr)
							    == 0) {
								break;
							}
						}
					}
					istgt_uctl_snprintf(uctl, "%s CONN c#%d"
					    " %"PRIx64"   %u     %u"
					    "   IP%d   %s    %s\n",
					    uctl->cmd,
					    conn->id,
					    conn->sess->isid, conn->sess->tsih,
					    conn->cid, x+1,
					    conn->initiator_addr,
					    conn->initiator_name);
					rc = istgt_uctl_writeline(uctl);
					if (rc != UCTL_CMD_OK) {
						MTX_UNLOCK(&sess->mutex);
						MTX_UNLOCK(&lu->mutex);
						istgt_unlock_gconns();
						MTX_UNLOCK(&uctl->istgt->mutex);
						return (rc);
					}
					ncount++;
				}
				MTX_UNLOCK(&sess->mutex);
			}
		}
		MTX_UNLOCK(&lu->mutex);
		istgt_unlock_gconns();
	}
	MTX_UNLOCK(&uctl->istgt->mutex);

	istgt_uctl_snprintf(uctl, "%s TOTAL LOGICAL_UNITS:%d CONNECTIONS:%d\n",
	    uctl->cmd, lu_num, ncount);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}

	if (ncount == 0) {
		istgt_uctl_snprintf(uctl, "%s no login\n", uctl->cmd);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
	}

	/* info succeeded */
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

static int
istgt_uctl_cmd_rsv(UCTL_Ptr uctl)
{
	ISTGT_LU_Ptr lu;
	const char *delim = ARGS_DELIM;
	char *arg;
	char *iqn;
	int rc;
	int i;

	arg = uctl->arg;
	iqn = strsepq(&arg, delim);

	if (arg != NULL) {
		istgt_uctl_snprintf(uctl, "ERR invalid parameters\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	MTX_LOCK(&uctl->istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (iqn != NULL && strcasecmp(iqn, lu->name) != 0)
			continue;
		(void) istgt_lu_disk_print_reservation(
		    lu, 0); // CB has only lun 0
	}
	MTX_UNLOCK(&uctl->istgt->mutex);

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

extern clockid_t clockid;

static int
istgt_uctl_cmd_que(UCTL_Ptr uctl)
{
#define	adjbuf() {    \
	if (wn < 0)    \
		wn = 0;    \
	else if (wn > brem) \
		wn = brem; \
	bptr += wn;    \
	brem -= wn;    \
}
#define	tdiff(_s, _n, _r) {                     \
	if ((_n.tv_nsec - _s.tv_nsec) < 0) {        \
		_r.tv_sec  = _n.tv_sec - _s.tv_sec-1;   \
		_r.tv_nsec = 1000000000 + _n.tv_nsec - _s.tv_nsec; \
	} else {                                    \
		_r.tv_sec  = _n.tv_sec - _s.tv_sec;     \
		_r.tv_nsec = _n.tv_nsec - _s.tv_nsec;   \
	}                                           \
}
#define	_BSZ_ 4086
	char buf[_BSZ_+10];
	int  brem = _BSZ_, wn = 0;
	int  toprint, chunk, j;
	char *bptr = buf;

	ISTGT_LU_Ptr lu;
	const char *delim = ARGS_DELIM;
	char *arg, *subcmd;
	char *iqn = NULL, *lu_str = NULL;
	int rc, err = 0;
	int i, levels;
	int lu_num = -1;
	int cq, bq, inf, inflight;
	ISTGT_LU_DISK *spec;
#if 0
	ISTGT_LU_TASK_Ptr tptr;
	void *cookie = NULL;
	struct timespec r;
#endif
	struct timespec now, now1;
	int unlocked = 0;

	arg = uctl->arg;
	subcmd = strsepq(&arg, delim);
	if (subcmd != NULL) {
		if (strcasecmp(subcmd, "IQN") == 0)
			iqn = strsepq(&arg, delim);
		else if (strcasecmp(subcmd, "LU") == 0)
			lu_str = strsepq(&arg, delim);
		else if (strcasecmp(subcmd, "ALL") == 0)
			;
		else
			err = 1;
	}
	if (arg != NULL || err == 1) {
		istgt_uctl_snprintf(uctl,
		    "ERR invalid parameters."
		    " usage: 'QUE IQN <iqn_name>'  or  'QUE LU <lu_number>'\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (rc);
		}
		return (UCTL_CMD_ERR);
	}
	if (lu_str != NULL) {
		lu_num = (int) strtol(lu_str, NULL, 10);
	}

	MTX_LOCK(&uctl->istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = uctl->istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (iqn != NULL && strcasecmp(iqn, lu->name) != 0)
			continue;
		if (lu_num != -1 && lu->num != lu_num)
			continue;

		if (lu->lun[0].type != ISTGT_LU_LUN_TYPE_STORAGE)
			continue;
		spec = (ISTGT_LU_DISK *) lu->lun[0].spec;
		if (spec == NULL)
			continue;

		levels = 24;

		clock_gettime(clockid, &now);
		if (spec->do_avg == 0)
		{
			for (j = 0; j < 32; j++)
			{
				spec->avgs[j].count = 0;
				spec->avgs[j].tot_sec = 0;
				spec->avgs[j].tot_nsec = 0;
			}
			spec->do_avg = 1;
			clock_gettime(clockid, &now1);
			spec->avgs[0].tot_sec = now1.tv_sec;
			spec->avgs[0].tot_nsec = now1.tv_nsec;
		}
		else
		{
			spec->do_avg = 0;
			wn = snprintf(bptr, brem, " Avgs:");
			adjbuf()
			if (((signed long)((signed long)(now.tv_nsec) -
			    (signed long)(spec->avgs[0].tot_nsec))) < 0) {
				spec->avgs[0].tot_sec  =
				    now.tv_sec - spec->avgs[0].tot_sec - 1;
				spec->avgs[0].tot_nsec = 1000000000 +
				    now.tv_nsec - spec->avgs[0].tot_nsec;
			} else {
				spec->avgs[0].tot_sec =
				    now.tv_sec - spec->avgs[0].tot_sec;
				spec->avgs[0].tot_nsec =
				    now.tv_nsec - spec->avgs[0].tot_nsec;
			}

			for (j = 0; j < levels; j++)
			{
				wn = snprintf(bptr, brem, " %d:%d %ld.%9.9ld",
				    j, spec->avgs[j].count,
				    spec->avgs[j].tot_sec,
				    spec->avgs[j].tot_nsec);
				adjbuf()
			}
			wn = snprintf(bptr, brem, " %d:%d",
				j, spec->error_count);
			adjbuf()
		}

		inflight = spec->inflight;
//		MTX_LOCK(&spec->cmd_queue_mutex);
		cq = spec->cmd_queue.num;
		bq = spec->blocked_queue.num;
		inf = spec->ludsk_ref;
#if 0
		while ((tptr = (ISTGT_LU_TASK_Ptr)
		    istgt_queue_walk(&spec->cmd_queue, &cookie)) != NULL) {
			tdiff(tptr->lu_cmd.times[0], now, r)
			wn = snprintf(bptr, brem,
			    " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
			    i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
			    tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
			    r.tv_sec, r.tv_nsec);
			adjbuf()
		}
		*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem -= 3;
		cookie = NULL;
		while ((tptr = (ISTGT_LU_TASK_Ptr)
		    istgt_queue_walk(&spec->blocked_queue, &cookie)) != NULL) {
			tdiff(tptr->lu_cmd.times[0], now, r)
			wn = snprintf(bptr, brem,
			    " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
			    i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
			    tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
			    r.tv_sec, r.tv_nsec);
			adjbuf()
		}
		*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem -= 3;
#endif
//		MTX_UNLOCK(&spec->cmd_queue_mutex);
		/* luworker waiting data from zvol */
#if 0
		for (i = 0; i < spec->luworkers; i++) {
			MTX_LOCK(&spec->luworker_mutex[i]);
			if (spec->inflight_io[i] != NULL) {
				tdiff(spec->inflight_io[i]->lu_cmd.times[0],
				    now, r)
				wn = snprintf(bptr, brem,
				    " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				    i, spec->inflight_io[i]->lu_cmd.CmdSN,
				    spec->inflight_io[i]->lu_cmd.cdb0,
				    spec->inflight_io[i]->lu_cmd.lba,
				    spec->inflight_io[i]->lu_cmd.lblen,
				    r.tv_sec, r.tv_nsec);
				adjbuf()
			}
			MTX_UNLOCK(&spec->luworker_mutex[i]);
		}
		// MTX_UNLOCK(&spec->cmd_queue_mutex);
		*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem -= 3;
		/* luworker waiting data from network */
		MTX_LOCK(&spec->wait_lu_task_mutex);
		for (i = 0; i < ISTGT_MAX_NUM_LUWORKERS; i++) {
			if (spec->wait_lu_task[i] != NULL) {
				tdiff(spec->wait_lu_task[i]->lu_cmd.times[0],
				    now, r)
				wn = snprintf(bptr, brem,
				    " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				    i, spec->wait_lu_task[i]->lu_cmd.CmdSN,
				    spec->wait_lu_task[i]->lu_cmd.cdb0,
				    spec->wait_lu_task[i]->lu_cmd.lba,
				    spec->wait_lu_task[i]->lu_cmd.lblen,
				    r.tv_sec, r.tv_nsec);
				adjbuf()
			}
		}
		MTX_UNLOCK(&spec->wait_lu_task_mutex);
#endif
		MTX_UNLOCK(&uctl->istgt->mutex);
		unlocked = 1;

		ISTGT_TRACELOG(ISTGT_TRACE_CMD,
		    "LU%d:QUE %s %s Q[%d %d %d %d] q:%d thr:%d/%d [sz:%lu,"
		    " %lu blks of %lu bytes, phy:%u %s%s] er_cnt:%d\n",
		    lu->num, lu->name ? lu->name : "-",
		    (spec->fd != -1) ? "on" : "off", cq, bq, inf, inflight,
		    spec->queue_depth, spec->luworkers, spec->luworkersActive,
		    spec->size, spec->blockcnt, spec->blocklen, spec->rshift,
		    spec->readcache ?  "" : "RCD",
		    spec->writecache ? " WCE" : "",
		    spec->error_count);

		istgt_uctl_snprintf(uctl,
		    "%s LU%d:%s %s Q[%d %d %d %d] q:%d thr:%d/%d [sz:%lu,"
		    " %lu blks of %lu bytes, phy:%u %s%s] er_cnt:%d\n",
		    uctl->cmd, lu->num, lu->name ? lu->name : "-",
		    (spec->fd != -1) ? "on" : "off", cq, bq, inf, inflight,
		    spec->queue_depth, spec->luworkers, spec->luworkersActive,
		    spec->size, spec->blockcnt, spec->blocklen, spec->rshift,
		    spec->readcache ?  "" : "RCD",
		    spec->writecache ? " WCE" : "",
		    spec->error_count);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK)
			return (rc);

		toprint = _BSZ_ - brem;
		bptr = buf;
		i = 0;
		while (toprint > 0)
		{
			if (toprint > 1023)
				chunk = 1024;
			else
				chunk = toprint;
			ISTGT_TRACELOG(ISTGT_TRACE_CMD, "LU%d:QUE%d [%.*s]\n",
			    lu->num, i, chunk, bptr);
			istgt_uctl_snprintf(uctl, "%s LU%d:%d [%.*s]\n",
			    uctl->cmd, lu->num, i, chunk, bptr);
			rc = istgt_uctl_writeline(uctl);
			if (rc != UCTL_CMD_OK)
				return (rc);
			toprint -= chunk;
			bptr += chunk;
			++i;
		}
		break;
	}
	if (unlocked == 0) {
		MTX_UNLOCK(&uctl->istgt->mutex);
	}

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

extern _verb_istat ISCSIstat_rest[ISCSI_ARYSZ];
extern _verb_stat SCSIstat_rest[SCSI_ARYSZ];

_verb_stat SCSIstat_last[SCSI_ARYSZ] = { {0, 0, 0} };
_verb_stat SCSIstat_now[SCSI_ARYSZ] = { {0, 0, 0} };
_verb_stat SCSIstat_rslt[SCSI_ARYSZ] = { {0, 0, 0} };

_verb_istat ISCSIstat_last[ISCSI_ARYSZ] = { {0, 0, 0} };
_verb_istat ISCSIstat_now[ISCSI_ARYSZ] = { {0, 0, 0} };
_verb_istat ISCSIstat_rslt[ISCSI_ARYSZ] = { {0, 0, 0} };

#ifdef REPLICATION
/*
 * istgt_uctl_cmd_iostats collects the iostats from the spec structure
 * and marshal them into json format using json-c library.The returned
 * string memory is managed by the json_object and will be freed when
 * the reference count of the json_object drops to zero.Following command
 * can be used to fetch the iostats.
 * USE : sudo istgtcontrol iostats
 * TODO: Add the fields for getting the latency, used capacity etc.
 */
static int
istgt_uctl_cmd_iostats(UCTL_Ptr uctl)
{
	int rc, replica_cnt;
	uint64_t usedlogicalblocks;
	struct timespec now;
	uint64_t time_diff;
	/* instantiate json_object from json-c library. */
	struct json_object *jobj;
	replica_t *replica;
	ISTGT_LU_DISK *spec;

	MTX_LOCK(&specq_mtx);
	TAILQ_FOREACH(spec, &spec_q, spec_next) {
		jobj = json_object_new_object();	/* create new object */

		json_object_object_add(jobj, "iqn",
		    json_object_new_string(spec->lu->name));
		json_object_object_add(jobj, "WriteIOPS",
		    json_object_new_uint64(spec->writes));
		json_object_object_add(jobj, "ReadIOPS",
		    json_object_new_uint64(spec->reads));
		json_object_object_add(jobj, "TotalWriteBytes",
		    json_object_new_uint64(spec->writebytes));
		json_object_object_add(jobj, "TotalReadBytes",
		    json_object_new_uint64(spec->reads));
		json_object_object_add(jobj, "Size",
		    json_object_new_uint64(spec->size));

		usedlogicalblocks = (spec->stats.used / spec->blocklen);
		json_object_object_add(jobj, "UsedLogicalBlocks",
		    json_object_new_uint64(usedlogicalblocks));

		json_object_object_add(jobj, "SectorSize",
		    json_object_new_uint64(spec->blocklen));

		clock_gettime(CLOCK_MONOTONIC_RAW, &now);
		time_diff = (uint64_t)(now.tv_sec - istgt_start_time.tv_sec);
		json_object_object_add(jobj, "UpTime",
		    json_object_new_uint64(time_diff));

		json_object_object_add(jobj, "TotalReadTime",
		    json_object_new_uint64(spec->totalreadtime));
		json_object_object_add(jobj, "TotalWriteTime",
		    json_object_new_uint64(spec->totalwritetime));
		json_object_object_add(jobj, "TotalReadBlockCount",
		    json_object_new_uint64(spec->totalreadblockcount));
		json_object_object_add(jobj, "TotalWriteBlockCount",
		    json_object_new_uint64(spec->totalwriteblockcount));

                replica_cnt = spec->healthy_rcount + spec->degraded_rcount;
		json_object_object_add(jobj, "ReplicaCounter",
		    json_object_new_int(replica_cnt));
		json_object_object_add(jobj, "RevisionCounter",
		    json_object_new_uint64(spec->io_seq));
		json_object_object_add(jobj, "Status",
		    json_object_new_string(get_cv_status(spec, replica_cnt,
                        spec->healthy_rcount)));

		json_object *jobj_arr = json_object_new_array();
		MTX_LOCK(&spec->rq_mtx);
		TAILQ_FOREACH(replica, &spec->rq, r_next) {
		    MTX_LOCK(&replica->r_mtx);
		    json_object *jobjarr = json_object_new_object();
		    json_object_object_add(jobjarr, "Address",
			json_object_new_string(replica->ip));
		    json_object_object_add(jobjarr, "Mode",
			json_object_new_string(((replica->state ==
			    ZVOL_STATUS_HEALTHY) ? "HEALTHY" : "DEGRADED")));
		    MTX_UNLOCK(&replica->r_mtx);
		    json_object_array_add(jobj_arr, jobjarr);
		}
		MTX_UNLOCK(&spec->rq_mtx);

		json_object_object_add(jobj, "Replicas", jobj_arr);

		istgt_uctl_snprintf(uctl, "%s  %s\n",
		    uctl->cmd, json_object_to_json_string(jobj));
		rc = istgt_uctl_writeline(uctl);

		/* freeing root json_object will free all the allocated memory
		** associated with the json_object.
		*/
		json_object_put(jobj);

		if (rc != UCTL_CMD_OK) {
			MTX_UNLOCK(&specq_mtx);
			return (rc);
		}
	}
	MTX_UNLOCK(&specq_mtx);
	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}
#endif

static int
istgt_uctl_cmd_stats(UCTL_Ptr uctl)
{
	const char *delim = ARGS_DELIM;
	char *arg;
	int rc;
	int i;
	char *zero = NULL;
	int setzero = 0;

	int is_li = 0;
	int s_li = 0;
	_verb_istat is_l[ISCSI_ARYSZ];
	_verb_stat s_l[SCSI_ARYSZ];

	arg = uctl->arg;
	if (arg)
		zero = strsepq(&arg, delim);

	if (zero != NULL) {
		i = (int) strtol(zero, NULL, 10);
		if (i == 1)
			setzero = 1;
	}

	bcopy(&ISCSIstat_rest, &ISCSIstat_now, sizeof (ISCSIstat_now));
	bcopy(&SCSIstat_rest, &SCSIstat_now, sizeof (SCSIstat_now));
	if (setzero == 1) {
		for (i = 0; i < ISCSI_ARYSZ; ++i) {
			ISCSIstat_rslt[i].pdu_read =
			    ISCSIstat_now[i].pdu_read >=
				    ISCSIstat_last[i].pdu_read ?
			    ISCSIstat_now[i].pdu_read -
				    ISCSIstat_last[i].pdu_read :
			    (0xffffffff - ISCSIstat_last[i].pdu_read) +
				    ISCSIstat_now[i].pdu_read;
			ISCSIstat_rslt[i].pdu_sent =
			    ISCSIstat_now[i].pdu_sent >=
				    ISCSIstat_last[i].pdu_sent ?
			    ISCSIstat_now[i].pdu_sent -
				    ISCSIstat_last[i].pdu_sent :
			    (0xffffffff - ISCSIstat_last[i].pdu_sent) +
				    ISCSIstat_now[i].pdu_sent;
			if (ISCSIstat_rslt[i].pdu_read ||
			    ISCSIstat_rslt[i].pdu_sent) {
				is_l[is_li].opcode = ISCSIstat_now[i].opcode;
				is_l[is_li].pdu_read =
				    ISCSIstat_rslt[i].pdu_read;
				is_l[is_li++].pdu_sent =
				    ISCSIstat_rslt[i].pdu_sent;
			}
		}
		for (i = 0; i < SCSI_ARYSZ; ++i) {
			SCSIstat_rslt[i].req_start =
			    SCSIstat_now[i].req_start >=
				    SCSIstat_last[i].req_start ?
			    SCSIstat_now[i].req_start -
				    SCSIstat_last[i].req_start :
			    (0xffffffff - SCSIstat_last[i].req_start) +
				    SCSIstat_now[i].req_start;
			SCSIstat_rslt[i].req_finish =
			    SCSIstat_now[i].req_finish >=
				    SCSIstat_last[i].req_finish ?
			    SCSIstat_now[i].req_finish -
				    SCSIstat_last[i].req_finish :
			    (0xffffffff - SCSIstat_last[i].req_finish) +
				    SCSIstat_now[i].req_finish;
			if (SCSIstat_rslt[i].req_start ||
			    SCSIstat_rslt[i].req_finish) {
				s_l[s_li].opcode = SCSIstat_now[i].opcode;
				s_l[s_li].req_start =
				    SCSIstat_rslt[i].req_start;
				s_l[s_li++].req_finish =
				    SCSIstat_rslt[i].req_finish;
			}
		}
	} else {
		for (i = 0; i < ISCSI_ARYSZ; ++i) {
			if (ISCSIstat_now[i].pdu_read ||
			    ISCSIstat_now[i].pdu_sent) {
				is_l[is_li].opcode = ISCSIstat_now[i].opcode;
				is_l[is_li].pdu_read =
				    ISCSIstat_now[i].pdu_read;
				is_l[is_li++].pdu_sent =
				    ISCSIstat_now[i].pdu_sent;
			}
		}
		for (i = 0; i < SCSI_ARYSZ; ++i) {
			if (SCSIstat_now[i].req_start ||
			    SCSIstat_now[i].req_finish) {
				s_l[s_li].opcode = SCSIstat_now[i].opcode;
				s_l[s_li].req_start = SCSIstat_now[i].req_start;
				s_l[s_li++].req_finish =
				    SCSIstat_now[i].req_finish;
			}
		}
	}
	bcopy(&ISCSIstat_now, &ISCSIstat_last, sizeof (ISCSIstat_last));
	bcopy(&SCSIstat_now, &SCSIstat_last, sizeof (SCSIstat_last));

	i = is_li;
	while (i < 12) {
		is_l[i].opcode = 0;
		is_l[i].pdu_read = 0;
		is_l[i++].pdu_sent = 0;
	}
	i = s_li;
	while (i < 12) {
		s_l[i].opcode = 0;
		s_l[i].req_start = 0;
		s_l[i++].req_finish = 0;
	}

	if (is_li) {
		istgt_uctl_snprintf(uctl, "%s  PDU:%d [%x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u]\n",
		    uctl->cmd, is_li,
		    is_l[0].opcode, is_l[0].pdu_read, is_l[0].pdu_sent,
		    is_l[1].opcode, is_l[1].pdu_read, is_l[1].pdu_sent,
		    is_l[2].opcode, is_l[2].pdu_read, is_l[2].pdu_sent,
		    is_l[3].opcode, is_l[3].pdu_read, is_l[3].pdu_sent,
		    is_l[4].opcode, is_l[4].pdu_read, is_l[4].pdu_sent,
		    is_l[5].opcode, is_l[5].pdu_read, is_l[5].pdu_sent,
		    is_l[6].opcode, is_l[6].pdu_read, is_l[6].pdu_sent,
		    is_l[7].opcode, is_l[7].pdu_read, is_l[7].pdu_sent,
		    is_l[8].opcode, is_l[8].pdu_read, is_l[8].pdu_sent,
		    is_l[9].opcode, is_l[9].pdu_read, is_l[9].pdu_sent);
		rc = istgt_uctl_writeline(uctl);

		ISTGT_TRACELOG(ISTGT_TRACE_CMD, "%s  PDU:%d [%x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u]\n",
		    uctl->cmd, is_li,
		    is_l[0].opcode, is_l[0].pdu_read, is_l[0].pdu_sent,
		    is_l[1].opcode, is_l[1].pdu_read, is_l[1].pdu_sent,
		    is_l[2].opcode, is_l[2].pdu_read, is_l[2].pdu_sent,
		    is_l[3].opcode, is_l[3].pdu_read, is_l[3].pdu_sent,
		    is_l[4].opcode, is_l[4].pdu_read, is_l[4].pdu_sent,
		    is_l[5].opcode, is_l[5].pdu_read, is_l[5].pdu_sent,
		    is_l[6].opcode, is_l[6].pdu_read, is_l[6].pdu_sent,
		    is_l[7].opcode, is_l[7].pdu_read, is_l[7].pdu_sent,
		    is_l[8].opcode, is_l[8].pdu_read, is_l[8].pdu_sent,
		    is_l[9].opcode, is_l[9].pdu_read, is_l[9].pdu_sent);
		if (rc != UCTL_CMD_OK)
			return (rc);
	}
	if (s_li) {
		istgt_uctl_snprintf(uctl, "%s SCSI:%d [%x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u]\n",
		    uctl->cmd, s_li,
		    s_l[0].opcode, s_l[0].req_start, s_l[0].req_finish,
		    s_l[1].opcode, s_l[1].req_start, s_l[1].req_finish,
		    s_l[2].opcode, s_l[2].req_start, s_l[2].req_finish,
		    s_l[3].opcode, s_l[3].req_start, s_l[3].req_finish,
		    s_l[4].opcode, s_l[4].req_start, s_l[4].req_finish,
		    s_l[5].opcode, s_l[5].req_start, s_l[5].req_finish,
		    s_l[6].opcode, s_l[6].req_start, s_l[6].req_finish,
		    s_l[7].opcode, s_l[7].req_start, s_l[7].req_finish,
		    s_l[8].opcode, s_l[8].req_start, s_l[8].req_finish,
		    s_l[9].opcode, s_l[9].req_start, s_l[9].req_finish);
		rc = istgt_uctl_writeline(uctl);

		ISTGT_TRACELOG(ISTGT_TRACE_CMD, "%s SCSI:%d [%x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u,"
		    " %x:%u %u, %x:%u %u, %x:%u %u]\n",
		    uctl->cmd, s_li,
		    s_l[0].opcode, s_l[0].req_start, s_l[0].req_finish,
		    s_l[1].opcode, s_l[1].req_start, s_l[1].req_finish,
		    s_l[2].opcode, s_l[2].req_start, s_l[2].req_finish,
		    s_l[3].opcode, s_l[3].req_start, s_l[3].req_finish,
		    s_l[4].opcode, s_l[4].req_start, s_l[4].req_finish,
		    s_l[5].opcode, s_l[5].req_start, s_l[5].req_finish,
		    s_l[6].opcode, s_l[6].req_start, s_l[6].req_finish,
		    s_l[7].opcode, s_l[7].req_start, s_l[7].req_finish,
		    s_l[8].opcode, s_l[8].req_start, s_l[8].req_finish,
		    s_l[9].opcode, s_l[9].req_start, s_l[9].req_finish);
		if (rc != UCTL_CMD_OK)
			return (rc);
	}
	if (!is_li && !s_li) {
		istgt_uctl_snprintf(uctl, "%s PDU:0 SCSI:0 \n", uctl->cmd);
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK)
			return (rc);
	}

	istgt_uctl_snprintf(uctl, "OK %s\n", uctl->cmd);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		return (rc);
	}
	return (UCTL_CMD_OK);
}

typedef struct istgt_uctl_cmd_table_t
{
	const char *name;
	int (*func) (UCTL_Ptr uctl);
} ISTGT_UCTL_CMD_TABLE;

static ISTGT_UCTL_CMD_TABLE istgt_uctl_cmd_table[] =
{
	{ "AUTH",    istgt_uctl_cmd_auth },
	{ "QUIT",    istgt_uctl_cmd_quit },
	{ "NOOP",    istgt_uctl_cmd_noop },
	{ "VERSION", istgt_uctl_cmd_version },
	{ "LIST",    istgt_uctl_cmd_list },
	{ "UNLOAD",  istgt_uctl_cmd_unload },
	{ "LOAD",    istgt_uctl_cmd_load },
	{ "CHANGE",  istgt_uctl_cmd_change },
	{ "SYNC", istgt_uctl_cmd_sync },
	{ "PERSIST", istgt_uctl_cmd_persist },
	{ "RESET",   istgt_uctl_cmd_reset },
	{ "CLEAR",   istgt_uctl_cmd_clear },
	{ "REFRESH", istgt_uctl_cmd_refresh },
	{ "START",  istgt_uctl_cmd_start },
	{ "STOP",    istgt_uctl_cmd_stop },
	{ "MODIFY", istgt_uctl_cmd_modify },
	{ "STATUS", istgt_uctl_cmd_status },
	{ "INFO", istgt_uctl_cmd_info },
	{ "DUMP", istgt_uctl_cmd_dump },
	{ "MEM", istgt_uctl_cmd_mem},
	{ "MEMDEBUG", istgt_uctl_cmd_memdebug},
	{ "LOG", istgt_uctl_cmd_log},
	{ "RSV", istgt_uctl_cmd_rsv},
	{ "QUE", istgt_uctl_cmd_que},
	{ "STATS", istgt_uctl_cmd_stats},
#ifdef REPLICATION
	{ "IOSTATS", istgt_uctl_cmd_iostats},
	{ "MEMPOOL", istgt_uctl_cmd_mempoolstats},
#endif
	{ "SET", istgt_uctl_cmd_set},
	{ "MAXTIME", istgt_uctl_cmd_maxtime},
#ifdef	REPLICATION
	{ "SNAPCREATE", istgt_uctl_cmd_snap},
	{ "SNAPDESTROY", istgt_uctl_cmd_snap},
	{ "REPLICA", istgt_uctl_cmd_replica_stats},
#endif
	{ NULL, NULL },
};

static int
istgt_uctl_cmd_execute(UCTL_Ptr uctl)
{
	int (*func) (UCTL_Ptr);
	const char *delim = ARGS_DELIM;
	char *arg;
	char *cmd;
	int rc;
	int i;

	arg = trim_string(uctl->recvbuf);
	cmd = strsepq(&arg, delim);
	uctl->arg = arg;
	uctl->cmd = strupr(cmd);

	func = NULL;
	for (i = 0; istgt_uctl_cmd_table[i].name != NULL; i++) {
		if (cmd[0] == istgt_uctl_cmd_table[i].name[0] &&
		    strcmp(cmd, istgt_uctl_cmd_table[i].name) == 0) {
			func = istgt_uctl_cmd_table[i].func;
			break;
		}
	}
	if (func == NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET,
		    "uctl_cmd:%d ERR unknown command\n", i);
		istgt_uctl_snprintf(uctl, "ERR unknown command\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (UCTL_CMD_DISCON);
		}
		return (UCTL_CMD_ERR);
	}

#ifdef	REPLICATION
	if (!replication_initialized) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "uctl_cmd:%d ERR replication"
		    " not initialized\n", i);
		istgt_uctl_snprintf(uctl,
		    "ERR replication module not initialized\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (UCTL_CMD_DISCON);
		}
		return (UCTL_CMD_QUIT);
	}
#endif

	if (uctl->no_auth && (strcasecmp(cmd, "AUTH") == 0)) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET,
		    "uctl_cmd:%d ERR auth not requried\n", i);
		istgt_uctl_snprintf(uctl, "ERR auth not required\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (UCTL_CMD_DISCON);
		}
		return (UCTL_CMD_ERR);
	}
	if (uctl->req_auth && uctl->authenticated == 0 &&
	    !(strcasecmp(cmd, "QUIT") == 0 ||
	    strcasecmp(cmd, "AUTH") == 0)) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET,
		    "uctl_cmd:%d ERR auth requried\n", i);
		istgt_uctl_snprintf(uctl, "ERR auth required\n");
		rc = istgt_uctl_writeline(uctl);
		if (rc != UCTL_CMD_OK) {
			return (UCTL_CMD_DISCON);
		}
		return (UCTL_CMD_ERR);
	}

	ISTGT_TRACELOG(ISTGT_TRACE_NET, "uctl_cmd: %d:%s executing\n",
	    i, istgt_uctl_cmd_table[i].name);
	rc = func(uctl);
	return (rc);
}

static void istgt_free_uctl(UCTL_Ptr uctl);

static void *
uctlworker(void *arg)
{
	UCTL_Ptr uctl = (UCTL_Ptr) arg;
	int rc;
	pthread_t self = pthread_self();
	snprintf(tinfo, sizeof (tinfo), "u#%d.%ld",
	    uctl->sock, (uint64_t)(((uint64_t *)self)[0]));
#ifdef HAVE_PTHREAD_SET_NAME_NP
	pthread_set_name_np(pthread_self(), tinfo);
#endif

	ISTGT_TRACELOG(ISTGT_TRACE_NET, "connect to %s:%s,%d  (%s->%s)\n",
	    uctl->portal.host, uctl->portal.port, uctl->portal.tag,
	    uctl->caddr, uctl->saddr);

	istgt_uctl_snprintf(uctl, "iSCSI Target Controller version %s"
	    " on %s from %s\n",
	    istgtvers,
	    uctl->saddr, uctl->caddr);
	rc = istgt_uctl_writeline(uctl);
	if (rc != UCTL_CMD_OK) {
		ISTGT_ERRLOG("uctl_writeline() failed\n");
		return (NULL);
	}

	while (1) {
		if (istgt_get_state(uctl->istgt) != ISTGT_STATE_RUNNING) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "uctl_.. not running..\n");
			break;
		}

		/* read from socket */
		rc = istgt_uctl_readline(uctl);
		if (rc == UCTL_CMD_EOF) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "uctl_readline() EOF\n");
			break;
		}
		if (rc != UCTL_CMD_OK) {
			ISTGT_ERRLOG("uctl_readline() failed\n");
			break;
		}
		/* execute command */
		rc = istgt_uctl_cmd_execute(uctl);
		if (rc == UCTL_CMD_QUIT) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "receive QUIT\n");
			break;
		}
		if (rc == UCTL_CMD_DISCON) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "request disconnect\n");
			break;
		}
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "exiting ctlworker\n");

	close(uctl->sock);
	uctl->sock = -1;
	istgt_free_uctl(uctl);
	return (NULL);
}

static void
istgt_free_uctl(UCTL_Ptr uctl)
{
	if (uctl == NULL)
		return;
	xfree(uctl->mediadirectory);
	xfree(uctl->portal.label);
	xfree(uctl->portal.host);
	xfree(uctl->portal.port);
	xfree(uctl->auth.user);
	xfree(uctl->auth.secret);
	xfree(uctl->auth.muser);
	xfree(uctl->auth.msecret);
	xfree(uctl);
}

int
istgt_create_uctl(ISTGT_Ptr istgt, PORTAL_Ptr portal, int sock,
    struct sockaddr *sa, socklen_t salen __attribute__((__unused__)))
{
	char buf[MAX_TMPBUF];
	UCTL_Ptr uctl;
	int rc;
	int i;

	uctl = xmalloc(sizeof (*uctl));
	memset(uctl, 0, sizeof (*uctl));

	uctl->istgt = istgt;
	MTX_LOCK(&istgt->mutex);
	uctl->auth_group = istgt->uctl_auth_group;
	uctl->no_auth = istgt->no_uctl_auth;
	uctl->req_auth = istgt->req_uctl_auth;
	uctl->req_mutual = istgt->req_uctl_auth_mutual;
	uctl->mediadirectory = xstrdup(istgt->mediadirectory);
	MTX_UNLOCK(&istgt->mutex);

	uctl->portal.label = xstrdup(portal->label);
	uctl->portal.host = xstrdup(portal->host);
	uctl->portal.port = xstrdup(portal->port);
	uctl->portal.tag = portal->tag;
	uctl->portal.sock = -1;
	uctl->sock = sock;

	uctl->timeout = TIMEOUT_RW;
	uctl->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_A;
	uctl->auth.user = NULL;
	uctl->auth.secret = NULL;
	uctl->auth.muser = NULL;
	uctl->auth.msecret = NULL;
	uctl->authenticated = 0;

	uctl->recvtmpcnt = 0;
	uctl->recvtmpidx = 0;
	uctl->recvtmpsize = sizeof (uctl->recvtmp);
	uctl->recvbufsize = sizeof (uctl->recvbuf);
	uctl->sendbufsize = sizeof (uctl->sendbuf);
	uctl->worksize = sizeof (uctl->work);

	memset(uctl->caddr, 0, sizeof (uctl->caddr));
	memset(uctl->saddr, 0, sizeof (uctl->saddr));

	switch (sa->sa_family) {
	case AF_INET6:
		uctl->family = AF_INET6;
		rc = istgt_getaddr(sock, uctl->saddr, sizeof (uctl->saddr),
		    uctl->caddr, sizeof (uctl->caddr), &uctl->iaddr,
		    (uint16_t *)&uctl->iport);
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_getaddr() failed\n");
			goto error_return;
		}
		break;
	case AF_INET:
		uctl->family = AF_INET;
		rc = istgt_getaddr(sock, uctl->saddr, sizeof (uctl->saddr),
		    uctl->caddr, sizeof (uctl->caddr), &uctl->iaddr,
		    (uint16_t *)&uctl->iport);
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_getaddr() failed\n");
			goto error_return;
		}
		break;
	case AF_UNIX:
		uctl->family = AF_UNIX;
		break;
	default:
		ISTGT_ERRLOG("unsupported family\n");
		goto error_return;
	}

	if (istgt->nuctl_netmasks != 0 && (uctl->family != AF_UNIX)) {
		rc = -1;
		for (i = 0; i < istgt->nuctl_netmasks; i++) {
			rc = istgt_lu_allow_netmask(istgt->uctl_netmasks[i],
			    uctl->caddr);
			if (rc > 0) {
				/* OK netmask */
				break;
			}
		}
		if (rc <= 0) {
			ISTGT_WARNLOG("UCTL access denied from %s to (%s:%s)\n",
			    uctl->caddr, uctl->portal.host, uctl->portal.port);
			goto error_return;
		}
	}

	/* wildcard? */
	if (uctl->family != AF_UNIX) {
	if (strcasecmp(uctl->portal.host, "[::]") == 0 ||
	    strcasecmp(uctl->portal.host, "[*]") == 0) {
		if (uctl->family != AF_INET6) {
			ISTGT_ERRLOG("address family error\n");
			goto error_return;
		}
		snprintf(buf, sizeof (buf), "[%s]", uctl->caddr);
		xfree(uctl->portal.host);
		uctl->portal.host = xstrdup(buf);
	} else if (strcasecmp(uctl->portal.host, "0.0.0.0") == 0 ||
	    strcasecmp(uctl->portal.host, "*") == 0) {
		if (uctl->family != AF_INET) {
			ISTGT_ERRLOG("address family error\n");
			goto error_return;
		}
		snprintf(buf, sizeof (buf), "%s", uctl->caddr);
		xfree(uctl->portal.host);
		uctl->portal.host = xstrdup(buf);
	}
	}

	/* set timeout msec. */
	rc = istgt_set_recvtimeout(uctl->sock, uctl->timeout * 1000);
	if (rc != 0) {
		ISTGT_ERRLOG("istgt_set_recvtimeo() failed\n");
		goto error_return;
	}
	rc = istgt_set_sendtimeout(uctl->sock, uctl->timeout * 1000);
	if (rc != 0) {
		ISTGT_ERRLOG("istgt_set_sendtimeo() failed\n");
		goto error_return;
	}

	/* create new thread */
#ifdef ISTGT_STACKSIZE
	rc = pthread_create(&uctl->thread, &istgt->attr, &uctlworker,
	    (void *)uctl);
#else
	rc = pthread_create(&uctl->thread, NULL, &uctlworker, (void *)uctl);
#endif
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed\n");
	error_return:
		xfree(uctl->portal.label);
		xfree(uctl->portal.host);
		xfree(uctl->portal.port);
		xfree(uctl);
		return (-1);
	}
	rc = pthread_detach(uctl->thread);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_detach() failed\n");
		goto error_return;
	}
	return (0);
}

int
istgt_uctl_init(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	const char *val;
	const char *ag_tag;
	int alloc_len;
	int ag_tag_i;
	int masks;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_init_uctl_section\n");

	sp = istgt_find_cf_section(istgt->config, "UnitControl");
	if (sp == NULL) {
		ISTGT_ERRLOG("find_cf_section failed()\n");
		return (-1);
	}

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	for (i = 0; ; i++) {
		val = istgt_get_nval(sp, "Netmask", i);
		if (val == NULL)
			break;
	}
	masks = i;
	if (masks > MAX_NETMASK) {
		ISTGT_ERRLOG("%d > MAX_NETMASK\n", masks);
		return (-1);
	}
	istgt->nuctl_netmasks = masks;
	alloc_len = sizeof (char *) * masks;
	istgt->uctl_netmasks = xmalloc(alloc_len);
	for (i = 0; i < masks; i++) {
		val = istgt_get_nval(sp, "Netmask", i);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Netmask %s\n", val);
		istgt->uctl_netmasks[i] = xstrdup(val);
	}

	val = istgt_get_val(sp, "AuthMethod");
	if (val == NULL) {
		istgt->no_uctl_auth = 0;
		istgt->req_uctl_auth = 0;
	} else {
		istgt->no_uctl_auth = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "AuthMethod", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "CHAP") == 0) {
				istgt->req_uctl_auth = 1;
			} else if (strcasecmp(val, "Mutual") == 0) {
				istgt->req_uctl_auth_mutual = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				istgt->req_uctl_auth = 0;
				istgt->req_uctl_auth_mutual = 0;
			} else if (strcasecmp(val, "None") == 0) {
				istgt->no_uctl_auth = 1;
				istgt->req_uctl_auth = 0;
				istgt->req_uctl_auth_mutual = 0;
			} else {
				ISTGT_ERRLOG("unknown auth\n");
				return (-1);
			}
		}
	}
	if (istgt->no_uctl_auth == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod None\n");
	} else if (istgt->req_uctl_auth == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod Auto\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod %s %s\n",
		    istgt->req_uctl_auth ? "CHAP" : "",
		    istgt->req_uctl_auth_mutual ? "Mutual" : "");
	}

	val = istgt_get_val(sp, "AuthGroup");
	if (val == NULL) {
		istgt->uctl_auth_group = 0;
	} else {
		ag_tag = val;
		if (strcasecmp(ag_tag, "None") == 0) {
			ag_tag_i = 0;
		} else {
			if (strncasecmp(ag_tag, "AuthGroup",
				    strlen("AuthGroup")) != 0 ||
			    sscanf(ag_tag, "%*[^0-9]%d", &ag_tag_i) != 1) {
				ISTGT_ERRLOG("auth group error\n");
				return (-1);
			}
			if (ag_tag_i == 0) {
				ISTGT_ERRLOG("invalid auth group %d\n",
				    ag_tag_i);
				return (-1);
			}
		}
		istgt->uctl_auth_group = ag_tag_i;
	}
	if (istgt->uctl_auth_group == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthGroup None\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthGroup AuthGroup%d\n",
		    istgt->uctl_auth_group);
	}

	return (0);
}

int
istgt_uctl_shutdown(ISTGT_Ptr istgt)
{
	int i;

	for (i = 0; i < istgt->nuctl_netmasks; i++) {
		xfree(istgt->uctl_netmasks[i]);
	}
	xfree(istgt->uctl_netmasks);
	return (0);
}
