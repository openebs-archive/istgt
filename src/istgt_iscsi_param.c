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

#include <stdint.h>
#include <inttypes.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "istgt.h"
#include "istgt_log.h"
#include "istgt_misc.h"
#include "istgt_iscsi.h"
#include "istgt_iscsi_param.h"

void
istgt_iscsi_param_free(ISCSI_PARAM *params)
{
	ISCSI_PARAM *param, *next_param;

	if (params == NULL)
		return;
	for (param = params; param != NULL; param = next_param) {
		next_param = param->next;
		xfree(param->list);
		xfree(param->val);
		xfree(param->key);
		xfree(param);
	}
}

ISCSI_PARAM *
istgt_iscsi_param_find(ISCSI_PARAM *params, const char *key)
{
	ISCSI_PARAM *param;

	if (params == NULL || key == NULL)
		return NULL;
	for (param = params; param != NULL; param = param->next) {
		if (param->key != NULL && param->key[0] == key[0]
			&& strcasecmp(param->key, key) == 0) {
			return param;
		}
	}
	return NULL;
}

int
istgt_iscsi_param_del(ISCSI_PARAM **params, const char *key)
{
	ISCSI_PARAM *param, *prev_param = NULL;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "del %s\n", key);
	if (params == NULL || key == NULL)
		return 0;
	for (param = *params; param != NULL; param = param->next) {
		if (param->key != NULL && param->key[0] == key[0]
			&& strcasecmp(param->key, key) == 0) {
			if (prev_param != NULL) {
				prev_param->next = param->next;
			} else {
				*params = param->next;
			}
			param->next = NULL;
			istgt_iscsi_param_free(param);
			return 0;
		}
		prev_param = param;
	}
	return -1;
}

int
istgt_iscsi_param_add(ISCSI_PARAM **params, const char *key, const char *val, const char *list, int type)
{
	ISCSI_PARAM *param, *last_param;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "add %s=%s, list=[%s], type=%d\n",
				   key, val, list, type);
	if (key == NULL)
		return -1;
	param = istgt_iscsi_param_find(*params, key);
	if (param != NULL) {
		istgt_iscsi_param_del(params, key);
	}
	param = xmalloc(sizeof *param);
	memset(param, 0, sizeof *param);
	param->next = NULL;
	param->key = xstrdup(key);
	param->val = xstrdup(val);
	param->list = xstrdup(list);
	param->type = type;

	last_param = *params;
	if (last_param != NULL) {
		while (last_param->next != NULL) {
			last_param = last_param->next;
		}
		last_param->next = param;
	} else {
		*params = param;
	}

	return 0;
}

int
istgt_iscsi_param_set(ISCSI_PARAM *params, const char *key, const char *val)
{
	ISCSI_PARAM *param;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set %s=%s\n", key, val);
	param = istgt_iscsi_param_find(params, key);
	if (param == NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "no key %s\n", key);
		return -1;
	}

	xfree(param->val);
	param->val = xstrdup(val);

	return 0;
}

int
istgt_iscsi_param_set_int(ISCSI_PARAM *params, const char *key, int val)
{
	char buf[MAX_TMPBUF];
	ISCSI_PARAM *param;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set %s=%d\n", key, val);
	param = istgt_iscsi_param_find(params, key);
	if (param == NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "no key %s\n", key);
		return -1;
	}

	xfree(param->val);
	snprintf(buf, sizeof buf, "%d", val);
	param->val = xstrdup(buf);

	return 0;
}

int
istgt_iscsi_parse_params(ISCSI_PARAM **params, const uint8_t *data, int len)
{
	const uint8_t *p, *q;
	const uint8_t *last;
	char *key = NULL;
	char *val = NULL;
	int rc;
	int n;

	/* for each key/val temporary store */
	key = xmalloc(ISCSI_TEXT_MAX_KEY_LEN + 1);
	val = xmalloc(ISCSI_TEXT_MAX_VAL_LEN + 1);

	/* data = "KEY=VAL<NUL>KEY=VAL<NUL>..." */
	p = data;
	last = data + len;
	while (p < last && *p != '\0') {
		q = p;
		/* q = "KEY=VAL<NUL>" */
		while (q < last && *q != '\0') {
			if (*q == '=') {
				break;
			}
			q++;
		}
		if (q >= last || *q == '\0') {
			ISTGT_ERRLOG("'=' not found\n");
		error_return:
			xfree(key);
			xfree(val);
			return -1;
		}
		n = q - p;
		if (n > ISCSI_TEXT_MAX_KEY_LEN) {
			ISTGT_ERRLOG("Overflow Key %d\n", n);
			goto error_return;
		}
		memcpy(key, p, n);
		key[n] = '\0';

		p = q + 1;
		q = p;
		/* q = "VAL<NUL>" */
		while (q < last && *q != '\0') {
			q++;
		}
		n = q - p;
		if (n > ISCSI_TEXT_MAX_VAL_LEN) {
			ISTGT_ERRLOG("Overflow Val %d\n", n);
			goto error_return;
		}
		memcpy(val, p, n);
		val[n] = '\0';

		rc = istgt_iscsi_param_add(params, key, val, NULL, 0);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_param_add() failed\n");
			goto error_return;
		}

		p = q + 1;
		while (p < last && *p == '\0') {
			p++;
		}
	}

	xfree(key);
	xfree(val);
	return 0;
}
