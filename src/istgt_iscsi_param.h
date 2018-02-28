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

#ifndef ISTGT_ISCSI_PARAM_H
#define ISTGT_ISCSI_PARAM_H

#include <stdint.h>

typedef enum {
	ISPT_INVALID = -1,
	ISPT_NOTSPECIFIED = 0,
	ISPT_LIST,
	ISPT_NUMERICAL,
	ISPT_NUMERICAL_MAX,
	ISPT_DECLARATIVE,
	ISPT_BOOLEAN_OR,
	ISPT_BOOLEAN_AND,
} ISCSI_PARAM_TYPE;

typedef struct iscsi_param_t {
	struct iscsi_param_t *next;
	char *key;
	char *val;
	char *list;
	int type;
} ISCSI_PARAM;

void istgt_iscsi_param_free(ISCSI_PARAM *params);
ISCSI_PARAM *istgt_iscsi_param_find(ISCSI_PARAM *params, const char *key);
int istgt_iscsi_param_del(ISCSI_PARAM **params, const char *key);
int istgt_iscsi_param_add(ISCSI_PARAM **params, const char *key, const char *val, const char *list, int type);
int istgt_iscsi_param_set(ISCSI_PARAM *params, const char *key, const char *val);
int istgt_iscsi_param_set_int(ISCSI_PARAM *params, const char *key, int val);
int istgt_iscsi_parse_params(ISCSI_PARAM **params, const uint8_t *data, int len);

#endif /* ISTGT_ISCSI_PARAM_H */
