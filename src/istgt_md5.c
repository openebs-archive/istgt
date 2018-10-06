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

#include <inttypes.h>
#include <stdint.h>

#include <stddef.h>
#include <openssl/md5.h>

#include "istgt.h"
#include "istgt_md5.h"

int
istgt_md5init(ISTGT_MD5CTX *md5ctx)
{
	int rc;

	if (md5ctx == NULL)
		return (-1);
	rc = MD5_Init(&md5ctx->md5ctx);
	return (rc);
}

int
istgt_md5final(void *md5, ISTGT_MD5CTX *md5ctx)
{
	int rc;

	if (md5ctx == NULL || md5 == NULL)
		return (-1);
	rc = MD5_Final(md5, &md5ctx->md5ctx);
	return (rc);
}

int
istgt_md5update(ISTGT_MD5CTX *md5ctx, const void *data, size_t len)
{
	int rc;

	if (md5ctx == NULL)
		return (-1);
	if (data == NULL || len <= 0)
		return (0);
	rc = MD5_Update(&md5ctx->md5ctx, data, len);
	return (rc);
}
