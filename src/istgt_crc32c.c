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

#include <ctype.h>
#include <stdio.h>
#include <string.h>

#ifdef __FreeBSD__
#include <sys/uio.h>
#endif

#include "istgt_iscsi.h"
#include "istgt_crc32c.h"

/* defined in RFC3720(12.1) */
static uint32_t istgt_crc32c_initial    = ISTGT_CRC32C_INITIAL;
static uint32_t istgt_crc32c_xor	  = ISTGT_CRC32C_XOR;
static uint32_t istgt_crc32c_polynomial = ISTGT_CRC32C_POLYNOMIAL;
#ifdef ISTGT_USE_CRC32C_TABLE
static uint32_t istgt_crc32c_table[256];
static int istgt_crc32c_initialized = 0;
#endif /* ISTGT_USE_CRC32C_TABLE */

static uint32_t
istgt_reflect(uint32_t val, int bits)
{
	int i;
	uint32_t r;

	if (bits < 1 || bits > 32)
		return (0);
	r = 0;
	for (i = 0; i < bits; i++) {
		r |= ((val >> ((bits - 1) - i)) & 1) << i;
	}
	return (r);
}

#ifdef ISTGT_USE_CRC32C_TABLE
void
istgt_init_crc32c_table(void)
{
	int i, j;
	uint32_t val;
	uint32_t reflect_polynomial;

	reflect_polynomial = istgt_reflect(istgt_crc32c_polynomial, 32);
	for (i = 0; i < 256; i++) {
		val = i;
		for (j = 0; j < 8; j++) {
			if (val & 1) {
				val = (val >> 1) ^ reflect_polynomial;
			} else {
				val = (val >> 1);
			}
		}
		istgt_crc32c_table[i] = val;
	}
	istgt_crc32c_initialized = 1;
}
#endif /* ISTGT_USE_CRC32C_TABLE */

uint32_t
istgt_update_crc32c(const uint8_t *buf, size_t len, uint32_t crc)
{
	size_t s;
#ifndef ISTGT_USE_CRC32C_TABLE
	int i;
	uint32_t val;
	uint32_t reflect_polynomial;
#endif /* ISTGT_USE_CRC32C_TABLE */

#ifdef ISTGT_USE_CRC32C_TABLE
#if 0
	/* initialize by main() */
	if (!istgt_crc32c_initialized) {
		istgt_init_crc32c_table();
	}
#endif
#else
	reflect_polynomial = istgt_reflect(istgt_crc32c_polynomial, 32);
#endif /* ISTGT_USE_CRC32C_TABLE */

	for (s = 0; s < len; s++) {
#ifdef ISTGT_USE_CRC32C_TABLE
		crc = (crc >> 8) ^ istgt_crc32c_table[(crc ^ buf[s]) & 0xff];
#else
		val = buf[s];
		for (i = 0; i < 8; i++) {
			if ((crc ^ val) & 1) {
				crc = (crc >> 1) ^ reflect_polynomial;
			} else {
				crc = (crc >> 1);
			}
			val = val >> 1;
		}
#endif /* ISTGT_USE_CRC32C_TABLE */
	}
	return (crc);
}

uint32_t
istgt_fixup_crc32c(size_t total, uint32_t crc)
{
	uint8_t padding[ISCSI_ALIGNMENT];
	size_t pad_length;
	size_t rest;

	if (total == 0)
		return (crc);
#if 0
	/* alignment must be power of 2 */
	rest = total & ~(ISCSI_ALIGNMENT - 1);
#endif
	rest = total % ISCSI_ALIGNMENT;
	if (rest != 0) {
		pad_length = ISCSI_ALIGNMENT;
		pad_length -= rest;
		if (pad_length > 0 && pad_length < sizeof (padding)) {
			memset(padding, 0, sizeof (padding));
			crc = istgt_update_crc32c(padding, pad_length, crc);
		}
	}
	return (crc);
}

uint32_t
istgt_crc32c(const uint8_t *buf, size_t len)
{
	uint32_t crc32c;

	crc32c = istgt_crc32c_initial;
	crc32c = istgt_update_crc32c(buf, len, crc32c);
	if ((len % ISCSI_ALIGNMENT) != 0) {
		crc32c = istgt_fixup_crc32c(len, crc32c);
	}
	crc32c = crc32c ^ istgt_crc32c_xor;
	return (crc32c);
}

uint32_t
istgt_iovec_crc32c(const struct iovec *iovp, int iovc,
	uint32_t offset, uint32_t len)
{
	const uint8_t *p;
	uint32_t total;
	uint32_t pos;
	uint32_t n;
	uint32_t crc32c;
	int i;

	pos = 0;
	total = 0;
	crc32c = istgt_crc32c_initial;
	for (i = 0; i < iovc; i++) {
		if (len == 0)
			break;
		if (pos + iovp[i].iov_len > offset) {
			p = (const uint8_t *) iovp[i].iov_base + (offset - pos);
			if (iovp[i].iov_len > len) {
				n = len;
				len = 0;
			} else {
				n = iovp[i].iov_len;
				len -= n;
			}
			crc32c = istgt_update_crc32c(p, n, crc32c);
			offset += n;
			total += n;
		}
		pos += iovp[i].iov_len;
	}
#if 0
	printf("update %d bytes\n", total);
#endif
	crc32c = istgt_fixup_crc32c(total, crc32c);
	crc32c = crc32c ^ istgt_crc32c_xor;
	return (crc32c);
}
