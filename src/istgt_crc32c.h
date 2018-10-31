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

#ifndef ISTGT_CRC32C_H
#define	ISTGT_CRC32C_H

#include <stdint.h>
#include <stddef.h>

#ifdef __FreeBSD__
#include <sys/uio.h>
#endif

#define	ISTGT_USE_CRC32C_TABLE
#define	ISTGT_CRC32C_INITIAL    0xffffffffUL
#define	ISTGT_CRC32C_XOR	0xffffffffUL
#define	ISTGT_CRC32C_POLYNOMIAL 0x1edc6f41UL

void istgt_init_crc32c_table(void);
uint32_t istgt_update_crc32c(const uint8_t *buf, size_t len, uint32_t crc);
uint32_t istgt_fixup_crc32c(size_t total, uint32_t crc);
uint32_t istgt_crc32c(const uint8_t *buf, size_t len);
uint32_t istgt_iovec_crc32c(const struct iovec *iovp, int iovc,\
	uint32_t offset, uint32_t len);

#endif /* ISTGT_CRC32C_H */
