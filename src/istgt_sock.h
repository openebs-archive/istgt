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

#ifndef ISTGT_SOCK_H
#define        ISTGT_SOCK_H

#include <stddef.h>
#include <unistd.h>

int istgt_getaddr(int sock, char *saddr, int slen, char *caddr, int clen,
    uint32_t *iaddr, uint16_t *iport);
int istgt_listen(const char *ip, int port, int que);
int istgt_listen_unx(const char *lpath, int que);
int istgt_connect(const char *host, int port);
int istgt_connect_unx(const char *path);
int istgt_set_recvtimeout(int s, int msec);
int istgt_set_sendtimeout(int s, int msec);
int istgt_set_recvlowat(int s, int nbytes);
ssize_t istgt_read_socket(int s, void *buf, size_t nbytes, int timeout);
ssize_t istgt_write_socket(int s, const void *buf, size_t nbytes, int timeout);
ssize_t istgt_readline_socket(int sock, char *buf, size_t size, char *tmp,
    size_t tmpsize, int *tmpidx, int *tmpcnt, int timeout);
ssize_t istgt_writeline_socket(int sock, const char *buf, int timeout);

#endif /* ISTGT_SOCK_H */
