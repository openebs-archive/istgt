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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "istgt.h"
#include "istgt_log.h"
#include "istgt_sock.h"
#include "istgt_misc.h"

// #define USE_POLLWAIT

#undef USE_POLLWAIT
#define	TIMEOUT_RW 60
#define	POLLWAIT 1000
#define	PORTNUMLEN 32

#ifndef AI_NUMERICSERV
#define	AI_NUMERICSERV 0
#endif

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

int
istgt_getaddr(int sock, char *saddr, int slen, char *caddr, int clen,
		uint32_t *iaddr, uint16_t *iport)
{
	struct sockaddr_storage sa;
	socklen_t salen;
	int rc;
	*iaddr = 0;
	*iport = 0;
	memset(&sa, 0, sizeof (sa));
	salen = sizeof (sa);
	rc = getsockname(sock, (struct sockaddr *) &sa, &salen);
	if (rc != 0) {
		ISTGT_ERRLOG("getsockname() failed (errno=%d)\n", errno);
		return (-1);
	}
	rc = getnameinfo((struct sockaddr *) &sa, salen,
	    saddr, slen, NULL, 0, NI_NUMERICHOST);
	if (rc != 0) {
		ISTGT_ERRLOG("getnameinfo() failed (errno=%d)\n", errno);
		return (-1);
	}

	memset(&sa, 0, sizeof (sa));
	salen = sizeof (sa);
	rc = getpeername(sock, (struct sockaddr *) &sa, &salen);
	if (rc != 0) {
		ISTGT_ERRLOG("getpeername() failed (errno=%d)\n", errno);
		return (-1);
	}
	if (salen >= sizeof (struct sockaddr_in)) {
		*iaddr = ((struct sockaddr_in *)&sa)->sin_addr.s_addr;
		*iport = ((struct sockaddr_in *)&sa)->sin_port;
	}
	rc = getnameinfo((struct sockaddr *) &sa, salen,
	    caddr, clen, NULL, 0, NI_NUMERICHOST);
	if (rc != 0) {
		ISTGT_ERRLOG("getnameinfo() failed (errno=%d)\n", errno);
		return (-1);
	}

	return (0);
}
int
istgt_listen_unx(const char *lpath, int que)
{
	/* open a UNIX socket */
	struct sockaddr_un sun;
	int local_s = socket(AF_UNIX, SOCK_STREAM, 0);
	if (que < 2)
		que = 2;
	else if (que > 127)
		que = 127;

	if (local_s == -1) {
		ISTGT_ERRLOG(
			"uctl_listen_unx AF_UNIX socket failed %s (errno=%d)\n",
			lpath, errno);
		return (-1);
	}
	memset(&sun, 0, sizeof (struct sockaddr_un));
	sun.sun_family = AF_UNIX;
	strlcpy(sun.sun_path, lpath, sizeof (sun.sun_path));
	unlink(sun.sun_path);
	if (bind(local_s, (struct sockaddr *)&sun, SUN_LEN(&sun)) != 0) {
		ISTGT_ERRLOG(
			"uctl_listen_unx failed to bind %s (errno=%d)\n",
			lpath, errno);
		close(local_s);
		return (-2);
	}
	if (listen(local_s, que) != 0) {
		ISTGT_ERRLOG(
			"uctl_listen_unx failed to listen at %s (errno=%d)\n",
			lpath, errno);
		close(local_s);
		return (-3);
	}
	return (local_s);
}
int
istgt_listen(const char *ip, int port, int que)
{
	char buf[MAX_TMPBUF];
	char portnum[PORTNUMLEN];
	char *p;
	struct addrinfo hints, *res, *res0;
	int sock;
	int val = 1;
	int rc;

	if (ip == NULL)
		return (-1);
	if (ip[0] == '[') {
		strlcpy(buf, ip + 1, sizeof (buf));
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		ip = (const char *) &buf[0];
		if (strcasecmp(ip, "*") == 0) {
			strlcpy(buf, "::", sizeof (buf));
			ip = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(ip, "*") == 0) {
			strlcpy(buf, "0.0.0.0", sizeof (buf));
			ip = (const char *) &buf[0];
		}
	}
	snprintf(portnum, sizeof (portnum), "%d", port);
	memset(&hints, 0, sizeof (hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	hints.ai_flags |= AI_PASSIVE;
	hints.ai_flags |= AI_NUMERICHOST;
	rc = getaddrinfo(ip, portnum, &hints, &res0);
	if (rc != 0) {
		ISTGT_ERRLOG("getaddrinfo() failed (errno=%d)\n", errno);
		return (-1);
	}
	if (que < 2)
		que = 2;
	else if (que > 255)
		que = 255;

	/* try listen */
	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
	retry:
		sock = socket(res->ai_family, res->ai_socktype,
			res->ai_protocol);
		if (sock < 0) {
			/* error */
			continue;
		}
		rc = setsockopt(sock, SOL_SOCKET,
			SO_REUSEADDR, &val, sizeof (val));
		if (rc != 0) {
			/* error */
			continue;
		}
		rc = setsockopt(sock, IPPROTO_TCP,
			TCP_NODELAY, &val, sizeof (val));
		if (rc != 0) {
			/* error */
			continue;
		}
		rc = bind(sock, res->ai_addr, res->ai_addrlen);
		if (rc == -1 && errno == EINTR) {
			/* interrupted? */
			close(sock);
			sock = -1;
			goto retry;
		}
		if (rc != 0) {
			/* try next family */
			close(sock);
			sock = -1;
			continue;
		}
		/* bind OK */
		rc = listen(sock, que);
		if (rc != 0) {
			close(sock);
			sock = -1;
			break;
		}
		break;
	}
	freeaddrinfo(res0);

	if (sock < 0) {
		return (-1);
	}
	return (sock);
}

int
istgt_connect_unx(const char *lpath)
{
	/* open a UNIX socket */
	struct sockaddr_un sun;
	int local_s = socket(AF_UNIX, SOCK_STREAM, 0);
	memset(&sun, 0, sizeof (struct sockaddr_un));
	sun.sun_family = AF_UNIX;
	strlcpy(sun.sun_path, lpath, sizeof (sun.sun_path));
	if (connect(local_s, (struct sockaddr *)&sun, SUN_LEN(&sun)) != 0) {
		ISTGT_ERRLOG(
			"uctl_connect_unx failed to %s (errno=%d)\n",
			lpath, errno);
		close(local_s);
		return (-1);
	}
	return (local_s);
}

int
istgt_connect(const char *host, int port)
{
	char buf[MAX_TMPBUF];
	char portnum[PORTNUMLEN];
	char *p;
	struct addrinfo hints, *res, *res0;
	int sock;
	int val = 1;
	int rc;

	if (host == NULL)
		return (-1);
	if (host[0] == '[') {
		strlcpy(buf, host + 1, sizeof (buf));
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		host = (const char *) &buf[0];
		if (strcasecmp(host, "*") == 0) {
			strlcpy(buf, "::", sizeof (buf));
			host = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(host, "*") == 0) {
			strlcpy(buf, "0.0.0.0", sizeof (buf));
			host = (const char *) &buf[0];
		}
	}
	snprintf(portnum, sizeof (portnum), "%d", port);
	memset(&hints, 0, sizeof (hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	rc = getaddrinfo(host, portnum, &hints, &res0);
	if (rc != 0) {
		ISTGT_ERRLOG("getaddrinfo() failed (errno=%d)\n", errno);
		return (-1);
	}

	/* try connect */
	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
	retry:
		sock = socket(res->ai_family,
			res->ai_socktype, res->ai_protocol);
		if (sock < 0) {
			/* error */
			continue;
		}
		rc = setsockopt(sock, IPPROTO_TCP,
			TCP_NODELAY, &val, sizeof (val));
		if (rc != 0) {
			/* error */
			continue;
		}
		rc = connect(sock, res->ai_addr, res->ai_addrlen);
		if (rc == -1 && errno == EINTR) {
			/* interrupted? */
			close(sock);
			sock = -1;
			goto retry;
		}
		if (rc != 0) {
			/* try next family */
			close(sock);
			sock = -1;
			continue;
		}
		/* connect OK */
		break;
	}
	freeaddrinfo(res0);

	if (sock < 0) {
		return (-1);
	}
	return (sock);
}

int
istgt_set_recvtimeout(int s, int msec)
{
	struct timeval tv;
	int rc;

	tv.tv_sec = msec / 1000;
	tv.tv_usec = (msec % 1000) * 1000;
	rc = setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof (tv));
	if (rc != 0)
		return (-1);
	return (0);
}

int
istgt_set_sendtimeout(int s, int msec)
{
	struct timeval tv;
	int rc;

	tv.tv_sec = msec / 1000;
	tv.tv_usec = (msec % 1000) * 1000;
	rc = setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof (tv));
	if (rc != 0)
		return (-1);
	return (0);
}

int
istgt_set_recvlowat(int s, int nbytes)
{
	int val;
	int rc;

	val = nbytes;
	rc = setsockopt(s, SOL_SOCKET, SO_RCVLOWAT, &val, sizeof (val));
	if (rc != 0)
		return (-1);
	return (0);
}

#ifdef USE_POLLWAIT
static int
can_read_socket(int s, int msec)
{
	struct pollfd fds[1];
	int rc;

	fds[0].fd = s;
	fds[0].events = POLLIN;
	retry:
	do {
		rc = poll(fds, 1, msec);
	} while (rc == -1 && errno == EINTR);
	if (rc == -1 && errno == EAGAIN) {
		goto retry;
	}
	if (rc < 0) {
		/* error */
		return (-1);
	}
	if (fds[0].revents & POLLIN) {
		/* read OK */
		return (1);
	}
	return (0);
}

static int
can_write_socket(int s, int msec)
{
	struct pollfd fds[1];
	int rc;

	fds[0].fd = s;
	fds[0].events = POLLOUT;
	retry:
	do {
		rc = poll(fds, 1, msec);
	} while (rc == -1 && errno == EINTR);
	if (rc == -1 && errno == EAGAIN) {
		goto retry;
	}
	if (rc < 0) {
		/* error */
		return (-1);
	}
	if (fds[0].revents & POLLOUT) {
		/* write OK */
		return (1);
	}
	return (0);
}
#endif /* USE_POLLWAIT */

#ifdef USE_POLLWAIT
#define	UNUSED_POLLWAIT(x) x
#else
#define	UNUSED_POLLWAIT(x) x __attribute__((__unused__))
#endif

ssize_t
istgt_read_socket(int s, void *buf, size_t nbytes, int UNUSED_POLLWAIT(timeout))
{
	ssize_t n;
#ifdef USE_POLLWAIT
	int msec = POLLWAIT;
	int rc;
#endif /* USE_POLLWAIT */

	if (nbytes == 0)
		return (0);

#ifdef USE_POLLWAIT
	msec = timeout * 1000;
	rc = can_read_socket(s, msec);
	if (rc < 0) {
		return (-1);
	}
	if (rc == 0) {
		/* TIMEOUT */
		return (-2);
	}
	retry:
	do {
		n = read(s, buf, nbytes);
	} while (n == -1 && errno == EINTR);
	if (n == -1 && errno == EAGAIN) {
		goto retry;
	}
	if (n < 0) {
		return (-1);
	}
#else
	do {
		n = recv(s, buf, nbytes, 0);
	} while (n == -1 && errno == EINTR);
	if (n == -1 && errno == EAGAIN) {
		/* TIMEOUT */
		return (-2);
	}
	if (n == -1) {
		return (-1);
	}
#endif /* USE_POLLWAIT */
	return (n);
}

ssize_t
istgt_write_socket(int s, const void *buf, size_t nbytes,
	int UNUSED_POLLWAIT(timeout))
{
	ssize_t n;
#ifdef USE_POLLWAIT
	int msec = POLLWAIT;
	int rc;
#endif /* USE_POLLWAIT */

	if (nbytes == 0)
		return (0);

#ifdef USE_POLLWAIT
	msec = timeout * 1000;
	rc = can_write_socket(s, msec);
	if (rc < 0) {
		return (-1);
	}
	if (rc == 0) {
		/* TIMEOUT */
		return (-2);
	}
	retry:
	do {
		n = write(s, buf, nbytes);
	} while (n == -1 && errno == EINTR);
	if (n == -1 && errno == EAGAIN) {
		goto retry;
	}
	if (n < 0) {
		ISTGT_ERRLOG("write() failed\n");
		return (-1);
	}
#else
	do {
		n = send(s, buf, nbytes, 0);
	} while (n == -1 && (errno == EINTR || errno == EAGAIN));
	if (n == -1) {
		return (-1);
	}
#endif /* USE_POLLWAIT */
	return (n);
}

ssize_t
istgt_readline_socket(int sock, char *buf, size_t size, char *tmp,
	size_t tmpsize, int *tmpidx, int *tmpcnt, int timeout)
{
	unsigned char *up, *utp;
	ssize_t maxsize;
	ssize_t total;
	ssize_t n;
	int got_cr;
	int idx, cnt;
	int ch;

	if (size < 2) {
		return (-1);
	}

	up = (unsigned char *) buf;
	utp = (unsigned char *) tmp;
	maxsize = size - 2; /* LF + NUL */
	total = 0;
	idx = *tmpidx;
	cnt = *tmpcnt;
	got_cr = 0;

	/* receive with LF */
	while (total < maxsize) {
		/* fill temporary buffer */
		if (idx == cnt) {
			*tmpidx = idx;
			up[total] = '\0';
			n = istgt_read_socket(sock, tmp, tmpsize, timeout);
			if (n < 0) {
				if (total != 0) {
					up[total] = '\0';
					return (total);
				}
				return (-1);
			}
			if (n == 0) {
				/* EOF */
				up[total] = '\0';
				return (total);
			}
			/* got n bytes */
			cnt = *tmpcnt = n;
			idx = 0;
		}

		/* copy from temporary until LF */
		ch = utp[idx++];
		if (got_cr && ch != '\n') {
			/* CR only */
			/* back to temporary */
			idx--;
			/* remove CR */
			total--;
			break;
		} else if (ch == '\n') {
			if (got_cr) {
				/* CRLF */
				/* remove CR */
				total--;
			} else {
				/* LF only */
			}
			break;
		} else if (ch == '\r') {
			got_cr = 1;
		}
		up[total++] = ch;
	}
	*tmpidx = idx;
	/* always append LF + NUL */
	up[total++] = '\n';
	up[total] = '\0';
	return (total);
}

static ssize_t
istgt_allwrite_socket(int s, const void *buf, size_t nbytes, int timeout)
{
	const uint8_t *cp;
	size_t total;
	ssize_t n;

	total = 0;
	cp = (const uint8_t *) buf;
	do {
		n = istgt_write_socket(s, cp + total,
			(nbytes - total), timeout);
		if (n < 0) {
			return (n);
		}
		total += n;
	} while (total < nbytes);
	return (total);
}

ssize_t
istgt_writeline_socket(int sock, const char *buf, int timeout)
{
	const unsigned char *up;
	ssize_t total;
	ssize_t n;
	int idx;
	int ch;

	up = (const unsigned char *) buf;
	total = 0;
	idx = 0;

	if (up[0] == '\0') {
		/* empty string */
		n = istgt_allwrite_socket(sock, "\r\n", 2, timeout);
		if (n < 0) {
			return (-1);
		}
		if (n != 2) {
			return (-1);
		}
		total = n;
		return (total);
	}

	/* send with CRLF */
	while ((ch = up[idx]) != '\0') {
		if (ch == '\r') {
			if (up[idx + 1] == '\n') {
				/* CRLF */
				n = istgt_allwrite_socket(sock,
					up, idx + 2, timeout);
				if (n < 0) {
					return (-1);
				}
				if (n != idx + 2) {
					return (-1);
				}
				idx += 2;
			} else {
				/* CR Only */
				n = istgt_allwrite_socket(sock,
					up, idx, timeout);
				if (n < 0) {
					return (-1);
				}
				if (n != idx) {
					return (-1);
				}
				idx += 1;
				n = istgt_allwrite_socket(sock,
					"\r\n", 2, timeout);
				if (n < 0) {
					return (-1);
				}
				if (n != 2) {
					return (-1);
				}
			}
		} else if (ch == '\n') {
			/* LF Only */
			n = istgt_allwrite_socket(sock, up, idx, timeout);
			if (n < 0) {
				return (-1);
			}
			if (n != idx) {
				return (-1);
			}
			idx += 1;
			n = istgt_allwrite_socket(sock, "\r\n", 2, timeout);
			if (n < 0) {
				return (-1);
			}
			if (n != 2) {
				return (-1);
			}
		} else {
			idx++;
			continue;
		}
		up += idx;
		total += idx;
		idx = 0;
	}

	if (idx != 0) {
		/* no CRLF string */
		n = istgt_allwrite_socket(sock, up, idx, timeout);
		if (n < 0) {
			return (-1);
		}
		if (n != idx) {
			return (-1);
		}
		n = istgt_allwrite_socket(sock, "\r\n", 2, timeout);
		if (n < 0) {
			return (-1);
		}
		if (n != 2) {
			return (-1);
		}
		up += idx;
		total += idx + 2;
		idx = 0;
	}

	return (total);
}
