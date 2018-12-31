
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <json-c/json_object.h>

#include "replication.h"
#include "replication_misc.h"

#define	POLLWAIT 5000
#define	PORTNUMLEN 32

int
replication_connect(const char *host, int port)
{
	char buf[MAX_TMPBUF];
	char portnum[PORTNUMLEN];
	char *p;
	struct addrinfo hints, *res, *res0;
	int sock;
	int val = 1;
	int rc;

	if (host == NULL) {
		REPLICA_ERRLOG("host is NULL!\n");
		return (-1);
	}

	if (host[0] == '[') {
		strncpy(buf, host + 1, sizeof (buf));
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		host = (const char *) &buf[0];
		if (strcasecmp(host, "*") == 0) {
			strncpy(buf, "::", sizeof (buf));
			host = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(host, "*") == 0) {
			strncpy(buf, "0.0.0.0", sizeof (buf));
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
		ISTGT_ERRLOG("getaddrinfo() failed.. err(%d)\n", rc);
		return (-1);
	}

	/* try connect */
	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
retry:
		sock = socket(
						res->ai_family,
						res->ai_socktype,
						res->ai_protocol
					);
		if (sock < 0) {
			/* error */
			continue;
		}
		rc = setsockopt(
					sock,
					IPPROTO_TCP,
					TCP_NODELAY,
					&val,
					sizeof (val)
				);
		if (rc != 0) {
			/* error */
			continue;
		}
		rc = connect(sock, res->ai_addr, res->ai_addrlen);
		if (rc == -1 && errno == EINTR) {
			/* interrupted? */
			ISTGT_ERRLOG("connect() failed .. err(%d)\n", errno);
			close(sock);
			sock = -1;
			goto retry;
		}
		if (rc != 0) {
			/* try next family */
			ISTGT_ERRLOG("connect() failed .. err(%d)\n", errno);
			close(sock);
			sock = -1;
			continue;
		}
		rc = set_socket_keepalive(sock);
		if (rc != 0) {
			ISTGT_ERRLOG(
				"failed to set keepalive for fd(%d)\n",
				sock
			);
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
replication_listen(const char *ip, int port, int que, int non_blocking)
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
		strncpy(buf, ip + 1, sizeof (buf));
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		ip = (const char *) &buf[0];
		if (strcasecmp(ip, "*") == 0) {
			strncpy(buf, "::", sizeof (buf));
			ip = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(ip, "*") == 0) {
			strncpy(buf, "0.0.0.0", sizeof (buf));
			ip = (const char *) &buf[0];
		}
	}
	snprintf(portnum, sizeof (portnum), "%d", port);
	memset(&hints, 0, sizeof (hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	hints.ai_flags |= AI_PASSIVE;
	hints.ai_flags |= AI_NUMERICHOST;
	rc = getaddrinfo(ip, portnum, &hints, &res0);
	if (rc != 0) {
		REPLICA_ERRLOG("getaddrinfo() failed err(%d)\n", rc);
		return (-1);
	}
	if (que < 2)
		que = 2;
	else if (que > 255)
		que = 255;

	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
retry:
		sock = socket(
						res->ai_family,
						res->ai_socktype,
						res->ai_protocol
					);
		if (sock < 0) {
			REPLICA_ERRLOG("failed to create socket .. err(%d)\n",
			    errno);
			continue;
		}

		rc = setsockopt(
					sock,
					SOL_SOCKET,
					SO_REUSEADDR,
					&val,
					sizeof (val)
				);
		if (rc != 0) {
			REPLICA_ERRLOG("failed to set SO_REUSEADDR for "
			    "sock(%d).. err(%d)\n", sock, errno);
			continue;
		}
		rc = setsockopt(
					sock,
					IPPROTO_TCP,
					TCP_NODELAY,
					&val,
					sizeof (val)
				);
		if (rc != 0) {
			REPLICA_ERRLOG("failed to set TCP_NODELAY for "
			    "sock(%d).. err(%d)\n", sock, errno);
			continue;
		}
		rc = bind(sock, res->ai_addr, res->ai_addrlen);
		if (rc == -1 && errno == EINTR) {
			REPLICA_ERRLOG("bind() failed err(%d)\n", errno);
			close(sock);
			sock = -1;
			goto retry;
		}
		if (rc != 0) {
			REPLICA_ERRLOG("bind() failed err(%d)\n", errno);
			close(sock);
			sock = -1;
			continue;
		}
		if (non_blocking == 1)
			make_socket_non_blocking(sock);
		rc = listen(sock, que);
		if (rc != 0) {
			REPLICA_ERRLOG("listen() failed err(%d)\n", errno);
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
make_socket_non_blocking(int sfd)
{
	int flags, s;

	flags = fcntl(sfd, F_GETFL, 0);
	if (flags == -1) {
		REPLICA_ERRLOG("fcntl failed err(%d)\n", errno);
		return (-1);
	}

	flags |= O_NONBLOCK;
	s = fcntl(sfd, F_SETFL, flags);
	if (s == -1) {
		REPLICA_ERRLOG("fcntl failed err(%d)\n", errno);
		return (-1);
	}

	return (0);
}

int
set_socket_keepalive(int sfd)
{
	int val = 1;
	int ret = 0;
	int max_idle_time = 5;
	int max_try = 5;
	int probe_interval = 5;

	if (setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof (val)) < 0) {
		REPLICA_ERRLOG(
			"Failed to set SO_KEEPALIVE for fd(%d) err(%d)\n",
			sfd,
			errno
		);
		ret = errno;
		goto out;
	}

	if (setsockopt(sfd, SOL_TCP, TCP_KEEPCNT, &max_try, sizeof (max_try))) {
		REPLICA_ERRLOG(
			"Failed to set TCP_KEEPCNT for fd(%d) err(%d)\n",
			sfd,
			errno
		);
		ret = errno;
		goto out;
	}

	if (
			setsockopt(sfd, SOL_TCP, TCP_KEEPIDLE,
				&max_idle_time, sizeof (max_idle_time))
		) {
		REPLICA_ERRLOG(
			"Failed to set TCP_KEEPIDLE for fd(%d) err(%d)\n",
			sfd,
			errno
		);
		ret = errno;
		goto out;
	}

	if (
			setsockopt(sfd, SOL_TCP, TCP_KEEPINTVL,
				&probe_interval, sizeof (probe_interval))
		) {
		REPLICA_ERRLOG(
			"Failed to set TCP_KEEPINTVL for fd(%d) err(%d)\n",
			sfd,
			errno
		);
		ret = errno;
	}

out:
	return (ret);
}

json_object *
json_object_new_uint64(uint64_t value)
{
	/* 18446744073709551615 */
	char num[21];
	int r;
	json_object *jobj;

	r = snprintf(num, sizeof(num), "%" PRIu64, value);
	if (r < 0 || (size_t)r >= sizeof(num))
		return NULL;

	jobj = json_object_new_string(num);
	return jobj;
}
