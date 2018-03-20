
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"

#define POLLWAIT 5000
#define PORTNUMLEN 32

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

	if (host == NULL)
		return -1;
	if (host[0] == '[') {
		strncpy(buf, host + 1, sizeof buf);
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		host = (const char *) &buf[0];
		if (strcasecmp(host, "*") == 0) {
			strncpy(buf, "::", sizeof buf);
			host = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(host, "*") == 0) {
			strncpy(buf, "0.0.0.0", sizeof buf);
			host = (const char *) &buf[0];
		}
	}
	snprintf(portnum, sizeof portnum, "%d", port);
	memset(&hints, 0, sizeof hints);
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	rc = getaddrinfo(host, portnum, &hints, &res0);
	if (rc != 0) {
		ISTGT_ERRLOG("getaddrinfo() failed (errno=%d)\n", errno);
		return -1;
	}

	/* try connect */
	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
retry:
		sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (sock < 0) {
			/* error */
			continue;
		}
		rc = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &val, sizeof val);
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
		return -1;
	}
	return sock;
}
int
replication_listen(const char *ip, int port, int que)
{
	char buf[MAX_TMPBUF];
	char portnum[PORTNUMLEN];
	char *p;
	struct addrinfo hints, *res, *res0;
	int sock;
	int val = 1;
	int rc;

	if (ip == NULL)
		return -1;
	if (ip[0] == '[') {
		strncpy(buf, ip + 1, sizeof buf);
		p = strchr(buf, ']');
		if (p != NULL)
			*p = '\0';
		ip = (const char *) &buf[0];
		if (strcasecmp(ip, "*") == 0) {
			strncpy(buf, "::", sizeof buf);
			ip = (const char *) &buf[0];
		}
	} else {
		if (strcasecmp(ip, "*") == 0) {
			strncpy(buf, "0.0.0.0", sizeof buf);
			ip = (const char *) &buf[0];
		}
	}
	snprintf(portnum, sizeof portnum, "%d", port);
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV;
	hints.ai_flags |= AI_PASSIVE;
	hints.ai_flags |= AI_NUMERICHOST;
	rc = getaddrinfo(ip, portnum, &hints, &res0);
	if (rc != 0) {
		REPLICA_ERRLOG("getaddrinfo() failed (errno=%d)\n", errno);
		return -1;
	}
	if (que < 2)
		que = 2;
	else if (que > 255)
		que = 255;

	sock = -1;
	for (res = res0; res != NULL; res = res->ai_next) {
retry:
		sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (sock < 0) {
			continue;
		}

		rc = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof val);
		if (rc != 0) {
			continue;
		}
		rc = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &val, sizeof val);
		if (rc != 0) {
			continue;
		}
		rc = bind(sock, res->ai_addr, res->ai_addrlen);
		if (rc == -1 && errno == EINTR) {
			close(sock);
			sock = -1;
			goto retry;
		}
		if (rc != 0) {
			close(sock);
			sock = -1;
			continue;
		}
		make_socket_non_blocking(sock);
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
		return -1;
	}
	return sock;
}

int
make_socket_non_blocking(int sfd)
{
	int flags, s;

	flags = fcntl(sfd, F_GETFL, 0);
	if (flags == -1)
	{
		REPLICA_ERRLOG("fcntl failed errno:%d\n", errno);
		return -1;
	}

	flags |= O_NONBLOCK;
	s = fcntl (sfd, F_SETFL, flags);
	if (s == -1)
	{
		REPLICA_ERRLOG("fcntl failed errno:%d\n", errno);
		return -1;
	}

	return 0;
}
