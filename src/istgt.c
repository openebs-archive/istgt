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

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <fcntl.h>
#include <unistd.h>

#ifdef __FreeBSD__
#include <sys/types.h>
#include <sys/event.h>
#include <sys/sysctl.h>
#endif

#ifdef __linux__
// #include <kqueue/sys/event.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#endif

#include <sys/socket.h>
#include <netinet/in.h>

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_conf.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_crc32c.h"
#include "istgt_iscsi.h"
#include "istgt_lu.h"
#include "istgt_proto.h"
#ifdef	REPLICATION
#include "replication.h"
#include "istgt_integration.h"
#endif
#include "istgt_misc.h"
#include <execinfo.h>

#include <sys/time.h>




#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

#define	POLLWAIT    5000
#define	PORTNUMLEN    32

ISTGT g_istgt;
#ifdef	REPLICATION
extern int replica_timeout;
extern int extraWait;
extern rte_smempool_t rcmd_mempool;
extern rte_smempool_t rcommon_cmd_mempool;
#endif

/*
 * Global - number of luworker threads per lun
 */
int g_num_luworkers;

static int
istgt_parse_portal(const char *portal, char **host, char **port, int *que)
{
	const char *p;
	int n;
	int ret;
	if (portal == NULL) {
		ISTGT_ERRLOG("portal error\n");
		return (-1);
	}
	if (host == NULL || port == NULL || que == NULL)  {
		return (0);
	}

	if (portal[0] == '[') {
		/* IPv6 */
		p = strchr(portal + 1, ']');
		if (p == NULL) {
			ISTGT_ERRLOG("portal error\n");
			return (-1);
		}
		p++;
		n = p - portal;
		if (host != NULL) {
			*host = xmalloc(n + 1);
			memcpy(*host, portal, n);
			(*host)[n] = '\0';
		}
		if (p[0] == '\0') {
			if (port != NULL) {
				*port = xmalloc(PORTNUMLEN);
				snprintf(*port, PORTNUMLEN, "%d", DEFAULT_PORT);
			}
		} else {
			if (p[0] != ':') {
				ISTGT_ERRLOG("portal error\n");
				if (host != NULL)
					xfree(*host);
				return (-1);
			}
			if (port != NULL)
				*port = xstrdup(p + 1);
		}
	} else {
		/* IPv4 */
		/*
		 * p = strchr(portal, ':');
		 * if (p == NULL) {
		 * 	p = portal + strlen(portal);
		 * }
		 * n = p - portal;
		 * if (host != NULL) {
		 * 	*host = xmalloc(n + 1);
		 * 	memcpy(*host, portal, n);
		 * 	(*host)[n] = '\0';
		 * }
		 * if (p[0] == '\0') {
		 * 	if (port != NULL) {
		 * 		*port = xmalloc(PORTNUMLEN);
		 * 		snprintf(*port, PORTNUMLEN, "%d", DEFAULT_PORT);
		 * 	}
		 * } else {
		 * 	if (p[0] != ':') {
		 * 		ISTGT_ERRLOG("portal error\n");
		 * 		if (host != NULL)
		 * 			xfree(*host);
		 * 		return (-1);
		 * 	}
		 * 	if (port != NULL)
		 * 		*port = xstrdup(p + 1);
		 * }
		 */


		*host = xmalloc(64);
		*port = xmalloc(32);
		if (*host == NULL || *port == NULL) {
			ISTGT_ERRLOG("portal:%s alloc failed\n", portal);
			return (-1);
		}
		*que = 32;
		ret = sscanf(portal,  "%64[^:]:%32[^:]:%d", *host, *port, que);
		if (ret == 2) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET,
			    "portal host:%s port:%s q:--\n", *host, *port);
		} else if (ret == 3) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET,
			    "portal host:%s port:%s q:%d\n",
			    *host, *port, *que);
		} else {
			ISTGT_ERRLOG("portal:%s ret:%d host:%s port:%s q:%d\n",
			    portal, ret, *host, *port, *que);
		}
	}
	return (0);
}

static int
istgt_add_portal_group(ISTGT_Ptr istgt, CF_SECTION *sp, int *pgp_idx)
{
	const char *val;
	char *label, *portal, *host, *port;
	int que;
	int alloc_len;
	int idx, free_idx;
	int portals;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "add portal group %d\n", sp->num);

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	/* counts number of definition */
	for (i = 0; ; i++) {
		label = istgt_get_nmval(sp, "Portal", i, 0);
		portal = istgt_get_nmval(sp, "Portal", i, 1);
		if (label == NULL || portal == NULL)
			break;
		rc = istgt_parse_portal(portal, NULL, NULL, NULL);
		if (rc < 0) {
			ISTGT_ERRLOG("parse portal error (%s)\n", portal);
			return (-1);
		}
	}
	portals = i;
	if (portals > MAX_PORTAL) {
		ISTGT_ERRLOG("%d > MAX_PORTAL\n", portals);
		return (-1);
	}

	MTX_LOCK(&istgt->mutex);
	idx = istgt->nportal_group;
	free_idx = -1;
	for (i = 0; i < istgt->nportal_group; i++) {
		if (istgt->portal_group[i].tag != 0)
			continue;
		if (istgt->portal_group[i].nportals == portals) {
			free_idx = i;
			break;
		}
	}
	if (free_idx >= 0)
		idx = free_idx;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "Index=%d, Tag=%d, Portals=%d\n",
	    idx, sp->num, portals);
	if (idx < MAX_PORTAL_GROUP) {
		if (free_idx < 0) {
			istgt->portal_group[idx].nportals = portals;
			alloc_len = sizeof (PORTAL *) * portals;
			istgt->portal_group[idx].portals = xmalloc(alloc_len);
		}
		istgt->portal_group[idx].ref = 0;
		istgt->portal_group[idx].idx = idx;
		istgt->portal_group[idx].tag = sp->num;

		for (i = 0; i < portals; i++) {
			label = istgt_get_nmval(sp, "Portal", i, 0);
			portal = istgt_get_nmval(sp, "Portal", i, 1);
			if (label == NULL || portal == NULL) {
				if (free_idx < 0) {
					xfree(istgt->portal_group[idx].portals);
					istgt->portal_group[idx].nportals = 0;
				}
				istgt->portal_group[idx].tag = 0;
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("portal error\n");
				return (-1);
			}
			rc = istgt_parse_portal(portal, &host, &port, &que);
			if (rc < 0) {
				if (free_idx < 0) {
					xfree(istgt->portal_group[idx].portals);
					istgt->portal_group[idx].nportals = 0;
				}
				istgt->portal_group[idx].tag = 0;
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("parse portal error (%s)\n",
				    portal);
				return (-1);
			}
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "RIndex=%d, Host=%s, Port=%s, Tag=%d\n",
			    i, host, port, sp->num);

			if (free_idx < 0) {
				istgt->portal_group[idx].portals[i] =
				    xmalloc(sizeof (PORTAL));
			} else {
				xfree(istgt->portal_group[idx]
				    .portals[i]->label);
				xfree(istgt->portal_group[idx]
				    .portals[i]->host);
				xfree(istgt->portal_group[idx]
				    .portals[i]->port);
			}
			istgt->portal_group[idx].portals[i]->label =
			    xstrdup(label);
			istgt->portal_group[idx].portals[i]->host = host;
			istgt->portal_group[idx].portals[i]->port = port;
			istgt->portal_group[idx].portals[i]->que = que;
			istgt->portal_group[idx].portals[i]->ref = 0;
			istgt->portal_group[idx].portals[i]->idx = i;
			istgt->portal_group[idx].portals[i]->tag = sp->num;
			istgt->portal_group[idx].portals[i]->sock = -1;
		}

		if (pgp_idx != NULL)
			*pgp_idx = idx;
		if (free_idx < 0) {
			idx++;
			istgt->nportal_group = idx;
		}
	} else {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_ERRLOG("nportal_group(%d) >= MAX_PORTAL_GROUP\n", idx);
		return (-1);
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_pg_match_all(PORTAL_GROUP *pgp, CF_SECTION *sp)
{
	char *label = NULL, *portal = NULL, *host = NULL, *port = NULL;
	int que;
	int rc;
	int ret = 0;
	int i;

	for (i = 0; i < pgp->nportals; i++) {
		label = istgt_get_nmval(sp, "Portal", i, 0);
		portal = istgt_get_nmval(sp, "Portal", i, 1);
		if (label == NULL || portal == NULL)
			goto no_match;
		rc = istgt_parse_portal(portal, &host, &port, &que);
		if (rc < 0)
			goto no_match;
		if (strcmp(pgp->portals[i]->label, label) != 0)
			goto no_match;
		if (strcmp(pgp->portals[i]->host, host) != 0)
			goto no_match;
		if (strcmp(pgp->portals[i]->port, port) != 0)
			goto no_match;
		if (pgp->portals[i]->que != que)
			goto no_match;
	}

	label = istgt_get_nmval(sp, "Portal", i, 0);
	portal = istgt_get_nmval(sp, "Portal", i, 1);
	if (label != NULL || portal != NULL)
		goto no_match;
	ret = 1;

no_match:
	if (port)
		xfree(port);
	if (host)
		xfree(host);
	return (ret);
}

static int
istgt_update_portal_group(ISTGT_Ptr istgt, CF_SECTION *sp, int *pgp_idx)
{
	const char *val;
	char *label, *portal, *host, *port;
	int que;
	int alloc_len;
	int idx, free_idx;
	int portals;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "update portal group %d\n", sp->num);

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	/* counts number of definition */
	for (i = 0; ; i++) {
		label = istgt_get_nmval(sp, "Portal", i, 0);
		portal = istgt_get_nmval(sp, "Portal", i, 1);
		if (label == NULL || portal == NULL)
			break;
		rc = istgt_parse_portal(portal, NULL, NULL, NULL);
		if (rc < 0) {
			ISTGT_ERRLOG("parse portal error (%s)\n", portal);
			return (-1);
		}
	}
	portals = i;
	if (portals > MAX_PORTAL) {
		ISTGT_ERRLOG("%d > MAX_PORTAL\n", portals);
		return (-1);
	}

	MTX_LOCK(&istgt->mutex);
	idx = -1;
	for (i = 0; i < istgt->nportal_group; i++) {
		if (istgt->portal_group[i].tag == sp->num) {
			idx = i;
			break;
		}
	}
	if (idx < 0) {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_ERRLOG("can't find PG%d\n", sp->num);
		return (-1);
	}
	if (istgt_pg_match_all(&istgt->portal_group[i], sp)) {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "skip for PG%d\n", sp->num);
		return (0);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "Index=%d, Tag=%d, Portals=%d\n",
	    idx, sp->num, portals);
	if (istgt->portal_group[idx].nportals == portals) {
		/* udpate PG */
		for (i = 0; i < portals; i++) {
			label = istgt_get_nmval(sp, "Portal", i, 0);
			portal = istgt_get_nmval(sp, "Portal", i, 1);
			if (label == NULL || portal == NULL) {
				xfree(istgt->portal_group[idx].portals);
				istgt->portal_group[idx].nportals = 0;
				istgt->portal_group[idx].tag = 0;
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("portal error\n");
				return (-1);
			}
			rc = istgt_parse_portal(portal, &host, &port, &que);
			if (rc < 0) {
				xfree(istgt->portal_group[idx].portals);
				istgt->portal_group[idx].nportals = 0;
				istgt->portal_group[idx].tag = 0;
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("parse portal error (%s)\n",
				    portal);
				return (-1);
			}
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "RIndex=%d, Host=%s, Port=%s, Q=%d, Tag=%d\n",
			    i, host, port, que, sp->num);

			/* free old PG */
			xfree(istgt->portal_group[idx].portals[i]->label);
			xfree(istgt->portal_group[idx].portals[i]->host);
			xfree(istgt->portal_group[idx].portals[i]->port);

			/* allocate new PG */
			istgt->portal_group[idx].portals[i]->label =
			    xstrdup(label);
			istgt->portal_group[idx].portals[i]->host = host;
			istgt->portal_group[idx].portals[i]->port = port;
			istgt->portal_group[idx].portals[i]->que = que;
			// istgt->portal_group[idx].portals[i]->ref = 0;
			// istgt->portal_group[idx].portals[i]->idx = i;
			// istgt->portal_group[idx].portals[i]->tag = sp->num;
			// istgt->portal_group[idx].portals[i]->sock = -1;
		}
		if (pgp_idx != NULL)
			*pgp_idx = idx;
	} else {
		/* mark as free */
		istgt->portal_group[*pgp_idx].tag = 0;

		/* allocate new PG */
		idx = istgt->nportal_group;
		free_idx = -1;
		for (i = 0; i < istgt->nportal_group; i++) {
			if (istgt->portal_group[i].tag != 0)
				continue;
			if (istgt->portal_group[i].nportals == portals) {
				free_idx = i;
				break;
			}
		}
		if (free_idx >= 0)
			idx = free_idx;
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "Index=%d, Tag=%d, Portals=%d -> %d\n",
		    idx, sp->num,
		    istgt->portal_group[*pgp_idx].nportals, portals);
		if (idx < MAX_PORTAL_GROUP) {
			if (free_idx < 0) {
				istgt->portal_group[idx].nportals = portals;
				alloc_len = sizeof (PORTAL *) * portals;
				istgt->portal_group[idx].portals =
				    xmalloc(alloc_len);
			}
			istgt->portal_group[idx].ref =
			    istgt->portal_group[*pgp_idx].ref;
			istgt->portal_group[idx].idx = idx;
			istgt->portal_group[idx].tag = sp->num;

			for (i = 0; i < portals; i++) {
				label = istgt_get_nmval(sp, "Portal", i, 0);
				portal = istgt_get_nmval(sp, "Portal", i, 1);
				if (label == NULL || portal == NULL) {
					if (free_idx < 0) {
						xfree(istgt->portal_group[idx]
						    .portals);
						istgt->portal_group[idx]
						    .nportals = 0;
					}
					istgt->portal_group[idx].tag = 0;
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("portal error\n");
					return (-1);
				}
				rc = istgt_parse_portal(portal, &host,
				    &port, &que);
				if (rc < 0) {
					if (free_idx < 0) {
						xfree(istgt->portal_group[idx]
						    .portals);
						istgt->portal_group[idx]
						    .nportals = 0;
					}
					istgt->portal_group[idx].tag = 0;
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("parse portal error "
					    "(%s)\n", portal);
					return (-1);
				}
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				    "RIndex=%d, Host=%s, Port=%s,"
				    "Q=%d, Tag=%d\n",
				    i, host, port, que, sp->num);

				if (free_idx < 0) {
					istgt->portal_group[idx].portals[i] =
					    xmalloc(sizeof (PORTAL));
				} else {
					xfree(istgt->portal_group[idx]
					    .portals[i]->label);
					xfree(istgt->portal_group[idx]
					    .portals[i]->host);
					xfree(istgt->portal_group[idx]
					    .portals[i]->port);
				}
				istgt->portal_group[idx].portals[i]->label =
				    xstrdup(label);
				istgt->portal_group[idx].portals[i]->host =
				    host;
				istgt->portal_group[idx].portals[i]->port =
				    port;
				istgt->portal_group[idx].portals[i]->que =
				    que;
				istgt->portal_group[idx].portals[i]->ref =
				    0;
				istgt->portal_group[idx].portals[i]->idx =
				    i;
				istgt->portal_group[idx].portals[i]->tag =
				    sp->num;
				istgt->portal_group[idx].portals[i]->sock =
				    -1;
			}

			if (pgp_idx != NULL)
				*pgp_idx = idx;
			if (free_idx < 0) {
				idx++;
				istgt->nportal_group = idx;
			}
		} else {
			MTX_UNLOCK(&istgt->mutex);
			ISTGT_ERRLOG("nportal_group(%d) >= MAX_PORTAL_GROUP\n",
			    idx);
			return (-1);
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (1);
}

static int
istgt_build_portal_group_array(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	int rc;

	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_PORTALGROUP) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Group 0 is invalid\n");
				return (-1);
			}
			rc = istgt_add_portal_group(istgt, sp, NULL);
			if (rc < 0) {
				ISTGT_ERRLOG("add_portal_group() failed\n");
				return (-1);
			}
		}
		sp = sp->next;
	}
	return (0);
}

static void
istgt_destroy_portal_group_array(ISTGT_Ptr istgt)
{
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_destory_portal_group_array\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		for (j = 0; j < istgt->portal_group[i].nportals; j++) {
			xfree(istgt->portal_group[i].portals[j]->label);
			xfree(istgt->portal_group[i].portals[j]->host);
			xfree(istgt->portal_group[i].portals[j]->port);
			xfree(istgt->portal_group[i].portals[j]);
		}
		xfree(istgt->portal_group[i].portals);

		istgt->portal_group[i].nportals = 0;
		istgt->portal_group[i].portals = NULL;
		istgt->portal_group[i].ref = 0;
		istgt->portal_group[i].idx = i;
		istgt->portal_group[i].tag = 0;
	}
	istgt->nportal_group = 0;
	MTX_UNLOCK(&istgt->mutex);
}

static int
istgt_open_portal_group(PORTAL_GROUP *pgp)
{
	int port;
	int sock;
	int i;

	for (i = 0; i < pgp->nportals; i++) {
		if (pgp->portals[i]->sock < 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET,
			    "open host %s, port %s, q:%d tag %d\n",
			    pgp->portals[i]->host, pgp->portals[i]->port,
			    pgp->portals[i]->que,
			    pgp->portals[i]->tag);
			port = (int)strtol(pgp->portals[i]->port, NULL, 0);
			sock = istgt_listen("*", port, pgp->portals[i]->que);
			if (sock < 0) {
				ISTGT_ERRLOG("listen error %.64s:%d\n",
				    pgp->portals[i]->host, port);
				return (-1);
			}
			pgp->portals[i]->sock = sock;
		}
	}
	return (0);
}

static int
istgt_open_all_portals(ISTGT_Ptr istgt)
{
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_open_portal\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		rc = istgt_open_portal_group(&istgt->portal_group[i]);
		if (rc < 0) {
			MTX_UNLOCK(&istgt->mutex);
			return (-1);
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_close_portal_group(PORTAL_GROUP *pgp)
{
	int i;

	for (i = 0; i < pgp->nportals; i++) {
		if (pgp->portals[i]->sock >= 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET,
			    "close host %s, port %s, tag %d\n",
			    pgp->portals[i]->host, pgp->portals[i]->port,
			    pgp->portals[i]->tag);
			close(pgp->portals[i]->sock);
			pgp->portals[i]->sock = -1;
		}
	}
	return (0);
}

static int
istgt_close_all_portals(ISTGT_Ptr istgt)
{
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_close_portal\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		rc = istgt_close_portal_group(&istgt->portal_group[i]);
		if (rc < 0) {
			MTX_UNLOCK(&istgt->mutex);
			return (-1);
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_add_initiator_group(ISTGT_Ptr istgt, CF_SECTION *sp)
{
	const char *val;
	int alloc_len;
	int idx;
	int names;
	int masks;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "add initiator group %d\n", sp->num);

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	/* counts number of definition */
	for (i = 0; ; i++) {
		val = istgt_get_nval(sp, "InitiatorName", i);
		if (val == NULL)
			break;
	}
	names = i;
	if (names > MAX_INITIATOR) {
		ISTGT_ERRLOG("%d > MAX_INITIATOR\n", names);
		return (-1);
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

	MTX_LOCK(&istgt->mutex);
	idx = istgt->ninitiator_group;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "Index=%d, Tag=%d, Names=%d, Masks=%d\n",
	    idx, sp->num, names, masks);
	if (idx < MAX_INITIATOR_GROUP) {
		istgt->initiator_group[idx].ninitiators = names;
		alloc_len = sizeof (char *) * names;
		istgt->initiator_group[idx].initiators = xmalloc(alloc_len);
		istgt->initiator_group[idx].nnetmasks = masks;
		alloc_len = sizeof (char *) * masks;
		istgt->initiator_group[idx].netmasks = xmalloc(alloc_len);
		istgt->initiator_group[idx].ref = 0;
		istgt->initiator_group[idx].idx = idx;
		istgt->initiator_group[idx].tag = sp->num;

		for (i = 0; i < names; i++) {
			val = istgt_get_nval(sp, "InitiatorName", i);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "InitiatorName %s\n", val);
			istgt->initiator_group[idx].initiators[i] =
			    xstrdup(val);
		}
		for (i = 0; i < masks; i++) {
			val = istgt_get_nval(sp, "Netmask", i);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Netmask %s\n", val);
			istgt->initiator_group[idx].netmasks[i] = xstrdup(val);
		}

		idx++;
		istgt->ninitiator_group = idx;
	} else {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_ERRLOG("ninitiator_group(%d) >= MAX_INITIATOR_GROUP\n",
		    idx);
		return (-1);
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_ig_match_all(INITIATOR_GROUP *igp, CF_SECTION *sp)
{
	const char *val;
	int i;

	for (i = 0; i < igp->ninitiators; i++) {
		val = istgt_get_nval(sp, "InitiatorName", i);
		if (val == NULL)
			return (0);
		if (strcmp(igp->initiators[i], val) != 0)
			return (0);
	}
	val = istgt_get_nval(sp, "InitiatorName", i);
	if (val != NULL)
		return (0);
	for (i = 0; i < igp->nnetmasks; i++) {
		val = istgt_get_nval(sp, "Netmask", i);
		if (val == NULL)
			return (0);
		if (strcmp(igp->netmasks[i], val) != 0)
			return (0);
	}
	val = istgt_get_nval(sp, "Netmask", i);
	if (val != NULL)
		return (0);
	return (1);
}

static int
istgt_update_initiator_group(ISTGT_Ptr istgt, CF_SECTION *sp)
{
	const char *val;
	int alloc_len;
	int idx;
	int names;
	int masks;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "update initiator group %d\n", sp->num);

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	/* counts number of definition */
	for (i = 0; ; i++) {
		val = istgt_get_nval(sp, "InitiatorName", i);
		if (val == NULL)
			break;
	}
	names = i;
	if (names > MAX_INITIATOR) {
		ISTGT_ERRLOG("%d > MAX_INITIATOR\n", names);
		return (-1);
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

	MTX_LOCK(&istgt->mutex);
	idx = -1;
	for (i = 0; i < istgt->ninitiator_group; i++) {
		if (istgt->initiator_group[i].tag == sp->num) {
			idx = i;
			break;
		}
	}
	if (idx < 0) {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_ERRLOG("can't find IG%d\n", sp->num);
		return (-1);
	}
	if (istgt_ig_match_all(&istgt->initiator_group[i], sp)) {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "skip for IG%d\n", sp->num);
		return (0);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "Index=%d, Tag=%d, Names=%d, Masks=%d\n",
	    idx, sp->num, names, masks);

	/* free old IG */
	for (i = 0; i < istgt->initiator_group[idx].ninitiators; i++) {
		xfree(istgt->initiator_group[idx].initiators[i]);
	}
	xfree(istgt->initiator_group[idx].initiators);
	for (i = 0; i < istgt->initiator_group[idx].nnetmasks; i++) {
		xfree(istgt->initiator_group[idx].netmasks[i]);
	}
	xfree(istgt->initiator_group[idx].netmasks);

	/* allocate new IG */
	istgt->initiator_group[idx].ninitiators = names;
	alloc_len = sizeof (char *) * names;
	istgt->initiator_group[idx].initiators = xmalloc(alloc_len);
	istgt->initiator_group[idx].nnetmasks = masks;
	alloc_len = sizeof (char *) * masks;
	istgt->initiator_group[idx].netmasks = xmalloc(alloc_len);
	/* istgt->initiator_group[idx].ref = 0; */
	/* istgt->initiator_group[idx].idx = idx; */
	/* istgt->initiator_group[idx].tag = sp->num; */

	/* copy new strings */
	for (i = 0; i < names; i++) {
		val = istgt_get_nval(sp, "InitiatorName", i);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "InitiatorName %s\n", val);
		istgt->initiator_group[idx].initiators[i] = xstrdup(val);
	}
	for (i = 0; i < masks; i++) {
		val = istgt_get_nval(sp, "Netmask", i);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Netmask %s\n", val);
		istgt->initiator_group[idx].netmasks[i] = xstrdup(val);
	}
	istgt_lu_update_ig(istgt, &istgt->initiator_group[idx]);
	MTX_UNLOCK(&istgt->mutex);
	return (1);
}

static int
istgt_build_initiator_group_array(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	int rc;

	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_INITIATORGROUP) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Group 0 is invalid\n");
				return (-1);
			}
			rc = istgt_add_initiator_group(istgt, sp);
			if (rc < 0) {
				ISTGT_ERRLOG("add_initiator_group() failed\n");
				return (-1);
			}
		}
		sp = sp->next;
	}
	return (0);
}

static void
istgt_destory_initiator_group_array(ISTGT_Ptr istgt)
{
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "istgt_destory_initiator_group_array\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->ninitiator_group; i++) {
		for (j = 0; j < istgt->initiator_group[i].ninitiators; j++) {
			xfree(istgt->initiator_group[i].initiators[j]);
		}
		xfree(istgt->initiator_group[i].initiators);
		for (j = 0; j < istgt->initiator_group[i].nnetmasks; j++) {
			xfree(istgt->initiator_group[i].netmasks[j]);
		}
		xfree(istgt->initiator_group[i].netmasks);

		istgt->initiator_group[i].ninitiators = 0;
		istgt->initiator_group[i].initiators = NULL;
		istgt->initiator_group[i].nnetmasks = 0;
		istgt->initiator_group[i].netmasks = NULL;
		istgt->initiator_group[i].ref = 0;
		istgt->initiator_group[i].idx = i;
		istgt->initiator_group[i].tag = 0;
	}
	istgt->ninitiator_group = 0;
	MTX_UNLOCK(&istgt->mutex);
}

static int
istgt_build_uctl_portal(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	const char *val;
	char *label, *portal, *host, *port;
	int que;
	int tag;
	int idx;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_build_uctl_portal\n");

	sp = istgt_find_cf_section(istgt->config, "UnitControl");
	if (sp == NULL) {
		ISTGT_ERRLOG("find_cf_section failed()\n");
		return (-1);
	}

	for (i = 0; ; i++) {
		val = istgt_get_nval(sp, "Portal", i);
		if (val == NULL)
			break;

		label = istgt_get_nmval(sp, "Portal", i, 0);
		portal = istgt_get_nmval(sp, "Portal", i, 1);
		if (label == NULL || portal == NULL) {
			ISTGT_ERRLOG("uctl portal error\n");
			return (-1);
		}

		rc = istgt_parse_portal(portal, &host, &port, &que);
		if (rc < 0) {
			ISTGT_ERRLOG("parse uctl portal error\n");
			return (-1);
		}

		idx = istgt->nuctl_portal;
		tag = ISTGT_UC_TAG;
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "Index=%d, Host=%s, Port=%s, Q=%d, Tag=%d\n",
		    idx, host, port, que, tag);
		if (idx < MAX_UCPORTAL) {
			istgt->uctl_portal[idx].label = xstrdup(label);
			istgt->uctl_portal[idx].host = host;
			istgt->uctl_portal[idx].port = port;
			istgt->uctl_portal[idx].que = que;
			istgt->uctl_portal[idx].ref = 0;
			istgt->uctl_portal[idx].idx = idx;
			istgt->uctl_portal[idx].tag = tag;
			istgt->uctl_portal[idx].sock = -1;
			idx++;
			istgt->nuctl_portal = idx;
		} else {
			ISTGT_ERRLOG("nportal(%d) >= MAX_UCPORTAL\n", idx);
			xfree(host);
			xfree(port);
			return (-1);
		}
	}

	return (0);
}

static void
istgt_destroy_uctl_portal(ISTGT_Ptr istgt)
{
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_destroy_uctl_portal\n");
	for (i = 0; i < istgt->nuctl_portal; i++) {
		xfree(istgt->uctl_portal[i].label);
		xfree(istgt->uctl_portal[i].host);
		xfree(istgt->uctl_portal[i].port);

		istgt->uctl_portal[i].label = NULL;
		istgt->uctl_portal[i].host = NULL;
		istgt->uctl_portal[i].port = NULL;
		istgt->uctl_portal[i].ref = 0;
		istgt->uctl_portal[i].idx = i;
		istgt->uctl_portal[i].tag = 0;
	}
	istgt->nuctl_portal = 0;
}

static int
istgt_open_uctl_portal(ISTGT_Ptr istgt)
{
	int port;
	int sock;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_open_uctl_portal\n");
	for (i = 0; i < istgt->nuctl_portal; i++) {
		if (istgt->uctl_portal[i].sock < 0) {
			port = (int)strtol(istgt->uctl_portal[i].port, NULL, 0);
			sock = istgt_listen_unx(ISTGT_UCTL_UNXPATH,
			    istgt->uctl_portal[i].que);
			if (sock < 0) {
				ISTGT_ERRLOG("listen error for unx_domain "
				    "%s, trying tcp listener\n",
				    ISTGT_UCTL_UNXPATH);
				sock = istgt_listen(istgt->uctl_portal[i].host,
				    port, istgt->uctl_portal[i].que);
			}
			if (sock < 0) {
				ISTGT_ERRLOG("listen error %.64s:%d\n",
				    istgt->uctl_portal[i].host, port);
				return (-1);
			}
			istgt->uctl_portal[i].sock = sock;
		}
	}
	return (0);
}

static int
istgt_close_uctl_portal(ISTGT_Ptr istgt)
{
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_close_uctl_portal\n");
	for (i = 0; i < istgt->nuctl_portal; i++) {
		if (istgt->uctl_portal[i].sock >= 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET,
			    "close host %s, port %s, tag %d\n",
			    istgt->uctl_portal[i].host,
			    istgt->uctl_portal[i].port,
			    istgt->uctl_portal[i].tag);
			close(istgt->uctl_portal[i].sock);
			istgt->uctl_portal[i].sock = -1;
		}
	}
	return (0);
}

static int
istgt_write_pidfile(ISTGT_Ptr istgt)
{
	FILE *fp;
	pid_t pid;
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_write_pidfile\n");
	rc = remove(istgt->pidfile);
	if (rc != 0) {
		if (errno != ENOENT) {
			ISTGT_ERRLOG("pidfile remove error %d\n", errno);
			return (-1);
		}
	}
	fp = fopen(istgt->pidfile, "w");
	if (fp == NULL) {
		ISTGT_ERRLOG("pidfile open error %d\n", errno);
		return (-1);
	}
	pid = getpid();
	fprintf(fp, "%d\n", (int)pid);
	fclose(fp);
	return (0);
}

static void
istgt_remove_pidfile(ISTGT_Ptr istgt)
{
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_remove_pidfile\n");
	rc = remove(istgt->pidfile);
	if (rc != 0) {
		ISTGT_ERRLOG("pidfile remove error %d\n", errno);
		/* ignore error */
	}
}

char *
istgt_get_nmval(CF_SECTION *sp, const char *key, int idx1, int idx2)
{
	CF_ITEM *ip;
	CF_VALUE *vp;
	int i;

	ip = istgt_find_cf_nitem(sp, key, idx1);
	if (ip == NULL)
		return (NULL);
	vp = ip->val;
	if (vp == NULL)
		return (NULL);
	for (i = 0; vp != NULL; vp = vp->next) {
		if (i == idx2)
			return (vp->value);
		i++;
	}
	return (NULL);
}

char *
istgt_get_nval(CF_SECTION *sp, const char *key, int idx)
{
	CF_ITEM *ip;
	CF_VALUE *vp;

	ip = istgt_find_cf_nitem(sp, key, idx);
	if (ip == NULL)
		return (NULL);
	vp = ip->val;
	if (vp == NULL)
		return (NULL);
	return (vp->value);
}

char *
istgt_get_val(CF_SECTION *sp, const char *key)
{
	return (istgt_get_nval(sp, key, 0));
}

int
istgt_get_nintval(CF_SECTION *sp, const char *key, int idx)
{
	const char *v;
	int value;

	v = istgt_get_nval(sp, key, idx);
	if (v == NULL)
		return (-1);
	value = (int)strtol(v, NULL, 10);
	return (value);
}

int
istgt_get_intval(CF_SECTION *sp, const char *key)
{
	return (istgt_get_nintval(sp, key, 0));
}

static const char *
istgt_get_log_facility(CONFIG *config)
{
	CF_SECTION *sp;
	const char *logfacility;

	sp = istgt_find_cf_section(config, "Global");
	if (sp == NULL) {
		return (NULL);
	}
	logfacility = istgt_get_val(sp, "LogFacility");
	if (logfacility == NULL) {
		logfacility = DEFAULT_LOG_FACILITY;
	}
#if 0
	if (g_trace_flag & ISTGT_TRACE_DEBUG) {
		fprintf(stderr, "LogFacility %s\n", logfacility);
	}
#endif

	return (logfacility);
}

static int
istgt_init(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	const char *ag_tag;
	const char *val;
	size_t stacksize;
	int ag_tag_i;
	int MaxSessions;
	int MaxConnections;
	int MaxOutstandingR2T;
	int DefaultTime2Wait;
	int DefaultTime2Retain;
	int FirstBurstLength;
	int MaxBurstLength;
	int MaxRecvDataSegmentLength;
	int InitialR2T;
	int ImmediateData;
	int DataPDUInOrder;
	int DataSequenceInOrder;
	int ErrorRecoveryLevel;
	int timeout;
	int nopininterval;
	int maxr2t;
	int OperationalMode;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_init\n");
	sp = istgt_find_cf_section(istgt->config, "Global");
	if (sp == NULL) {
		ISTGT_ERRLOG("find_cf_section failed()\n");
		return (-1);
	}

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	val = istgt_get_val(sp, "PidFile");
	if (val == NULL) {
		val = DEFAULT_PIDFILE;
	}
	istgt->pidfile = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "PidFile %s\n",
	    istgt->pidfile);

	val = istgt_get_val(sp, "AuthFile");
	if (val == NULL) {
		val = DEFAULT_AUTHFILE;
	}
	istgt->authfile = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthFile %s\n",
	    istgt->authfile);

	val = istgt_get_val(sp, "LogFile");
	if (val == NULL) {
		val = DEFAULT_LOG_FILE;
	}
	istgt->logfile = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LogFile %s\n",
	    istgt->logfile);
#if 0
	val = istgt_get_val(sp, "MediaFile");
	if (val == NULL) {
		val = DEFAULT_MEDIAFILE;
	}
	istgt->mediafile = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MediaFile %s\n",
	    istgt->mediafile);
#endif

#if 0
	val = istgt_get_val(sp, "LiveFile");
	if (val == NULL) {
		val = DEFAULT_LIVEFILE;
	}
	istgt->livefile = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LiveFile %s\n",
	    istgt->livefile);
#endif
	clear_resv = istgt_get_intval(sp, "ClearResv");
	if (clear_resv < 0) {
		clear_resv = 1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "clear_resv = %d", clear_resv);

	val = istgt_get_val(sp, "MediaDirectory");
	if (val == NULL) {
		val = DEFAULT_MEDIADIRECTORY;
	}
	istgt->mediadirectory = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MediaDirectory %s\n",
	    istgt->mediadirectory);

	val = istgt_get_val(sp, "NodeBase");
	if (val == NULL) {
		val = DEFAULT_NODEBASE;
	}
	istgt->nodebase = xstrdup(val);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "NodeBase %s\n",
	    istgt->nodebase);

	OperationalMode = istgt_get_intval(sp, "OperationalMode");
	if (OperationalMode < 1) {
		OperationalMode = DEFAULT_OPERATIONAL_MODE;
	}
	istgt->OperationalMode = OperationalMode;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Operational mode %s\n",
		(istgt->OperationalMode) ?
		    "Fake_Mode_Of_Operation" : "Normal_Mode_Of_Operation");

	MaxSessions = istgt_get_intval(sp, "MaxSessions");
	if (MaxSessions < 1) {
		MaxSessions = DEFAULT_MAX_SESSIONS;
	}
	istgt->MaxSessions = MaxSessions;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxSessions %d\n",
	    istgt->MaxSessions);

	MaxConnections = istgt_get_intval(sp, "MaxConnections");
	if (MaxConnections < 1) {
		MaxConnections = DEFAULT_MAX_CONNECTIONS;
	}
	istgt->MaxConnections = MaxConnections;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxConnections %d\n",
	    istgt->MaxConnections);

	/* Set number of luworker threads from DEFAULT_CONF */
	if (g_num_luworkers == 0) {
		g_num_luworkers = istgt_get_intval(sp, "Luworkers");
		if (g_num_luworkers < 1 ||
		    g_num_luworkers > (ISTGT_MAX_NUM_LUWORKERS - 1)) {
			g_num_luworkers = ISTGT_NUM_LUWORKERS_DEFAULT;
		}
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Luworkers %d\n",
	    g_num_luworkers);

	/* limited to 16bits - RFC3720(12.2) */
	if (MaxSessions > 0xffff) {
		ISTGT_ERRLOG("over 65535 sessions are not supported\n");
		return (-1);
	}
	if (MaxConnections > 0xffff) {
		ISTGT_ERRLOG("over 65535 connections are not supported\n");
		return (-1);
	}

	MaxOutstandingR2T = istgt_get_intval(sp, "MaxOutstandingR2T");
	if (MaxOutstandingR2T < 1) {
		MaxOutstandingR2T = DEFAULT_MAXOUTSTANDINGR2T;
	}
	istgt->MaxOutstandingR2T = MaxOutstandingR2T;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxOutstandingR2T %d\n",
	    istgt->MaxOutstandingR2T);

	DefaultTime2Wait = istgt_get_intval(sp, "DefaultTime2Wait");
	if (DefaultTime2Wait < 0) {
		DefaultTime2Wait = DEFAULT_DEFAULTTIME2WAIT;
	}
	istgt->DefaultTime2Wait = DefaultTime2Wait;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DefaultTime2Wait %d\n",
	    istgt->DefaultTime2Wait);

	DefaultTime2Retain = istgt_get_intval(sp, "DefaultTime2Retain");
	if (DefaultTime2Retain < 0) {
		DefaultTime2Retain = DEFAULT_DEFAULTTIME2RETAIN;
	}
	istgt->DefaultTime2Retain = DefaultTime2Retain;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DefaultTime2Retain %d\n",
	    istgt->DefaultTime2Retain);

	/* check size limit - RFC3720(12.15, 12.16, 12.17) */
	if (istgt->MaxOutstandingR2T > 65535) {
		ISTGT_ERRLOG("MaxOutstandingR2T(%d) > 65535\n",
		    istgt->MaxOutstandingR2T);
		return (-1);
	}
	if (istgt->DefaultTime2Wait > 3600) {
		ISTGT_ERRLOG("DefaultTime2Wait(%d) > 3600\n",
		    istgt->DefaultTime2Wait);
		return (-1);
	}
	if (istgt->DefaultTime2Retain > 3600) {
		ISTGT_ERRLOG("DefaultTime2Retain(%d) > 3600\n",
		    istgt->DefaultTime2Retain);
		return (-1);
	}

	FirstBurstLength = istgt_get_intval(sp, "FirstBurstLength");
	if (FirstBurstLength < 0) {
		FirstBurstLength = DEFAULT_FIRSTBURSTLENGTH;
	}
	istgt->FirstBurstLength = FirstBurstLength;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "FirstBurstLength %d\n",
	    istgt->FirstBurstLength);

	MaxBurstLength = istgt_get_intval(sp, "MaxBurstLength");
	if (MaxBurstLength < 0) {
		MaxBurstLength = DEFAULT_MAXBURSTLENGTH;
	}
	istgt->MaxBurstLength = MaxBurstLength;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxBurstLength %d\n",
	    istgt->MaxBurstLength);

	MaxRecvDataSegmentLength
		= istgt_get_intval(sp, "MaxRecvDataSegmentLength");
	if (MaxRecvDataSegmentLength < 0) {
		MaxRecvDataSegmentLength = DEFAULT_MAXRECVDATASEGMENTLENGTH;
	}
	istgt->MaxRecvDataSegmentLength = MaxRecvDataSegmentLength;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxRecvDataSegmentLength %d\n",
	    istgt->MaxRecvDataSegmentLength);

	/* check size limit (up to 24bits - RFC3720(12.12)) */
	if (istgt->MaxBurstLength < 512) {
		ISTGT_ERRLOG("MaxBurstLength(%d) < 512\n",
		    istgt->MaxBurstLength);
		return (-1);
	}
	if (istgt->FirstBurstLength < 512) {
		ISTGT_ERRLOG("FirstBurstLength(%d) < 512\n",
		    istgt->FirstBurstLength);
		return (-1);
	}
	if (istgt->FirstBurstLength > istgt->MaxBurstLength) {
		ISTGT_ERRLOG("FirstBurstLength(%d) > MaxBurstLength(%d)\n",
		    istgt->FirstBurstLength, istgt->MaxBurstLength);
		return (-1);
	}
	if (istgt->MaxBurstLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxBurstLength(%d) > 0x00ffffff\n",
		    istgt->MaxBurstLength);
		return (-1);
	}
	if (istgt->MaxRecvDataSegmentLength < 512) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) < 512\n",
		    istgt->MaxRecvDataSegmentLength);
		return (-1);
	}
	if (istgt->MaxRecvDataSegmentLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) > 0x00ffffff\n",
		    istgt->MaxRecvDataSegmentLength);
		return (-1);
	}

	val = istgt_get_val(sp, "InitialR2T");
	if (val == NULL) {
		InitialR2T = DEFAULT_INITIALR2T;
	} else if (strcasecmp(val, "Yes") == 0) {
		InitialR2T = 1;
	} else if (strcasecmp(val, "No") == 0) {
#if 0
		InitialR2T = 0;
#else
		ISTGT_ERRLOG("not supported value %s\n", val);
		return (-1);
#endif
	} else {
		ISTGT_ERRLOG("unknown value %s\n", val);
		return (-1);
	}
	istgt->InitialR2T = InitialR2T;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "InitialR2T %s\n",
	    istgt->InitialR2T ? "Yes" : "No");

	val = istgt_get_val(sp, "ImmediateData");
	if (val == NULL) {
		ImmediateData = DEFAULT_IMMEDIATEDATA;
	} else if (strcasecmp(val, "Yes") == 0) {
		ImmediateData = 1;
	} else if (strcasecmp(val, "No") == 0) {
		ImmediateData = 0;
	} else {
		ISTGT_ERRLOG("unknown value %s\n", val);
		return (-1);
	}
	istgt->ImmediateData = ImmediateData;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ImmediateData %s\n",
	    istgt->ImmediateData ? "Yes" : "No");

	val = istgt_get_val(sp, "DataPDUInOrder");
	if (val == NULL) {
		DataPDUInOrder = DEFAULT_DATAPDUINORDER;
	} else if (strcasecmp(val, "Yes") == 0) {
		DataPDUInOrder = 1;
	} else if (strcasecmp(val, "No") == 0) {
#if 0
		DataPDUInOrder = 0;
#else
		ISTGT_ERRLOG("not supported value %s\n", val);
		return (-1);
#endif
	} else {
		ISTGT_ERRLOG("unknown value %s\n", val);
		return (-1);
	}
	istgt->DataPDUInOrder = DataPDUInOrder;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DataPDUInOrder %s\n",
	    istgt->DataPDUInOrder ? "Yes" : "No");

	val = istgt_get_val(sp, "DataSequenceInOrder");
	if (val == NULL) {
		DataSequenceInOrder = DEFAULT_DATASEQUENCEINORDER;
	} else if (strcasecmp(val, "Yes") == 0) {
		DataSequenceInOrder = 1;
	} else if (strcasecmp(val, "No") == 0) {
#if 0
		DataSequenceInOrder = 0;
#else
		ISTGT_ERRLOG("not supported value %s\n", val);
		return (-1);
#endif
	} else {
		ISTGT_ERRLOG("unknown value %s\n", val);
		return (-1);
	}
	istgt->DataSequenceInOrder = DataSequenceInOrder;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DataSequenceInOrder %s\n",
	    istgt->DataSequenceInOrder ? "Yes" : "No");

	ErrorRecoveryLevel = istgt_get_intval(sp, "ErrorRecoveryLevel");
	if (ErrorRecoveryLevel < 0) {
		ErrorRecoveryLevel = DEFAULT_ERRORRECOVERYLEVEL;
	} else if (ErrorRecoveryLevel == 0) {
		ErrorRecoveryLevel = 0;
	} else if (ErrorRecoveryLevel == 1) {
#if 0
		ErrorRecoveryLevel = 1;
#else
		ISTGT_ERRLOG("not supported value %d\n", ErrorRecoveryLevel);
		return (-1);
#endif
	} else if (ErrorRecoveryLevel == 2) {
#if 0
		ErrorRecoveryLevel = 2;
#else
		ISTGT_ERRLOG("not supported value %d\n", ErrorRecoveryLevel);
		return (-1);
#endif
	} else {
		ISTGT_ERRLOG("not supported value %d\n", ErrorRecoveryLevel);
		return (-1);
	}
	istgt->ErrorRecoveryLevel = ErrorRecoveryLevel;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ErrorRecoveryLevel %d\n",
	    istgt->ErrorRecoveryLevel);

	timeout = istgt_get_intval(sp, "Timeout");
	if (timeout < 0) {
		timeout = DEFAULT_TIMEOUT;
	}
	istgt->timeout = timeout;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Timeout %d\n",
	    istgt->timeout);

	nopininterval = istgt_get_intval(sp, "NopInInterval");
	if (nopininterval < 0) {
		nopininterval = DEFAULT_NOPININTERVAL;
	}
	istgt->nopininterval = nopininterval;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "NopInInterval %d\n",
	    istgt->nopininterval);

	maxr2t = istgt_get_intval(sp, "MaxR2T");
	if (maxr2t < 0) {
		maxr2t = DEFAULT_MAXR2T;
	}
	if (maxr2t > MAX_R2T) {
		ISTGT_ERRLOG("MaxR2T(%d) > %d\n",
		    maxr2t, MAX_R2T);
		return (-1);
	}
	istgt->maxr2t = maxr2t;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxR2T %d\n",
	    istgt->maxr2t);

	val = istgt_get_val(sp, "DiscoveryAuthMethod");
	if (val == NULL) {
		istgt->no_discovery_auth = 0;
		istgt->req_discovery_auth = 0;
		istgt->req_discovery_auth_mutual = 0;
	} else {
		istgt->no_discovery_auth = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "DiscoveryAuthMethod", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "CHAP") == 0) {
				istgt->req_discovery_auth = 1;
			} else if (strcasecmp(val, "Mutual") == 0) {
				istgt->req_discovery_auth_mutual = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				istgt->req_discovery_auth = 0;
				istgt->req_discovery_auth_mutual = 0;
			} else if (strcasecmp(val, "None") == 0) {
				istgt->no_discovery_auth = 1;
				istgt->req_discovery_auth = 0;
				istgt->req_discovery_auth_mutual = 0;
			} else {
				ISTGT_ERRLOG("unknown auth\n");
				return (-1);
			}
		}
		if (istgt->req_discovery_auth_mutual &&
		    !istgt->req_discovery_auth) {
			ISTGT_ERRLOG("Mutual but not CHAP\n");
			return (-1);
		}
	}
	if (istgt->no_discovery_auth != 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "DiscoveryAuthMethod None\n");
	} else if (istgt->req_discovery_auth == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "DiscoveryAuthMethod Auto\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "DiscoveryAuthMethod %s %s\n",
		    istgt->req_discovery_auth ? "CHAP" : "",
		    istgt->req_discovery_auth_mutual ? "Mutual" : "");
	}

	val = istgt_get_val(sp, "DiscoveryAuthGroup");
	if (val == NULL) {
		istgt->discovery_auth_group = 0;
	} else {
		ag_tag = val;
		if (strcasecmp(ag_tag, "None") == 0) {
			ag_tag_i = 0;
		} else {
			if (strncasecmp(ag_tag, "AuthGroup",
				strlen("AuthGroup")) != 0 ||
				    sscanf(ag_tag, "%*[^0-9]%d",
				    &ag_tag_i) != 1) {
				ISTGT_ERRLOG("auth group error\n");
				return (-1);
			}
			if (ag_tag_i == 0) {
				ISTGT_ERRLOG("invalid auth group %d\n",
				    ag_tag_i);
				return (-1);
			}
		}
		istgt->discovery_auth_group = ag_tag_i;
	}
	if (istgt->discovery_auth_group == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "DiscoveryAuthGroup None\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "DiscoveryAuthGroup AuthGroup%d\n",
		    istgt->discovery_auth_group);
	}

	rc = pthread_attr_init(&istgt->attr);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_attr_init() failed\n");
		return (-1);
	}
	rc = pthread_attr_getstacksize(&istgt->attr, &stacksize);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_attr_getstacksize() failed\n");
		return (-1);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "current thread stack = %zd\n",
	    stacksize);
	if (stacksize < ISTGT_STACKSIZE) {
		stacksize = ISTGT_STACKSIZE;
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "new thread stack = %zd\n",
		    stacksize);
		rc = pthread_attr_setstacksize(&istgt->attr, stacksize);
		if (rc != 0) {
			ISTGT_ERRLOG("pthread_attr_setstacksize() failed\n");
			return (-1);
		}
	}

	rc = pthread_mutexattr_init(&istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutexattr_init() failed\n");
		return (-1);
	}
#ifdef HAVE_PTHREAD_MUTEX_ADAPTIVE_NP
	rc = pthread_mutexattr_settype(&istgt->mutex_attr,
	    PTHREAD_MUTEX_ADAPTIVE_NP);
#else
	rc = pthread_mutexattr_settype(&istgt->mutex_attr,
	    PTHREAD_MUTEX_ERRORCHECK);
#endif
	if (rc != 0) {
		ISTGT_ERRLOG("mutexattr_settype() failed\n");
		return (-1);
	}
	rc = pthread_mutex_init(&istgt->mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	rc = pthread_mutex_init(&istgt->state_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	rc = pthread_mutex_init(&istgt->reload_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	rc = pthread_cond_init(&istgt->reload_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		return (-1);
	}

	rc = istgt_uctl_init(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_uctl_init() failed\n");
		return (-1);
	}
	rc = istgt_build_uctl_portal(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_build_uctl_portal() failed\n");
		return (-1);
	}
	rc = istgt_build_portal_group_array(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_build_portal_array() failed\n");
		return (-1);
	}
	rc = istgt_build_initiator_group_array(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("build_initiator_group_array() failed\n");
		return (-1);
	}

	rc = pipe(istgt->sig_pipe);
	if (rc != 0) {
		ISTGT_ERRLOG("pipe() failed\n");
		istgt->sig_pipe[0] = -1;
		istgt->sig_pipe[1] = -1;
		return (-1);
	}

	/* XXX TODO: add initializer */

	istgt_set_state(istgt, ISTGT_STATE_INITIALIZED);

	return (0);
}

static void
istgt_shutdown(ISTGT_Ptr istgt)
{
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_shutdown\n");

	istgt_destory_initiator_group_array(istgt);
	istgt_destroy_portal_group_array(istgt);
	istgt_destroy_uctl_portal(istgt);
	istgt_uctl_shutdown(istgt);
	istgt_remove_pidfile(istgt);
	xfree(istgt->pidfile);
	xfree(istgt->authfile);
	xfree(istgt->logfile);
#if 0
	xfree(istgt->mediafile);
	xfree(istgt->livefile);
#endif
	xfree(istgt->mediadirectory);
	xfree(istgt->nodebase);

	if (istgt->sig_pipe[0] != -1)
	    close(istgt->sig_pipe[0]);
	if (istgt->sig_pipe[1] != -1)
	    close(istgt->sig_pipe[1]);

	(void) pthread_cond_destroy(&istgt->reload_cond);
	(void) pthread_mutex_destroy(&istgt->reload_mutex);
	(void) pthread_mutex_destroy(&istgt->state_mutex);
	(void) pthread_mutex_destroy(&istgt->mutex);
	(void) pthread_attr_destroy(&istgt->attr);
}

static int
istgt_pg_exist_num(CONFIG *config, int num)
{
	CF_SECTION *sp;

	sp = config->section;
	while (sp != NULL) {
		if (sp->type == ST_PORTALGROUP) {
			if (sp->num == num) {
				return (1);
			}
		}
		sp = sp->next;
	}
	return (-1);
}

static PORTAL_GROUP *
istgt_get_tag_portal(ISTGT_Ptr istgt, int tag)
{
	int i;

	if (tag == 0)
		return (NULL);
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		if (istgt->portal_group[i].tag == tag) {
			MTX_UNLOCK(&istgt->mutex);
			return (&istgt->portal_group[i]);
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (NULL);
}

#if 0
static int
istgt_get_num_of_portals(CF_SECTION *sp)
{
	char *label, *portal;
	int portals;
	int rc;
	int i;

	for (i = 0; ; i++) {
		label = istgt_get_nmval(sp, "Portal", i, 0);
		portal = istgt_get_nmval(sp, "Portal", i, 1);
		if (label == NULL || portal == NULL)
			break;
		rc = istgt_parse_portal(portal, NULL, NULL);
		if (rc < 0) {
			ISTGT_ERRLOG("parse portal error (%s)\n", portal);
			return (-1);
		}
	}
	portals = i;
	if (portals > MAX_PORTAL) {
		ISTGT_ERRLOG("%d > MAX_PORTAL\n", portals);
		return (-1);
	}
	return (portals);
}
#endif

#define	RELOAD_CMD_LENGTH 5
static int
istgt_pg_reload_delete(ISTGT_Ptr istgt)
{
	char tmp[RELOAD_CMD_LENGTH];
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_pg_reload_delete\n");

	istgt->pg_reload = 0;
	/* request delete */
	tmp[0] = 'D';
	DSET32(&tmp[1], 0);
	rc = write(istgt->sig_pipe[1], tmp, RELOAD_CMD_LENGTH);
	if (rc < 0 || rc != RELOAD_CMD_LENGTH) {
		ISTGT_ERRLOG("write() failed\n");
		return (-1);
	}
	/* wait for completion */
	MTX_LOCK(&istgt->reload_mutex);
	while (istgt->pg_reload == 0) {
		pthread_cond_wait(&istgt->reload_cond, &istgt->reload_mutex);
	}
	rc = istgt->pg_reload;
	MTX_UNLOCK(&istgt->reload_mutex);
	if (rc < 0) {
		if (istgt_get_state(istgt) != ISTGT_STATE_RUNNING) {
			ISTGT_WARNLOG("%s\n", "pg_reload abort");
			return (-1);
		}
	}
	return (0);
}

static int
istgt_pg_reload_update(ISTGT_Ptr istgt)
{
	char tmp[RELOAD_CMD_LENGTH];
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_pg_reload_update\n");

	istgt->pg_reload = 0;
	/* request update */
	tmp[0] = 'U';
	DSET32(&tmp[1], 0);
	rc = write(istgt->sig_pipe[1], tmp, RELOAD_CMD_LENGTH);
	if (rc < 0 || rc != RELOAD_CMD_LENGTH) {
		ISTGT_ERRLOG("write() failed\n");
		return (-1);
	}
	/* wait for completion */
	MTX_LOCK(&istgt->reload_mutex);
	while (istgt->pg_reload == 0) {
		pthread_cond_wait(&istgt->reload_cond, &istgt->reload_mutex);
	}
	rc = istgt->pg_reload;
	MTX_UNLOCK(&istgt->reload_mutex);
	if (rc < 0) {
		if (istgt_get_state(istgt) != ISTGT_STATE_RUNNING) {
			ISTGT_WARNLOG("%s\n", "pg_reload abort");
			return (-1);
		}
	}
	return (0);
}

static int
istgt_ig_exist_num(CONFIG *config, int num)
{
	CF_SECTION *sp;

	sp = config->section;
	while (sp != NULL) {
		if (sp->type == ST_INITIATORGROUP) {
			if (sp->num == num) {
				return (1);
			}
		}
		sp = sp->next;
	}
	return (-1);
}

static int
istgt_ig_reload_delete(ISTGT_Ptr istgt)
{
	INITIATOR_GROUP *igp;
	int rc;
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_ig_reload_delete\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->ninitiator_group; i++) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "IG reload idx=%d, (%d)\n",
		    i, istgt->ninitiator_group);
		igp = &istgt->initiator_group[i];
		rc = istgt_ig_exist_num(istgt->config, igp->tag);
		if (rc < 0) {
			if (igp->ref != 0) {
				ISTGT_ERRLOG("delete request for "
				    "referenced IG%d\n", igp->tag);
			} else {
				ISTGT_NOTICELOG("delete IG%d\n", igp->tag);
				/* free old IG */
				for (j = 0;
				    j < istgt->initiator_group[i].ninitiators;
				    j++) {
					xfree(istgt->initiator_group[i]
					    .initiators[j]);
				}
				xfree(istgt->initiator_group[i].initiators);
				for (j = 0;
				    j < istgt->initiator_group[i].nnetmasks;
				    j++) {
					xfree(istgt->initiator_group[i]
					    .netmasks[j]);
				}
				xfree(istgt->initiator_group[i].netmasks);

				/* move from beyond the IG */
				for (j = i; j < istgt->ninitiator_group - 1;
				    j++) {
					istgt->initiator_group[j].ninitiators
						= istgt->initiator_group[j+1]
						    .ninitiators;
					istgt->initiator_group[j].initiators
						= istgt->initiator_group[j+1]
						    .initiators;
					istgt->initiator_group[j].nnetmasks
						= istgt->initiator_group[j+1]
						    .nnetmasks;
					istgt->initiator_group[j].netmasks
						= istgt->initiator_group[j+1]
						    .netmasks;
					istgt->initiator_group[j].ref
						= istgt->initiator_group[j+1]
						    .ref;
					istgt->initiator_group[j].idx
						= istgt->initiator_group[j+1]
						    .idx;
					istgt->initiator_group[j].tag
						= istgt->initiator_group[j+1]
						    .tag;
				}
				istgt->ninitiator_group--;
			}
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_ig_reload_update(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_ig_reload_update\n");
	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_INITIATORGROUP) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Group 0 is invalid\n");
				goto skip_ig;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "IG reload tag=%d\n", sp->num);
#if 0
			rc = istgt_ig_exist_num(istgt->config_old, sp->num);
#else
			rc = -1;
			MTX_LOCK(&istgt->mutex);
			for (i = 0; i < istgt->ninitiator_group; i++) {
				if (istgt->initiator_group[i].tag == sp->num) {
					rc = 1;
					break;
				}
			}
			MTX_UNLOCK(&istgt->mutex);
#endif
			if (rc < 0) {
				rc = istgt_add_initiator_group(istgt, sp);
				if (rc < 0) {
					ISTGT_ERRLOG("add_initiator_group() "
					    "failed\n");
					goto skip_ig;
				}
				ISTGT_NOTICELOG("add IG%d\n", sp->num);
			} else {
				rc = istgt_update_initiator_group(istgt, sp);
				if (rc < 0) {
					ISTGT_ERRLOG(
					    "update_initiator_group() "
					    "failed\n");
					goto skip_ig;
				} else if (rc == 0) {
					// not modified
				} else if (rc > 0) {
					ISTGT_NOTICELOG("update IG%d\n",
					    sp->num);
				}
			}
		}
	skip_ig:
		sp = sp->next;
	}
	return (0);
}

int  istgtversn = 0;
char istgtvers[80];


int
istgt_reload(ISTGT_Ptr istgt)
{
	CONFIG *config_new, *config_old;
	char *config_file;
	int rc;
	uint32_t gen;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_reload\n");
	/* prepare config structure */
	config_new = istgt_allocate_config();
	config_old = istgt->config;
	config_file = config_old->file;

	rc = istgt_read_config(config_new, config_file);
	if (rc < 0) {
		ISTGT_ERRLOG("config error\n");
		istgt_free_config(config_new);
		return (-1);
	}
	if (config_new->section == NULL) {
		ISTGT_ERRLOG("empty config\n");
		istgt_free_config(config_new);
		return (-1);
	}
	istgt->config = config_new;
	istgt->config_old = config_old;
	gen = ++istgt->generation;

	/* reload sub groups */
	ISTGT_NOTICELOG("reload configuration#%u start [%s]\n", gen, istgtvers);
	rc = istgt_lu_reload_delete(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("LU reload del error\n");
		return (-1);
	}
	rc = istgt_ig_reload_delete(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("IG reload del error\n");
		return (-1);
	}
	rc = istgt_pg_reload_delete(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("PG reload del error\n");
		return (-1);
	}

	rc = istgt_pg_reload_update(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("PG reload add error\n");
		return (-1);
	}
	rc = istgt_ig_reload_update(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("IG reload add error\n");
		return (-1);
	}
	rc = istgt_lu_reload_update(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("LU reload add error\n");
		return (-1);
	}

	istgt->config_old = NULL;
	istgt_free_config(config_old);
	ISTGT_NOTICELOG("reload configuration#%"PRIu32" end "
	    "[%s]\n", gen, istgtvers);
	return (0);
}

static PORTAL *
istgt_get_sock_portal(ISTGT_Ptr istgt, int sock)
{
	int i, j;

	if (sock < 0)
		return (NULL);
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		for (j = 0; j < istgt->portal_group[i].nportals; j++) {
			if (istgt->portal_group[i].portals[j]->sock == sock) {
				MTX_UNLOCK(&istgt->mutex);
				return (istgt->portal_group[i].portals[j]);
			}
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (NULL);
}

static int
istgt_pg_delete(ISTGT_Ptr istgt)
{
	PORTAL_GROUP *pgp;
	int rc;
	int i;

	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		pgp = &istgt->portal_group[i];
		if (pgp->tag == 0)
			continue;
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "PG reload idx=%d, tag=%d, (%d)\n",
		    i, pgp->tag, istgt->nportal_group);
		rc = istgt_pg_exist_num(istgt->config, pgp->tag);
		if (rc < 0) {
			if (pgp->ref != 0) {
				ISTGT_ERRLOG("delete request for "
				    "referenced PG%d\n", pgp->tag);
			} else {
				ISTGT_NOTICELOG("delete PG%d\n", pgp->tag);
				pgp->tag = 0;
				(void) istgt_close_portal_group(pgp);
			}
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_pg_update(ISTGT_Ptr istgt)
{
	PORTAL_GROUP *pgp;
	CF_SECTION *sp;
	int pgp_idx;
	int rc;
	int i;

	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_PORTALGROUP) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Group 0 is invalid\n");
				goto skip_pg;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "PG reload tag=%d\n", sp->num);
#if 0
			rc = istgt_pg_exist_num(istgt->config_old, sp->num);
#else
			rc = -1;
			MTX_LOCK(&istgt->mutex);
			for (i = 0; i < istgt->nportal_group; i++) {
				if (istgt->portal_group[i].tag == sp->num) {
					rc = 1;
					break;
				}
			}
			MTX_UNLOCK(&istgt->mutex);
#endif
			if (rc < 0) {
				rc = istgt_add_portal_group(istgt,
				    sp, &pgp_idx);
				if (rc < 0) {
					ISTGT_ERRLOG("add_portal_group() "
					    "failed\n");
					goto skip_pg;
				}
				MTX_LOCK(&istgt->mutex);
				pgp = &istgt->portal_group[pgp_idx];
				(void) istgt_open_portal_group(pgp);
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_NOTICELOG("add PG%d\n", sp->num);
			} else {
				// portals = istgt_get_num_of_portals(sp);
				pgp = istgt_get_tag_portal(istgt, sp->num);
				if (istgt_pg_match_all(pgp, sp)) {
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					    "skip for PG%d\n", sp->num);
				} else if (pgp->ref != 0) {
					ISTGT_ERRLOG("update request for "
					    "referenced PG%d\n", pgp->tag);
				} else {
					/* delete old sock */
					MTX_LOCK(&istgt->mutex);
					pgp_idx = pgp->idx;
					(void) istgt_close_portal_group(pgp);
					MTX_UNLOCK(&istgt->mutex);
					rc = istgt_update_portal_group(istgt,
					    sp, &pgp_idx);
					if (rc < 0) {
						ISTGT_ERRLOG(
						    "update_portal_group() "
						    "failed\n");
						goto skip_pg;
					} else if (rc == 0) {
						// not modified
					} else if (rc > 0) {
						/* add new sock */
						MTX_LOCK(&istgt->mutex);
						pgp =
						    &istgt->portal_group
						    [pgp_idx];
						(void)
						    istgt_open_portal_group
						    (pgp);
						MTX_UNLOCK(&istgt->mutex);
						ISTGT_NOTICELOG("update PG%d\n",
						    sp->num);
					}
				}
			}
		}
	skip_pg:
		sp = sp->next;
	}
	return (0);
}

static void
exit_handler(int sig)
{
	ISTGT_LOG("Caught SIGTERM(%d). Exiting...", sig);
	exit(0);
}

/*
 * Print a stack trace before program exits.
 */
static void
fatal_handler(int sig)
{
	void *array[20];
	size_t size;

	fprintf(stderr, "Fatal signal received: %d\n", sig);
	fprintf(stderr, "Stack trace:\n");

	size = backtrace(array, 20);
	backtrace_symbols_fd(array, size, STDERR_FILENO);

	/*
	 * Hand over the sig for default processing to system to generate
	 * a coredump
	 */
	signal(sig, SIG_DFL);
	kill(getpid(), sig);
}

static int
istgt_acceptor(ISTGT_Ptr istgt)
{
	PORTAL *pp;
	int epfd;
	// int kq;
	int epsocks[MAX_PORTAL_GROUP + MAX_UCPORTAL];
	// int kqsocks[MAX_PORTAL_GROUP + MAX_UCPORTAL];
	struct epoll_event event, events;
	// struct kevent kev;
	struct timespec ep_timeout;
	// struct timespec kev_timeout;
	struct sockaddr_storage sa;
	socklen_t salen;
	int sock;
	int rc, n;
	int ucidx;
	int nidx;
	int i, j;

	if (istgt_get_state(istgt) != ISTGT_STATE_INITIALIZED) {
		ISTGT_ERRLOG("not initialized\n");
		return (-1);
	}
	/* now running main thread */
	istgt_set_state(istgt, ISTGT_STATE_RUNNING);

reload:
	nidx = 0;
	epfd = epoll_create1(0);
	if (epfd == -1) {
		ISTGT_ERRLOG("epoll_create1() failed, errno:%d\n", errno);
		return (-1);
	}
	for (i = 0; i < (int)(sizeof (epsocks) / sizeof (*epsocks)); i++) {
		epsocks[i] = -1;
	}
	/*
	 * kq = kqueue();
	 * if (kq == -1) {
	 *	ISTGT_ERRLOG("kqueue() failed\n");
	 *	return -1;
	 * }
	 * for (i = 0; i < (int)(sizeof kqsocks / sizeof *kqsocks); i++) {
	 *	kqsocks[i] = -1;
	 * }
	 */
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < istgt->nportal_group; i++) {
		for (j = 0; j < istgt->portal_group[i].nportals; j++) {
			if (istgt->portal_group[i].portals[j]->sock >= 0) {
				event.data.fd =
				    istgt->portal_group[i].portals[j]->sock;
				event.events = EPOLLIN;
				rc = epoll_ctl(epfd, EPOLL_CTL_ADD,
				    istgt->portal_group[i].portals[j]->sock,
				    &event);
				if (rc == -1) {
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("epoll_ctl() failed, "
					    "errno:%d\n", errno);
					close(epfd);
					return (-1);
				}
				epsocks[nidx] =
				    istgt->portal_group[i].portals[j]->sock;
				nidx++;
			}
		}
	}
	/*
	 * for (i = 0; i < istgt->nportal_group; i++) {
	 * 	for (j = 0; j < istgt->portal_group[i].nportals; j++) {
	 * 		if (istgt->portal_group[i].portals[j]->sock >= 0) {
	 *			ISTGT_EV_SET(&kev,
	 *    istgt->portal_group[i].portals[j]->sock,
	 *			    EVFILT_READ, EV_ADD, 0, 0, NULL);
	 *			rc = kevent(kq, &kev, 1, NULL, 0, NULL);
	 *			if (rc == -1) {
	 *				MTX_UNLOCK(&istgt->mutex);
	 *				ISTGT_ERRLOG("kevent() failed\n");
	 *				close(kq);
	 *				return (-1);
	 *			}
	 *			kqsocks[nidx] =
	 *    istgt->portal_group[i].portals[j]->sock;
	 * 			nidx++;
	 *		}
	 * 	}
	 * }
	 */
	ucidx = nidx;
	for (i = 0; i < istgt->nuctl_portal; i++) {
		event.data.fd = istgt->uctl_portal[i].sock;
		event.events = EPOLLIN;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD,
		    istgt->uctl_portal[i].sock, &event);
		if (rc == -1) {
			MTX_UNLOCK(&istgt->mutex);
			ISTGT_ERRLOG("epoll_ctl() failed, errno:%d\n", errno);
			close(epfd);
			return (-1);
		}
		epsocks[nidx] = istgt->uctl_portal[i].sock;
		nidx++;
	}
	/*
	 * for (i = 0; i < istgt->nuctl_portal; i++) {
	 *	ISTGT_EV_SET(&kev, istgt->uctl_portal[i].sock,
	 *	    EVFILT_READ, EV_ADD, 0, 0, NULL);
	 *	rc = kevent(kq, &kev, 1, NULL, 0, NULL);
	 *	if (rc == -1) {
	 *		ISTGT_ERRLOG("kevent() failed\n");
	 *		close(kq);
	 *		return -1;
	 *	}
	 *	kqsocks[nidx] = istgt->uctl_portal[i].sock;
	 *	nidx++;
	 * }
	 */
	event.data.fd = istgt->sig_pipe[0];
	event.events = EPOLLIN;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, istgt->sig_pipe[0], &event);
	if (rc == -1) {
		MTX_UNLOCK(&istgt->mutex);
		ISTGT_ERRLOG("epoll_ctl() failed, errno:%d\n", errno);
		close(epfd);
		return (-1);
	}
	MTX_UNLOCK(&istgt->mutex);

	epsocks[nidx] = istgt->sig_pipe[0];
	nidx++;
	/*
	 * ISTGT_EV_SET(&kev, istgt->sig_pipe[0],
	 *    EVFILT_READ, EV_ADD, 0, 0, NULL);
	 * rc = kevent(kq, &kev, 1, NULL, 0, NULL);
	 * if (rc == -1) {
	 *	ISTGT_ERRLOG("kevent() failed\n");
	 *	close(kq);
	 *	return (-1);
	 * }
	 * kqsocks[nidx] = istgt->sig_pipe[0];
	 * nidx++;
	 */
//	if (!istgt->daemon)
// TODO
	/*
	 * event.data.fd = SIGINT;
	 * event.events = EPOLLIN;
	 * rc = epoll_ctl(epfd, EPOLL_CTL_ADD, SIGINT, &event); // TODO CHECK
	 * if (rc == -1) {
	 * 	MTX_UNLOCK(&istgt->mutex);
	 * 	ISTGT_ERRLOG("epoll_ctl() failed, errno:%d\n", errno);
	 * 	close(epfd);
	 * 	return (-1);
	 * }
	 * event.data.fd = SIGTERM;
	 * event.events = EPOLLIN;
	 * rc = epoll_ctl(epfd, EPOLL_CTL_ADD, SIGTERM, &event); // TODO CHECK
	 * if (rc == -1) {
	 * 	MTX_UNLOCK(&istgt->mutex);
	 * 	ISTGT_ERRLOG("epoll_ctl() failed, errno:%d\n", errno);
	 * 	close(epfd);
	 * 	return (-1);
	 * }
	 */

	/*
	 * {
	 * 	ISTGT_EV_SET(&kev, SIGINT, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);
	 * 	rc = kevent(kq, &kev, 1, NULL, 0, NULL);
	 * 	if (rc == -1) {
	 * 		ISTGT_ERRLOG("kevent() failed\n");
	 * 		close(kq);
	 * 		return (-1);
	 * 	}
	 * 	ISTGT_EV_SET(&kev, SIGTERM, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);
	 * 	rc = kevent(kq, &kev, 1, NULL, 0, NULL);
	 * 	if (rc == -1) {
	 * 		ISTGT_ERRLOG("kevent() failed\n");
	 * 		close(kq);
	 * 		return (-1);
	 * 	}
	 * }
	 */

	while (1) {
		if (istgt_get_state(istgt) != ISTGT_STATE_RUNNING) {
			break;
		}
		// ISTGT_TRACELOG(ISTGT_TRACE_NET, "kevent %d\n", nidx);
		ep_timeout.tv_sec = 10;
		ep_timeout.tv_nsec = 0;
		rc = epoll_wait(epfd, &events, 1, ep_timeout.tv_sec*1000);
		// rc = kevent(kq, NULL, 0, &kev, 1, &kev_timeout);
		if (rc == -1 && errno == EINTR) {
			continue;
		}
		if (rc == -1) {
			ISTGT_ERRLOG("kevent() failed\n");
			break;
		}
		if (rc == 0) {
			/* idle timeout */
			/*
			 * ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			 *    "kevent TIMEOUT\n");
			 */
			continue;
		}
		// TODO
		/*
		 * if (events.data.fd == SIGINT || events.data.fd == SIGTERM) {
		 * 	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		 * 			"kevent SIGNAL SIGINT/SIGTERM\n");
		 * 	break;
		 * }
		 */

		/*
		 * if (events.events == SIGINT || events.events == SIGTERM) {
		 * 	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "kevent SIGNAL\n");
		 * 	if (kev.ident == SIGINT || kev.ident == SIGTERM) {
		 * 		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		 *     "kevent SIGNAL SIGINT/SIGTERM\n");
		 * 		break;
		 * 	}
		 * 	continue;
		 * }
		 */

		n = rc;
		for (i = 0; n != 0 && i < ucidx; i++) {
			if (events.data.fd == epsocks[i]) {
				/*
				 * if (kev.flags) {
				 * 	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				 *    "flags %x\n", kev.flags);
				 * }
				 */
				n--;
				memset(&sa, 0, sizeof (sa));
				salen = sizeof (sa);
				ISTGT_TRACELOG(ISTGT_TRACE_NET, "accept %ld\n",
				    (unsigned long)events.data.fd);
				pp = istgt_get_sock_portal(istgt,
				    events.data.fd);
				rc = accept(events.data.fd,
				    (struct sockaddr *) &sa, &salen);
				if (rc < 0) {
					ISTGT_ERRLOG("accept error errno:%d "
					    "rc:%d\n", errno, rc);
					continue;
				}
				sock = rc;
				rc = istgt_create_conn(istgt, pp, sock,
				    (struct sockaddr *) &sa, salen);
				if (rc < 0) {
					close(sock);
					ISTGT_ERRLOG("istgt_create_conn() "
					    "failed\n");
					continue;
				}
			}
		}

		/* check for control */
		for (i = 0; n != 0 && i < istgt->nuctl_portal; i++) {
			if (events.data.fd == istgt->uctl_portal[i].sock) {
				/*
				 * if (kev.flags) {
				 *     ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				 *    "flags %x\n", kev.flags);
				 * }
				 */
				n--;
				memset(&sa, 0, sizeof (sa));
				salen = sizeof (sa);
				ISTGT_TRACELOG(ISTGT_TRACE_NET,
				    "accept %ld\n",
					(unsigned long)events.data.fd);
				rc = accept(events.data.fd,
				    (struct sockaddr *) &sa, &salen);
				if (rc < 0) {
					ISTGT_ERRLOG("accept error errno:%d "
					    "rc:%d\n", errno, rc);
					continue;
				}
				sock = rc;
				rc = istgt_create_uctl(istgt,
				    &istgt->uctl_portal[i], sock,
				    (struct sockaddr *) &sa, salen);
				if (rc < 0) {
					close(sock);
					ISTGT_ERRLOG("istgt_create_uctl() "
					    "failed\n");
					continue;
				}
			}
		}

		/* check for signal thread */
		if (events.data.fd == istgt->sig_pipe[0]) {
			/*
			 * if (kev.flags & (EV_EOF|EV_ERROR)) {
			 *     ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			 *	   "kevent EOF/ERROR\n");
			 *	   break;
			 * }
			 */
			char tmp[RELOAD_CMD_LENGTH];
			// int pgp_idx;
			int rc2;

			rc = read(istgt->sig_pipe[0], tmp, RELOAD_CMD_LENGTH);
			if (rc < 0 || rc == 0 || rc != RELOAD_CMD_LENGTH) {
				ISTGT_ERRLOG("read() failed\n");
				break;
			}
			// pgp_idx = (int)DGET32(&tmp[1]);

			if (tmp[0] == 'E') {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				    "exit request (main loop)\n");
				break;
			}
			if (tmp[0] == 'D') {
				rc = istgt_pg_delete(istgt);
				MTX_LOCK(&istgt->reload_mutex);
				istgt->pg_reload = rc < 0 ? -1 : 1;
				rc2 =
				    pthread_cond_broadcast(&istgt->reload_cond);
				if (rc2 != 0) {
					ISTGT_ERRLOG("cond_broadcast() "
					"failed\n");
				}
				MTX_UNLOCK(&istgt->reload_mutex);
				if (rc < 0) {
					ISTGT_ERRLOG("pg_delete() failed\n");
					// break;
				}
			}
			if (tmp[0] == 'U') {
				rc = istgt_pg_update(istgt);
				MTX_LOCK(&istgt->reload_mutex);
				istgt->pg_reload = rc < 0 ? -1 : 1;
				rc2 =
				    pthread_cond_broadcast(&istgt->reload_cond);
				if (rc2 != 0) {
					ISTGT_ERRLOG("cond_broadcast() "
					"failed\n");
				}
				MTX_UNLOCK(&istgt->reload_mutex);
				if (rc < 0) {
					ISTGT_ERRLOG("pg_update() failed\n");
					// break;
				}
			}
			close(epfd);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "reload accept loop\n");
			goto reload;
		}
	}
	close(epfd);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "loop ended\n");
	istgt_set_state(istgt, ISTGT_STATE_EXITING);
	istgt_lu_set_all_state(istgt, ISTGT_STATE_EXITING);

	return (0);
}

static void
usage(void)
{
	printf("istgt [options]\n");
	printf("options:\n");
	printf(" -c config  config file (default %s)\n", DEFAULT_CONFIG);
	printf(" -p pidfile use specific file\n");
#ifdef	REPLICATION
	printf(" -l log level (info, error, debug default:info)\n");
#else
	printf(" -l facility use specific syslog facility (default %s)\n",
	    DEFAULT_LOG_FACILITY);
#endif
	printf(" -m mode    operational mode (default %d, 0=traditional, "
	    "1=normal, 2=experimental)\n", DEFAULT_ISTGT_SWMODE);
	printf(" -t flag    trace flag (all, net, iscsi, scsi, lu)\n");
	printf(" -q         quiet warnings\n");
	printf(" -D         don't detach from tty\n");
	printf(" -H         show this usage\n");
	printf(" -V         show version\n");
	printf(" -P         Persist Disabled\n");
#ifdef	REPLICATION
	printf(" -R         IO timeout in seconds at replicas in seconds\n");
#endif
}

#if 0
int unmap_support_global = 0;
static void
get_zvol_unmap_enabled(void)
{
	uint64_t unmap_val = 0;
	size_t size = sizeof (unmap_val);
	if (sysctlbyname("vfs.zfs.zvol_unmap_enabled",
	    &unmap_val, &size, NULL, 0) == 0 &&
	    unmap_val != 0)
		unmap_support = 1;
	else
		unmap_support = 0;
	unmap_support_global = unmap_support;
}

int
is_unmap_enabled(void)
{
	return (unmap_support);
}
#endif

static int persist = 1;
int
is_persist_enabled(void)
{
	return (persist);
}

clockid_t clockid = CLOCK_UPTIME_FAST; // CLOCK_SECOND  CLOCK_MONOTONIC_FAST
extern int detectDoubleFree;
// int enable_xcopy = 0;
int enable_oldBL = 0;

void *timerfn(void
	*ptr __attribute__((__unused__)))
{
	ISTGT_QUEUE backupconns;
	istgt_queue_init(&backupconns);
	CONN *conn;
#ifdef	REPLICATION
	spec_t *spec;
	ISTGT_LU_TASK_Ptr lu_task;
	ISTGT_LU_CMD_Ptr lu_cmd;
	int ms;
	struct timespec now, diff, last_check;
	clock_gettime(clockid, &last_check);
#endif

	while (1) {
		while ((conn = (CONN *)(istgt_queue_dequeue(&closedconns))) !=
		    NULL) {
			if (((time(NULL) - conn->closetime) > 300) &&
			    (conn->inflight == 0))
				istgt_free_conn(conn);
			else
				istgt_queue_enqueue(&backupconns, conn);
		}
		while ((conn = (CONN *)(istgt_queue_dequeue(&backupconns))) !=
		    NULL)
			istgt_queue_enqueue(&closedconns, conn);

#ifdef	REPLICATION
		const char *s_extra_wait_time = getenv("extraWait");
		int extra_wait = -1;
		if (s_extra_wait_time != NULL)
			extra_wait = (int)strtol(s_extra_wait_time,
			    NULL, 10);
		if ((extra_wait >= 0) && (extra_wait != extraWait)) {
			ISTGT_NOTICELOG("changing extraWait time from %d to "
			    "%d\n", extraWait, extra_wait);
			extraWait = extra_wait;
		}

		const char *s_replica_timeout = getenv("replicaTimeout");
		int rep_timeout = 0;
		if (s_replica_timeout != NULL)
			rep_timeout = (int)strtol(s_replica_timeout,
			    NULL, 10);
		if ((rep_timeout > 30) && (rep_timeout != replica_timeout)) {
			ISTGT_NOTICELOG("changing replica timeout "
			    "from %d to %d", replica_timeout,
			    rep_timeout);
			replica_timeout = rep_timeout;
		}
		int check_interval = (replica_timeout / 4) * 1000;


		clock_gettime(clockid, &now);
		timesdiff(clockid, last_check, now, diff);
		ms = diff.tv_sec * 1000;
		ms += diff.tv_nsec / 1000000;

		/*
		 * Here, we are checking if IOs are taking much time to
		 * complete than expected time at an interval of
		 * (replica_timeout /4). Expected time is set
		 * to (replica_timeout / 4) in ms.
		 *
		 * complete_queue holds the IOs scheduled for the target.
		 * we will calculate the time difference of first IO from
		 * complete_queue as first IO is the oldest one in the queue.
		 * If the time difference is more than (replica_timeout / 4)
		 * then we will log the IO's details.
		 */

		if (ms > check_interval) {
			MTX_LOCK(&specq_mtx);
			TAILQ_FOREACH(spec, &spec_q, spec_next) {
				MTX_LOCK(&spec->complete_queue_mutex);
				lu_task = (ISTGT_LU_TASK_Ptr)
				    istgt_queue_first(&spec->complete_queue);
				if (lu_task) {
					lu_cmd = &lu_task->lu_cmd;
					clock_gettime(clockid, &now);
					timesdiff(clockid,
					    lu_cmd->times[0], now, diff);
					ms = diff.tv_sec * 1000;
					ms += diff.tv_nsec / 1000000;
					if (ms > check_interval) {
						ISTGT_NOTICELOG("LU:%lu "
						    "CSN:0x%x TT:%x "
						    "OP:%2.2x:%x:%s(%lu+%u) "
						    "not responded since "
						    "%d seconds\n",
						    lu_cmd->lun,
						    lu_cmd->CmdSN,
						    lu_cmd->task_tag,
						    lu_cmd->cdb0,
						    lu_cmd->status,
						    lu_cmd->info,
						    lu_cmd->lba,
						    lu_cmd->lblen,
						    ms / 1000);
					}
				}

				MTX_UNLOCK(&spec->complete_queue_mutex);
			}
			MTX_UNLOCK(&specq_mtx);
			clock_gettime(clockid, &last_check);
		}
#endif

		sleep(10);
	}
	return ((void *)NULL);
}
void *zv;
void *spa;
int
main(int argc, char **argv)
{
	ISTGT_Ptr istgt;
	const char *config_file = DEFAULT_CONFIG;
	const char *pidfile = NULL;
	const char *logfacility = NULL;
	const char *logpriority = NULL;
	CONFIG *config;

	signal(SIGPIPE, SIG_IGN);
	signal(SIGTERM, exit_handler);
	signal(SIGABRT, fatal_handler);
	signal(SIGFPE, fatal_handler);
	signal(SIGSEGV, fatal_handler);
	signal(SIGBUS, fatal_handler);
	signal(SIGILL, fatal_handler);
#if 0
	pthread_t sigthread;
	struct sigaction sigact, sigoldact_pipe, sigoldact_int, sigoldact_term;
	struct sigaction sigoldact_hup, sigoldact_info;
	struct sigaction sigoldact_wakeup, sigoldact_io;
	sigset_t signew, sigold;
	int retry = 10;
#endif
	pthread_t timerthread;
	int detach = 1;
	int swmode;
	int ch;
	int rc;
#ifdef	REPLICATION
	pthread_t replication_thread;
	replication_log_level = LOG_LEVEL_INFO;
#endif

	(void) detach;
	send_abrt_resp = 0;
	abort_result_queue = 0;
	wait_inflights = 1;
	clear_resv = 1;

	if (sizeof (ISCSI_BHS) != ISCSI_BHS_LEN) {
		fprintf(stderr, "Internal Error\n");
		exit(EXIT_FAILURE);
	}

	detectDoubleFree = 0;
	memset(&g_istgt, 0, sizeof (g_istgt));
	istgt = &g_istgt;
	istgt->state = ISTGT_STATE_INVALID;
	istgt->swmode = DEFAULT_ISTGT_SWMODE;
	istgt->sig_pipe[0] = istgt->sig_pipe[1] = -1;
	istgt->daemon = 0;
	istgt->generation = 0;

	g_num_luworkers = 0;

	istgtversn = snprintf(istgtvers, 79, "istgt:%s.%s:%s:%s",
	    ISTGT_VERSION, ISTGT_EXTRA_VERSION, __TIME__, __DATE__);
	istgtvers[79] = '\0';

	pthread_t slf = pthread_self();
	snprintf(tinfo, sizeof (tinfo), "m#%d.%d",
	    (int)(((uint64_t *)slf)[0]), getpid());
#ifdef HAVE_PTHREAD_SET_NAME_NP
	pthread_set_name_np(pthread_self(), tinfo);
#endif

#ifdef	REPLICATION
	while ((ch = getopt(argc, argv, "c:p:l:m:t:N:qDHVFOPR:")) != -1) {
#else
	while ((ch = getopt(argc, argv, "c:p:l:m:t:N:qDHVFOP")) != -1) {
#endif
		switch (ch) {
		case 'c':
			config_file = optarg;
			break;
		case 'p':
			pidfile = optarg;
			break;
		case 'l':
#ifdef	REPLICATION
			if (strncmp(optarg, "debug", sizeof ("debug")) == 0)
				replication_log_level = LOG_LEVEL_DEBUG;
			else if (strncmp(optarg, "info", sizeof ("info")) == 0)
				replication_log_level = LOG_LEVEL_INFO;
			else if (strncmp(optarg, "error",
			    sizeof ("error")) == 0)
				replication_log_level = LOG_LEVEL_ERR;
			else {
				fprintf(stderr, "Log level should be one of "
				    "\"debug\", \"info\" or \"error\"\n");
				return (-1);
			}
#else
			logfacility = optarg;
#endif
			break;
		case 'm':
			swmode = strtol(optarg, NULL, 10);
			if (swmode == ISTGT_SWMODE_TRADITIONAL ||
			    swmode == ISTGT_SWMODE_NORMAL ||
			    swmode == ISTGT_SWMODE_EXPERIMENTAL) {
				istgt->swmode = swmode;
			} else {
				fprintf(stderr, "unknown mode %x\n", swmode);
				usage();
				exit(EXIT_FAILURE);
			}
			break;
		case 't':
			if (strcasecmp(optarg, "NET") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_NET);
			} else if (strcasecmp(optarg, "ISCSI") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_ISCSI);
			} else if (strcasecmp(optarg, "SCSI") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_SCSI);
			} else if (strcasecmp(optarg, "LU") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_LU);
			} else if (strcasecmp(optarg, "PQ") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_PQ);
			} else if (strcasecmp(optarg, "MEM") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_MEM);
			} else if (strcasecmp(optarg, "PROF") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_PROF);
			} else if (strcasecmp(optarg, "PROFX") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_PROFX);
			} else if (strcasecmp(optarg, "CMD") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_CMD);
			} else if (strcasecmp(optarg, "ALL") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_ALL);
			} else if (strcasecmp(optarg, "NONE") == 0) {
				istgt_set_trace_flag(ISTGT_TRACE_NONE);
			} else {
				fprintf(stderr, "unknown flag\n");
				usage();
				exit(EXIT_FAILURE);
			}
			break;
		case 'q':
			g_warn_flag = 0;
			break;
			/*
			 * CloudByte ES1.1 - added option for spawning more
			 * than one luworker thread for a lun
			 * This could probably be moved to the conf file but
			 * for now spawning this from cmd line seems safer as
			 * istgt can easily be restarted on the go and
			 * importantly, didn't want to diverge from the
			 * mainline/std conf file semantics
			 */
		case 'N':
			g_num_luworkers = strtol(optarg, NULL, 10);
			if ((g_num_luworkers > (ISTGT_MAX_NUM_LUWORKERS - 1)) ||
			    (g_num_luworkers <= 0)) {
				g_num_luworkers = 0;
				fprintf(stderr, "Incorrect number of "
				    "lu worker threads specified\n");
				usage();
				exit(EXIT_FAILURE);
			}
			break;
		case 'D':
			detach = 0;
			break;
		case 'F':
			detectDoubleFree = 1;
			break;
		case 'O':
			enable_oldBL = 1;
			break;
		case 'V':
			printf("istgt version %s  %s\n",
			    ISTGT_VERSION, istgtvers);
			printf("istgt extra version %s\n", ISTGT_EXTRA_VERSION);
			exit(EXIT_SUCCESS);
		case 'P':
			persist = 0;
			break;
#ifdef	REPLICATION
		case 'R':
			replica_timeout = strtol(optarg, NULL, 10);
			if (replica_timeout <= 0) {
				fprintf(stderr,
				    "Incorrect timeout for replica\n");
				usage();
				exit(EXIT_FAILURE);
			}
			break;
#endif
		case 'H':
		default:
			usage();
			exit(EXIT_SUCCESS);
		}
	}

	ISTGT_NOTICELOG("%s: starting\n", istgtvers);
#ifndef	REPLICATION
	poolinit();
#endif
	/* read config files */
	config = istgt_allocate_config();
	rc = istgt_read_config(config, config_file);
	if (rc < 0) {
		fprintf(stderr, "config error\n");
#ifndef	REPLICATION
		poolfini();
#endif
		exit(EXIT_FAILURE);
	}
	if (config->section == NULL) {
		fprintf(stderr, "empty config\n");
		istgt_free_config(config);
#ifndef	REPLICATION
		poolfini();
#endif
		exit(EXIT_FAILURE);
	}
	istgt->config = config;
	istgt->config_old = NULL;
	// istgt_print_config(config);


	/* open log files */
	if (logfacility == NULL) {
		logfacility = istgt_get_log_facility(config);
	}
	rc = istgt_set_log_facility(logfacility);
	if (rc < 0) {
		fprintf(stderr, "log facility error\n");
		istgt_free_config(config);
		exit(EXIT_FAILURE);
	}
	if (logpriority == NULL) {
		logpriority = DEFAULT_LOG_PRIORITY;
	}
	rc = istgt_set_log_priority(logpriority);
	if (rc < 0) {
		fprintf(stderr, "log priority error\n");
		istgt_free_config(config);
		exit(EXIT_FAILURE);
	}
	istgt_open_log();

	ISTGT_NOTICELOG("%s read config mode:%d %s", istgtvers,
			istgt->swmode,
#ifdef USE_ATOMIC
		"host-atomic"
#elif defined(USE_GCC_ATOMIC)
		"gcc-atomic"
#else
		"gen-atomic"
#endif /* USE_ATOMIC */
		);

#ifdef ISTGT_USE_CRC32C_TABLE
	/* build crc32c table */
	istgt_init_crc32c_table();
#endif /* ISTGT_USE_CRC32C_TABLE */

	/* initialize sub modules */
	rc = istgt_init(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_init() failed\n");
initialize_error:
		istgt_close_log();
		istgt_free_config(config);
#ifndef	REPLICATION
		poolfini();
#endif
		exit(EXIT_FAILURE);
	}

#ifdef	REPLICATION
	/* Initialize replication library */
	rc = initialize_replication();
	if (rc != 0) {
		ISTGT_ERRLOG("initialize_replication() failed\n");
		goto initialize_error;
	}

	rc = pthread_create(&replication_thread, &istgt->attr,
	    &init_replication, (void *)NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create(replication_thread) failed\n");
		goto initialize_error;
	}
#endif

	rc = istgt_lu_init(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_lu_init() failed\n");
		goto initialize_error;
	}
	rc = istgt_iscsi_init(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_iscsi_init() failed\n");
		goto initialize_error;
	}

	/* override by command line */
	if (pidfile != NULL) {
		xfree(istgt->pidfile);
		istgt->pidfile = xstrdup(pidfile);
	}

	/* detach from tty and run background */
	fflush(stdout);
	/*
	 * if (detach) {
	 *     istgt->daemon = 1;
	 *	  rc = daemon(0, 0);
	 *	  if (rc < 0) {
	 *	      ISTGT_ERRLOG("daemon() failed\n");
	 *		  goto initialize_error;
	 *	  }
	 * }
	 */

#if 0
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "setup signal handler\n");
	memset(&sigact, 0, sizeof (sigact));
	memset(&sigoldact_pipe, 0, sizeof (sigoldact_pipe));
	memset(&sigoldact_int, 0, sizeof (sigoldact_int));
	memset(&sigoldact_term, 0, sizeof (sigoldact_term));
	memset(&sigoldact_hup, 0, sizeof (sigoldact_hup));
	memset(&sigoldact_info, 0, sizeof (sigoldact_info));
	memset(&sigoldact_wakeup, 0, sizeof (sigoldact_wakeup));
	memset(&sigoldact_io, 0, sizeof (sigoldact_io));
	sigact.sa_handler = SIG_IGN;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGPIPE, &sigact, &sigoldact_pipe);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGPIPE) failed\n");
		goto initialize_error;
	}
	sigact.sa_handler = istgt_sigint;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGINT, &sigact, &sigoldact_int);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGINT) failed\n");
		goto initialize_error;
	}
	sigact.sa_handler = istgt_sigterm;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGTERM, &sigact, &sigoldact_term);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGTERM) failed\n");
		goto initialize_error;
	}
	sigact.sa_handler = istgt_sighup;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGHUP, &sigact, &sigoldact_hup);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGHUP) failed\n");
		goto initialize_error;
	}
#ifdef SIGINFO
	sigact.sa_handler = istgt_siginfo;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGINFO, &sigact, &sigoldact_info);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGINFO) failed\n");
		goto initialize_error;
	}
#endif
#ifdef ISTGT_USE_SIGRT
	if ((ISTGT_SIGWAKEUP < SIGRTMIN) || (ISTGT_SIGWAKEUP > SIGRTMAX)) {
		ISTGT_ERRLOG("SIGRT error\n");
		goto initialize_error;
	}
#endif /* ISTGT_USE_SIGRT */
	sigact.sa_handler = istgt_sigwakeup;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(ISTGT_SIGWAKEUP, &sigact, &sigoldact_wakeup);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(ISTGT_SIGWAKEUP) failed\n");
		goto initialize_error;
	}
#ifdef SIGIO
	sigact.sa_handler = istgt_sigio;
	sigemptyset(&sigact.sa_mask);
	rc = sigaction(SIGIO, &sigact, &sigoldact_io);
	if (rc < 0) {
		ISTGT_ERRLOG("sigaction(SIGIO) failed\n");
		goto initialize_error;
	}
#endif
	pthread_sigmask(SIG_SETMASK, NULL, &signew);
	sigaddset(&signew, SIGINT);
	sigaddset(&signew, SIGTERM);
	sigaddset(&signew, SIGQUIT);
	sigaddset(&signew, SIGHUP);
#ifdef SIGINFO
	sigaddset(&signew, SIGINFO);
#endif
	sigaddset(&signew, SIGUSR1);
	sigaddset(&signew, SIGUSR2);
#ifdef SIGIO
	sigaddset(&signew, SIGIO);
#endif
	sigaddset(&signew, ISTGT_SIGWAKEUP);
	pthread_sigmask(SIG_SETMASK, &signew, &sigold);
#ifdef ISTGT_STACKSIZE
	rc = pthread_create(&sigthread, &istgt->attr, &istgt_sighandler,
	    (void *) istgt);
#else
	rc = pthread_create(&sigthread, NULL, &istgt_sighandler,
	    (void *) istgt);
#endif
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed\n");
		goto initialize_error;
	}
#if 0
	rc = pthread_detach(sigthread);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_detach() failed\n");
		goto initialize_error;
	}
#endif

#endif

	/* create LUN threads for command queuing */
	istgt_queue_init(&closedconns);
	rc = pthread_create(&timerthread, &istgt->attr, &timerfn, (void *)NULL);
	if (rc != 0) {
	    ISTGT_ERRLOG("pthread_create(timerthread) failed\n");
	    goto initialize_error;
	}

	rc = istgt_lu_create_threads(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("lu_create_threads() failed\n");
		goto initialize_error;
	}
	rc = istgt_lu_set_all_state(istgt, ISTGT_STATE_RUNNING);
	if (rc < 0) {
		ISTGT_ERRLOG("lu_set_all_state() failed\n");
		goto initialize_error;
	}
	/* open portals */
	rc = istgt_open_all_portals(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_open_all_portals() failed\n");
		goto initialize_error;
	}
	rc = istgt_open_uctl_portal(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_open_uctl_portal() failed\n");
		goto initialize_error;
	}

	/* write pid */
	rc = istgt_write_pidfile(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_write_pid() failed\n");
		goto initialize_error;
	}

	/* accept loop */
	rc = istgt_acceptor(istgt);
	if (rc < 0) {
		ISTGT_ERRLOG("istgt_acceptor() failed:%d\n", rc);
		istgt_close_all_portals(istgt);
		istgt_close_uctl_portal(istgt);
		istgt_iscsi_shutdown(istgt);
		istgt_lu_shutdown(istgt);
		istgt_shutdown(istgt);
		istgt_close_log();
		config = istgt->config;
		istgt->config = NULL;
		istgt_free_config(config);
#ifndef	REPLICATION
		poolfini();
#endif
		exit(EXIT_FAILURE);
	}

	/* wait threads */
	istgt_stop_conns();
#if 0
	while (retry > 0) {
		if (istgt_get_active_conns() == 0) {
			break;
		}
		sleep(1);
		retry--;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "retry=%d\n", retry);
#endif

	ISTGT_NOTICELOG("%s exiting", istgtvers);

	/* stop signal thread */
#if 0
	rc = pthread_join(sigthread, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_join() failed\n");
#ifndef	REPLICATION
		poolfini();
#endif
		exit(EXIT_FAILURE);
	}
#endif

	/* cleanup */
	istgt_close_all_portals(istgt);
	istgt_close_uctl_portal(istgt);
	istgt_iscsi_shutdown(istgt);
	istgt_lu_shutdown(istgt);
	istgt_shutdown(istgt);
	istgt_close_log();

	config = istgt->config;
	istgt->config = NULL;
	istgt_free_config(config);
	istgt->state = ISTGT_STATE_SHUTDOWN;
#ifndef	REPLICATION
	poolfini();
#endif
	return (0);
}
