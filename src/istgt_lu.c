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

#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#ifdef HAVE_SYS_DISK_H
#include <sys/disk.h>
#endif
#ifdef HAVE_SYS_DISKLABEL_H
#include <sys/disklabel.h>
#endif
#ifdef __linux__
#include <linux/fs.h>
#define	pthread_set_name_np pthread_setname_np
#endif

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_conf.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_md5.h"
#include "istgt_iscsi.h"
#include "istgt_lu.h"
#include "istgt_proto.h"
#include "istgt_scsi.h"

#define	MAX_MASKBUF 128


static int
istgt_lu_allow_ipv6(const char *netmask, const char *addr)
{
	struct in6_addr in6_mask;
	struct in6_addr in6_addr;
	char mask[MAX_MASKBUF];
	const char *p;
	size_t n;
	int bits, bmask;
	int i;

	if (netmask[0] != '[')
		return (0);
	p = strchr(netmask, ']');
	if (p == NULL)
		return (0);
	n = p - (netmask + 1);
	if (n + 1 > sizeof (mask))
		return (0);

	memcpy(mask, netmask + 1, n);
	mask[n] = '\0';
	p++;

	if (p[0] == '/') {
		bits = (int) strtol(p + 1, NULL, 10);
		if (bits < 0 || bits > 128)
			return (0);
	} else {
		bits = 128;
	}

#if 0
	printf("input %s\n", addr);
	printf("mask  %s / %d\n", mask, bits);
#endif

	/* presentation to network order binary */
	if (inet_pton(AF_INET6, mask, &in6_mask) <= 0 ||
		inet_pton(AF_INET6, addr, &in6_addr) <= 0) {
		return (0);
	}

	/* check 128bits */
	for (i = 0; i < (bits / 8); i++) {
		if (in6_mask.s6_addr[i] != in6_addr.s6_addr[i])
			return (0);
	}
	if (bits % 8) {
		bmask = (0xffU << (8 - (bits % 8))) & 0xffU;
		if ((in6_mask.s6_addr[i] & bmask) != (in6_addr.s6_addr[i] & bmask))
			return (0);
	}

	/* match */
	return (1);
}

static int
istgt_lu_allow_ipv4(const char *netmask, const char *addr)
{
	struct in_addr in4_mask;
	struct in_addr in4_addr;
	char mask[MAX_MASKBUF];
	const char *p;
	uint32_t bmask;
	size_t n;
	int bits;

	p = strchr(netmask, '/');
	if (p == NULL) {
		p = netmask + strlen(netmask);
	}
	n = p - netmask;
	if (n + 1 > sizeof (mask))
		return (0);

	memcpy(mask, netmask, n);
	mask[n] = '\0';

	if (p[0] == '/') {
		bits = (int) strtol(p + 1, NULL, 10);
		if (bits < 0 || bits > 32)
			return (0);
	} else {
		bits = 32;
	}

#if 0
	printf("input %s\n", addr);
	printf("mask  %s / %d\n", mask, bits);
#endif

	/* presentation to network order binary */
	if (inet_pton(AF_INET, mask, &in4_mask) <= 0 ||
		inet_pton(AF_INET, addr, &in4_addr) <= 0) {
		return (0);
	}

	/* check 32bits */
	bmask = (0xffffffffU << (32 - bits)) & 0xffffffffU;
	if ((ntohl(in4_mask.s_addr) & bmask) != (ntohl(in4_addr.s_addr) & bmask))
		return (0);

	/* match */
	return (1);
}

int
istgt_lu_allow_netmask(const char *netmask, const char *addr)
{
	if (netmask == NULL || addr == NULL)
		return (0);
	if (strcasecmp(netmask, "ALL") == 0)
		return (1);
	if (netmask[0] == '[') {
		/* IPv6 */
		if (istgt_lu_allow_ipv6(netmask, addr))
			return (1);
	} else {
		/* IPv4 */
		if (istgt_lu_allow_ipv4(netmask, addr))
			return (1);
	}
	return (0);
}

/* Print the connection status to a logfile */
void
istgt_connection_status(CONN_Ptr conn, const char *status)
{
	FILE *fp;
	char *logfile = NULL;
	time_t current_time;
	char *c_time_string;
	char *time_val;

	/* Obtain current time as seconds elapsed since the Epoch. */
	current_time = time(NULL);

	/* Convert to local time format. */
	c_time_string = ctime(&current_time);
	time_val = strtok(c_time_string, "\n");
	MTX_LOCK(&conn->istgt->mutex);
	logfile = xstrdup(conn->istgt->logfile);
	MTX_UNLOCK(&conn->istgt->mutex);
	fp = fopen(logfile, "a");
	if (fp == NULL) {
		xfree(logfile);
		fprintf(stderr, "Cannot open the file %s \n", logfile);
		return;
	}
	fprintf(fp, "%s From %s To %s : %s\n", time_val, conn->initiator_name, conn->target_name, status);
	fclose(fp);
	xfree(logfile);
}

int
istgt_lu_access(CONN_Ptr conn, ISTGT_LU_Ptr lu, const char *iqn, const char *addr)
{
	ISTGT_Ptr istgt;
	INITIATOR_GROUP *igp;
	int pg_tag;
	int ig_tag;
	int rc;
	int i, j, k;

	if (conn == NULL || lu == NULL || iqn == NULL || addr == NULL)
		return (0);
	istgt = conn->istgt;
	pg_tag = conn->portal.tag;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "pg=%d, iqn=%s, addr=%s\n",
				    pg_tag, iqn, addr);
	for (i = 0; i < lu->maxmap; i++) {
		/* skip excluding self portal group tag */
		if (pg_tag != lu->map[i].pg_tag)
			continue;
		/* iqn is initiator group? */
		ig_tag = lu->map[i].ig_tag;
		igp = istgt_lu_find_initiatorgroup(istgt, ig_tag);
		if (igp == NULL) {
			ISTGT_ERRLOG("LU%d: ig_tag not found\n", lu->num);
			continue;
		}
		for (j = 0; j < igp->ninitiators; j++) {
			/* deny initiators */
			if (igp->initiators[j][0] == '!' &&
				(strcasecmp(&igp->initiators[j][1], "ALL") == 0 ||
					strcasecmp(&igp->initiators[j][1], iqn) == 0)) {
				/* NG */
				MTX_UNLOCK(&conn->istgt->mutex);
				istgt_connection_status(conn, "ACCESS DENIED");
				MTX_LOCK(&conn->istgt->mutex);
				ISTGT_WARNLOG("access denied from %s (%s) to %s (%s:%s,%d)\n",
				    iqn, addr, conn->target_name, conn->portal.host,
				    conn->portal.port, conn->portal.tag);
				return (0);
			}
			/* allow initiators */
			if (strcasecmp(igp->initiators[j], "ALL") == 0 ||
				strcasecmp(igp->initiators[j], iqn) == 0) {
				/* OK iqn, check netmask */
				if (igp->nnetmasks == 0) {
					/* OK, empty netmask as ALL */
					return (1);
				}
				for (k = 0; k < igp->nnetmasks; k++) {
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					    "netmask=%s, addr=%s\n",
					    igp->netmasks[k], addr);
					rc = istgt_lu_allow_netmask(igp->netmasks[k], addr);
					if (rc > 0) {
						/* OK netmask */
						return (1);
					}
				}
				/* NG netmask in this group */
			}
		}
	}

	/* NG */
	MTX_UNLOCK(&conn->istgt->mutex);
	istgt_connection_status(conn, "ACCESS DENIED");
	MTX_LOCK(&conn->istgt->mutex);
	ISTGT_WARNLOG("access denied from %s (%s) to %s (%s:%s,%d)\n",
	    iqn, addr, conn->target_name, conn->portal.host,
	    conn->portal.port, conn->portal.tag);
	return (0);
}

int
istgt_lu_visible(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu, const char *iqn, int pg_tag)
{
	INITIATOR_GROUP *igp;
	int match_pg_tag;
	int ig_tag;
	int i, j;

	if (istgt == NULL || lu == NULL || iqn == NULL)
		return (0);
	/* pg_tag exist map? */
	match_pg_tag = 0;
	for (i = 0; i < lu->maxmap; i++) {
		if (lu->map[i].pg_tag == pg_tag) {
			match_pg_tag = 1;
			break;
		}
	}
	if (match_pg_tag == 0) {
		/* cat't access from pg_tag */
		return (0);
	}
	for (i = 0; i < lu->maxmap; i++) {
		/* iqn is initiator group? */
		ig_tag = lu->map[i].ig_tag;
		igp = istgt_lu_find_initiatorgroup(istgt, ig_tag);
		if (igp == NULL) {
			ISTGT_ERRLOG("LU%d: ig_tag not found\n", lu->num);
			continue;
		}
		for (j = 0; j < igp->ninitiators; j++) {
			if (igp->initiators[j][0] == '!' &&
				(strcasecmp(&igp->initiators[j][1], "ALL") == 0 ||
					strcasecmp(&igp->initiators[j][1], iqn) == 0)) {
				/* NG */
				return (0);
			}
			if (strcasecmp(igp->initiators[j], "ALL") == 0 ||
				strcasecmp(igp->initiators[j], iqn) == 0) {
				/* OK iqn, no check addr */
				return (1);
			}
		}
	}

	/* NG */
	return (0);
}

static int
istgt_pg_visible(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu, const char *iqn, int pg_tag)
{
	INITIATOR_GROUP *igp;
	int match_idx;
	int ig_tag;
	int i, j;

	if (istgt == NULL || lu == NULL || iqn == NULL)
		return (0);
	match_idx = -1;
	for (i = 0; i < lu->maxmap; i++) {
		if (lu->map[i].pg_tag == pg_tag) {
			match_idx = i;
			break;
		}
	}
	if (match_idx < 0) {
		/* cant't find pg_tag */
		return (0);
	}

	/* iqn is initiator group? */
	ig_tag = lu->map[match_idx].ig_tag;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "iqn=%s, pg=%d, ig=%d\n", iqn, pg_tag, ig_tag);
	igp = istgt_lu_find_initiatorgroup(istgt, ig_tag);
	if (igp == NULL) {
		ISTGT_ERRLOG("LU%d: ig_tag not found\n", lu->num);
		return (0);
	}
	for (j = 0; j < igp->ninitiators; j++) {
		if (igp->initiators[j][0] == '!' &&
			(strcasecmp(&igp->initiators[j][1], "ALL") == 0 ||
				strcasecmp(&igp->initiators[j][1], iqn) == 0)) {
			/* NG */
			return (0);
		}
		if (strcasecmp(igp->initiators[j], "ALL") == 0 ||
			strcasecmp(igp->initiators[j], iqn) == 0) {
			/* OK iqn, no check addr */
			return (1);
		}
	}

	/* NG */
	return (0);
}

int
istgt_lu_sendtargets(CONN_Ptr conn, const char *iiqn, const char *iaddr, const char *tiqn, uint8_t *data, int alloc_len, int data_len)
{
	char buf[MAX_TMPBUF];
	ISTGT_Ptr istgt;
	ISTGT_LU_Ptr lu;
	char *host;
	int total;
	int len;
	int rc;
	int pg_tag;
	int i, j, k, l;

	if (conn == NULL)
		return (0);
	istgt = conn->istgt;

	total = data_len;
	if (alloc_len < 1) {
		return (0);
	}
	if (total > alloc_len) {
		total = alloc_len;
		data[total - 1] = '\0';
		return (total);
	}

	if (alloc_len - total < 1) {
		ISTGT_ERRLOG("data space small %d\n", alloc_len);
		return (total);
	}

	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (strcasecmp(tiqn, "ALL") != 0 &&
			strcasecmp(tiqn, lu->name) != 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "SKIP iqn=%s for %s from %s (%s)\n",
			    tiqn, lu->name, iiqn, iaddr);
			continue;
		}
		rc = istgt_lu_visible(istgt, lu, iiqn, conn->portal.tag);
		if (rc == 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "SKIP iqn=%s for %s from %s (%s)\n",
			    tiqn, lu->name, iiqn, iaddr);
			continue;
		}

		/* DO SENDTARGETS */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "OK iqn=%s for %s from %s (%s)\n",
		    tiqn, lu->name, iiqn, iaddr);

		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "TargetName=%s\n", lu->name);
		len = snprintf((char *) data + total, alloc_len - total,
		    "TargetName=%s", lu->name);
		total += len + 1;

		for (j = 0; j < lu->maxmap; j++) {
			pg_tag = lu->map[j].pg_tag;
			/* skip same pg_tag */
			for (k = 0; k < j; k++) {
				if (lu->map[k].pg_tag == pg_tag) {
					goto skip_pg_tag;
				}
			}
			rc = istgt_pg_visible(istgt, lu, iiqn, pg_tag);
			if (rc == 0) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				    "SKIP pg=%d, iqn=%s for %s from %s (%s)\n",
				    pg_tag, tiqn, lu->name, iiqn, iaddr);
				goto skip_pg_tag;
			}

			/* write to data */
			for (k = 0; k < istgt->nportal_group; k++) {
				if (istgt->portal_group[k].tag != pg_tag)
					continue;
				for (l = 0; l < istgt->portal_group[k].nportals; l++) {
					if (alloc_len - total < 1) {
						MTX_UNLOCK(&istgt->mutex);
						ISTGT_ERRLOG("data space small %d\n",
						    alloc_len);
						return (total);
					}
					host = istgt->portal_group[k].portals[l]->host;
					/* wildcard? */
					if (strcasecmp(host, "[::]") == 0 ||
						strcasecmp(host, "[*]") == 0 ||
						strcasecmp(host, "0.0.0.0") == 0 ||
						strcasecmp(host, "*") == 0) {
						if ((strcasecmp(host, "[::]") == 0 ||
							strcasecmp(host, "[*]") == 0) &&
							conn->initiator_family == AF_INET6) {
							snprintf(buf, sizeof (buf), "[%s]",
							    conn->target_addr);
							host = buf;
						} else if ((strcasecmp(host, "0.0.0.0") == 0 ||
										strcasecmp(host, "*") == 0) &&
									conn->initiator_family == AF_INET) {
							snprintf(buf, sizeof (buf), "%s",
							    conn->target_addr);
							host = buf;
						} else {
							/* skip portal for the family */
							continue;
						}
					}
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					    "TargetAddress=%s:%s,%d\n",
					    host,
					    istgt->portal_group[k].portals[l]->port,
					    istgt->portal_group[k].portals[l]->tag);
					len = snprintf((char *) data + total,
					    alloc_len - total,
					    "TargetAddress=%s:%s,%d",
					    host,
					    istgt->portal_group[k].portals[l]->port,
					    istgt->portal_group[k].portals[l]->tag);
					total += len + 1;
				}
			}
		skip_pg_tag:
			;
		}
	}
	MTX_UNLOCK(&istgt->mutex);

	return (total);
}

ISTGT_LU_Ptr
istgt_lu_find_target_by_volname(ISTGT_Ptr istgt, const char *volname)
{
	ISTGT_LU_Ptr lu;
	int i;

	if (istgt == NULL || volname == NULL)
		return (NULL);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (strcasecmp(volname, lu->volname) == 0) {
			return (lu);
		}
	}
	ISTGT_WARNLOG("can't find target %s\n",
	    volname);
	return (NULL);
}

ISTGT_LU_Ptr
istgt_lu_find_target(ISTGT_Ptr istgt, const char *target_name)
{
	ISTGT_LU_Ptr lu;
	int i;

	if (istgt == NULL || target_name == NULL)
		return (NULL);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		if (strcasecmp(target_name, lu->name) == 0) {
			return (lu);
		}
	}
	ISTGT_WARNLOG("can't find target %s\n",
	    target_name);
	return (NULL);
}

int
istgt_lu_add_nexus(ISTGT_LU_Ptr lu, char *initiator_port)
{
	int lun = 0, rc = 0;

	if (lu == NULL ||initiator_port == NULL)
		return (-1);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_add_nexus LU%d Initiator %s\n", lu->num, initiator_port);
	/* Add the nexus for each Disk */
	for (lun = 0; lun < lu->maxlun; lun++) {
		if (lu->type == ISTGT_LU_TYPE_DISK)
			rc = istgt_lu_disk_add_nexus(lu, lun,  initiator_port);
	}
	return (rc);
}

int
istgt_lu_remove_nexus(ISTGT_LU_Ptr lu, char *initiator_port)
{
	int lun = 0, rc = 0;

	if (lu == NULL || initiator_port == NULL)
		return (-1);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_remove_nexus LU%d Initiator %s \n", lu->num, initiator_port);
	for (lun = 0; lun < lu->maxlun; lun++) {
		if (lu->type == ISTGT_LU_TYPE_DISK)
			rc = istgt_lu_disk_remove_nexus(lu, lun, initiator_port);
		}

	return (rc);
}

uint16_t
istgt_lu_allocate_tsih(ISTGT_LU_Ptr lu, const char *initiator_port, int tag)
{
	uint16_t tsih;
	int retry = 10;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_allocate_tsih\n");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "initiator_port=%s, tag=%d\n",
	    initiator_port, tag);
	if (lu == NULL || initiator_port == NULL || tag == 0)
		return (0);
	/* tsih 0 is reserved */
	tsih = 0;
#if 0
	for (i = 1; i < MAX_LU_TSIH; i++) {
		if (lu->tsih[i].initiator_port == NULL)
			continue;
		if (tag != lu->tsih[i].tag)
			continue;
		if (strcasecmp(initiator_port, lu->tsih[i].initiator_port) == 0) {
			tsih = lu->tsih[i].tsih;
			break;
		}
	}
#endif
	if (tsih == 0) {
		if (lu->maxtsih >= MAX_LU_TSIH) {
			ISTGT_ERRLOG("LU%d: tsih is maximum\n", lu->num);
			return (0);
		}
	retry:
		lu->last_tsih++;
		tsih = lu->last_tsih;
		if (tsih == 0) {
			if (retry > 0) {
				retry--;
				goto retry;
			}
			ISTGT_ERRLOG("LU%d: retry error\n", lu->num);
			return (0);
		}
		for (i = 1; i < MAX_LU_TSIH; i++) {
			if (lu->tsih[i].initiator_port != NULL &&
				lu->tsih[i].tsih == tsih) {
				ISTGT_ERRLOG("tsih is found in list\n");
				if (retry > 0) {
					retry--;
					goto retry;
				}
				ISTGT_ERRLOG("LU%d: retry error\n", lu->num);
				return (0);
			}
		}
		for (i = 1; i < MAX_LU_TSIH; i++) {
			if (lu->tsih[i].initiator_port == NULL) {
				lu->tsih[i].tag = tag;
				lu->tsih[i].tsih = tsih;
				lu->tsih[i].initiator_port = xstrdup(initiator_port);
				lu->maxtsih++;
				break;
			}
		}
	}
	return (tsih);
}

void
istgt_lu_free_tsih(ISTGT_LU_Ptr lu, uint16_t tsih, char *initiator_port)
{
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_free_tsih\n");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "tsih=%u, initiator_port=%s\n",
	    tsih, initiator_port);
	if (lu == NULL || initiator_port == NULL)
		return;
	if (tsih == 0)
		return;

	MTX_LOCK(&lu->mutex);
	for (i = 1; i < MAX_LU_TSIH; i++) {
		if (lu->tsih[i].initiator_port == NULL)
			continue;
		if (lu->tsih[i].tsih != tsih)
			continue;

		if (strcasecmp(initiator_port, lu->tsih[i].initiator_port) == 0) {
			lu->tsih[i].tag = 0;
			lu->tsih[i].tsih = 0;
			xfree(lu->tsih[i].initiator_port);
			lu->tsih[i].initiator_port = NULL;
			lu->maxtsih--;
			break;
		}
	}
	MTX_UNLOCK(&lu->mutex);
}

char *
istgt_lu_get_media_flags_string(int flags, char *buf, size_t len)
{
	char *p;
	size_t rest;

	p = buf;
	rest = len;
	if (flags & ISTGT_LU_FLAG_MEDIA_READONLY) {
		snprintf(p, rest, "%s", "ro");
	} else {
		snprintf(p, rest, "%s", "rw");
	}
	p = buf + strlen(buf);
	rest = len - strlen(buf);
	if (flags & ISTGT_LU_FLAG_MEDIA_EXTEND) {
		snprintf(p, rest, ",%s", "extend");
	}
	p = buf + strlen(buf);
	rest = len - strlen(buf);
	if (flags & ISTGT_LU_FLAG_MEDIA_DYNAMIC) {
		snprintf(p, rest, ",%s", "dynamic");
	}
	return (buf);
}

uint64_t
istgt_lu_get_devsize(const char *file)
{
	uint64_t val;
	struct stat st;
	int fd;
	int rc;

	val = 0ULL;
#ifdef ALLOW_SYMLINK_DEVICE
	rc = stat(file, &st);
#else
	rc = lstat(file, &st);
#endif /* ALLOW_SYMLINK_DEVICE */
	if (rc != 0)
		return (val);
	if (!S_ISCHR(st.st_mode) && !S_ISBLK(st.st_mode))
		return (val);

	fd = open(file, O_RDONLY, 0);
	if (fd >= 0) {
#ifdef DIOCGMEDIASIZE
		if (val == 0) {
			off_t offset;
			rc = ioctl(fd, DIOCGMEDIASIZE, &offset);
			if (rc != -1) {
				val = (uint64_t) offset;
			}
		}
#endif /* DIOCGMEDIASIZE */
#ifdef DIOCGDINFO
		if (val == 0) {
			struct disklabel dl;
			rc = ioctl(fd, DIOCGDINFO, &dl);
			if (rc != -1) {
				val = (uint64_t) dl.d_secperunit;
				val *= (uint64_t) dl.d_secsize;
			}
		}
#endif /* DIOCGDINFO */
#if defined(DKIOCGETBLOCKSIZE) && defined(DKIOCGETBLOCKCOUNT)
		if (val == 0) {
			uint32_t blocklen;
			uint64_t blockcnt;
			rc = ioctl(fd, DKIOCGETBLOCKSIZE, &blocklen);
			if (rc != -1) {
				rc = ioctl(fd, DKIOCGETBLOCKCOUNT, &blockcnt);
				if (rc != -1) {
					val = (uint64_t) blocklen;
					val *= (uint64_t) blockcnt;
				}
			}
		}
#endif /* DKIOCGETBLOCKSIZE && DKIOCGETBLOCKCOUNT */
#ifdef __linux__
#ifdef BLKGETSIZE64
		if (val == 0) {
			uint64_t blocksize;
			rc = ioctl(fd, BLKGETSIZE64, &blocksize);
			if (rc != -1) {
				val = (uint64_t) blocksize;
			}
		}
#endif /* BLKGETSIZE64 */
#ifdef BLKGETSIZE
		if (val == 0) {
			uint32_t blocksize;
			rc = ioctl(fd, BLKGETSIZE, &blocksize);
			if (rc != -1) {
				val = (uint64_t) 512;
				val *= (uint64_t) blocksize;
			}
		}
#endif /* BLKGETSIZE */
#endif /* __linux__ */
		if (val == 0) {
			ISTGT_ERRLOG("unknown device size\n");
		}
		(void) close(fd);
	} else {
		if (g_trace_flag) {
			ISTGT_WARNLOG("open error %s (errno=%d)\n", file, errno);
		}
		val = 0ULL;
	}
	return (val);
}



uint64_t
istgt_lu_get_filesize(const char *file)
{
	uint64_t val;
	struct stat st;
	int rc;

	val = 0ULL;
#ifdef ALLOW_SYMLINK_DEVICE
	rc = stat(file, &st);
#else
	rc = lstat(file, &st);
#endif /* ALLOW_SYMLINK_DEVICE */

	if (rc < 0)
		return (val);
#ifndef ALLOW_SYMLINK_DEVICE
	if (S_ISLNK(st.st_mode))
		return (val);
#endif /* ALLOW_SYMLINK_DEVICE */
	if (S_ISCHR(st.st_mode)) {
		val = istgt_lu_get_devsize(file);
	} else if (S_ISBLK(st.st_mode)) {
		val = istgt_lu_get_devsize(file);
	} else if (S_ISREG(st.st_mode)) {
		val = st.st_size;
	} else {
#ifdef ALLOW_SYMLINK_DEVICE
		ISTGT_ERRLOG("stat is neither REG, CHR nor BLK\n");
#else
		ISTGT_ERRLOG("lstat is neither REG, CHR nor BLK\n");
#endif /* ALLOW_SYMLINK_DEVICE */
		val = 0ULL;
	}
	return (val);
}


uint64_t
istgt_lu_parse_size(const char *size)
{
	uint64_t val, val1, val2;
	char *endp, *p;
	size_t idx;
	int sign;

	val1 = (uint64_t) strtoull(size, &endp, 10);
	val = val1;
	val2 = 0;
	if (endp != NULL) {
		p = endp;
		switch (toupper((int) *p)) {
		case 'Z': val1 *= (uint64_t) 1024ULL;
		case 'E': val1 *= (uint64_t) 1024ULL;
		case 'P': val1 *= (uint64_t) 1024ULL;
		case 'T': val1 *= (uint64_t) 1024ULL;
		case 'G': val1 *= (uint64_t) 1024ULL;
		case 'M': val1 *= (uint64_t) 1024ULL;
		case 'K': val1 *= (uint64_t) 1024ULL;
			break;
		}
		val = val1;
		p++;
		idx = strspn(p, "Bb \t");
		p += idx;
		if (*p == '-' || *p == '+') {
			sign = (int) *p++;
			idx = strspn(p, " \t");
			p += idx;
			val2 = (uint64_t) strtoull(p, &endp, 10);
			if (endp != NULL) {
				p = endp;
				switch (toupper((int) *p)) {
				case 'Z': val2 *= (uint64_t) 1024ULL;
				case 'E': val2 *= (uint64_t) 1024ULL;
				case 'P': val2 *= (uint64_t) 1024ULL;
				case 'T': val2 *= (uint64_t) 1024ULL;
				case 'G': val2 *= (uint64_t) 1024ULL;
				case 'M': val2 *= (uint64_t) 1024ULL;
				case 'K': val2 *= (uint64_t) 1024ULL;
					break;
				}
			}
			if (sign == '-') {
				if (val2 > val1) {
					/* underflow */
					val = (uint64_t) 0ULL;
				} else {
					val = val1 - val2;
				}
			} else {
				if (val2 > (UINT64_MAX - val1)) {
					/* overflow */
					val = UINT64_MAX;
				} else {
					val = val1 + val2;
				}
			}
		}
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "size=%s, val=%"PRIu64", val1=%"PRIu64", val2=%"PRIu64"\n",
	    size, val, val1, val2);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "size=%s, val=%"PRIx64", val1=%"PRIx64", val2=%"PRIx64"\n",
	    size, val, val1, val2);

	return (val);
}

int
istgt_lu_parse_media_flags(const char *flags)
{
	char buf[MAX_TMPBUF];
	const char *delim = ",";
	char *next_p;
	char *p;
	int mflags;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "mflags=%s\n", flags);
	mflags = 0;
	strlcpy(buf, flags, MAX_TMPBUF);
	next_p = buf;
	while ((p = strsep(&next_p, delim)) != NULL) {
		if (strcasecmp(p, "ro") == 0) {
			mflags |= ISTGT_LU_FLAG_MEDIA_READONLY;
		} else if (strcasecmp(p, "rw") == 0) {
			mflags &= ~ISTGT_LU_FLAG_MEDIA_READONLY;
		} else if (strcasecmp(p, "extend") == 0) {
			mflags |= ISTGT_LU_FLAG_MEDIA_EXTEND;
		} else if (strcasecmp(p, "dynamic") == 0) {
			mflags |= ISTGT_LU_FLAG_MEDIA_DYNAMIC;
		} else {
			ISTGT_ERRLOG("unknown media flag %.64s\n", p);
		}
	}

	return (mflags);
}

uint64_t
istgt_lu_parse_media_size(const char *file, const char *size, int *flags)
{
	uint64_t msize, fsize;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "msize=%s\n", size);
	if (strcasecmp(file, "/dev/null") == 0) {
		return (0);
	}
	if (strcasecmp(size, "Auto") == 0 ||
		strcasecmp(size, "Size") == 0) {
		msize = istgt_lu_get_filesize(file);
		if (msize == 0) {
			msize = ISTGT_LU_MEDIA_SIZE_MIN;
		}
		*flags |= ISTGT_LU_FLAG_MEDIA_AUTOSIZE;
	} else {
		msize = istgt_lu_parse_size(size);
		if (*flags & ISTGT_LU_FLAG_MEDIA_EXTEND) {
			fsize = istgt_lu_get_filesize(file);
			if (fsize > msize) {
				msize = fsize;
			}
		}
	}

	if (*flags & ISTGT_LU_FLAG_MEDIA_DYNAMIC) {
		if (msize < ISTGT_LU_MEDIA_SIZE_MIN) {
			msize = ISTGT_LU_MEDIA_SIZE_MIN;
		}
	} else {
		if (msize < ISTGT_LU_MEDIA_SIZE_MIN) {
			ISTGT_ERRLOG("media size too small\n");
			return (0ULL);
		}
	}

	return (msize);
}

PORTAL_GROUP *
istgt_lu_find_portalgroup(ISTGT_Ptr istgt, int tag)
{
	PORTAL_GROUP *pgp;
	int i;

	for (i = 0; i < istgt->nportal_group; i++) {
		if (istgt->portal_group[i].tag == tag) {
			pgp = &istgt->portal_group[i];
			return (pgp);
		}
	}
	return (NULL);
}

INITIATOR_GROUP *
istgt_lu_find_initiatorgroup(ISTGT_Ptr istgt, int tag)
{
	INITIATOR_GROUP *igp;
	int i;

	for (i = 0; i < istgt->ninitiator_group; i++) {
		if (istgt->initiator_group[i].tag == tag) {
			igp = &istgt->initiator_group[i];
			return (igp);
		}
	}
	return (NULL);
}

static int
istgt_lu_check_iscsi_name(const char *name)
{
	const unsigned char *up = (const unsigned char *) name;
	size_t n;

	/* valid iSCSI name? */
	for (n = 0; up[n] != 0; n++) {
		if (up[n] > 0x00U && up[n] <= 0x2cU)
			return (-1);
		if (up[n] == 0x2fU)
			return (-1);
		if (up[n] >= 0x3bU && up[n] <= 0x40U)
			return (-1);
		if (up[n] >= 0x5bU && up[n] <= 0x60U)
			return (-1);
		if (up[n] >= 0x7bU && up[n] <= 0x7fU)
			return (-1);
		if (isspace(up[n]))
			return (-1);
	}
	/* valid format? */
	if (strncasecmp(name, "iqn.", 4) == 0) {
		/* iqn.YYYY-MM.reversed.domain.name */
		if (!isdigit(up[4]) || !isdigit(up[5]) || !isdigit(up[6]) ||
			!isdigit(up[7]) || up[8] != '-' || !isdigit(up[9]) ||
			!isdigit(up[10]) || up[11] != '.') {
			ISTGT_ERRLOG("invalid iqn format. "
			    "expect \"iqn.YYYY-MM.reversed.domain.name\"\n");
			return (-1);
		}
	} else if (strncasecmp(name, "eui.", 4) == 0) {
		/* EUI-64 -> 16bytes */
		/* XXX */
	} else if (strncasecmp(name, "naa.", 4) == 0) {
		/* 64bit -> 16bytes, 128bit -> 32bytes */
		/* XXX */
	}
	/* OK */
	return (0);
}

#if 0
static uint64_t
istgt_lu_get_nbserial(const char *nodebase)
{
	ISTGT_MD5CTX md5ctx;
	uint8_t nbsmd5[ISTGT_MD5DIGEST_LEN];
	char buf[MAX_TMPBUF];
	uint64_t nbs;
	int idx;
	int i;

	snprintf(buf, sizeof (buf), "%s", nodebase);
	if (strcasecmp(buf, "iqn.2007-09.jp.ne.peach.istgt") == 0 ||
		strcasecmp(buf, "iqn.2007-09.jp.ne.peach") == 0) {
		/* always zero */
		return (0);
	}

	istgt_md5init(&md5ctx);
	istgt_md5update(&md5ctx, buf, strlen(buf));
	istgt_md5final(nbsmd5, &md5ctx);

	nbs = 0U;
	idx = ISTGT_MD5DIGEST_LEN - 8;
	if (idx < 0) {
		ISTGT_WARNLOG("missing MD5 length\n");
		idx = 0;
	}
	for (i = idx; i < ISTGT_MD5DIGEST_LEN; i++) {
		nbs |= (uint64_t) nbsmd5[i];
		nbs = nbs << 8;
	}
	return (nbs);
}
#endif

static int
istgt_lu_set_local_settings(ISTGT_Ptr istgt, CF_SECTION *sp, ISTGT_LU_Ptr lu)
{
	const char *val;

	val = istgt_get_val(sp, "MaxOutstandingR2T");
	if (val == NULL) {
		lu->MaxOutstandingR2T = lu->istgt->MaxOutstandingR2T;
	} else {
		lu->MaxOutstandingR2T = (int)strtol(val, NULL, 10);
		if (lu->MaxOutstandingR2T < 1) {
			lu->MaxOutstandingR2T = DEFAULT_MAXOUTSTANDINGR2T;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxOutstandingR2T %d\n",
		    lu->MaxOutstandingR2T);
	}

	val = istgt_get_val(sp, "DefaultTime2Wait");
	if (val == NULL) {
		lu->DefaultTime2Wait = lu->istgt->DefaultTime2Wait;
	} else {
		lu->DefaultTime2Wait = (int)strtol(val, NULL, 10);
		if (lu->DefaultTime2Wait < 0) {
			lu->DefaultTime2Wait = DEFAULT_DEFAULTTIME2WAIT;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DefaultTime2Wait %d\n",
		    lu->DefaultTime2Wait);
	}

	val = istgt_get_val(sp, "DefaultTime2Retain");
	if (val == NULL) {
		lu->DefaultTime2Retain = lu->istgt->DefaultTime2Retain;
	} else {
		lu->DefaultTime2Retain = (int)strtol(val, NULL, 10);
		if (lu->DefaultTime2Retain < 0) {
			lu->DefaultTime2Retain = DEFAULT_DEFAULTTIME2RETAIN;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DefaultTime2Retain %d\n",
		    lu->DefaultTime2Retain);
	}

	/* check size limit - RFC3720(12.15, 12.16, 12.17) */
	if (lu->MaxOutstandingR2T > 65535) {
		ISTGT_ERRLOG("MaxOutstandingR2T(%d) > 65535\n",
		    lu->MaxOutstandingR2T);
		return (-1);
	}
	if (lu->DefaultTime2Wait > 3600) {
		ISTGT_ERRLOG("DefaultTime2Wait(%d) > 3600\n",
		    lu->DefaultTime2Wait);
		return (-1);
	}
	if (lu->DefaultTime2Retain > 3600) {
		ISTGT_ERRLOG("DefaultTime2Retain(%d) > 3600\n",
		    lu->DefaultTime2Retain);
		return (-1);
	}

	val = istgt_get_val(sp, "FirstBurstLength");
	if (val == NULL) {
		lu->FirstBurstLength = lu->istgt->FirstBurstLength;
	} else {
		lu->FirstBurstLength = (int)strtol(val, NULL, 10);
		if (lu->FirstBurstLength < 0) {
			lu->FirstBurstLength = DEFAULT_FIRSTBURSTLENGTH;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "FirstBurstLength %d\n",
		    lu->FirstBurstLength);
	}

	val = istgt_get_val(sp, "MaxBurstLength");
	if (val == NULL) {
		lu->MaxBurstLength = lu->istgt->MaxBurstLength;
	} else {
		lu->MaxBurstLength = (int)strtol(val, NULL, 10);
		if (lu->MaxBurstLength < 0) {
			lu->MaxBurstLength = DEFAULT_MAXBURSTLENGTH;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "MaxBurstLength %d\n",
		    lu->MaxBurstLength);
	}

	val = istgt_get_val(sp, "MaxRecvDataSegmentLength");
	if (val == NULL) {
		lu->MaxRecvDataSegmentLength
			= lu->istgt->MaxRecvDataSegmentLength;
	} else {
		lu->MaxRecvDataSegmentLength = (int)strtol(val, NULL, 10);
		if (lu->MaxRecvDataSegmentLength < 0) {
			lu->MaxRecvDataSegmentLength
				= DEFAULT_MAXRECVDATASEGMENTLENGTH;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "MaxRecvDataSegmentLength %d\n",
		    lu->MaxRecvDataSegmentLength);
	}

	/* check size limit (up to 24bits - RFC3720(12.12)) */
	if (lu->MaxBurstLength < 512) {
		ISTGT_ERRLOG("MaxBurstLength(%d) < 512\n",
		    lu->MaxBurstLength);
		return (-1);
	}
	if (lu->FirstBurstLength < 512) {
		ISTGT_ERRLOG("FirstBurstLength(%d) < 512\n",
		    lu->FirstBurstLength);
		return (-1);
	}
	if (lu->FirstBurstLength > lu->MaxBurstLength) {
		ISTGT_ERRLOG("FirstBurstLength(%d) > MaxBurstLength(%d)\n",
		    lu->FirstBurstLength, istgt->MaxBurstLength);
		return (-1);
	}
	if (lu->MaxBurstLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxBurstLength(%d) > 0x00ffffff\n",
		    lu->MaxBurstLength);
		return (-1);
	}
	if (lu->MaxRecvDataSegmentLength < 512) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) < 512\n",
		    lu->MaxRecvDataSegmentLength);
		return (-1);
	}
	if (lu->MaxRecvDataSegmentLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) > 0x00ffffff\n",
		    lu->MaxRecvDataSegmentLength);
		return (-1);
	}

	val = istgt_get_val(sp, "InitialR2T");
	if (val == NULL) {
		lu->InitialR2T = lu->istgt->InitialR2T;
	} else {
		if (strcasecmp(val, "Yes") == 0) {
			lu->InitialR2T = 1;
		} else if (strcasecmp(val, "No") == 0) {
#if 0
			lu->InitialR2T = 0;
#else
			ISTGT_ERRLOG("not supported value %s\n", val);
			return (-1);
#endif
		} else {
			ISTGT_ERRLOG("unknown value %s\n", val);
			return (-1);
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "InitialR2T %s\n",
		    lu->InitialR2T ? "Yes" : "No");
	}

	val = istgt_get_val(sp, "ImmediateData");
	if (val == NULL) {
		lu->ImmediateData = lu->istgt->ImmediateData;
	} else {
		if (strcasecmp(val, "Yes") == 0) {
			lu->ImmediateData = 1;
		} else if (strcasecmp(val, "No") == 0) {
			lu->ImmediateData = 0;
		} else {
			ISTGT_ERRLOG("unknown value %s\n", val);
			return (-1);
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ImmediateData %s\n",
		    lu->ImmediateData ? "Yes" : "No");
	}

	val = istgt_get_val(sp, "DataPDUInOrder");
	if (val == NULL) {
		lu->DataPDUInOrder = lu->istgt->DataPDUInOrder;
	} else {
		if (strcasecmp(val, "Yes") == 0) {
			lu->DataPDUInOrder = 1;
		} else if (strcasecmp(val, "No") == 0) {
#if 0
			lu->DataPDUInOrder = 0;
#else
			ISTGT_ERRLOG("not supported value %s\n", val);
			return (-1);
#endif
		} else {
			ISTGT_ERRLOG("unknown value %s\n", val);
			return (-1);
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DataPDUInOrder %s\n",
		    lu->DataPDUInOrder ? "Yes" : "No");
	}

	val = istgt_get_val(sp, "DataSequenceInOrder");
	if (val == NULL) {
		lu->DataSequenceInOrder = lu->istgt->DataSequenceInOrder;
	} else {
		if (strcasecmp(val, "Yes") == 0) {
			lu->DataSequenceInOrder = 1;
		} else if (strcasecmp(val, "No") == 0) {
#if 0
			lu->DataSequenceInOrder = 0;
#else
			ISTGT_ERRLOG("not supported value %s\n", val);
			return (-1);
#endif
		} else {
			ISTGT_ERRLOG("unknown value %s\n", val);
			return (-1);
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "DataSequenceInOrder %s\n",
		    lu->DataSequenceInOrder ? "Yes" : "No");
	}

	val = istgt_get_val(sp, "ErrorRecoveryLevel");
	if (val == NULL) {
		lu->ErrorRecoveryLevel = lu->istgt->ErrorRecoveryLevel;
	} else {
		lu->ErrorRecoveryLevel = (int)strtol(val, NULL, 10);
		if (lu->ErrorRecoveryLevel < 0) {
			lu->ErrorRecoveryLevel = DEFAULT_ERRORRECOVERYLEVEL;
		} else if (lu->ErrorRecoveryLevel == 0) {
			lu->ErrorRecoveryLevel = 0;
		} else if (lu->ErrorRecoveryLevel == 1) {
#if 0
			lu->ErrorRecoveryLevel = 1;
#else
			ISTGT_ERRLOG("not supported value %d\n",
			    lu->ErrorRecoveryLevel);
			return (-1);
#endif
		} else if (lu->ErrorRecoveryLevel == 2) {
#if 0
			lu->ErrorRecoveryLevel = 2;
#else
			ISTGT_ERRLOG("not supported value %d\n",
			    lu->ErrorRecoveryLevel);
			return (-1);
#endif
		} else {
			ISTGT_ERRLOG("not supported value %d\n",
			    lu->ErrorRecoveryLevel);
			return (-1);
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ErrorRecoveryLevel %d\n",
		    istgt->ErrorRecoveryLevel);
	}

	return (0);
}

extern int g_num_luworkers;

static int
istgt_lu_add_unit(ISTGT_Ptr istgt, CF_SECTION *sp)
{
	char buf[MAX_TMPBUF], buf2[MAX_TMPBUF];
	ISTGT_LU_Ptr lu;
	PORTAL_GROUP *pgp;
	INITIATOR_GROUP *igp;
	const char *vendor, *product, *revision, *serial;
	const char *pg_tag, *ig_tag;
	const char *ag_tag;
	const char *flags, *file, *size, *rsz;
	const char *key, *val;
	uint64_t msize;
	// uint64_t nbs64;
	int pg_tag_i, ig_tag_i;
	int ag_tag_i;
	int rpm, formfactor, opt_tlen;
	int mflags;
	int slot;
	int nbs;
	int i, j, k;
	int rc;
	int gotstorage = 0;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "add unit %d\n", sp->num);

	if (sp->num >= MAX_LOGICAL_UNIT) {
		ISTGT_ERRLOG("LU%d: over maximum unit number\n", sp->num);
		return (-1);
	}
	if (istgt->logical_unit[sp->num] != NULL) {
		ISTGT_ERRLOG("LU%d: duplicate unit\n", sp->num);
		return (-1);
	}

	lu = xmalloc(sizeof (*lu));
	memset(lu, 0, sizeof (*lu));
	lu->num = sp->num;
	lu->istgt = istgt;
	lu->state = ISTGT_STATE_INVALID;
#if 0
	/* disabled now */
	nbs64 = istgt_lu_get_nbserial(istgt->nodebase);
	nbs = (int) (nbs64 % 900) * 100000;
#else
	nbs = 0;
#endif

	val = istgt_get_val(sp, "Comment");
	if (val != NULL) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Comment %s\n", val);
	}

	val = istgt_get_val(sp, "TargetName");
	if (val == NULL) {
		ISTGT_ERRLOG("LU%d: TargetName not found\n", lu->num);
		goto error_return;
	}
	lu->volname = xstrdup(val);
	if (strncasecmp(val, "iqn.", 4) != 0 &&
		strncasecmp(val, "eui.", 4) != 0 &&
		strncasecmp(val, "naa.", 4) != 0) {
		snprintf(buf, sizeof (buf), "%s:%s", istgt->nodebase, val);
	} else {
		snprintf(buf, sizeof (buf), "%s", val);
	}
	if (istgt_lu_check_iscsi_name(buf) != 0) {
		ISTGT_ERRLOG("TargetName %s contains an invalid character or format.\n",
		    buf);
#if 0
		goto error_return;
#endif
	}
	lu->name = xstrdup(buf);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "TargetName %s\n",
				    lu->name);

	val = istgt_get_val(sp, "TargetAlias");
	if (val == NULL) {
		lu->alias = NULL;
	} else {
		lu->alias = xstrdup(val);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "TargetAlias %s\n",
	    lu->alias);

	val = istgt_get_val(sp, "Mapping");
	if (val == NULL) {
		/* no map */
		lu->maxmap = 0;
	} else {
		lu->maxmap = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "Mapping", i, 0);
			if (val == NULL)
				break;
			if (lu->maxmap >= MAX_LU_MAP) {
				ISTGT_ERRLOG("LU%d: too many mapping\n", lu->num);
				goto error_return;
			}
			pg_tag = istgt_get_nmval(sp, "Mapping", i, 0);
			ig_tag = istgt_get_nmval(sp, "Mapping", i, 1);
			if (pg_tag == NULL || ig_tag == NULL) {
				ISTGT_ERRLOG("LU%d: mapping error\n", lu->num);
				goto error_return;
			}
			if (strncasecmp(pg_tag, "PortalGroup",
				strlen("PortalGroup")) != 0 ||
				sscanf(pg_tag, "%*[^0-9]%d", &pg_tag_i) != 1) {
				ISTGT_ERRLOG("LU%d: mapping portal error\n", lu->num);
				goto error_return;
			}
			if (strncasecmp(ig_tag, "InitiatorGroup",
				strlen("InitiatorGroup")) != 0 ||
				sscanf(ig_tag, "%*[^0-9]%d", &ig_tag_i) != 1) {
				ISTGT_ERRLOG("LU%d: mapping initiator error\n", lu->num);
				goto error_return;
			}
			if (pg_tag_i < 1 || ig_tag_i < 1) {
				ISTGT_ERRLOG("LU%d: invalid group tag\n", lu->num);
				goto error_return;
			}
			MTX_LOCK(&istgt->mutex);
			pgp = istgt_lu_find_portalgroup(istgt, pg_tag_i);
			if (pgp == NULL) {
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("LU%d: PortalGroup%d not found\n",
							    lu->num, pg_tag_i);
				goto error_return;
			}
			igp = istgt_lu_find_initiatorgroup(istgt, ig_tag_i);
			if (igp == NULL) {
				MTX_UNLOCK(&istgt->mutex);
				ISTGT_ERRLOG("LU%d: InitiatorGroup%d not found\n",
				    lu->num, ig_tag_i);
				goto error_return;
			}
			pgp->ref++;
			igp->ref++;
			MTX_UNLOCK(&istgt->mutex);
			lu->map[i].pg_tag = pg_tag_i;
			lu->map[i].pg_aas = AAS_ACTIVE_OPTIMIZED;
			// lu->map[i].pg_aas = AAS_ACTIVE_NON_OPTIMIZED;
			lu->map[i].pg_aas |= AAS_STATUS_IMPLICIT;
			lu->map[i].ig_tag = ig_tag_i;
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "Mapping PortalGroup%d InitiatorGroup%d\n",
			    lu->map[i].pg_tag, lu->map[i].ig_tag);
			lu->maxmap = i + 1;
		}
	}
	if (lu->maxmap == 0) {
		ISTGT_ERRLOG("LU%d: no Mapping\n", lu->num);
		goto error_return;
	}

	val = istgt_get_val(sp, "AuthMethod");
	if (val == NULL) {
		/* none */
		lu->no_auth_chap = 0;
		lu->auth_chap = 0;
		lu->auth_chap_mutual = 0;
	} else {
		lu->no_auth_chap = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "AuthMethod", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "CHAP") == 0) {
				lu->auth_chap = 1;
			} else if (strcasecmp(val, "Mutual") == 0) {
				lu->auth_chap_mutual = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				lu->auth_chap = 0;
				lu->auth_chap_mutual = 0;
			} else if (strcasecmp(val, "None") == 0) {
				lu->no_auth_chap = 1;
				lu->auth_chap = 0;
				lu->auth_chap_mutual = 0;
			} else {
				ISTGT_ERRLOG("LU%d: unknown auth\n", lu->num);
				goto error_return;
			}
		}
		if (lu->auth_chap_mutual && !lu->auth_chap) {
			ISTGT_ERRLOG("LU%d: Mutual but not CHAP\n", lu->num);
			goto error_return;
		}
	}
	if (lu->no_auth_chap != 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod None\n");
	} else if (lu->auth_chap == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod Auto\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod %s %s\n",
		    lu->auth_chap ? "CHAP" : "",
		    lu->auth_chap_mutual ? "Mutual" : "");
	}

	val = istgt_get_val(sp, "AuthGroup");
	if (val == NULL) {
		lu->auth_group = 0;
	} else {
		ag_tag = val;
		if (strcasecmp(ag_tag, "None") == 0) {
			ag_tag_i = 0;
		} else {
			if (strncasecmp(ag_tag, "AuthGroup",
				strlen("AuthGroup")) != 0 ||
				sscanf(ag_tag, "%*[^0-9]%d", &ag_tag_i) != 1) {
				ISTGT_ERRLOG("LU%d: auth group error\n", lu->num);
				goto error_return;
			}
			if (ag_tag_i == 0) {
				ISTGT_ERRLOG("LU%d: invalid auth group %d\n", lu->num,
				    ag_tag_i);
				goto error_return;
			}
		}
		lu->auth_group = ag_tag_i;
	}
	if (lu->auth_group == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthGroup None\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthGroup AuthGroup%d\n",
		    lu->auth_group);
	}

	val = istgt_get_val(sp, "UseDigest");
	if (val != NULL) {
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "UseDigest", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "Header") == 0) {
				lu->header_digest = 1;
			} else if (strcasecmp(val, "Data") == 0) {
				lu->data_digest = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				lu->header_digest = 0;
				lu->data_digest = 0;
			} else {
				ISTGT_ERRLOG("LU%d: unknown digest\n", lu->num);
				goto error_return;
			}
		}
	}
	if (lu->header_digest == 0 && lu->data_digest == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UseDigest Auto\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UseDigest %s %s\n",
		    lu->header_digest ? "Header" : "",
		    lu->data_digest ? "Data" : "");
	}

	val = istgt_get_val(sp, "ReadOnly");
	if (val == NULL) {
		lu->readonly = 0;
	} else if (strcasecmp(val, "No") == 0) {
		lu->readonly = 0;
	} else if (strcasecmp(val, "Yes") == 0) {
		lu->readonly = 1;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ReadOnly %s\n",
	    lu->readonly ? "Yes" : "No");
#ifdef REPLICATION
	val = istgt_get_val(sp, "ReplicationFactor");
	if (val == NULL) {
		ISTGT_ERRLOG("ReplicationFactor not found in conf file\n");
		goto error_return;
	} else {
		lu->replication_factor = (int) strtol(val, NULL, 10);
		if (lu->replication_factor > MAXREPLICA) {
			ISTGT_ERRLOG("Max replication factor is %d.. "
			    "given %d\n", MAXREPLICA, lu->replication_factor);
			goto error_return;
		}
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ReplicationFactor %d\n",
	    lu->replication_factor);

	val = istgt_get_val(sp, "ConsistencyFactor");
	if (val == NULL) {
		ISTGT_ERRLOG("ConsistencyFactor not found in conf file\n");
		goto error_return;
	} else {
		lu->consistency_factor = (int) strtol(val, NULL, 10);
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ConsistencyFactor %d\n",
	    lu->consistency_factor);

	if (lu->replication_factor <= 0 || lu->consistency_factor <= 0 ||
		lu->replication_factor < lu->consistency_factor) {
		ISTGT_ERRLOG("Invalid ReplicationFactor/ConsistencyFactor or their ratio\n");
		goto error_return;
	}
#endif
	val = istgt_get_val(sp, "UnitType");
	if (val == NULL) {
		ISTGT_ERRLOG("LU%d: unknown unit type\n", lu->num);
		goto error_return;
	}
	if (strcasecmp(val, "Pass") == 0) {
		lu->type = ISTGT_LU_TYPE_PASS;
	} else if (strcasecmp(val, "Disk") == 0) {
		lu->type = ISTGT_LU_TYPE_DISK;
	} else if (strcasecmp(val, "DVD") == 0) {
		lu->type = ISTGT_LU_TYPE_DVD;
	} else if (strcasecmp(val, "Tape") == 0) {
		lu->type = ISTGT_LU_TYPE_TAPE;
	} else {
		ISTGT_ERRLOG("LU%d: unknown unit type\n", lu->num);
		goto error_return;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UnitType %d (%s)\n",
	    lu->type, val);

	val = istgt_get_val(sp, "UnitOnline");
	if (val == NULL) {
		lu->online = 1;
	} else if (strcasecmp(val, "Yes") == 0) {
		lu->online = 1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UnitOnline %s\n",
	    lu->online ? "Yes" : "No");

	vendor = istgt_get_nmval(sp, "UnitInquiry", 0, 0);
	product = istgt_get_nmval(sp, "UnitInquiry", 0, 1);
	revision = istgt_get_nmval(sp, "UnitInquiry", 0, 2);
	serial = istgt_get_nmval(sp, "UnitInquiry", 0, 3);
	switch (lu->type) {
	case ISTGT_LU_TYPE_DISK:
		if (vendor == NULL || strlen(vendor) == 0)
			vendor = DEFAULT_LU_VENDOR_DISK;
		if (product == NULL || strlen(product) == 0)
			product = DEFAULT_LU_PRODUCT_DISK;
		if (revision == NULL || strlen(revision) == 0)
			revision = DEFAULT_LU_REVISION_DISK;
		if (serial == NULL || strlen(serial) == 0) {
			snprintf(buf, sizeof (buf), "%.8d", 10000000 + nbs + lu->num);
			serial = (const char *) &buf[0];
		}
		break;
	case ISTGT_LU_TYPE_DVD:
		if (vendor == NULL || strlen(vendor) == 0)
			vendor = DEFAULT_LU_VENDOR_DVD;
		if (product == NULL || strlen(product) == 0)
			product = DEFAULT_LU_PRODUCT_DVD;
		if (revision == NULL || strlen(revision) == 0)
			revision = DEFAULT_LU_REVISION_DVD;
		if (serial == NULL || strlen(serial) == 0) {
			snprintf(buf, sizeof (buf), "%.8d", 10000000 + nbs + lu->num);
			serial = (const char *) &buf[0];
		}
		break;
	case ISTGT_LU_TYPE_TAPE:
		if (vendor == NULL || strlen(vendor) == 0)
			vendor = DEFAULT_LU_VENDOR_TAPE;
		if (product == NULL || strlen(product) == 0)
			product = DEFAULT_LU_PRODUCT_TAPE;
		if (revision == NULL || strlen(revision) == 0)
			revision = DEFAULT_LU_REVISION_TAPE;
		if (serial == NULL || strlen(serial) == 0) {
#ifdef USE_LU_TAPE_DLT8000
			snprintf(buf, sizeof (buf), "CX%.8d", 10000000 + nbs + lu->num);
#else
			snprintf(buf, sizeof (buf), "%.8d", 10000000 + nbs + lu->num);
#endif /* USE_LU_TAPE_DLT8000 */
			serial = (const char *) &buf[0];
		}
		break;
	default:
		if (vendor == NULL || strlen(vendor) == 0)
			vendor = DEFAULT_LU_VENDOR;
		if (product == NULL || strlen(product) == 0)
			product = DEFAULT_LU_PRODUCT;
		if (revision == NULL || strlen(revision) == 0)
			revision = DEFAULT_LU_REVISION;
		if (serial == NULL || strlen(serial) == 0) {
			snprintf(buf, sizeof (buf), "%.8d", 10000000 + nbs + lu->num);
			serial = (const char *) &buf[0];
		}
		break;
	}
	lu->inq_vendor = xstrdup(vendor);
	lu->inq_product = xstrdup(product);
	lu->inq_revision = xstrdup(revision);
	lu->inq_serial = xstrdup(serial);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UnitInquiry %s %s %s %s\n",
	    lu->inq_vendor, lu->inq_product, lu->inq_revision,
	    lu->inq_serial);

	val = istgt_get_val(sp, "BlockLength");
	if (val == NULL) {
		switch (lu->type) {
		case ISTGT_LU_TYPE_DISK:
			lu->blocklen = DEFAULT_LU_BLOCKLEN_DISK;
			break;
		case ISTGT_LU_TYPE_DVD:
			lu->blocklen = DEFAULT_LU_BLOCKLEN_DVD;
			break;
		case ISTGT_LU_TYPE_TAPE:
			lu->blocklen = DEFAULT_LU_BLOCKLEN_TAPE;
			break;
		default:
			lu->blocklen = DEFAULT_LU_BLOCKLEN;
			break;
		}
	} else {
		lu->blocklen = (int) strtol(val, NULL, 10);
	}

	lu->rshift = 0;
	lu->recordsize = lu->blocklen; // old volumes
	val = istgt_get_val(sp, "PhysRecordLength");
	if (val != NULL) {
		int lbPerRecord, lrsize;
		lrsize = (int) strtol(val, NULL, 10);
		if (lrsize > lu->blocklen && lrsize < 131073) {
			lbPerRecord = lrsize / lu->blocklen;
			if (lbPerRecord & (lbPerRecord-1)) {
				ISTGT_ERRLOG("LU%d: ignore invalid recordsize:%u blocklen:%u\n",
						lu->num,  lrsize, lu->blocklen);
			} else {
				lu->recordsize = lrsize;
				lu->rshift = fls(lbPerRecord) - 1;
			}
		}
	}

	val = istgt_get_val(sp, "QueueDepth");
	if (val == NULL) {
		switch (lu->type) {
		case ISTGT_LU_TYPE_DISK:
			lu->queue_depth = DEFAULT_LU_QUEUE_DEPTH;
			// lu->queue_depth = 0;
			break;
		case ISTGT_LU_TYPE_DVD:
		case ISTGT_LU_TYPE_TAPE:
		default:
			lu->queue_depth = 0;
			break;
		}
	} else {
		lu->queue_depth = (int) strtol(val, NULL, 10);
	}
	if (lu->queue_depth < 0 || lu->queue_depth >= MAX_LU_QUEUE_DEPTH) {
		ISTGT_ERRLOG("LU%d: queue depth %d is not in range, resetting to %d\n", lu->num, lu->queue_depth, DEFAULT_LU_QUEUE_DEPTH);
		lu->queue_depth = DEFAULT_LU_QUEUE_DEPTH;
		goto error_return;
	}

	lu->luworkers = 0;
	lu->luworkersActive = 0;
	lu->conns = 0;
	lu->limit_q_size = 0;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "BlockLength %d, PhysRecordLength %d, QueueDepth %d\n",
		lu->blocklen, lu->recordsize, lu->queue_depth);

	val = istgt_get_val(sp, "Luworkers");
	if (val == NULL) {
		switch (lu->type) {
		case ISTGT_LU_TYPE_DISK:
			lu->luworkers = g_num_luworkers;
			break;
		case ISTGT_LU_TYPE_DVD:
		case ISTGT_LU_TYPE_TAPE:
		default:
			lu->luworkers = 0;
			break;
		}
	} else {
		lu->luworkers = (int) strtol(val, NULL, 10);
		if (lu->luworkers > (ISTGT_MAX_NUM_LUWORKERS - 1))
			lu->luworkers = (ISTGT_MAX_NUM_LUWORKERS - 1);
		else if (lu->luworkers < 1)
			lu->luworkers = 1;
	}

	lu->maxlun = 0;
	for (i = 0; i < MAX_LU_LUN; i++) {
		lu->lun[i].type = ISTGT_LU_LUN_TYPE_NONE;
		lu->lun[i].rotationrate = DEFAULT_LU_ROTATIONRATE;
		lu->lun[i].formfactor = DEFAULT_LU_FORMFACTOR;
		lu->lun[i].readcache = 1;
		lu->lun[i].writecache = 1;
		lu->lun[i].unmap = 0;
		lu->lun[i].wzero = 0;
		lu->lun[i].ats = 0;
		lu->lun[i].xcopy = 0;
		lu->lun[i].wsame = 0;
		lu->lun[i].dpofua = 0;
		lu->lun[i].serial = NULL;
		lu->lun[i].spec = NULL;
		lu->lun[i].opt_tlen = 1;
		snprintf(buf, sizeof (buf), "LUN%d", i);
		val = istgt_get_val(sp, buf);
		if (val == NULL)
			continue;
		if (i != 0) {
			/* default LUN serial (except LUN0) */
			snprintf(buf2, sizeof (buf2), "%sL%d", lu->inq_serial, i);
			lu->lun[i].serial = xstrdup(buf2);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LUN%d Serial %s (default)\n",
			    i, buf2);
		}
		for (j = 0; ; j++) {
			val = istgt_get_nmval(sp, buf, j, 0);
			if (val == NULL)
				break;
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LUN%d\n", i);
			if (strcasecmp(val, "Device") == 0) {
				if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_NONE) {
					ISTGT_ERRLOG("LU%d: duplicate LUN%d\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].type = ISTGT_LU_LUN_TYPE_DEVICE;

				file = istgt_get_nmval(sp, buf, j, 1);
				if (file == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].u.device.file = xstrdup(file);
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Device file=%s\n",
							    lu->lun[i].u.device.file);
			} else if (strcasecmp(val, "Storage") == 0) {
				if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_NONE) {
					ISTGT_ERRLOG("LU%d: duplicate LUN%d\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].type = ISTGT_LU_LUN_TYPE_STORAGE;

				size = istgt_get_nmval(sp, buf, j, 1);
				rsz  = istgt_get_nmval(sp, buf, j, 2);
#ifdef	REPLICATION
				if (size == NULL) {
#else
				if (file == NULL || size == NULL) {
#endif
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				if (rsz == NULL) {
					lu->lun[i].u.storage.rsize = 0;
				} else {
					uint64_t vall;
					char *endp, *p;
					vall = (uint64_t) strtoul(rsz, &endp, 10);
					// error will mean vall == ULONG_MAX
					if (endp != NULL) {
						p = endp;
						switch (toupper((int) *p)) {
							case 'Z': vall *= (uint64_t) 1024ULL;
							case 'E': vall *= (uint64_t) 1024ULL;
							case 'P': vall *= (uint64_t) 1024ULL;
							case 'T': vall *= (uint64_t) 1024ULL;
							case 'G': vall *= (uint64_t) 1024ULL;
							case 'M': vall *= (uint64_t) 1024ULL;
							case 'K': vall *= (uint64_t) 1024ULL;
									    break;
						}
					}
					if (vall > 131072)
						lu->lun[i].u.storage.rsize = 131072;
					else
						lu->lun[i].u.storage.rsize = (uint32_t)vall;
				}
#ifdef	REPLICATION
				lu->lun[i].u.storage.size = istgt_lu_parse_size(size);
#else
				if ((strcasecmp(size, "Auto") == 0 ||
					strcasecmp(size, "Size") == 0) && istgt->OperationalMode == 0) {
					lu->lun[i].u.storage.size = istgt_lu_get_filesize(file);
				} else {
					lu->lun[i].u.storage.size = istgt_lu_parse_size(size);
				}
#endif
				if (lu->lun[i].u.storage.size == 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: Auto size error (%s)\n", lu->num, i, file);
					goto error_return;
				}
				lu->lun[i].u.storage.fd = -1;
#ifdef	REPLICATION
				lu->lun[i].u.storage.file = NULL;
#else
				lu->lun[i].u.storage.file = xstrdup(file);
#endif
				gotstorage = 1;
			} else if (strcasecmp(val, "Removable") == 0) {
				if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_NONE) {
					ISTGT_ERRLOG("LU%d: duplicate LUN%d\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].type = ISTGT_LU_LUN_TYPE_REMOVABLE;

				flags = istgt_get_nmval(sp, buf, j, 1);
				file = istgt_get_nmval(sp, buf, j, 2);
				size = istgt_get_nmval(sp, buf, j, 3);
				if (flags == NULL || file == NULL || size == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				mflags = istgt_lu_parse_media_flags(flags);
				msize = istgt_lu_parse_media_size(file, size, &mflags);
				if (msize == 0 && strcasecmp(file, "/dev/null") == 0) {
					/* empty media */
				} else if (msize == 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].u.removable.type = 0;
				lu->lun[i].u.removable.id = 0;
				lu->lun[i].u.removable.fd = -1;
				lu->lun[i].u.removable.flags = mflags;
				lu->lun[i].u.removable.file = xstrdup(file);
				lu->lun[i].u.removable.size = msize;
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				    "Removable file=%s, size=%"PRIu64", flags=%x\n",
				    lu->lun[i].u.removable.file,
				    lu->lun[i].u.removable.size,
				    lu->lun[i].u.removable.flags);
			} else if (strncasecmp(val, "Slot", 4) == 0) {
				if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
					lu->lun[i].u.slot.maxslot = 0;
					for (k = 0; k < MAX_LU_LUN_SLOT; k++) {
						lu->lun[i].u.slot.present[k] = 0;
						lu->lun[i].u.slot.flags[k] = 0;
						lu->lun[i].u.slot.file[k] = NULL;
						lu->lun[i].u.slot.size[k] = 0;
					}
				} else if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_SLOT) {
					ISTGT_ERRLOG("LU%d: duplicate LUN%d\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].type = ISTGT_LU_LUN_TYPE_SLOT;
				if (sscanf(val, "%*[^0-9]%d", &slot) != 1) {
					ISTGT_ERRLOG("LU%d: slot number error\n", lu->num);
					goto error_return;
				}
				if (slot < 0 || slot >= MAX_LU_LUN_SLOT) {
					ISTGT_ERRLOG("LU%d: slot number range error\n", lu->num);
					goto error_return;
				}
				if (lu->lun[i].u.slot.present[slot]) {
					ISTGT_ERRLOG("LU%d: duplicate slot %d\n", lu->num, slot);
					goto error_return;
				}
				lu->lun[i].u.slot.present[slot] = 1;
				if (slot + 1 > lu->lun[i].u.slot.maxslot) {
					lu->lun[i].u.slot.maxslot = slot + 1;
				}

				flags = istgt_get_nmval(sp, buf, j, 1);
				file = istgt_get_nmval(sp, buf, j, 2);
				size = istgt_get_nmval(sp, buf, j, 3);
				if (flags == NULL || file == NULL || size == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				mflags = istgt_lu_parse_media_flags(flags);
				msize = istgt_lu_parse_media_size(file, size, &mflags);
				if (msize == 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].u.slot.flags[slot] = mflags;
				lu->lun[i].u.slot.file[slot] = xstrdup(file);
				lu->lun[i].u.slot.size[slot] = msize;
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				    "Slot%d file=%s, size=%"PRIu64", flags=%x\n",
				    slot,
				    lu->lun[i].u.slot.file[slot],
				    lu->lun[i].u.slot.size[slot],
				    lu->lun[i].u.slot.flags[slot]);
			} else if (strncasecmp(val, "Option", 6) == 0) {
				key = istgt_get_nmval(sp, buf, j, 1);
				val = istgt_get_nmval(sp, buf, j, 2);
				if (key == NULL || val == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				if (strcasecmp(key, "Serial") == 0) {
					/* set LUN serial */
					if (strlen(val) == 0) {
						ISTGT_ERRLOG("LU%d: LUN%d: no serial\n",
						    lu->num, i);
						goto error_return;
					}
					xfree(lu->lun[i].serial);
					lu->lun[i].serial = xstrdup(val);
				} else if (strcasecmp(key, "RPM") == 0) {
					rpm = (int)strtol(val, NULL, 10);
					if (rpm < 0) {
						rpm = 0;
					} else if (rpm > 0xfffe) {
						rpm = 0xfffe;
					}
					/* 0 not reported, 1 SSD, 5400/7200/10000/15000 - rotation rate(rpm) */
					lu->lun[i].rotationrate = rpm;
				} else if (strcasecmp(key, "FormFactor") == 0) {
					formfactor = (int)strtol(val, NULL, 10);
					if (formfactor < 0) {
						formfactor = 0;
					} else if (formfactor > 0x0f) {
						formfactor = 0xf;
					}
					lu->lun[i].formfactor = formfactor;
				} else if (strcasecmp(key, "ReadCache") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].readcache = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].readcache = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "WriteCache") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].writecache = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].writecache = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "Unmap") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].unmap = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].unmap = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "WZero") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (lu->lun[i].unmap == 0) {
							ISTGT_ERRLOG("LU%d: LUN%d: Wzero enabled while Unmap disabled\n", lu->num, i);
							goto error_return;
						}
						lu->lun[i].wzero = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].wzero = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "ats") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].ats = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].ats = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "xcopy") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].xcopy = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].xcopy = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "wsame") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].wsame = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].wsame = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "dpofua") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						lu->lun[i].dpofua = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						lu->lun[i].dpofua = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "OptTlen") == 0) {
					opt_tlen = (int)strtol(val, NULL, 10);
					if (opt_tlen < 0) {
						opt_tlen = 0;
					} else if (opt_tlen > 0xffff) {
						opt_tlen = 0xffff;
					}
					lu->lun[i].opt_tlen = opt_tlen;
					ISTGT_ERRLOG("LU%d: LUN%d: opt_transfer_len (%d)\n",
						    lu->num, i, opt_tlen);
				} else {
					ISTGT_WARNLOG("LU%d: LUN%d: unknown key(%s)\n",
					    lu->num, i, key);
					continue;
				}
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LUN%d Option %s => %s\n",
				    i, key, val);
				continue;
			} else {
				ISTGT_ERRLOG("LU%d: unknown lun type\n", lu->num);
				goto error_return;
			}
		}
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_SLOT) {
			if (lu->lun[i].u.slot.maxslot == 0) {
				ISTGT_ERRLOG("LU%d: no slot\n", lu->num);
				goto error_return;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "maxslot=%d\n",
			    lu->lun[i].u.slot.maxslot);
		}
		if (gotstorage == 1) {
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
					"Lu%d: LUN%d: ADD Storage:%s, size=%lu rsz:%u que:%d rpm:%d %s%s%s%s%s%s%s\n",
					lu->num, i, lu->lun[i].u.storage.file, lu->lun[i].u.storage.size,
					lu->lun[i].u.storage.rsize, lu->queue_depth, lu->lun[i].rotationrate,
					lu->lun[i].readcache ? "" : "RCD",
					lu->lun[i].writecache ? " WCE" : "",
					lu->lun[i].ats ? " ATS" : "",
					lu->lun[i].unmap ? " UNMAP" : "",
					lu->lun[i].wsame ? " WSAME" : "",
					lu->lun[i].dpofua ? " DPOFUA" : "",
					lu->lun[i].wzero ? " WZERO" : "");

			gotstorage = 0;
		}
		lu->maxlun = i + 1;
	}
	if (lu->maxlun == 0) {
		ISTGT_ERRLOG("LU%d: no LUN\n", lu->num);
		goto error_return;
	}
	if (lu->lun[0].type == ISTGT_LU_LUN_TYPE_NONE) {
		ISTGT_ERRLOG("LU%d: no LUN0\n", lu->num);
		goto error_return;
	}

	/* set local values if any */
	rc = istgt_lu_set_local_settings(istgt, sp, lu);
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: local setting error\n", lu->num);
		goto error_return;
	}

	/* tsih 0 is reserved */
	for (i = 0; i < MAX_LU_TSIH; i++) {
		lu->tsih[i].tag = 0;
		lu->tsih[i].tsih = 0;
		lu->tsih[i].initiator_port = NULL;
	}
	lu->maxtsih = 1;
	lu->last_tsih = 0;

	MTX_LOCK(&istgt->mutex);
	istgt->nlogical_unit++;
	istgt->logical_unit[lu->num] = lu;
	MTX_UNLOCK(&istgt->mutex);
	return (0);

error_return:
	xfree(lu->name);
	xfree(lu->volname);
	xfree(lu->alias);
	xfree(lu->inq_vendor);
	xfree(lu->inq_product);
	xfree(lu->inq_revision);
	for (i = 0; i < MAX_LU_LUN; i++) {
		switch (lu->lun[i].type) {
		case ISTGT_LU_LUN_TYPE_DEVICE:
			xfree(lu->lun[i].u.device.file);
			break;
		case ISTGT_LU_LUN_TYPE_STORAGE:
			if (lu->lun[i].u.storage.file)
				xfree(lu->lun[i].u.storage.file);
			break;
		case ISTGT_LU_LUN_TYPE_REMOVABLE:
			xfree(lu->lun[i].u.removable.file);
			break;
		case ISTGT_LU_LUN_TYPE_SLOT:
			for (j = 0; j < lu->lun[i].u.slot.maxslot; j++) {
				xfree(lu->lun[i].u.slot.file[j]);
			}
			break;
		case ISTGT_LU_LUN_TYPE_NONE:
		default:
			break;
		}
	}
	for (i = 0; i < MAX_LU_TSIH; i++) {
		xfree(lu->tsih[i].initiator_port);
	}
	for (i = 0; i < lu->maxmap; i++) {
		pg_tag_i = lu->map[i].pg_tag;
		ig_tag_i = lu->map[i].ig_tag;
		MTX_LOCK(&istgt->mutex);
		pgp = istgt_lu_find_portalgroup(istgt, pg_tag_i);
		igp = istgt_lu_find_initiatorgroup(istgt, ig_tag_i);
		if (pgp != NULL && igp != NULL) {
			pgp->ref--;
			igp->ref--;
		}
		MTX_UNLOCK(&istgt->mutex);
	}

	xfree(lu);
	return (-1);
}

static int
istgt_lu_close_connection(ISTGT_LU_Ptr lu, INITIATOR_GROUP *igp_new)
{
	CONN_Ptr conn;
	int i, j, found;

	if (igp_new == NULL) {
		return (-1);
	}
	for (i = 0; i < igp_new->ninitiators; i++) {
		if (strcasecmp(igp_new->initiators[i], "ALL") == 0)
			return (0);
	}
	/* Traverse all connections to Disconnect the invalid ones */
		for (j = 1; j < MAX_LU_TSIH; j++) {
			if (lu->tsih[j].initiator_port != NULL &&
				lu->tsih[j].tsih != 0) {
				found = 0;
				conn = istgt_find_conn(lu->tsih[j].initiator_port, lu->name, lu->tsih[j].tsih);
				if (conn == NULL)
					continue;
				for (i = 0; i < igp_new->ninitiators; i++) {
					if (igp_new->initiators[i] == NULL)
						continue;
					if (strcasecmp(igp_new->initiators[i], conn->initiator_name) == 0) {
						found = 1;
						break;
					}
				}
				if (!found) {
					ISTGT_NOTICELOG("Close all connections for the Initiator %s\n", conn->initiator_name);
					MTX_UNLOCK(&conn->istgt->mutex);
					istgt_iscsi_send_async(conn);
					MTX_LOCK(&conn->istgt->mutex);
				}
			}
		}

	return (0);
}

extern clockid_t clockid;
static int
istgt_lu_update_unit(ISTGT_LU_Ptr lu, CF_SECTION *sp)
{
	char buf[MAX_TMPBUF];
	INITIATOR_GROUP *igp_old, *igp_new;
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	const char *ig_tag_new;
	const char *ag_tag;
	const char *file, *size, *rsz;
	const char *val, *key;
	int ig_tag_i_new;
	int ag_tag_i;
	int i, j;
	int rc, flags;
	uint64_t old_size = 0, new_size;
	uint32_t old_rsize = 0, new_rsize;
	int rpm, formfactor, opt_tlen;
	int storagechange = 0;

	val = istgt_get_val(sp, "Mapping");
	if (val == NULL) {
		/* no map */
		lu->maxmap = 0;
	} else {
		lu->maxmap = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "Mapping", i, 0);
			if (val == NULL)
				break;
			if (lu->maxmap >= MAX_LU_MAP) {
				ISTGT_ERRLOG("LU%d: too many mapping\n", lu->num);
				goto error_return;
			}
			ig_tag_new = istgt_get_nmval(sp, "Mapping", i, 1);
			if (ig_tag_new == NULL) {
				ISTGT_ERRLOG("LU%d: mapping error\n", lu->num);
				goto error_return;
			}
			if (strncasecmp(ig_tag_new, "InitiatorGroup",
				strlen("InitiatorGroup")) != 0 ||
				sscanf(ig_tag_new, "%*[^0-9]%d", &ig_tag_i_new) != 1) {
				ISTGT_ERRLOG("LU%d: mapping initiator error\n", lu->num);
				goto error_return;
			}
			if (ig_tag_i_new < 1) {
				ISTGT_ERRLOG("LU%d: invalid group tag\n", lu->num);
				goto error_return;
			}
			MTX_LOCK(&lu->istgt->mutex);
			igp_new = istgt_lu_find_initiatorgroup(lu->istgt, ig_tag_i_new);
			if (igp_new == NULL) {
				MTX_UNLOCK(&lu->istgt->mutex);
				ISTGT_ERRLOG("LU%d: InitiatorGroup%d not found\n",
				    lu->num, ig_tag_i_new);
				goto error_return;
			}
			igp_new->ref++;
			igp_old = istgt_lu_find_initiatorgroup(lu->istgt, lu->map[i].ig_tag);
			if (igp_old == NULL) {
				ISTGT_NOTICELOG("LU%d: InitiatoGroup%d not found\n",
				lu->num, lu->map[i].ig_tag);
			}
			ISTGT_NOTICELOG("initiator grp updated with new value %d\n", igp_new->tag);
			rc = istgt_lu_close_connection(lu, igp_new);
			MTX_UNLOCK(&lu->istgt->mutex);
			if (rc < 0) {
				/* handle the error */
				ISTGT_ERRLOG("Unable to close Unauthorised connections for LU%d\n", lu->num);
				goto error_out;
			}
			lu->map[i].ig_tag = ig_tag_i_new;
			lu->maxmap = i + 1;


		}
	}
	if (lu->maxmap == 0) {
		ISTGT_ERRLOG("LU%d: no Mapping\n", lu->num);
		goto error_return;
	}

	val = istgt_get_val(sp, "AuthMethod");
	if (val == NULL) {
		/* none */
		lu->no_auth_chap = 0;
		lu->auth_chap = 0;
		lu->auth_chap_mutual = 0;
	} else {
		lu->no_auth_chap = 0;
		lu->auth_chap = 0;
		lu->auth_chap_mutual = 0;
		for (i = 0; ; i++) {
			val = istgt_get_nmval(sp, "AuthMethod", 0, i);
			if (val == NULL)
				break;
			if (strcasecmp(val, "CHAP") == 0) {
				lu->auth_chap = 1;
			} else if (strcasecmp(val, "Mutual") == 0) {
				lu->auth_chap_mutual = 1;
			} else if (strcasecmp(val, "Auto") == 0) {
				lu->auth_chap = 0;
				lu->auth_chap_mutual = 0;
			} else if (strcasecmp(val, "None") == 0) {
				lu->no_auth_chap = 1;
				lu->auth_chap = 0;
				lu->auth_chap_mutual = 0;
			} else {
				ISTGT_ERRLOG("LU%d: unknown auth\n", lu->num);
				goto error_return;
			}
		}
		if (lu->auth_chap_mutual && !lu->auth_chap) {
			ISTGT_ERRLOG("LU%d: Mutual but not CHAP\n", lu->num);
			goto error_return;
		}
	}
	if (lu->no_auth_chap != 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod None\n");
	} else if (lu->auth_chap == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod Auto\n");
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "AuthMethod %s %s\n",
		    lu->auth_chap ? "CHAP" : "",
		    lu->auth_chap_mutual ? "Mutual" : "");
	}

	val = istgt_get_val(sp, "AuthGroup");
	if (val == NULL) {
		lu->auth_group = 0;
	} else {
			ag_tag = val;
		if (strcasecmp(ag_tag, "None") == 0) {
			ag_tag_i = 0;
		} else {
			if (strncasecmp(ag_tag, "AuthGroup",
				strlen("AuthGroup")) != 0 ||
				sscanf(ag_tag, "%*[^0-9]%d", &ag_tag_i) != 1) {
				ISTGT_ERRLOG("LU%d: auth group error\n", lu->num);
				goto error_return;
			}
			if (ag_tag_i == 0) {
				ISTGT_ERRLOG("LU%d: invalid auth group %d\n", lu->num,
				    ag_tag_i);
				goto error_return;
			}
		}
		lu->auth_group = ag_tag_i;
	}
	if (lu->auth_group == 0) {
		ISTGT_NOTICELOG("AuthGroup None\n");
	} else {
		ISTGT_NOTICELOG("AuthGroup AuthGroup%d\n",
		    lu->auth_group);
	}

	val = istgt_get_val(sp, "UnitOnline");
	if (val == NULL) {
		lu->online = 1;
	} else if (strcasecmp(val, "Yes") == 0) {
		lu->online = 1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "UnitOnline %s\n",
			lu->online ? "Yes" : "No");
	val = istgt_get_val(sp, "ReadOnly");
	if (val == NULL) {
		lu->readonly = 0;
	} else if (strcasecmp(val, "No") == 0) {
		lu->readonly = 0;
	} else if (strcasecmp(val, "Yes") == 0) {
		lu->readonly = 1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ReadOnly %s\n",
			lu->readonly ? "Yes" : "No");
	for (i = 0; i < MAX_LU_LUN; i++) {
		snprintf(buf, sizeof (buf), "LUN%d", i);
		val = istgt_get_val(sp, buf);
		if (val == NULL)
			continue;
		if (lu->lun[i].spec == NULL)
			continue;
		spec = lu->lun[i].spec;
		storagechange = 0;
		for (j = 0; ; j++) {
			val = istgt_get_nmval(sp, buf, j, 0);
			if (val == NULL)
				break;
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LUN%d\n", i);
			if (strcasecmp(val, "Device") == 0) {
				if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_NONE) {
					ISTGT_ERRLOG("LU%d: duplicate LUN%d\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].type = ISTGT_LU_LUN_TYPE_DEVICE;

				file = istgt_get_nmval(sp, buf, j, 1);
				if (file == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				lu->lun[i].u.device.file = xstrdup(file);
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Device file=%s\n",
							    lu->lun[i].u.device.file);
			} else if (strcasecmp(val, "Storage") == 0) {
				file = istgt_get_nmval(sp, buf, j, 1);
				size = istgt_get_nmval(sp, buf, j, 2);
				rsz  = istgt_get_nmval(sp, buf, j, 3);
				if (file == NULL || size == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				old_rsize = lu->lun[i].u.storage.rsize;
				old_size = lu->lun[i].u.storage.size;
#ifndef	REPLICATION
				if ((strcasecmp(size, "Auto") == 0 ||
					strcasecmp(size, "Size") == 0) &&
					lu->istgt->OperationalMode == 0) {
					new_size = istgt_lu_get_filesize(file);
				} else {
					new_size = istgt_lu_parse_size(size);
				}
#else
				new_size = istgt_lu_parse_size(size);
#endif
				new_rsize = 0;
				if (rsz != NULL) {
					uint64_t vall;
					char *endp, *p;
					vall = (uint64_t) strtoul(rsz, &endp, 10);
					// error will mean vall == ULONG_MAX
					if (endp != NULL) {
						p = endp;
						switch (toupper((int) *p)) {
							case 'Z': vall *= (uint64_t) 1024ULL;
							case 'E': vall *= (uint64_t) 1024ULL;
							case 'P': vall *= (uint64_t) 1024ULL;
							case 'T': vall *= (uint64_t) 1024ULL;
							case 'G': vall *= (uint64_t) 1024ULL;
							case 'M': vall *= (uint64_t) 1024ULL;
							case 'K': vall *= (uint64_t) 1024ULL;
									    break;
						}
					}
					if (vall > 131072)
						new_rsize = 131072;
					else
						new_rsize = (uint32_t)vall;
				}

				if (old_size == new_size && new_rsize == old_rsize &&
					spec != NULL && strcasecmp(file, spec->file) == 0)
					continue;

				lu->lun[i].u.storage.size = new_size;
				lu->lun[i].u.storage.rsize = new_rsize;

				if (lu->lun[i].u.storage.size == 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: Auto size error (%s)\n", lu->num, i, file);
					goto error_return;
				}
#ifndef	REPLICATION
				if (lu->lun[i].u.storage.file)
					xfree(lu->lun[i].u.storage.file);
				lu->lun[i].u.storage.file = xstrdup(file);
#endif
				++storagechange;
			} else  if (strncasecmp(val, "Option", 6) == 0) {
				key = istgt_get_nmval(sp, buf, j, 1);
				val = istgt_get_nmval(sp, buf, j, 2);
				if (key == NULL || val == NULL) {
					ISTGT_ERRLOG("LU%d: LUN%d: format error\n", lu->num, i);
					goto error_return;
				}
				if (strcasecmp(key, "Serial") == 0) {
					/* set LUN serial */
					if (strlen(val) == 0) {
						ISTGT_ERRLOG("LU%d: LUN%d: no serial\n",
						    lu->num, i);
						goto error_return;
					}
					if (!lu->lun[i].serial ||
							(strcasecmp(lu->lun[i].serial, val) != 0)) {
						++storagechange;
						xfree(lu->lun[i].serial);
						lu->lun[i].serial = xstrdup(val);
					}
				} else if (strcasecmp(key, "RPM") == 0) {
					rpm = (int)strtol(val, NULL, 10);
					if (rpm < 0) {
						rpm = 0;
					} else if (rpm > 0xfffe) {
						rpm = 0xfffe;
					}
					if (lu->lun[i].rotationrate != rpm)
						++storagechange;
					lu->lun[i].rotationrate = rpm;
				} else if (strcasecmp(key, "FormFactor") == 0) {
					formfactor = (int)strtol(val, NULL, 10);
					if (formfactor < 0) {
						formfactor = 0;
					} else if (formfactor > 0x0f) {
						formfactor = 0xf;
					}
					if (lu->lun[i].formfactor != formfactor)
						++storagechange;
					lu->lun[i].formfactor = formfactor;
				} else if (strcasecmp(key, "ReadCache") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->readcache)
							++storagechange;
						lu->lun[i].readcache = 1;
						spec->readcache = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->readcache)
							++storagechange;
						lu->lun[i].readcache = 0;
						spec->readcache = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "WriteCache") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->writecache)
							++storagechange;
						lu->lun[i].writecache = 1;
						spec->writecache = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->writecache)
							++storagechange;
						lu->lun[i].writecache = 0;
						spec->writecache = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "Unmap") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->unmap)
							++storagechange;
						lu->lun[i].unmap = 1;
						spec->unmap = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->unmap)
							++storagechange;
						lu->lun[i].unmap = 0;
						spec->unmap = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "WZero") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->wzero)
							++storagechange;
						lu->lun[i].wzero = 1;
						spec->wzero = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->wzero)
							++storagechange;
						lu->lun[i].wzero = 0;
						spec->wzero = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "ats") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->ats)
							++storagechange;
						lu->lun[i].ats = 1;
						spec->ats = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->ats)
							++storagechange;
						lu->lun[i].ats = 0;
						spec->ats = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "xcopy") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->xcopy)
							++storagechange;
						lu->lun[i].xcopy = 1;
						spec->xcopy = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->xcopy)
							++storagechange;
						lu->lun[i].xcopy = 0;
						spec->xcopy = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "wsame") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->wsame)
							++storagechange;
						lu->lun[i].wsame = 1;
						spec->wsame = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->wsame)
							++storagechange;
						lu->lun[i].wsame = 0;
						spec->wsame = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "dpofua") == 0) {
					if (strcasecmp(val, "Enable") == 0) {
						if (!spec->dpofua)
							++storagechange;
						lu->lun[i].dpofua = 1;
						spec->dpofua = 1;
					} else if (strcasecmp(val, "Disable") == 0) {
						if (spec->dpofua)
							++storagechange;
						lu->lun[i].dpofua = 0;
						spec->dpofua = 0;
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: unknown val(%s)\n",
						    lu->num, i, val);
					}
				} else if (strcasecmp(key, "OptTlen") == 0) {
					opt_tlen = (int)strtol(val, NULL, 10);
					if (opt_tlen < 0) {
						opt_tlen = 0;
					} else if (opt_tlen > 0xffff) {
						opt_tlen = 0xffff;
					}
					lu->lun[i].opt_tlen = opt_tlen;
					ISTGT_ERRLOG("LU%d: LUN%d: opt_transfer_len (%d)\n",
						    lu->num, i, opt_tlen);
				} else {
					ISTGT_WARNLOG("LU%d: LUN%d: unknown key(%s)\n",
					    lu->num, i, key);
					continue;
				}
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LUN%d Option %s => %s\n",
				    i, key, val);
				continue;
			} else {
				ISTGT_ERRLOG("LU%d: unknown lun type\n", lu->num);
				// goto error_return;
			}
		}
		/* update the quota */
		if ((storagechange != 0) && (lu->lun[i].type == ISTGT_LU_LUN_TYPE_STORAGE)) {
			ISTGT_NOTICELOG("LU%d: LUN%d: UPD Storage:%s, size=%lu rsz=%u rpm:%d %s%s%s%s%s%s%s%s\n",
					lu->num, i, lu->lun[i].u.storage.file, lu->lun[i].u.storage.size,
					lu->lun[i].u.storage.rsize, lu->lun[i].rotationrate,
					lu->lun[i].readcache ? "" : "RCD",
					lu->lun[i].writecache ? " WCE" : "",
					lu->lun[i].ats ? " ATS" : "",
					lu->lun[i].ats ? " XCOPY" : "",
					lu->lun[i].unmap ? " UNMAP" : "",
					lu->lun[i].wsame ? " WSAME" : "",
					lu->lun[i].dpofua ? " DPOFUA" : "",
					lu->lun[i].wzero ? " WZERO" : ""
					);

			if (lu->istgt->OperationalMode) {
				clock_gettime(clockid, &spec->close_started);
				rc = istgt_lu_disk_close(lu, i);
			} else {
				clock_gettime(clockid, &spec->open_started);
				rc = istgt_lu_disk_open(lu, i);
			}
			if (rc < 0 && !lu->istgt->OperationalMode) {
				ISTGT_ERRLOG("logical unit update error, retry with old values (size:%lu rsize:%u)\n", old_size, old_rsize);
				lu->lun[i].u.storage.size = old_size;
				lu->lun[i].u.storage.rsize = old_rsize;
				flags = lu->readonly ? O_RDONLY : O_RDWR;

				rc = spec->open(spec, flags, 0666);
				if (rc < 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: open error(errno=%d)\n",
							lu->num, i, errno);
					goto error_return;
				}
			}
			if (rc >= 0) {
				TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
					if (nexus == NULL)
						break;
					nexus->ua_pending |= ISTGT_UA_LUN_CHANGE;
				}
			}
		}
	}


	if (lu->maxlun == 0) {
		ISTGT_ERRLOG("LU%d: no LUN\n", lu->num);
		goto error_return;
	}

	/* tsih 0 is reserved */
	/*
	 * for (i = 0; i < MAX_LU_TSIH; i++) {
	 * 	lu->tsih[i].tag = 0;
	 * 	lu->tsih[i].tsih = 0;
	 * 	lu->tsih[i].initiator_port = NULL;
	 * }
	 * lu->maxtsih = 1;
	 * lu->last_tsih = 0;
	 *
	 * MTX_LOCK(&lu->istgt->mutex);
	 * lu->istgt->nlogical_unit++;
	 * lu->istgt->logical_unit[lu->num] = lu;
	 * MTX_UNLOCK(&lu->istgt->mutex);
	 */

	return (0);

error_out:
	for (i = 0; i < lu->maxmap; i++) {
		ig_tag_i_new = lu->map[i].ig_tag;
		MTX_LOCK(&lu->istgt->mutex);
		igp_new = istgt_lu_find_initiatorgroup(lu->istgt, ig_tag_i_new);
		if (igp_new != NULL) {
			igp_new->ref--;
		}
		MTX_UNLOCK(&lu->istgt->mutex);
	}

error_return:
	ISTGT_ERRLOG("Updating the logical unit failed LU%d\n", lu->num);
	return (-1);
}



static int
istgt_lu_del_unit(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu)
{
	PORTAL_GROUP *pgp;
	INITIATOR_GROUP *igp;
	int pg_tag_i, ig_tag_i;
	int i, j;

	if (lu == NULL)
		return (0);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "del unit %d\n", lu->num);

	// MTX_LOCK(&istgt->mutex);
	istgt->nlogical_unit--;
	istgt->logical_unit[lu->num] = NULL;
	// MTX_UNLOCK(&istgt->mutex);

	xfree(lu->name);
	xfree(lu->volname);
	xfree(lu->alias);
	xfree(lu->inq_vendor);
	xfree(lu->inq_product);
	xfree(lu->inq_revision);
	xfree(lu->inq_serial);
	for (i = 0; i < MAX_LU_LUN; i++) {
		xfree(lu->lun[i].serial);
		switch (lu->lun[i].type) {
		case ISTGT_LU_LUN_TYPE_DEVICE:
			xfree(lu->lun[i].u.device.file);
			break;
		case ISTGT_LU_LUN_TYPE_STORAGE:
			xfree(lu->lun[i].u.storage.file);
			break;
		case ISTGT_LU_LUN_TYPE_REMOVABLE:
			xfree(lu->lun[i].u.removable.file);
			break;
		case ISTGT_LU_LUN_TYPE_SLOT:
			for (j = 0; j < lu->lun[i].u.slot.maxslot; j++) {
				xfree(lu->lun[i].u.slot.file[j]);
			}
			break;
		case ISTGT_LU_LUN_TYPE_NONE:
		default:
			break;
		}
	}
	for (i = 0; i < MAX_LU_TSIH; i++) {
		xfree(lu->tsih[i].initiator_port);
	}
	for (i = 0; i < lu->maxmap; i++) {
		pg_tag_i = lu->map[i].pg_tag;
		ig_tag_i = lu->map[i].ig_tag;
		// MTX_LOCK(&istgt->mutex);
		pgp = istgt_lu_find_portalgroup(istgt, pg_tag_i);
		igp = istgt_lu_find_initiatorgroup(istgt, ig_tag_i);
		if (pgp != NULL && igp != NULL) {
			pgp->ref--;
			igp->ref--;
		}
		// MTX_UNLOCK(&istgt->mutex);
	}

	return (0);
}

static void *luworker(void *arg);

static int istgt_lu_init_unit(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu)
{
	int rc;

	rc = pthread_mutex_init(&lu->mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_init() failed\n", lu->num);
		return (-1);
	}
	rc = pthread_mutex_init(&lu->state_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_init() failed\n", lu->num);
		return (-1);
	}
	rc = pthread_mutex_init(&lu->queue_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_init() failed\n", lu->num);
		return (-1);
	}
	rc = pthread_cond_init(&lu->queue_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: cond_init() failed\n", lu->num);
		return (-1);
	}

	switch (lu->type) {
	case ISTGT_LU_TYPE_PASS:
		ISTGT_ERRLOG("LU%d: pass thru unsupported\n", lu->num);
		return (-1);
		break;

	case ISTGT_LU_TYPE_DISK:
		rc = istgt_lu_disk_init(istgt, lu);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_init() failed\n", lu->num);
			return (-1);
		}
		break;

	case ISTGT_LU_TYPE_DVD:
		ISTGT_ERRLOG("LU%d: dvd unsupported\n", lu->num);
		return (-1);
		break;

	case ISTGT_LU_TYPE_TAPE:
		ISTGT_ERRLOG("LU%d: tape unsupported\n", lu->num);
		return (-1);
		break;

	case ISTGT_LU_TYPE_NONE:
		// ISTGT_ERRLOG("LU%d: dummy type\n", lu->num);
		break;
	default:
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}

	return (0);
}

int
istgt_lu_init(ISTGT_Ptr istgt)
{
	ISTGT_LU_Ptr lu;
	CF_SECTION *sp;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_init\n");
	sp = istgt_find_cf_section(istgt->config, "Global");
	if (sp == NULL) {
		ISTGT_ERRLOG("find_cf_section failed()\n");
		return (-1);
	}

	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_LOGICAL_UNIT) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Unit 0 is invalid\n");
				return (-1);
			}
			if (sp->num > ISTGT_LU_TAG_MAX) {
				ISTGT_ERRLOG("tag %d is invalid\n", sp->num);
				return (-1);
			}
			rc = istgt_lu_add_unit(istgt, sp);
			if (rc < 0) {
				ISTGT_ERRLOG("lu_add_unit() failed\n");
				return (-1);
			}
		}
		sp = sp->next;
	}

	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		rc = istgt_lu_init_unit(istgt, lu);
		if (rc < 0) {
			MTX_UNLOCK(&istgt->mutex);
			ISTGT_ERRLOG("LU%d: lu_init_unit() failed\n", lu->num);
			return (-1);
		}
		istgt_lu_set_state(lu, ISTGT_STATE_INITIALIZED);
	}
	MTX_UNLOCK(&istgt->mutex);

	return (0);
}

static int
istgt_lu_exist_num(CONFIG *config, int num)
{
	CF_SECTION *sp;

	sp = config->section;
	while (sp != NULL) {
		if (sp->type == ST_LOGICAL_UNIT) {
			if (sp->num == num) {
				return (1);
			}
		}
		sp = sp->next;
	}
	return (-1);
}

static int istgt_lu_shutdown_unit(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);

int
istgt_lu_reload_delete(ISTGT_Ptr istgt)
{
	ISTGT_LU_Ptr lu;
	int warn_num, warn_msg;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_reload_delete\n");
	warn_num = warn_msg = 0;
retry:
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		rc = istgt_lu_exist_num(istgt->config, lu->num);
		if (rc < 0) {
			MTX_LOCK(&lu->mutex);
			if (lu->maxtsih > 1) {
				if (!warn_msg) {
					warn_msg = 1;
					ISTGT_WARNLOG("It is recommended that you disconnect the target before deletion.\n");
				}
				if (warn_num != lu->num) {
					warn_num = lu->num;
					ISTGT_WARNLOG("delete request for active LU%d\n",
					    lu->num);
				}
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "reload retry for LU%d\n",
				    lu->num);
				MTX_UNLOCK(&lu->mutex);
				MTX_UNLOCK(&istgt->mutex);
				istgt_yield();
				sleep(1);
				goto retry;
			}
			MTX_UNLOCK(&lu->mutex);
			istgt_lu_set_state(lu, ISTGT_STATE_SHUTDOWN);
			rc = istgt_lu_shutdown_unit(istgt, lu);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: lu_shutdown_unit() failed\n", lu->num);
				/* ignore error */
			}
			ISTGT_NOTICELOG("delete LU%d: Name=%s\n", lu->num, lu->name);
			xfree(lu);
			istgt->logical_unit[i] = NULL;
		}
	}
	MTX_UNLOCK(&istgt->mutex);
	return (0);
}

static int
istgt_lu_match_all(CF_SECTION *sp, CONFIG *config_old)
{
	CF_ITEM *ip, *ip_old;
	CF_VALUE *vp, *vp_old;
	CF_SECTION *sp_old;

	sp_old = istgt_find_cf_section(config_old, sp->name);
	if (sp_old == NULL)
		return (0);

	ip = sp->item;
	ip_old = sp_old->item;
	while (ip != NULL && ip_old != NULL) {
		vp = ip->val;
		vp_old = ip_old->val;
		while (vp != NULL && vp_old != NULL) {
			if (vp->value != NULL && vp_old->value != NULL) {
				if (strcmp(vp->value, vp_old->value) != 0)
					return (0);
			} else {
				return (0);
			}
			vp = vp->next;
			vp_old = vp_old->next;
		}
		if (vp != NULL || vp_old != NULL)
			return (0);
		ip = ip->next;
		ip_old = ip_old->next;
	}
	if (ip != NULL || ip_old != NULL)
		return (0);
	return (1);
}

/*
 * static int
 * istgt_lu_copy_sp(CF_SECTION *sp, CONFIG *config_old)
 * {
 * 	CF_SECTION *sp_old;
 *
 * 	sp_old = istgt_find_cf_section(config_old, sp->name);
 * 	if (sp_old == NULL)
 * 		return (-1);
 * 	istgt_copy_cf_item(sp, sp_old);
 * 	return (0);
 * }
 */

static int istgt_lu_create_thread(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);

int
istgt_lu_reload_update(ISTGT_Ptr istgt)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_Ptr lu_old;
	CF_SECTION *sp;
	int rc;
	ISTGT_LU_DISK *spec = NULL;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_reload_update\n");

	sp = istgt->config->section;
	while (sp != NULL) {
		if (sp->type == ST_LOGICAL_UNIT) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Unit 0 is invalid\n");
				goto skip_lu;
			}
			if (sp->num > ISTGT_LU_TAG_MAX) {
				ISTGT_ERRLOG("tag %d is invalid\n", sp->num);
				goto skip_lu;
			}
#if 0
			rc = istgt_lu_exist_num(istgt->config_old, sp->num);
#else
			rc = -1;
			MTX_LOCK(&istgt->mutex);
			lu = istgt->logical_unit[sp->num];
			if (lu != NULL)
				rc = 1;
			MTX_UNLOCK(&istgt->mutex);
#endif
			if (rc < 0) {
				rc = istgt_lu_add_unit(istgt, sp);
				if (rc < 0) {
					ISTGT_ERRLOG("lu_add_unit() failed\n");
					goto skip_lu;
				}
				MTX_LOCK(&istgt->mutex);
				lu = istgt->logical_unit[sp->num];
				if (lu == NULL) {
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("can't find new LU%d\n", sp->num);
					goto skip_lu;
				}
				rc = istgt_lu_init_unit(istgt, lu);
				if (rc < 0) {
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("LU%d: lu_init_unit() failed\n", sp->num);
					goto free_lu;
				}
				istgt_lu_set_state(lu, ISTGT_STATE_INITIALIZED);

				rc = istgt_lu_create_thread(istgt, lu);
				if (rc < 0) {
					MTX_UNLOCK(&istgt->mutex);
					ISTGT_ERRLOG("lu_create_thread() failed\n");
					goto skip_lu;
				}
				istgt_lu_set_state(lu, ISTGT_STATE_RUNNING);
				ISTGT_NOTICELOG("add LU%d: Name=%s\n", lu->num, lu->name);
				MTX_UNLOCK(&istgt->mutex);
			} else {
				if (istgt_lu_match_all(sp, istgt->config_old)) {
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					    "skip LU%d: Name=%s\n", lu->num, lu->name);
				} else {
					MTX_LOCK(&istgt->mutex);
					lu = istgt->logical_unit[sp->num];
					if (lu == NULL) {
						MTX_UNLOCK(&istgt->mutex);
						ISTGT_ERRLOG("can't find LU%d\n", sp->num);
						goto skip_lu;
					}
					MTX_LOCK(&lu->mutex);
					if (lu->maxtsih > 1) {
						/*
						 * ISTGT_ERRLOG("update active LU%d: Name=%s, "
						 *		"# of TSIH=%d\n",
						 *		lu->num, lu->name, lu->maxtsih - 1);
						 * rc = istgt_lu_copy_sp(sp, istgt->config_old);
						 * if (rc < 0) {
						 *	// ignore error
						 * }
						 */
						MTX_UNLOCK(&lu->mutex);
						MTX_UNLOCK(&istgt->mutex);
						rc = istgt_lu_update_unit(lu, sp);
						if (rc < 0) {
							/* erro in updating the lu */
						}
						spec = (ISTGT_LU_DISK *)(lu->lun[0].spec);
						MTX_LOCK(&spec->complete_queue_mutex);
						rc = pthread_cond_signal(&spec->maint_cmd_queue_cond);
						MTX_UNLOCK(&spec->complete_queue_mutex);
						goto skip_lu;
					} else {
						istgt->logical_unit[sp->num] = NULL;
						MTX_UNLOCK(&lu->mutex);
						MTX_UNLOCK(&istgt->mutex);

						/* add new LU */
						rc = istgt_lu_add_unit(istgt, sp);
						if (rc < 0) {
							ISTGT_ERRLOG("lu_add_unit() failed\n");
							MTX_LOCK(&istgt->mutex);
							istgt->logical_unit[sp->num] = lu;
							MTX_UNLOCK(&istgt->mutex);
							goto skip_lu;
						} else {
							/* delete old LU */
							lu_old = lu;
							MTX_LOCK(&istgt->mutex);
							lu = istgt->logical_unit[sp->num];
							istgt_lu_set_state(lu_old,
							    ISTGT_STATE_SHUTDOWN);
							rc = istgt_lu_shutdown_unit(istgt,
							    lu_old);
							if (rc < 0) {
								ISTGT_ERRLOG(
									"LU%d: lu_shutdown_unit() "
									"failed\n", lu->num);
								/* ignore error */
							}
							xfree(lu_old);
							istgt->logical_unit[sp->num] = lu;
							MTX_UNLOCK(&istgt->mutex);
						}
						MTX_LOCK(&istgt->mutex);
						lu = istgt->logical_unit[sp->num];
						if (lu == NULL) {
							MTX_UNLOCK(&istgt->mutex);
							ISTGT_ERRLOG("can't find new LU%d\n",
							    sp->num);
							goto skip_lu;
						}
						rc = istgt_lu_init_unit(istgt, lu);
						if (rc < 0) {
							MTX_UNLOCK(&istgt->mutex);
							ISTGT_ERRLOG("LU%d: lu_init_unit() "
							    "failed\n", sp->num);
							goto free_lu;
						}
						istgt_lu_set_state(lu,
						    ISTGT_STATE_INITIALIZED);

						rc = istgt_lu_create_thread(istgt, lu);
						if (rc < 0) {
							MTX_UNLOCK(&istgt->mutex);
							ISTGT_ERRLOG("lu_create_thread "
							    "failed\n");
							goto skip_lu;
						}
						istgt_lu_set_state(lu, ISTGT_STATE_RUNNING);
						ISTGT_NOTICELOG("update LU%d: Name=%s\n",
						    lu->num, lu->name);
					}
					MTX_UNLOCK(&istgt->mutex);
				}
			}
		}
	goto skip_lu;

	free_lu:
		ISTGT_NOTICELOG("free the lu\n");
		/* Delete LU */
		MTX_LOCK(&istgt->mutex);
		istgt_lu_set_state(lu,
		    ISTGT_STATE_SHUTDOWN);
		rc = istgt_lu_shutdown_unit(istgt,
		    lu);
		if (rc < 0) {
			ISTGT_ERRLOG(
				"LU%d: lu_shutdown_unit() "
				"failed\n", lu->num);
			/* ignore error */
		}
		ISTGT_NOTICELOG("delete LU%d: Name=%s\n", lu->num, lu->name);
		xfree(lu);
		istgt->logical_unit[sp->num] = NULL;
		MTX_UNLOCK(&istgt->mutex);

	skip_lu:
		sp = sp->next;
	}
	return (0);
}

int
istgt_lu_update_ig(ISTGT_Ptr istgt, INITIATOR_GROUP *igp_new)
{
	ISTGT_LU_Ptr lu;
	int i, j;

	if (igp_new == NULL)
		return (0);
	ISTGT_NOTICELOG("initiator group update\n");
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		for (j = 0; j < lu->maxmap; j++) {
			if (lu->map[j].ig_tag == igp_new->tag)
				istgt_lu_close_connection(lu, igp_new);
		}
	}
	return (0);
}
int
istgt_lu_set_all_state(ISTGT_Ptr istgt, ISTGT_STATE state)
{
	ISTGT_LU_Ptr lu;
	int i;

	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;

		istgt_lu_set_state(lu, state);
	}

	return (0);
}

static void *maintenance_io_worker(void *arg);
static void *luscheduler(void *arg);

static int
istgt_lu_create_thread(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu)
{
	int rc;
	int i = 0;

	rc = pthread_create(&lu->schdler_thread, &istgt->attr, &luscheduler,
						(void *)lu);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed ,lun %d, rc = %d\n",
					lu->num, rc);
		return (-1);
	}

	if (lu->queue_depth != 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "thread for LU%d\n", lu->num);
		/* create LU thread */
		for (i = 0; i < lu->luworkers; i++) {
			rc = pthread_create(&lu->luthread[i], &istgt->attr, &luworker,
											(void *)lu);
			if (rc != 0) {
				ISTGT_ERRLOG("pthread_create() failed for thread %d, lun %d, rc = %d\n",
											i, lu->num, rc);
				return (-1);
			}
			++lu->luworkersActive;
		}
	}

	rc = pthread_create(&lu->maintenance_thread, &istgt->attr, &maintenance_io_worker,
						(void *)lu);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed for maintenance thread ,lun %d, rc = %d\n",
					lu->num, rc);
		return (-1);
	}

	return (0);
}

int
istgt_lu_create_threads(ISTGT_Ptr istgt)
{
	ISTGT_LU_Ptr lu;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_create_threads\n");

	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		rc = istgt_lu_create_thread(istgt, lu);
		if (rc < 0) {
			ISTGT_ERRLOG("lu_create_thread() failed\n");
			return (-1);
		}
	}

	return (0);
}

static int
istgt_lu_shutdown_unit(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu)
{
	int rc;
	// int i = 0;

	switch (lu->type) {
	case ISTGT_LU_TYPE_PASS:
		ISTGT_ERRLOG("LU%d: lu_pass_shutdown() \n", lu->num);
		break;

	case ISTGT_LU_TYPE_DISK:
		rc = istgt_lu_disk_shutdown(istgt, lu);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_shutdown() failed\n", lu->num);
			/* ignore error */
		}
		break;

	case ISTGT_LU_TYPE_DVD:
		ISTGT_ERRLOG("LU%d: lu_dvd_shutdown()\n", lu->num);
		break;

	case ISTGT_LU_TYPE_TAPE:
		ISTGT_ERRLOG("LU%d: lu_tape_shutdown()\n", lu->num);
		break;

	case ISTGT_LU_TYPE_NONE:
		// ISTGT_ERRLOG("LU%d: dummy type\n", lu->num);
		break;
	default:
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}

	rc = istgt_lu_del_unit(istgt, lu);
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: lu_del_unit() failed\n", lu->num);
		/* ignore error */
	}

	rc = pthread_cond_destroy(&lu->queue_cond);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: cond_destroy() failed\n", lu->num);
		/* ignore error */
	}
	rc = pthread_mutex_destroy(&lu->queue_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
		/* ignore error */
	}
	rc = pthread_mutex_destroy(&lu->state_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
		/* ignore error */
	}
	rc = pthread_mutex_destroy(&lu->mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
		/* ignore error */
	}

	return (0);
}

int
istgt_lu_shutdown(ISTGT_Ptr istgt)
{
	ISTGT_LU_Ptr lu;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_shutdown\n");
	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		istgt_lu_set_state(lu, ISTGT_STATE_SHUTDOWN);
		rc = istgt_lu_shutdown_unit(istgt, lu);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: lu_shutdown_unit() failed\n", lu->num);
			/* ignore error */
		}
		xfree(lu);
		istgt->logical_unit[i] = NULL;
	}
	MTX_UNLOCK(&istgt->mutex);

	return (0);
}

int
istgt_lu_islun2lun(uint64_t islun)
{
	uint64_t fmt_lun;
	uint64_t method;
	int lun_i;

	fmt_lun = islun;
	method = (fmt_lun >> 62) & 0x03U;
	fmt_lun = fmt_lun >> 48;
	if (method == 0x00U) {
		lun_i = (int) (fmt_lun & 0x00ffU);
	} else if (method == 0x01U) {
		lun_i = (int) (fmt_lun & 0x3fffU);
	} else {
		lun_i = 0xffffU;
	}
	return (lun_i);
}

uint64_t
istgt_lu_lun2islun(int lun, int maxlun)
{
	uint64_t fmt_lun;
	uint64_t method;
	uint64_t islun;

	islun = (uint64_t) lun;
	if (maxlun <= 0x0100) {
		/* below 256 */
		method = 0x00U;
		fmt_lun = (method & 0x03U) << 62;
		fmt_lun |= (islun & 0x00ffU) << 48;
	} else if (maxlun <= 0x4000) {
		/* below 16384 */
		method = 0x01U;
		fmt_lun = (method & 0x03U) << 62;
		fmt_lun |= (islun & 0x3fffU) << 48;
	} else {
		/* XXX */
		fmt_lun = ~((uint64_t) 0);
	}
	return (fmt_lun);
}

int
istgt_lu_reset(ISTGT_LU_Ptr lu, uint64_t lun, istgt_ua_type ua_type)
{
	int lun_i;
	int cleared = 0;

	if (lu == NULL)
		return (-1);

	lun_i = istgt_lu_islun2lun(lun);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: Name=%s, LUN=%d\n",
	    lu->num, lu->name, lun_i);

	switch (lu->type) {
	case ISTGT_LU_TYPE_PASS:
		ISTGT_ERRLOG("LU%d: pass_thru reset\n", lu->num);
		break;

	case ISTGT_LU_TYPE_DISK:
		cleared = istgt_lu_disk_reset(lu, lun_i, ua_type);
		if (cleared < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_reset() failed\n", lu->num);
			return (-1);
		}
		return (cleared);
		break;

	case ISTGT_LU_TYPE_DVD:
		ISTGT_ERRLOG("LU%d: dvd reset\n", lu->num);
		break;

	case ISTGT_LU_TYPE_TAPE:
		ISTGT_ERRLOG("LU%d: tape reset\n", lu->num);
		break;

	case ISTGT_LU_TYPE_NONE:
		// ISTGT_ERRLOG("LU%d: dummy type\n", lu->num);
		break;
	default:
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}

	return (0);
}

/*
 * SAM3: Page 79, The target port shall perform
 * logical unit reset functions for every logical unit.
 * A unit attention condition for all initiators that
 * have access shall be created on each of
 * these logical units.
 */

int
istgt_lu_reset_all(ISTGT_Ptr istgt, istgt_ua_type ua_type)
{
	int i = 0, j = 0;
	int ret = 0;
	int cleared = 0, clear_temp = 0;
	ISTGT_LU_Ptr lu;
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		for (j = 0; j < lu->maxlun; j++) {
			clear_temp = istgt_lu_reset(lu, j, ua_type);
			if (clear_temp < 0) {
				ISTGT_ERRLOG("istgt_lu_reset() failed, LU %d, LUN %d\n", lu->num, j);
				ret = -1;
			} else
				cleared += clear_temp;
		}
	}
	return ((ret < 0) ? ret : cleared);
}


ISTGT_LU_TASK_Ptr
istgt_lu_create_task(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, int lun, ISTGT_LU_DISK *spec)
{
	ISTGT_LU_TASK_Ptr lu_task = NULL;
	ISCSI_PDU_Ptr dst_pdu, src_pdu;
	uint8_t *cdb;
	int alloc_len, i, gotlba = 0;
	int pbdata, lbdata, anchor;
	int64_t maxlen;
	uint64_t blkcnt = 0;
#if 0
	int rc;
#endif

	alloc_len = ISCSI_ALIGN(sizeof (*lu_task));
	alloc_len += ISCSI_ALIGN(sizeof (*lu_task->lu_cmd.pdu));

	lu_task = xmalloc(alloc_len);
	if (lu_task == NULL)
		return (NULL);

	lu_task->type = ISTGT_LU_TASK_RESPONSE;
	lu_task->conn = conn;
	lu_task->in_plen = strlen(conn->initiator_port);
	if (lu_task->in_plen > MAX_INITIATOR_NAME-1)
		lu_task->in_plen = MAX_INITIATOR_NAME-1;
	if (lu_task->in_plen != 0) {
		strncpy(lu_task->in_port, conn->initiator_port, lu_task->in_plen);
		lu_task->in_port[lu_task->in_plen] = '\0';
	}
	else
		lu_task->in_port[0] = '\0';
	lu_task->conn = conn;

	lu_task->lun = (int) lun;
	lu_task->use_cond = 0;
	lu_task->dup_iobuf = 0;
	lu_task->alloc_len = 0;

	lu_task->lu_cmd._andx = lu_cmd->_andx;
	lu_task->lu_cmd._lst = lu_cmd->_lst;
	lu_task->lu_cmd._roll_cnt = 0;
	for (i = 0; i <= lu_cmd->_andx; ++i) {
		lu_task->lu_cmd.times[i].tv_sec = lu_cmd->times[i].tv_sec;
		lu_task->lu_cmd.times[i].tv_nsec = lu_cmd->times[i].tv_nsec;
		lu_task->lu_cmd.tdiff[i].tv_sec = lu_cmd->tdiff[i].tv_sec;
		lu_task->lu_cmd.tdiff[i].tv_nsec = lu_cmd->tdiff[i].tv_nsec;
		lu_task->lu_cmd.caller[i] = lu_cmd->caller[i];
		lu_task->lu_cmd.line[i] = lu_cmd->line[i];
	}
	for (; i < _PSZ; i++) {
		lu_task->lu_cmd.times[i].tv_sec = 0;
		lu_task->lu_cmd.times[i].tv_nsec = 0;
		lu_task->lu_cmd.caller[i] = 0;
		lu_task->lu_cmd.line[i] = 0;
		lu_task->lu_cmd.tdiff[i].tv_sec = 0;
		lu_task->lu_cmd.tdiff[i].tv_nsec = 0;

	}

#ifdef REPLICATION
	lu_task->lu_cmd.start_rw_time = lu_cmd->start_rw_time;
#endif
	lu_task->condwait = 0;
	lu_task->offset = 0;
	lu_task->req_execute = 0;
	lu_task->req_transfer_out = 0;
	lu_task->error = 0;
	lu_task->abort = 0;
	lu_task->execute = 0;
	lu_task->complete = 0;
	lu_task->lock = 0;
	lu_task->complete_queue_ptr = NULL;
	lu_task->lu_cmd.flags = 0;
#if 0
	rc = pthread_mutex_init(&lu_task->trans_mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	rc = pthread_cond_init(&lu_task->trans_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		return (-1);
	}
	rc = pthread_cond_init(&lu_task->exec_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		return (-1);
	}
#endif
	lu_task->lu_cmd.pdu = (ISCSI_PDU_Ptr) ((uintptr_t)lu_task
							+ ISCSI_ALIGN(sizeof (*lu_task)));
	// lu_task->lu_cmd.pdu = xmalloc(sizeof *lu_task->lu_cmd.pdu);
	// memset(lu_task->lu_cmd.pdu, 0, sizeof *lu_task->lu_cmd.pdu);

	/* copy PDU */
	dst_pdu = lu_task->lu_cmd.pdu;
	src_pdu = lu_cmd->pdu;
	memcpy(&dst_pdu->bhs, &src_pdu->bhs, ISCSI_BHS_LEN);
	dst_pdu->ahs = src_pdu->ahs;
	dst_pdu->header_digestX = src_pdu->header_digestX;
	dst_pdu->data = src_pdu->data;
	dst_pdu->data_digestX = src_pdu->data_digestX;
	dst_pdu->total_ahs_len = src_pdu->total_ahs_len;
	dst_pdu->data_segment_len = src_pdu->data_segment_len;
	dst_pdu->opcode = src_pdu->opcode;
	src_pdu->data = NULL;
	src_pdu->data_segment_len = 0;
	src_pdu->ahs = NULL;
	src_pdu->total_ahs_len = 0;


	/* copy other lu_cmd */
	lu_task->lu_cmd.lu = lu_cmd->lu;
	cdb = ((uint8_t *) &lu_task->lu_cmd.pdu->bhs) + 32;
	lu_task->lu_cmd.I_bit = lu_cmd->I_bit;
	lu_task->lu_cmd.F_bit = lu_cmd->F_bit;
	lu_task->lu_cmd.R_bit = lu_cmd->R_bit;
	lu_task->lu_cmd.W_bit = lu_cmd->W_bit;
	lu_task->lu_cmd.Attr_bit = lu_cmd->Attr_bit;
	lu_task->lu_cmd.lun = lu_cmd->lun;
	lu_task->lu_cmd.task_tag = lu_cmd->task_tag;
	lu_task->lu_cmd.transfer_len = lu_cmd->transfer_len;
	// lu_task->lu_cmd.cdb = lu_cmd->cdb;
	lu_task->lu_cmd.cdb = cdb;
	lu_task->lu_cmd.CmdSN = lu_cmd->CmdSN;

	// lu_task->lu_cmd.iobuf = lu_cmd->iobuf;
	lu_task->lu_cmd.iobufindx = -1;
	lu_task->lu_cmd.iobufsize = 0;
	lu_task->lu_cmd.data = lu_cmd->data;
	lu_task->lu_cmd.data_len = lu_cmd->data_len;
	lu_task->lu_cmd.alloc_len = lu_cmd->alloc_len;

	lu_task->lu_cmd.status = lu_cmd->status;
	lu_task->lu_cmd.sense_data = lu_cmd->sense_data;
	lu_task->lu_cmd.sense_data_len = lu_cmd->sense_data_len;
	lu_task->lu_cmd.sense_alloc_len = lu_cmd->sense_alloc_len;
	lu_task->lu_cmd.cdb0 = lu_cmd->cdb0;
	lu_task->lu_cmd.lba = lu_cmd->lba;
	lu_task->lu_cmd.lblen = lu_cmd->lblen;
	lu_task->lu_cmd.infdx = lu_cmd->infdx;
	lu_task->lu_cmd.cdbflags = lu_cmd->cdbflags;
	lu_task->lu_cmd.infcpy = lu_cmd->infcpy;
	lu_task->lu_cmd.lunum = lu_cmd->lunum;
	lu_task->lu_cmd.connGone = 0;
	lu_task->lu_cmd.aborted = 0;
	lu_task->lu_cmd.release_aborted = 0;

	lu_task->alloc_len = 0; // alloc_len;

	lu_task->cdb0  = cdb[0];
	lu_task->lba   = 0;
	lu_task->lblen = 0;
	lu_task->lbE = 0;

	switch (cdb[0]) {
		case SBC_READ_6:
		case SBC_WRITE_6:
			lu_task->lba   = (uint64_t) (DGET24(&cdb[1]) & 0x001fffffU);
			lu_task->lblen = (uint32_t) DGET8(&cdb[4]);
			gotlba = 1;
			break;
		case SBC_READ_10:
		case SBC_WRITE_10:
		case SBC_WRITE_AND_VERIFY_10:
			lu_task->lba   = (uint64_t) DGET32(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET16(&cdb[7]);
			gotlba = 1;
			break;
		case SBC_READ_12:
		case SBC_WRITE_12:
		case SBC_WRITE_AND_VERIFY_12:
			lu_task->lba   = (uint64_t) DGET32(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET32(&cdb[6]);
			gotlba = 1;
			break;
		case SBC_READ_16:
		case SBC_WRITE_16:
		case SBC_WRITE_AND_VERIFY_16:
			lu_task->lba   = (uint64_t) DGET64(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET32(&cdb[10]);
			gotlba = 1;
			break;

		case SBC_WRITE_SAME_10:
			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			if (pbdata || lbdata) {
				// we don't support this
			} else {
				lu_task->lba = (uint64_t) DGET32(&cdb[2]);
				lu_task->lblen = (uint32_t) DGET16(&cdb[7]);
				gotlba = 1;
				if (lu_task->lblen == 0) {
					if (lu_task->lba < spec->blockcnt) {
						blkcnt = spec->blockcnt - lu_task->lba;
						lu_task->lblen = (uint32_t)blkcnt;
					} else {
						lu_task->lba = 0;
						gotlba = 0;
					}
				}
			}
			break;
		case SBC_WRITE_SAME_16:
			anchor = BGET8(&cdb[1], 4);
			// int unmap = BGET8(&cdb[1], 3);
			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			if (pbdata || lbdata || anchor) {
				// we don't support this
			} else {
				lu_task->lba = (uint64_t) DGET32(&cdb[2]);
				lu_task->lblen = (uint32_t) DGET32(&cdb[10]);
				gotlba = 1;
				if (lu_task->lblen == 0) {
					if (lu_task->lba < spec->blockcnt) {
						blkcnt = spec->blockcnt - lu_task->lba;
						lu_task->lblen = (uint32_t)blkcnt;
					} else {
						lu_task->lba = 0;
						gotlba = 0;
					}
				}
			}
			break;
		case SBC_COMPARE_AND_WRITE:
			maxlen = ISTGT_LU_WORK_ATS_BLOCK_SIZE / spec->blocklen;
			if (maxlen > 0xff)
				maxlen = 0xff;
			lu_task->lba = (uint64_t) DGET64(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET8(&cdb[13]);
			if (lu_task->lblen > maxlen) { // invalid
				lu_task->lba = 0;
				lu_task->lblen = 0;
			} else {
				gotlba = 1;
			}
			break;

		case SBC_SYNCHRONIZE_CACHE_10:
			lu_task->lba = (uint64_t) DGET32(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET16(&cdb[7]);
			if (lu_task->lblen == 0) {
				lu_task->lblen = (uint32_t) spec->blockcnt;
				blkcnt = spec->blockcnt;
			}
			gotlba = 1;
			break;
		case SBC_SYNCHRONIZE_CACHE_16:
			lu_task->lba = (uint64_t) DGET64(&cdb[2]);
			lu_task->lblen = (uint32_t) DGET32(&cdb[10]);
			if (lu_task->lblen == 0) {
				lu_task->lblen = (uint32_t)spec->blockcnt;
				blkcnt = spec->blockcnt;
			}
			gotlba = 1;
			break;

		case SBC_UNMAP:
			/* presently unmap is serializing all reads and writes */
			break;

		default:
			lu_task->cdb0  = 0xFF;
			lu_task->lba   = 0;
			lu_task->lblen = 0;
			lu_task->lbE = 0;
	}

	if (gotlba == 1) {
		if (blkcnt != 0) {
			lu_task->lbE = lu_task->lba + blkcnt - 1;
		} else {
			if (lu_task->lblen > 0)
				lu_task->lbE = lu_task->lba + lu_task->lblen - 1;
			else // would never be here
				lu_task->lbE = lu_task->lba;
		}
		lu_task->lu_cmd.lblen = lu_task->lblen;
	}
	/*
	 * if (lu_task->cdb0 != lu_task->lu_cmd.cdb0 ||
	 * 	lu_task->lba != lu_task->lu_cmd.lba ||
	 * 	lu_task->lblen != lu_task->lu_cmd.lblen) {
	 * 	log/fix
	 * }
	 */

	/* creation time */
	/* wait time */
	lu_task->condwait = conn->timeout;
	if (lu_task->condwait < ISTGT_CONDWAIT_MINS) {
		lu_task->condwait = ISTGT_CONDWAIT_MINS;
	}

	return (lu_task);
}

int
istgt_lu_destroy_task(ISTGT_LU_TASK_Ptr lu_task)
{
	int rc, i;

	if (lu_task == NULL)
		return (-1);

	if (lu_task->use_cond != 0) {
		rc = pthread_mutex_destroy(&lu_task->trans_mutex);
		if (rc != 0) {
			ISTGT_ERRLOG("mutex_destroy() failed\n");
			return (-1);
		}
		rc = pthread_cond_destroy(&lu_task->trans_cond);
		if (rc != 0) {
			ISTGT_ERRLOG("cond_destroy() failed\n");
			return (-1);
		}
		rc = pthread_cond_destroy(&lu_task->exec_cond);
		if (rc != 0) {
			ISTGT_ERRLOG("cond_destroy() failed\n");
			return (-1);
		}
	}
	if (lu_task->lu_cmd.pdu != NULL) {
		if (lu_task->lu_cmd.pdu->ahs != NULL) {
			xfree(lu_task->lu_cmd.pdu->ahs);
			lu_task->lu_cmd.pdu->ahs = NULL;
		}
		if (lu_task->lu_cmd.pdu->data != NULL) {
			xfree(lu_task->lu_cmd.pdu->data);
			lu_task->lu_cmd.pdu->data = NULL;
		}
		// xfree(lu_task->lu_cmd.pdu);
	}
	if (lu_task->lu_cmd.data != NULL) {
		xfree(lu_task->lu_cmd.data);
		lu_task->lu_cmd.data = NULL;
	}
	if (lu_task->lu_cmd.sense_data != NULL) {
		xfree(lu_task->lu_cmd.sense_data);
		lu_task->lu_cmd.sense_data = NULL;
	}
	if (lu_task->lu_cmd.iobufindx != -1) {
		for (i = 0; i <= lu_task->lu_cmd.iobufindx; ++i) {
			if (lu_task->lu_cmd.iobuf[i].iov_base) {
				xfree(lu_task->lu_cmd.iobuf[i].iov_base);
				lu_task->lu_cmd.iobuf[i].iov_base = NULL;
				lu_task->lu_cmd.iobuf[i].iov_len = 0;
			}
		}
		lu_task->lu_cmd.iobufindx = -1;
		lu_task->lu_cmd.iobufsize = 0;
	}
	xfree(lu_task);
	return (0);
}

int
istgt_lu_clear_task_IT(CONN_Ptr conn, ISTGT_LU_Ptr lu)
{
	int cleared = 0;

	if (lu == NULL)
		return (-1);

	if (lu->queue_depth == 0)
		return (0);

	switch (lu->type) {
	case ISTGT_LU_TYPE_DISK:
		cleared = istgt_lu_disk_queue_clear_IT(conn, lu);
		if (cleared < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_queue_clear_IT() failed\n", lu->num);
			return (-1);
		}
		break;

	case ISTGT_LU_TYPE_DVD:
	case ISTGT_LU_TYPE_TAPE:
	case ISTGT_LU_TYPE_NONE:
	default:
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}

	return (cleared);
}

int
istgt_lu_clear_task_ITL(CONN_Ptr conn, ISTGT_LU_Ptr lu, uint64_t lun)
{
	int lun_i;
	int cleared = 0;

	if (lu == NULL)
		return (-1);
	if (lu->queue_depth == 0)
		return (0);
	if (lu->type != ISTGT_LU_TYPE_DISK) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}
	lun_i = istgt_lu_islun2lun(lun);

	cleared = istgt_lu_disk_queue_clear_ITL(conn, lu, lun_i);
	if (cleared < 0) {
		ISTGT_ERRLOG("LU%d: lu_disk_queue_clear_ITL() failed\n", lu->num);
		return (-1);
	}
	return (cleared);
}

int
istgt_lu_clear_task_ITLQ(CONN_Ptr conn, ISTGT_LU_Ptr lu, uint64_t lun, uint32_t CmdSN)
{
	int lun_i;
	int cleared = 0;

	if (lu == NULL)
		return (-1);

	if (lu->queue_depth == 0)
		return (0);
	if (lu->type != ISTGT_LU_TYPE_DISK) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}
	lun_i = istgt_lu_islun2lun(lun);

	cleared = istgt_lu_disk_queue_clear_ITLQ(conn, lu, lun_i, CmdSN);
	if (cleared < 0) {
		ISTGT_ERRLOG("LU%d: lu_disk_queue_clear_ITLQ() failed\n", lu->num);
		return (-1);
	}
	return (cleared);
}

int
istgt_lu_clear_all_task(ISTGT_LU_Ptr lu, uint64_t lun)
{
	int cleared = 0;

	if (lu == NULL)
		return (-1);

	if (lu->queue_depth == 0)
		return (0);

	if (lu->type != ISTGT_LU_TYPE_DISK) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return (-1);
	}
	cleared = istgt_lu_disk_queue_clear_all(lu, lun);
	if (cleared < 0) {
		ISTGT_ERRLOG("LU%d: lu_disk_queue_clear_all() failed\n", lu->num);
		return (-1);
	}
	return (cleared);
}

static void
luw_cleanup(void *arg)
{
	    ISTGT_WARNLOG("luworker:%p thread_exit\n", arg);
}

static void *
maintenance_io_worker(void *arg)
{
	ISTGT_LU_Ptr lu = (ISTGT_LU_Ptr) arg;
	ISTGT_LU_DISK *spec = NULL;
	ISTGT_LU_TASK_Ptr lu_task;
	int qcnt;
	int lu_num;
	int rc = 0, i, j;
	int retry_runningstate_count = 0;
	int tind = 0;
	int do_open, do_close;
	errno = 0;

	pthread_t self = pthread_self();

	i = lu->luworkers + 0;
	if (lu->maintenance_thread == self) {
		tind = i;
		snprintf(tinfo, sizeof (tinfo), "mt#%d.%ld.%d", lu->num, (uint64_t)(((uint64_t *)self)[0]), i);
		pthread_set_name_np(lu->maintenance_thread, tinfo);
		for (j = 0; j < lu->maxlun; j++) {
			spec = (ISTGT_LU_DISK *) lu->lun[j].spec;
			spec->inflight_io[i] = NULL;
		}
	}
	else
	{
		ISTGT_ERRLOG("no match for maintenance thread\n");
		goto loop_exit;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d:%d loop start before running\n", lu->num, tind);
	while (istgt_get_state(lu->istgt) != ISTGT_STATE_RUNNING) {
		if (istgt_get_state(lu->istgt) == ISTGT_STATE_EXITING ||
			istgt_get_state(lu->istgt) == ISTGT_STATE_SHUTDOWN) {
			ISTGT_ERRLOG("exit before running\n");
			return (NULL);
		}
		// ISTGT_WARNLOG("Wait for running\n");
		sleep(1);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d:%d loop start waiting\n", lu->num, tind);
		continue;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d:%d loop start\n", lu->num, tind);
	lu_num = 0;
	qcnt = 0;
	pthread_cleanup_push(luw_cleanup, (void *)spec);

	while (1) {
		if (lu->type != ISTGT_LU_TYPE_DISK)
			break;
		while (1) {
			while ((retry_runningstate_count < ISTGT_MAX_LU_RUNNING_STATE_RETRY_COUNT) &&
						(istgt_lu_get_state(lu) == ISTGT_STATE_INITIALIZED)) {
				sleep(1);
				retry_runningstate_count++;
			}

			if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
				goto loop_exit;

			if (lu_num >= lu->maxlun) {
				lu_num = 0;
			}

			spec = (ISTGT_LU_DISK *) lu->lun[lu_num].spec;
			MTX_LOCK(&spec->complete_queue_mutex);
again:
			do {
				if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING) {
					MTX_UNLOCK(&spec->complete_queue_mutex);
					goto loop_exit;
				}
				if (spec->schdler_cmd_waiting == 1) {
					if (unlikely((qcnt = istgt_queue_count(&spec->cmd_queue)) != 0))
						pthread_cond_signal(&spec->cmd_queue_cond);
					else
						istgt_schedule_blocked_requests(spec, &spec->cmd_queue, &spec->blocked_queue, 0); // 0 for command queues
					if ((qcnt = istgt_queue_count(&spec->cmd_queue)) != 0)
						pthread_cond_signal(&spec->cmd_queue_cond);
				}
				if ((qcnt = istgt_queue_count(&spec->maint_cmd_queue)) == 0)
					istgt_schedule_blocked_requests(spec, &spec->maint_cmd_queue, &spec->maint_blocked_queue, 1); // 1 for maintenance queues
				if (((qcnt = istgt_queue_count(&spec->maint_cmd_queue)) == 0) && (spec->disk_modify_work_pending == 0) && (!(spec->rsv_pending & ISTGT_RSV_READ))) {
					spec->maint_thread_waiting = 1;
					pthread_cond_wait(&spec->maint_cmd_queue_cond, &spec->complete_queue_mutex);
					spec->maint_thread_waiting = 0;

					if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING) {
						MTX_UNLOCK(&spec->complete_queue_mutex);
						goto loop_exit;
					}
				}
			} while (((qcnt = istgt_queue_count(&spec->maint_cmd_queue)) == 0) && (spec->disk_modify_work_pending == 0) && (!(spec->rsv_pending & ISTGT_RSV_READ)));

			if (unlikely(spec->disk_modify_work_pending == 1)) {
				MTX_UNLOCK(&spec->complete_queue_mutex);
				do_open = 0; do_close = 0;
				MTX_LOCK(&spec->state_mutex);
				if (spec->ex_state == ISTGT_LUN_CLOSE_PENDING) {
					do_close = 1;
					if (spec->open_waiting4close == 1) // open after the pending close
						do_open = 1;
				} else if (spec->ex_state == ISTGT_LUN_OPEN_PENDING) {
						do_open = 1;
				}
				MTX_UNLOCK(&spec->state_mutex);

				rc = 0;
				if (do_close == 1)
					rc = istgt_lu_disk_close(spec->lu, spec->lun);
				if (do_open == 1)
					rc = istgt_lu_disk_open(spec->lu, spec->lun);

				MTX_LOCK(&spec->complete_queue_mutex);
				if (rc == 0)
					spec->disk_modify_work_pending = 0;
				else {
					lu_task = istgt_queue_dequeue(&spec->maint_cmd_queue);
					if (lu_task != NULL) {
						goto execute_task;
					} else {
						MTX_UNLOCK(&spec->complete_queue_mutex);
						sleep(1);
						MTX_LOCK(&spec->complete_queue_mutex);
					}
				}
			/* Lock is not released here */
				goto again;
			}
			MTX_UNLOCK(&spec->complete_queue_mutex);
			MTX_LOCK(&spec->pr_rsv_mutex);
			if (unlikely(spec->rsv_pending & ISTGT_RSV_READ)) {
				MTX_UNLOCK(&spec->pr_rsv_mutex);
				rc = istgt_lu_disk_post_open(spec);
				if (rc != 0) {
					MTX_LOCK(&spec->complete_queue_mutex);
					lu_task = istgt_queue_dequeue(&spec->maint_cmd_queue);
					if (lu_task != NULL) {
						goto execute_task;
					} else {
						MTX_UNLOCK(&spec->complete_queue_mutex);
						sleep(1);
						if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
							goto loop_exit;
					}
				}
				MTX_LOCK(&spec->complete_queue_mutex);
				goto again;
			}
			else
				MTX_UNLOCK(&spec->pr_rsv_mutex);
			MTX_LOCK(&spec->complete_queue_mutex);
			lu_task = istgt_queue_dequeue(&spec->maint_cmd_queue);
			if (lu_task == NULL) {
				goto again;
			}
execute_task:
			MTX_LOCK(&spec->luworker_mutex[tind]);
			spec->inflight_io[tind] = lu_task;
			lu_task->conn->inflight++;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			MTX_UNLOCK(&spec->luworker_mutex[tind]);

			lu_task->lu_cmd.flags |= ISTGT_MAINT_WORKER_PICKED;
			rc = istgt_lu_disk_queue_start(lu, lu_num, tind);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: lu_disk_queue_start() %s %d\n",
						lu->num, rc == -2 ? "aborted" : "failed", rc);
			}

			lu_num++;
		}
	}
loop_exit:
	;
	pthread_cleanup_pop(0);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d loop ended\n", lu->num);
	return (NULL);
}

static void *
luworker(void *arg)
{
	ISTGT_LU_Ptr lu = (ISTGT_LU_Ptr) arg;
	ISTGT_LU_DISK *spec = NULL;
	ISTGT_LU_TASK_Ptr lu_task;
	int lu_num;
	int rc, i, j, oldtimeset = 0;
	int retry_runningstate_count = 0;
	int tind = 0;
	struct timespec now1, r, oldtime, first, second2, third;
	int id;
	unsigned long secs, nsecs;

	oldtime.tv_sec = 0;
	oldtime.tv_nsec = 0;
#define	tdiff(_s, _n, _r) {                     \
	if (unlikely(spec->do_avg == 1))	\
	{	\
		if ((_n.tv_nsec - _s.tv_nsec) < 0) {        \
			_r.tv_sec  = _n.tv_sec - _s.tv_sec-1;   \
			_r.tv_nsec = 1000000000 + _n.tv_nsec - _s.tv_nsec; \
		} else {                                    \
			_r.tv_sec  = _n.tv_sec - _s.tv_sec;     \
			_r.tv_nsec = _n.tv_nsec - _s.tv_nsec;   \
		}                                           \
		spec->avgs[id].count++;	\
		spec->avgs[id].tot_sec += _r.tv_sec;	\
		spec->avgs[id].tot_nsec += _r.tv_nsec;	\
		secs = spec->avgs[id].tot_nsec/1000000000;	\
		nsecs = spec->avgs[id].tot_nsec%1000000000;	\
		spec->avgs[id].tot_sec += secs;	\
		spec->avgs[id].tot_nsec = nsecs;	\
	}	\
}

	pthread_t self = pthread_self();
	for (i = 0; i < lu->luworkers; i++) {
		if (lu->luthread[i] == self) {
			tind = i;
			snprintf(tinfo, sizeof (tinfo), "l#%d.%ld.%d", lu->num, (uint64_t)(((uint64_t *)self)[0]), i);
			pthread_set_name_np(lu->luthread[i], tinfo);
			for (j = 0; j < lu->maxlun; j++) {
				spec = (ISTGT_LU_DISK *) lu->lun[j].spec;
				spec->inflight_io[i] = NULL;
			}
			break;
		}
	}

	while (istgt_get_state(lu->istgt) != ISTGT_STATE_RUNNING) {
		if (istgt_get_state(lu->istgt) == ISTGT_STATE_EXITING ||
			istgt_get_state(lu->istgt) == ISTGT_STATE_SHUTDOWN) {
			ISTGT_ERRLOG("exit before running\n");
			return (NULL);
		}
		sleep(1);
		continue;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d loop start\n", lu->num);
	lu_num = 0;
	for (j = 0; j < lu->maxlun; j++) {
		spec = (ISTGT_LU_DISK *) lu->lun[j].spec;
		MTX_LOCK(&spec->luworker_mutex[tind]);
		if (likely(spec->inflight_io[tind] == NULL)) {
			MTX_LOCK(&spec->schdler_mutex);
			if (likely(BGET32(spec->lu_free_matrix[(tind >> 5)], (tind & 31)) == 0)) {
				BSET32(spec->lu_free_matrix[(tind >> 5)], (tind & 31));
				pthread_cond_signal(&spec->schdler_cond);
			} else {
				spec->error_count++;
				ISTGT_ERRLOG("LU%d: Error thread %d\n", lu->num, tind);
			}
			MTX_UNLOCK(&spec->schdler_mutex);
		} else {
			spec->error_count++;
			ISTGT_ERRLOG("LU%d: Error thread %d: inflight is not NULL \n", lu->num, tind);
		}
		MTX_UNLOCK(&spec->luworker_mutex[tind]);
	}

	while (1) {
		if (lu->type != ISTGT_LU_TYPE_DISK)
			break;
		while (1) {
				/*
				 * Change necessitated by kill -HUP of istgt for lu_reload case:
				 * The lu state is set to RUNNING by reload_delete once all threads
				 * have been created.
				 * But this causes a race when already spawned luworkers arrive
				 * at this check point to find the state is non-RUNNING; the threads
				 * ie. luworkers exit (usually due to the timing associated with
				 * pthread_create( luworker ), one of the luworkers manages to spawn
				 * and correctly find the state as RUNNING at this point so it continues
				 * while other 5 (default value of 6 luworkers per lun) exit
				 *
				 * To prevent this, lets retry giving more time for all threads to be
				 * spawned and arrive here, so caller istgt_lu_reload_update can set the
				 * lu state to RUNNING.
				 *
				 * checking for state RUNNING leads to the code taking too much time
				 * in the shutdown path. So simply check for INITIALIZED, expecting
				 * the transition to RUNNING. If it times out without getting the
				 * RUNNING state, bail
				 */
			clock_gettime(clockid, &first);
			while ((retry_runningstate_count < ISTGT_MAX_LU_RUNNING_STATE_RETRY_COUNT) &&
						(istgt_lu_get_state(lu) == ISTGT_STATE_INITIALIZED)) {
				sleep(1);
				retry_runningstate_count++;
			}

			if ((istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING))
				goto loop_exit;

			id = 14;
			clock_gettime(clockid, &second2);
			tdiff(first, second2, r);
			if (lu_num >= lu->maxlun) {
				lu_num = 0;
			}

			spec = (ISTGT_LU_DISK *) lu->lun[lu_num].spec;
			MTX_LOCK(&spec->luworker_mutex[tind]);
			while (spec->inflight_io[tind] == NULL) {
				if (unlikely(BGET32(spec->lu_free_matrix[(tind >> 5)], (tind & 31)) == 0))
				{
					MTX_LOCK(&spec->schdler_mutex);
					if (BGET32(spec->lu_free_matrix[(tind >> 5)], (tind & 31)) == 0) {
						spec->error_count++;
						ISTGT_ERRLOG("LU%d: Error thread %d busy\n", lu->num, tind);
						BSET32(spec->lu_free_matrix[(tind >> 5)], (tind & 31));
					}
					MTX_UNLOCK(&spec->schdler_mutex);
				}
				clock_gettime(clockid, &first);
				if (unlikely(spec->do_avg == 1))
				{
					spec->avgs[8].count++;
					spec->avgs[9].count++;
					spec->avgs[11].count++;

					spec->avgs[8].tot_sec += istgt_queue_count(&spec->cmd_queue);
					spec->avgs[9].tot_sec += istgt_queue_count(&spec->blocked_queue);
					spec->avgs[11].tot_sec += spec->inflight;
				}
				if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
				{
					MTX_UNLOCK(&spec->luworker_mutex[tind]);
					goto loop_exit;
				}
				spec->luworker_waiting[tind] = 1;
				pthread_cond_wait(&spec->luworker_cond[tind], &spec->luworker_mutex[tind]);
				spec->luworker_waiting[tind] = 0;

				if (unlikely(spec->do_avg == 1))
				{
					spec->avgs[8].tot_nsec += istgt_queue_count(&spec->cmd_queue);
					spec->avgs[9].tot_nsec += istgt_queue_count(&spec->blocked_queue);
					spec->avgs[11].tot_nsec += spec->inflight;
				}
				clock_gettime(clockid, &second2);
				id = 17;
				tdiff(first, second2, r);
				MTX_UNLOCK(&spec->luworker_mutex[tind]);

				if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
					goto loop_exit;
				MTX_LOCK(&spec->luworker_mutex[tind]);
			}
			lu_task = spec->inflight_io[tind];
			MTX_UNLOCK(&spec->luworker_mutex[tind]);

			if (unlikely(lu->limit_q_size != 0 &&
				tind == 0))
				usleep(50000); // 50ms

			clock_gettime(clockid, &third);
			id = 16;
			tdiff(second2, third, r);
#ifdef REPLICATION
			lu_task->lu_cmd.luworkerindx = tind;
#endif
			lu_task->lu_cmd.flags |= ISTGT_WORKER_PICKED;
			rc = istgt_lu_disk_queue_start(lu, lu_num, tind);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: lu_disk_queue_start() %s %d\n",
						lu->num, rc == -2 ? "aborted" : "failed", rc);
			}

			clock_gettime(clockid, &now1);
			id = 12;
			tdiff(third, now1, r);
			if (oldtimeset == 1)
			{
				id = 13;
				tdiff(oldtime, third, r);
			}
			oldtime = now1;
			oldtimeset = 1;

			lu_num++;
		}
	}
loop_exit:
	;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d loop ended\n", lu->num);
	return (NULL);
}

static void *
luscheduler(void *arg)
{
	ISTGT_LU_Ptr lu = (ISTGT_LU_Ptr) arg;
	ISTGT_LU_DISK *spec = (ISTGT_LU_DISK *) lu->lun[0].spec;
	ISTGT_LU_TASK_Ptr lu_task;
	int retry_runningstate_count = 0;
	struct timespec sch1, sch2, sch3, sch4, sch5, sch6, r;
	int qcnt = 0, ind = 0;
	int worker_id;
	int id, found_worker = 0;
	unsigned long secs, nsecs;
#define	tdiff(_s, _n, _r) {                     \
	if (unlikely(spec->do_avg == 1))	\
	{	\
		if ((_n.tv_nsec - _s.tv_nsec) < 0) {        \
			_r.tv_sec  = _n.tv_sec - _s.tv_sec-1;   \
			_r.tv_nsec = 1000000000 + _n.tv_nsec - _s.tv_nsec; \
		} else {                                    \
			_r.tv_sec  = _n.tv_sec - _s.tv_sec;     \
			_r.tv_nsec = _n.tv_nsec - _s.tv_nsec;   \
		}                                           \
		spec->avgs[id].count++;	\
		spec->avgs[id].tot_sec += _r.tv_sec;	\
		spec->avgs[id].tot_nsec += _r.tv_nsec;	\
		secs = spec->avgs[id].tot_nsec/1000000000;	\
		nsecs = spec->avgs[id].tot_nsec%1000000000;	\
		spec->avgs[id].tot_sec += secs;	\
		spec->avgs[id].tot_nsec = nsecs;	\
	}	\
}


	pthread_t sf = pthread_self();
	snprintf(tinfo, sizeof (tinfo), "sh#%d.%ld.%d", lu->num, (uint64_t)(((uint64_t *)sf)[0]), 0);
	pthread_set_name_np(lu->schdler_thread, tinfo);
	while (istgt_get_state(lu->istgt) != ISTGT_STATE_RUNNING) {
		if (istgt_get_state(lu->istgt) == ISTGT_STATE_EXITING ||
			istgt_get_state(lu->istgt) == ISTGT_STATE_SHUTDOWN) {
			ISTGT_ERRLOG("exit before running\n");
			return (NULL);
		}
		// ISTGT_WARNLOG("Wait for running\n");
		sleep(1);
		continue;
	}
	while (1) {
		/*
		 * Take the scheduler mutex lock, find if any of the luworkers are free from spec->lu_free_matrix.
		 * If so find the index of the least significant bit set in the spec->lu_free_matrix to get the
		 * luworker's id. If the all the luworkers are busy, then wait on schdler_cond variable.
		 */
start:
		while ((retry_runningstate_count < ISTGT_MAX_LU_RUNNING_STATE_RETRY_COUNT) &&
				(istgt_lu_get_state(lu) == ISTGT_STATE_INITIALIZED)) {
			sleep(1);
			retry_runningstate_count++;
		}

		if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
			goto loop_exit;

		clock_gettime(clockid, &sch1);
		ind = 0;
		MTX_LOCK(&spec->schdler_mutex);
		while (ind <= (ISTGT_MAX_NUM_LUWORKERS/32)) {
			found_worker = ((spec->lu_free_matrix[ind] != 0) &&
					((lu->limit_q_size == 0) || ((ind == 0) && ((spec->lu_free_matrix[ind] & 1) == 1))))
					? 1 : 0;
			if (found_worker == 0) {
				if (ind == (ISTGT_MAX_NUM_LUWORKERS/32)) {
					if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
					{
						MTX_UNLOCK(&spec->schdler_mutex);
						goto loop_exit;
					}
					spec->schdler_waiting = 1;
					pthread_cond_wait(&spec->schdler_cond, &spec->schdler_mutex);
					spec->schdler_waiting = 0;
					if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
					{
						MTX_UNLOCK(&spec->schdler_mutex);
						goto loop_exit;
					}
					ind = 0;
				} else
					ind++;
			} else {
				break;
			}
		}
		MTX_UNLOCK(&spec->schdler_mutex);
next_lu_worker:
		if ((worker_id = ffs(spec->lu_free_matrix[ind])) <= 0)
			goto start;
		worker_id--; // Bits are numbered starting at 1, the least significant bit.
		worker_id += (ind<<5);

		if (worker_id >= spec->luworkers) // worker_id can't be >= luworkers
			goto start;

		clock_gettime(clockid, &sch2);
		id = 15;
		tdiff(sch1, sch2, r);

		clock_gettime(clockid, &sch4);

		/*
		 * Try to schedule the blocked request and get the cmd_queue count,
		 * if count is zero wait on spec->cmd_queue_cond. If count > 0, dequeue the
		 * first request in the queue and assign it the luworker that is free and unset
		 * the bit in the free_matrix
		 */
		clock_gettime(clockid, &sch5);
		id = 21;
		tdiff(sch4, sch5, r);
		MTX_LOCK(&spec->complete_queue_mutex);
		do {
			if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
			{
				MTX_UNLOCK(&spec->complete_queue_mutex);
				goto loop_exit;
			}
			if (spec->maint_thread_waiting == 1)
			{
				if (unlikely((qcnt = istgt_queue_count(&spec->maint_cmd_queue)) != 0))
					pthread_cond_signal(&spec->maint_cmd_queue_cond);
				else
					istgt_schedule_blocked_requests(spec, &spec->maint_cmd_queue, &spec->maint_blocked_queue, 1); // 1 for maint queues
				if ((qcnt = istgt_queue_count(&spec->maint_cmd_queue)) != 0)
					pthread_cond_signal(&spec->maint_cmd_queue_cond);
			}

			if ((qcnt = istgt_queue_count(&spec->cmd_queue)) == 0)
				istgt_schedule_blocked_requests(spec, &spec->cmd_queue, &spec->blocked_queue, 0); // 0 for cmd queues
			if ((qcnt = istgt_queue_count(&spec->cmd_queue)) == 0) {
				spec->schdler_cmd_waiting = 1;
				pthread_cond_wait(&spec->cmd_queue_cond, &spec->complete_queue_mutex);
				spec->schdler_cmd_waiting = 0;
				if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
				{
					MTX_UNLOCK(&spec->complete_queue_mutex);
					goto loop_exit;
				}
			}
		} while ((qcnt = istgt_queue_count(&spec->cmd_queue)) == 0);
		clock_gettime(clockid, &sch6);
		id = 22;
		tdiff(sch5, sch6, r);
		if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING)
		{
			MTX_UNLOCK(&spec->complete_queue_mutex);
			goto loop_exit;
		}
		if (qcnt < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_queue_count() failed:%d\n",
			    lu->num, qcnt);
		}
		lu_task = istgt_queue_dequeue(&spec->cmd_queue);
		clock_gettime(clockid, &sch3);
		id = 19;
		tdiff(sch5, sch3, r);

		MTX_LOCK(&spec->luworker_mutex[worker_id]);
		if (likely(spec->inflight_io[worker_id] == NULL)) {
			spec->inflight_io[worker_id] = lu_task;
			lu_task->conn->inflight++;
			lu_task->lu_cmd.flags |= ISTGT_SCHEDULED;
			MTX_UNLOCK(&spec->complete_queue_mutex);
		} else {
			spec->error_count++;
			istgt_queue_enqueue_first(&spec->cmd_queue, lu_task);
			MTX_UNLOCK(&spec->complete_queue_mutex);
			MTX_UNLOCK(&spec->luworker_mutex[worker_id]);
			if ((spec->lu_free_matrix[ind] > 0) &&
				(likely(lu->limit_q_size == 0)))
				goto next_lu_worker;
			else
				continue;
		}

		MTX_LOCK(&spec->schdler_mutex);
		spec->inflight++;
		BUNSET32(spec->lu_free_matrix[(worker_id >> 5)], (worker_id&31));
		MTX_UNLOCK(&spec->schdler_mutex);
		if (spec->luworker_waiting[worker_id]) {
			MTX_UNLOCK(&spec->luworker_mutex[worker_id]);
			pthread_cond_signal(&spec->luworker_cond[worker_id]);
		} else {
			MTX_UNLOCK(&spec->luworker_mutex[worker_id]);
		}
		if ((spec->lu_free_matrix[ind] > 0) &&
			(likely(lu->limit_q_size == 0)))
			goto next_lu_worker;
	}
loop_exit:
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "scheduler loop ended\n");
	return (NULL);
}
