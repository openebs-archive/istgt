/*
 * Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	notice, this list of conditions and the following disclaimer in the
 *	documentation and/or other materials provided with the distribution.
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

#include <stdint.h>
#include <inttypes.h>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/uio.h>

#ifdef __FreeBSD__
#include <sys/event.h>
#define	DIO_ISCSIWR _IOW('d', 131, struct istgt_detail)
#endif

#ifdef __linux__
// #include <kqueue/sys/event.h>

#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#endif

#include <fcntl.h>
#include <time.h>

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_conf.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_crc32c.h"
#include "istgt_md5.h"
#include "istgt_iscsi.h"
#include "istgt_iscsi_param.h"
#include "istgt_lu.h"
#include "istgt_proto.h"
#include "istgt_scsi.h"
#include "istgt_queue.h"

#include <netinet/in.h>

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

/* according to RFC1982 */
#define	SN32_CMPMAX (((uint32_t)1U) << (32 - 1))
#define	SN32_LT(S1, S2) \
	(((uint32_t)(S1) != (uint32_t)(S2))				\
		&& (((uint32_t)(S1) < (uint32_t)(S2)			\
			&& ((uint32_t)(S2) - (uint32_t)(S1) < SN32_CMPMAX))	\
		|| ((uint32_t)(S1) > (uint32_t)(S2)			\
			&& ((uint32_t)(S1) - (uint32_t)(S2) > SN32_CMPMAX))))
#define	SN32_GT(S1, S2) \
	(((uint32_t)(S1) != (uint32_t)(S2))				\
		&& (((uint32_t)(S1) < (uint32_t)(S2)			\
			&& ((uint32_t)(S2) - (uint32_t)(S1) > SN32_CMPMAX))	\
		|| ((uint32_t)(S1) > (uint32_t)(S2)			\
			&& ((uint32_t)(S1) - (uint32_t)(S2) < SN32_CMPMAX))))

#define	POLLWAIT 5000
#define	MAX_MCSREVWAIT (10 * 1000)
#define	ISCMDQ 8
#define	ISCSI_SOCKET "/dev/eventctl"

enum iscsi_log {
		TYPE_LOGIN,
		TYPE_LOGOUT,
		TYPE_CONNBRK
};
struct istgt_detail {

	enum iscsi_log login_status;
		/* IP address */
		char initiator_addr[64];
		char target_addr[64];

		/* Initiator/Target name */
		char initiator_name[256];
		char target_name[256];
};

#define	ISCSI_GETVAL(PARAMS, KEY) \
	istgt_iscsi_param_get_val((PARAMS), (KEY))
#define	ISCSI_EQVAL(PARAMS, KEY, VAL) \
	istgt_iscsi_param_eq_val((PARAMS), (KEY), (VAL))
#define	ISCSI_DELVAL(PARAMS, KEY) \
	istgt_iscsi_param_del((PARAMS), (KEY))
#define	ISCSI_ADDVAL(PARAMS, KEY, VAL, LIST, TYPE) \
	istgt_iscsi_param_add((PARAMS), (KEY), (VAL), (LIST), (TYPE))

static int g_nconns;
static int g_max_connidx;
static CONN_Ptr *g_conns;
static pthread_mutex_t g_conns_mutex;

static uint16_t g_last_tsih;
static pthread_mutex_t g_last_tsih_mutex;

static int istgt_add_transfer_task(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);
static void istgt_clear_transfer_task(CONN_Ptr conn, uint32_t CmdSN);
static void istgt_clear_all_transfer_task(CONN_Ptr conn);
static int istgt_iscsi_send_r2t(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, int offset, int len, uint32_t transfer_tag, uint32_t *R2TSN);
static int istgt_append_sess(CONN_Ptr conn, uint64_t isid, uint16_t tsih, uint16_t cid);
static void istgt_remove_conn(CONN_Ptr conn);
static int istgt_iscsi_drop_all_conns(CONN_Ptr conn);
static int istgt_iscsi_drop_old_conns(CONN_Ptr conn);
static void ioctl_call(CONN_Ptr, enum iscsi_log);

// _verb_stat SCSIstat_0min[SCSI_ARYSZ]
// _verb_stat SCSIstat_1min[SCSI_ARYSZ]
_verb_stat SCSIstat_rest[SCSI_ARYSZ] = { {0, 0, 0} };
_verb_istat ISCSIstat_rest[ISCSI_ARYSZ] = { {0, 0, 0} };
extern int iscsi_ops_indx_table[256];

/* Switch to use readv/writev (assume blocking) */
#define	ISTGT_USE_IOVEC

#define	MATCH_DIGEST_WORD(BUF, CRC32C) \
	    (((((uint32_t) *((uint8_t *)(BUF)+0)) << 0)		\
		| (((uint32_t) *((uint8_t *)(BUF)+1)) << 8)		\
		| (((uint32_t) *((uint8_t *)(BUF)+2)) << 16)	\
		| (((uint32_t) *((uint8_t *)(BUF)+3)) << 24))	\
		== (CRC32C))

#define	MAKE_DIGEST_WORD(BUF, CRC32C) \
	   (((*((uint8_t *)(BUF)+0)) = (uint8_t)((uint32_t)(CRC32C) >> 0)), \
		((*((uint8_t *)(BUF)+1)) = (uint8_t)((uint32_t)(CRC32C) >> 8)), \
		((*((uint8_t *)(BUF)+2)) = (uint8_t)((uint32_t)(CRC32C) >> 16)), \
		((*((uint8_t *)(BUF)+3)) = (uint8_t)((uint32_t)(CRC32C) >> 24)))

#if 0
static int
istgt_match_digest_word(const uint8_t *buf, uint32_t crc32c)
{
	uint32_t l;

	l = (buf[0] & 0xffU) << 0;
	l |= (buf[1] & 0xffU) << 8;
	l |= (buf[2] & 0xffU) << 16;
	l |= (buf[3] & 0xffU) << 24;
	return (l == crc32c);
}

static uint8_t *
istgt_make_digest_word(uint8_t *buf, size_t len, uint32_t crc32c)
{
	if (len < ISCSI_DIGEST_LEN)
		return (NULL);

	buf[0] = (crc32c >> 0) & 0xffU;
	buf[1] = (crc32c >> 8) & 0xffU;
	buf[2] = (crc32c >> 16) & 0xffU;
	buf[3] = (crc32c >> 24) & 0xffU;
	return (buf);
}
#endif



extern clockid_t clockid;
static int
istgt_iscsi_read_pdu(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	struct iovec iovec[4]; /* AHS+HD+DATA+DD */
	uint32_t crc32c;
	int nbytes;
	int total_ahs_len;
	int data_len;
	int adata_len = 0;
	int segment_len;
	int total;
	int rc;
	int i;
	struct timespec  now;
	int opcode;
	pdu->ahs = NULL;
	pdu->total_ahs_len = 0;
	pdu->data = NULL;
	pdu->data_segment_len = 0;
	pdu->opcode = -1;
	total = 0;

	/* BHS (require for all PDU) */
	// ISTGT_TRACELOG(ISTGT_TRACE_NET, "BHS read %d\n",
	//   ISCSI_BHS_LEN);
	errno = 0;
	clock_gettime(clockid, &pdu->start0);
	rc = recv(conn->sock, &pdu->bhs, ISCSI_BHS_LEN, MSG_WAITALL);
	if (rc < 0) {
		clock_gettime(clockid, &now);
		if (errno == ECONNRESET) {
			ISTGT_WARNLOG("Connection reset by peer (%s,time=%lu)\n",
				conn->initiator_name, (unsigned long)(now.tv_sec - pdu->start0.tv_sec));
			conn->state = CONN_STATE_EXITING;
		} else if (errno == ETIMEDOUT) {
			ISTGT_WARNLOG("Operation timed out (%s,time=%lu)\n",
				conn->initiator_name, (unsigned long)(now.tv_sec - pdu->start0.tv_sec));
			conn->state = CONN_STATE_EXITING;
		} else {
			ISTGT_ERRLOG("iscsi_read() failed (errno=%d,%s,time=%lu)\n",
				errno, conn->initiator_name, (unsigned long)(now.tv_sec - pdu->start0.tv_sec));
		}
		return (-1);
	}
	if (rc == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "recv() EOF (%s)\n",
			conn->initiator_name);
		conn->state = CONN_STATE_EXITING;
		return (-1);
	}
	if (rc != ISCSI_BHS_LEN) {
		ISTGT_ERRLOG("invalid BHS length (%d,%s)\n", rc, conn->initiator_name);
		return (-1);
	}
	total += ISCSI_BHS_LEN;

	opcode = BGET8W(&pdu->bhs.opcode, 5, 6);
	pdu->opcode = (uint8_t)opcode;
	ISCSIstat_rest[ iscsi_ops_indx_table[pdu->opcode] ].opcode = opcode;
	++ISCSIstat_rest[ iscsi_ops_indx_table[pdu->opcode] ].pdu_read;

	/* AHS */
	total_ahs_len = DGET8(&pdu->bhs.total_ahs_len);
	if (total_ahs_len != 0) {
		pdu->ahs = xmalloc(ISCSI_ALIGN((4 * total_ahs_len)));
		pdu->total_ahs_len = total_ahs_len;
		total += (4 * total_ahs_len);
	} else {
		pdu->ahs = NULL;
		pdu->total_ahs_len = 0;
	}
	iovec[0].iov_base = pdu->ahs;
	iovec[0].iov_len = 4 * pdu->total_ahs_len;

	/* Header Digest */
	iovec[1].iov_base = pdu->header_digest;
	if (conn->header_digest) {
		iovec[1].iov_len = ISCSI_DIGEST_LEN;
		total += ISCSI_DIGEST_LEN;
	} else {
		iovec[1].iov_len = 0;
	}

	/* Data Segment */
	data_len = DGET24(&pdu->bhs.data_segment_len[0]);
	if (data_len != 0) {
		if (conn->sess == NULL) {
			segment_len = DEFAULT_FIRSTBURSTLENGTH;
		} else {
			segment_len = conn->MaxRecvDataSegmentLength;
		}
		if (data_len > segment_len) {
			ISTGT_ERRLOG("Data(%d) > Segment(%d)\n",
				data_len, segment_len);
			return (-1);
		}
		// pdu->data = xmalloc(ISCSI_ALIGN(segment_len));
		pdu->data_segment_len = data_len;
		adata_len = ISCSI_ALIGN(data_len);
		pdu->data = xmalloc(adata_len+400);
		total += adata_len;
	} else {
		pdu->data = NULL;
		pdu->data_segment_len = 0;
	}
	iovec[2].iov_base = pdu->data;
	iovec[2].iov_len = ISCSI_ALIGN(pdu->data_segment_len);

	/* Data Digest */
	iovec[3].iov_base = pdu->data_digest;
	if (conn->data_digest && data_len != 0) {
		iovec[3].iov_len = ISCSI_DIGEST_LEN;
		total += ISCSI_DIGEST_LEN;
	} else {
		iovec[3].iov_len = 0;
	}

	/* read all bytes to iovec */
	nbytes = total - ISCSI_BHS_LEN;
	if (nbytes > 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "PDU read %d (data:%ld ahs:%lu)\n", nbytes, pdu->data_segment_len, pdu->total_ahs_len);
	}
	clock_gettime(clockid, &pdu->start);
	errno = 0;
	while (nbytes > 0) {
		rc = readv(conn->sock, &iovec[0], 4);
		if (rc < 0) {
			clock_gettime(clockid, &now); // time(NULL);
			ISTGT_ERRLOG("readv() failed (%d,errno=%d,%s,time=%lu)\n",
				rc, errno, conn->initiator_name, (unsigned long)(now.tv_sec - pdu->start.tv_sec));
			return (-1);
		}
		if (rc == 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET, "readv() EOF (%s)\n",
				conn->initiator_name);
			conn->state = CONN_STATE_EXITING;
			return (-1);
		}
		nbytes -= rc;
		if (nbytes == 0)
			break;
		/* adjust iovec length */
		for (i = 0; i < 4; i++) {
			if (iovec[i].iov_len != 0 && iovec[i].iov_len > (size_t)rc) {
				iovec[i].iov_base
					= (void *) (((uintptr_t)iovec[i].iov_base) + rc);
				iovec[i].iov_len -= rc;
				break;
			} else {
				rc -= iovec[i].iov_len;
				iovec[i].iov_len = 0;
			}
		}
	}

	/* check digest */
	if (conn->header_digest) {
		if (total_ahs_len == 0) {
			crc32c = istgt_crc32c((uint8_t *) &pdu->bhs,
				ISCSI_BHS_LEN);
		} else {
			int upd_total = 0;
			crc32c = ISTGT_CRC32C_INITIAL;
			crc32c = istgt_update_crc32c((uint8_t *) &pdu->bhs,
				ISCSI_BHS_LEN, crc32c);
			upd_total += ISCSI_BHS_LEN;
			crc32c = istgt_update_crc32c((uint8_t *) pdu->ahs,
				(4 * total_ahs_len), crc32c);
			upd_total += (4 * total_ahs_len);
			crc32c = istgt_fixup_crc32c(upd_total, crc32c);
			crc32c = crc32c ^ ISTGT_CRC32C_XOR;
		}
		rc = MATCH_DIGEST_WORD(pdu->header_digest, crc32c);
		if (rc == 0) {
			ISTGT_ERRLOG("header digest error (%s)\n", conn->initiator_name);
			return (-1);
		}
	}
	if (conn->data_digest && data_len != 0) {
		crc32c = istgt_crc32c(pdu->data, ISCSI_ALIGN(data_len));
		rc = MATCH_DIGEST_WORD(pdu->data_digest, crc32c);
		if (rc == 0) {
			ISTGT_ERRLOG("data digest error (%s)\n", conn->initiator_name);
			return (-1);
		}
	}

	return (total);
}

static int istgt_iscsi_write_pdu_internal(CONN_Ptr conn, ISCSI_PDU_Ptr pdu);
static int istgt_iscsi_write_pdu_queue(CONN_Ptr conn, ISCSI_PDU_Ptr pdu, int req_type, int I_bit);

uint8_t istgt_get_sleep_val(ISTGT_LU_DISK *spec);

static int istgt_update_pdu(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	uint8_t *rsp;
	uint32_t task_tag;
	int opcode;
	int I_bit;
	ISTGT_LU_DISK *spec;
	uint8_t sleep_val;

	I_bit = lu_cmd->I_bit;
	rsp = (uint8_t *) &lu_cmd->pdu->bhs;
	opcode = BGET8W(&rsp[0], 5, 6);
	task_tag = DGET32(&rsp[16]);
	if ((opcode == ISCSI_OP_R2T)
		|| (opcode == ISCSI_OP_NOPIN && task_tag == 0xffffffffU)) {

		if (opcode == ISCSI_OP_R2T) {
			spec = (ISTGT_LU_DISK *)(conn->sess->lu->lun[0].spec);
			if (spec->error_inject & SEND_R2T)
			if (spec->inject_cnt > 0) {
				spec->inject_cnt--;
				sleep_val = istgt_get_sleep_val(spec);
				sleep(sleep_val);
			}
		}

		SESS_MTX_LOCK(conn);
		DSET32(&rsp[24], conn->StatSN);
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);
	} else if ((opcode == ISCSI_OP_TASK_RSP)
		|| (opcode == ISCSI_OP_NOPIN && task_tag != 0xffffffffU)) {

		if (opcode == ISCSI_OP_TASK_RSP) {
			spec = (ISTGT_LU_DISK *)(conn->sess->lu->lun[0].spec);
			if (spec->error_inject & SEND_TASK_RSP)
			if (spec->inject_cnt > 0) {
				spec->inject_cnt--;
				sleep_val = istgt_get_sleep_val(spec);
				sleep(sleep_val);
			}
		}

		SESS_MTX_LOCK(conn);
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		if (I_bit == 0) {
			if (likely(lu_cmd->lu->limit_q_size == 0 || ((int)(conn->sess->MaxCmdSN - conn->sess->ExpCmdSN) < lu_cmd->lu->limit_q_size))) {
				conn->sess->ExpCmdSN++;
				conn->sess->MaxCmdSN++;
				conn->sess->MaxCmdSN_local++;
				if (unlikely((conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local) && (lu_cmd->lu->limit_q_size == 0))) {
					ISTGT_LOG("conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local in tsk_rsp\n");
					conn->sess->MaxCmdSN = conn->sess->MaxCmdSN_local;
				}
			} else {
				conn->sess->ExpCmdSN++;
				conn->sess->MaxCmdSN_local++;
			}
		}
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);
	}
	return (0);
}

static int
istgt_iscsi_write_pdu(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	return (istgt_iscsi_write_pdu_queue(conn, pdu, ISTGT_LU_TASK_REQPDU, 0));
}

static int
istgt_iscsi_write_pdu_upd(CONN_Ptr conn, ISCSI_PDU_Ptr pdu, int I_bit)
{
	return (istgt_iscsi_write_pdu_queue(conn, pdu, ISTGT_LU_TASK_REQUPDPDU, I_bit));
}

static int
istgt_iscsi_write_pdu_queue(CONN_Ptr conn, ISCSI_PDU_Ptr pdu, int req_type, int I_bit)
{
	int rc = 0;
	ISTGT_QUEUE_Ptr r_ptr = NULL;
	ISTGT_LU_TASK_Ptr lu_task;
	ISCSI_PDU_Ptr src_pdu, dst_pdu;
	uint8_t *cp;
	int total_ahs_len;
	int data_len;
	int alloc_len;
	int total;

	cp = (uint8_t *) &pdu->bhs;
	total_ahs_len = DGET8(&cp[4]);
	data_len = DGET24(&cp[5]);
	total = 0;

#if 0
	ISTGT_LOG("W:PDU OP=%x, tag=%x, ExpCmdSN=%u, MaxCmdSN=%u\n",
		DGET8(&cp[0]), DGET32(&cp[32]), DGET32(&cp[28]), DGET32(&cp[32]));
#endif
	/* allocate for queued PDU */
	alloc_len = ISCSI_ALIGN(sizeof (*lu_task));
	alloc_len += ISCSI_ALIGN(sizeof (*lu_task->lu_cmd.pdu));
	// alloc_len += ISCSI_ALIGN(4 * total_ahs_len);
	// alloc_len += ISCSI_ALIGN(data_len);
	lu_task = xmalloc(alloc_len);
	memset(lu_task, 0, alloc_len);
	lu_task->lu_cmd.pdu = (ISCSI_PDU_Ptr) ((uintptr_t)lu_task
		+ ISCSI_ALIGN(sizeof (*lu_task)));
	// lu_task->lu_cmd.pdu->ahs = (ISCSI_AHS *) ((uintptr_t)lu_task->lu_cmd.pdu
	//	+ ISCSI_ALIGN(sizeof (*lu_task->lu_cmd.pdu)));
	// lu_task->lu_cmd.pdu->data = (uint8_t *) ((uintptr_t)lu_task->lu_cmd.pdu->ahs
	//	+ ISCSI_ALIGN(4 * total_ahs_len));

	/* specify type and self conn */
	// lu_task->type = ISTGT_LU_TASK_REQPDU;
	lu_task->type = req_type;
	lu_task->conn = conn;

	/* extra flags */
	lu_task->lu_cmd.I_bit = I_bit;

	clock_gettime(clockid, &lu_task->lu_cmd.times[0]);
	lu_task->lu_cmd.caller[0] = 't';
	lu_task->lu_cmd._andx = 0;
	lu_task->lu_cmd._lst = 0;
	lu_task->lu_cmd.infdx = 0;
	lu_task->lu_cmd.infcpy = 0;

	// aj ++ISCSIstat_rest[ iscsi_ops_indx_table[opcode] ].res_start;

	/* copy PDU structure */
	src_pdu = pdu;
	dst_pdu = lu_task->lu_cmd.pdu;
	memcpy(&dst_pdu->bhs, &src_pdu->bhs, ISCSI_BHS_LEN);
	total += ISCSI_BHS_LEN;
	if (total_ahs_len != 0) {
		dst_pdu->ahs = src_pdu->ahs;
		dst_pdu->total_ahs_len = (4 * total_ahs_len);
		total += (4 * total_ahs_len);
		src_pdu->ahs = NULL; src_pdu->total_ahs_len = 0;
	} else {
		dst_pdu->ahs = NULL; dst_pdu->total_ahs_len = 0;
	}
	if (conn->header_digest) {
		dst_pdu->header_digestX = src_pdu->header_digestX;
		total += ISCSI_DIGEST_LEN;
	} else {
		dst_pdu->header_digestX = 0;
	}
	if (data_len != 0) {
		dst_pdu->data_segment_len = data_len;
		dst_pdu->data = src_pdu->data;
		src_pdu->data = NULL; src_pdu->data_segment_len = 0;
		total += data_len;
	} else {
		dst_pdu->data = NULL; dst_pdu->data_segment_len = 0;
	}
	if (conn->data_digest && data_len != 0) {
		dst_pdu->data_digestX = src_pdu->data_digestX;
		total += ISCSI_DIGEST_LEN;
	} else {
		dst_pdu->data_digestX = 0;
	}

	/* insert to queue */
	MTX_LOCK(&conn->result_queue_mutex);
	r_ptr = istgt_queue_enqueue(&conn->result_queue, lu_task);
	if (r_ptr == NULL) {
		MTX_UNLOCK(&conn->result_queue_mutex);
		ISTGT_ERRLOG("queue_enqueue() failed\n");
		return (-1);
	}
	/* notify to thread */
	if (conn->sender_waiting == 1)
		rc = pthread_cond_broadcast(&conn->result_queue_cond);
	MTX_UNLOCK(&conn->result_queue_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_broadcast() failed\n");
		return (-1);
	}

	/* total bytes should be sent in queue */
	rc = total;
	return (rc);
}

static int
istgt_iscsi_write_pdu_internal(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	struct iovec iovec[5]; /* BHS+AHS+HD+DATA+DD */
	uint8_t *cp;
	uint32_t crc32c;
	time_t start, now;
	int nbytes;
	int enable_digest;
	int opcode;
	int total_ahs_len;
	int data_len;
	int total;
	int rc;
	int i;

	cp = (uint8_t *) &pdu->bhs;
	total_ahs_len = DGET8(&cp[4]);
	data_len = DGET24(&cp[5]);
	total = 0;

	enable_digest = 1;
	opcode = BGET8W(&cp[0], 5, 6);
	if (opcode == ISCSI_OP_LOGIN_RSP) {
		/* this PDU should be sent without digest */
		enable_digest = 0;
	}
	ISCSIstat_rest[ iscsi_ops_indx_table[(uint8_t)opcode] ].opcode = (uint8_t)opcode;
	++ISCSIstat_rest[ iscsi_ops_indx_table[(uint8_t)opcode] ].pdu_sent;

	/* BHS */
	iovec[0].iov_base = &pdu->bhs;
	iovec[0].iov_len = ISCSI_BHS_LEN;
	total += ISCSI_BHS_LEN;

	/* AHS */
	iovec[1].iov_base = pdu->ahs;
	iovec[1].iov_len = 4 * total_ahs_len;
	total += (4 * total_ahs_len);

	/* Header Digest */
	iovec[2].iov_base = pdu->header_digest;
	if (enable_digest && conn->header_digest) {
		if (total_ahs_len == 0) {
			crc32c = istgt_crc32c((uint8_t *) &pdu->bhs,
				ISCSI_BHS_LEN);
		} else {
			int upd_total = 0;
			crc32c = ISTGT_CRC32C_INITIAL;
			crc32c = istgt_update_crc32c((uint8_t *) &pdu->bhs,
				ISCSI_BHS_LEN, crc32c);
			upd_total += ISCSI_BHS_LEN;
			crc32c = istgt_update_crc32c((uint8_t *) pdu->ahs,
				(4 * total_ahs_len), crc32c);
			upd_total += (4 * total_ahs_len);
			crc32c = istgt_fixup_crc32c(upd_total, crc32c);
			crc32c = crc32c ^ ISTGT_CRC32C_XOR;
		}
		MAKE_DIGEST_WORD(pdu->header_digest, crc32c);

		iovec[2].iov_len = ISCSI_DIGEST_LEN;
		total += ISCSI_DIGEST_LEN;
	} else {
		iovec[2].iov_len = 0;
	}

	/* Data Segment */
	iovec[3].iov_base = pdu->data;
	iovec[3].iov_len = ISCSI_ALIGN(data_len);
	total += ISCSI_ALIGN(data_len);

	/* Data Digest */
	iovec[4].iov_base = pdu->data_digest;
	if (enable_digest && conn->data_digest && data_len != 0) {
		crc32c = istgt_crc32c(pdu->data, ISCSI_ALIGN(data_len));
		MAKE_DIGEST_WORD(pdu->data_digest, crc32c);

		iovec[4].iov_len = ISCSI_DIGEST_LEN;
		total += ISCSI_DIGEST_LEN;
	} else {
		iovec[4].iov_len = 0;
	}

	/* write all bytes from iovec */
	nbytes = total;
	ISTGT_TRACELOG(ISTGT_TRACE_NET, "PDU write %d[%lu, %lu/%lu]\n", nbytes, iovec[1].iov_len, iovec[3].iov_len, iovec[4].iov_len);
	errno = 0;
	start = time(NULL);
	while (nbytes > 0) {
		rc = writev(conn->sock, &iovec[0], 5);
		if (rc < 0) {
			now = time(NULL);
			ISTGT_ERRLOG("writev() failed (errno=%d,%s,time=%f)\n",
				errno, conn->initiator_name, difftime(now, start));
			return (-1);
		}
		nbytes -= rc;
		if (nbytes == 0)
			break;
		/* adjust iovec length */
		for (i = 0; i < 5; i++) {
			if (iovec[i].iov_len != 0 && iovec[i].iov_len > (size_t)rc) {
				iovec[i].iov_base
					= (void *) (((uintptr_t)iovec[i].iov_base) + rc);
				iovec[i].iov_len -= rc;
				break;
			} else {
				rc -= iovec[i].iov_len;
				iovec[i].iov_len = 0;
			}
		}
	}

	return (total);
}

static inline void
istgt_iscsi_copy_pdu(ISCSI_PDU_Ptr dst_pdu, ISCSI_PDU_Ptr src_pdu)
{
	memcpy(&dst_pdu->bhs, &src_pdu->bhs, ISCSI_BHS_LEN);
	dst_pdu->ahs = src_pdu->ahs;
	dst_pdu->header_digestX = src_pdu->header_digestX;
	dst_pdu->data = src_pdu->data;
	dst_pdu->data_digestX = src_pdu->data_digestX;
	dst_pdu->total_ahs_len = src_pdu->total_ahs_len;
	dst_pdu->data_segment_len = src_pdu->data_segment_len;
	dst_pdu->opcode = src_pdu->opcode;
	dst_pdu->start = src_pdu->start;
	src_pdu->ahs = NULL;
	src_pdu->data = NULL;
	src_pdu->data_segment_len = 0;
}

typedef struct iscsi_param_table_t
{
	const char *key;
	const char *val;
	const char *list;
	int type;
} ISCSI_PARAM_TABLE;

static ISCSI_PARAM_TABLE conn_param_table[] =
{
	{ "HeaderDigest", "None", "CRC32C,None", ISPT_LIST },
	{ "DataDigest", "None", "CRC32C,None", ISPT_LIST },
	{ "MaxRecvDataSegmentLength", "8192", "512,16777215", ISPT_NUMERICAL },
	{ "OFMarker", "No", "Yes,No", ISPT_BOOLEAN_AND },
	{ "IFMarker", "No", "Yes,No", ISPT_BOOLEAN_AND },
	{ "OFMarkInt", "1", "1,65535", ISPT_NUMERICAL },
	{ "IFMarkInt", "1", "1,65535", ISPT_NUMERICAL },
	{ "AuthMethod", "None", "CHAP,None", ISPT_LIST },
	{ "CHAP_A", "5", "5", ISPT_LIST },
	{ "CHAP_N", "", "", ISPT_DECLARATIVE },
	{ "CHAP_R", "", "", ISPT_DECLARATIVE },
	{ "CHAP_I", "", "", ISPT_DECLARATIVE },
	{ "CHAP_C", "", "", ISPT_DECLARATIVE },
	{ NULL, NULL, NULL, ISPT_INVALID },
};

static ISCSI_PARAM_TABLE sess_param_table[] =
{ 
	{ "MaxConnections", "1", "1,65535", ISPT_NUMERICAL },
#if 0
	/* need special handling */
	{ "SendTargets", "", "", ISPT_DECLARATIVE },
#endif 
	{ "TargetName", "", "", ISPT_DECLARATIVE },
	{ "InitiatorName", "", "", ISPT_DECLARATIVE },
	{ "TargetAlias", "", "", ISPT_DECLARATIVE },
	{ "InitiatorAlias", "", "", ISPT_DECLARATIVE },
	{ "TargetAddress", "", "", ISPT_DECLARATIVE },
	{ "TargetPortalGroupTag", "1", "1,65535", ISPT_NUMERICAL },
	{ "InitialR2T", "Yes", "Yes,No", ISPT_BOOLEAN_OR },
	{ "ImmediateData", "Yes", "Yes,No", ISPT_BOOLEAN_AND },
	{ "MaxBurstLength", "262144", "512,16777215", ISPT_NUMERICAL },
	{ "FirstBurstLength", "65536", "512,16777215", ISPT_NUMERICAL },
	{ "DefaultTime2Wait", "2", "0,3600", ISPT_NUMERICAL_MAX },
	{ "DefaultTime2Retain", "20", "0,3600", ISPT_NUMERICAL },
	{ "MaxOutstandingR2T", "1", "1,65536", ISPT_NUMERICAL },
	{ "DataPDUInOrder", "Yes", "Yes,No", ISPT_BOOLEAN_OR },
	{ "DataSequenceInOrder", "Yes", "Yes,No", ISPT_BOOLEAN_OR },
	{ "ErrorRecoveryLevel", "0", "0,2", ISPT_NUMERICAL },
	{ "SessionType", "Normal", "Normal,Discovery", ISPT_DECLARATIVE },
	{ NULL, NULL, NULL, ISPT_INVALID },
};

static int
istgt_iscsi_params_init_internal(ISCSI_PARAM **params, ISCSI_PARAM_TABLE *table)
{
	int rc;
	int i;

	for (i = 0; table[i].key != NULL; i++) {
		rc = istgt_iscsi_param_add(params, table[i].key, table[i].val,
			table[i].list, table[i].type);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_param_add() failed\n");
			return (-1);
		}
	}

	return (0);
}

static int
istgt_iscsi_conn_params_init(ISCSI_PARAM **params)
{
	return (istgt_iscsi_params_init_internal(params, &conn_param_table[0]));
}

static int
istgt_iscsi_sess_params_init(ISCSI_PARAM **params)
{
	return (istgt_iscsi_params_init_internal(params, &sess_param_table[0]));
}

static char *
istgt_iscsi_param_get_val(ISCSI_PARAM *params, const char *key)
{
	ISCSI_PARAM *param;

	param = istgt_iscsi_param_find(params, key);
	if (param == NULL)
		return (NULL);
	return (param->val);
}

static int
istgt_iscsi_param_eq_val(ISCSI_PARAM *params, const char *key, const char *val)
{
	ISCSI_PARAM *param;

	param = istgt_iscsi_param_find(params, key);
	if (param == NULL)
		return (0);
	if (strcasecmp(param->val, val) == 0)
		return (1);
	return (0);
}

#if 0
static int
istgt_iscsi_print_params(ISCSI_PARAM *params)
{
	ISCSI_PARAM *param;

	for (param = params; param != NULL; param = param->next) {
		printf("key=[%s] val=[%s] list=[%s] type=%d\n",
			param->key, param->val, param->list, param->type);
	}
	return (0);
}
#endif

static int
istgt_iscsi_negotiate_params(CONN_Ptr conn, ISCSI_PARAM *params, uint8_t *data, int alloc_len, int data_len)
{
	ISCSI_PARAM *param;
	ISCSI_PARAM *cur_param;
	char *valid_list, *in_val;
	char *valid_next, *in_next;
	char *cur_val;
	char *new_val;
	char *valid_val;
	char *min_val, *max_val;
	int discovery;
	int cur_type;
	int val_i, cur_val_i;
	int min_i, max_i;
	int total;
	int len;
	int sw;

	total = data_len;
	if (alloc_len < 1) {
		return (0);
	}
	if (total > alloc_len) {
		total = alloc_len;
		data[total - 1] = '\0';
		return (total);
	}

	if (params == NULL) {
		/* no input */
		return (total);
	}

	/* discovery? */
	discovery = 0;
	cur_param = istgt_iscsi_param_find(params, "SessionType");
	if (cur_param == NULL) {
		SESS_MTX_LOCK(conn);
		cur_param = istgt_iscsi_param_find(conn->sess->params, "SessionType");
		if (cur_param == NULL) {
			/* no session type */
		} else {
			if (strcasecmp(cur_param->val, "Discovery") == 0) {
				discovery = 1;
			}
		}
		SESS_MTX_UNLOCK(conn);
	} else {
		if (strcasecmp(cur_param->val, "Discovery") == 0) {
			discovery = 1;
		}
	}

	/* for temporary store */
	valid_list = xmalloc(ISCSI_TEXT_MAX_VAL_LEN + 1);
	in_val = xmalloc(ISCSI_TEXT_MAX_VAL_LEN + 1);
	cur_val = xmalloc(ISCSI_TEXT_MAX_VAL_LEN + 1);

	for (param = params; param != NULL; param = param->next) {
		/* sendtargets is special */
		if (strcasecmp(param->key, "SendTargets") == 0) {
			continue;
		}
		/* CHAP keys */
		if (strcasecmp(param->key, "CHAP_A") == 0
			|| strcasecmp(param->key, "CHAP_N") == 0
			|| strcasecmp(param->key, "CHAP_R") == 0
			|| strcasecmp(param->key, "CHAP_I") == 0
			|| strcasecmp(param->key, "CHAP_C") == 0) {
			continue;
		}

		if (discovery) {
			/* 12.2, 12.10, 12.11, 12.13, 12.14, 12.17, 12.18, 12.19 */
			if (strcasecmp(param->key, "MaxConnections") == 0
				|| strcasecmp(param->key, "InitialR2T") == 0
				|| strcasecmp(param->key, "ImmediateData") == 0
				|| strcasecmp(param->key, "MaxBurstLength") == 0
				|| strcasecmp(param->key, "FirstBurstLength") == 0
				|| strcasecmp(param->key, "MaxOutstandingR2T") == 0
				|| strcasecmp(param->key, "DataPDUInOrder") == 0
				|| strcasecmp(param->key, "DataSequenceInOrder") == 0) {
				strlcpy(in_val, "Irrelevant",
					ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
				cur_type = -1;
				goto add_val;
			}
		}

		/* get current param */
		sw = 0;
		cur_param = istgt_iscsi_param_find(conn->params, param->key);
		if (cur_param == NULL) {
			sw = 1;
			SESS_MTX_LOCK(conn);
			cur_param = istgt_iscsi_param_find(conn->sess->params,
				param->key);
			if (cur_param == NULL) {
				SESS_MTX_UNLOCK(conn);
				if (strncasecmp(param->key, "X-", 2) == 0
					|| strncasecmp(param->key, "X#", 2) == 0) {
					/* Extension Key */
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
						"extension key %.64s\n",
						param->key);
				} else {
					ISTGT_ERRLOG("unknown key %.64s\n",
						param->key);
				}
				strlcpy(in_val, "NotUnderstood",
					ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
				cur_type = -1;
				goto add_val;
			}
			strlcpy(valid_list, cur_param->list,
				ISCSI_TEXT_MAX_VAL_LEN);
			strlcpy(cur_val, cur_param->val,
				ISCSI_TEXT_MAX_VAL_LEN);
			cur_type = cur_param->type;
			SESS_MTX_UNLOCK(conn);
		} else {
			strlcpy(valid_list, cur_param->list,
				ISCSI_TEXT_MAX_VAL_LEN);
			strlcpy(cur_val, cur_param->val,
				ISCSI_TEXT_MAX_VAL_LEN);
			cur_type = cur_param->type;
		}

		/* negotiate value */
		switch (cur_type) {
		case ISPT_LIST:
			strlcpy(in_val, param->val, ISCSI_TEXT_MAX_VAL_LEN);
			in_next = in_val;
			while ((new_val = strsepq(&in_next, ",")) != NULL) {
				valid_next = valid_list;
				while ((valid_val = strsepq(&valid_next, ",")) != NULL) {
					if (strcasecmp(new_val, valid_val) == 0) {
						ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "match %s\n",
							new_val);
						goto update_val;
					}
				}
			}
			if (new_val == NULL) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"key %.64s reject\n",
					param->key);
				strlcpy(in_val, "Reject",
					ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
				goto add_val;
			}
			break;

		case ISPT_NUMERICAL:
			val_i = (int) strtol(param->val, NULL, 10);
			cur_val_i = (int) strtol(cur_val, NULL, 10);
			valid_next = valid_list;
			min_val = strsepq(&valid_next, ",");
			max_val = strsepq(&valid_next, ",");
			if (min_val != NULL) {
				min_i = (int) strtol(min_val, NULL, 10);
			} else {
				min_i = 0;
			}
			if (max_val != NULL) {
				max_i = (int) strtol(max_val, NULL, 10);
			} else {
				max_i = 0;
			}
			if (val_i < min_i || val_i > max_i) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"key %.64s reject\n",
					param->key);
				strlcpy(in_val, "Reject",
					ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
				goto add_val;
			}
			if (strcasecmp(param->key, "MaxRecvDataSegmentLength") == 0) {
				/* Declarative, but set as same value */
				cur_val_i = conn->TargetMaxRecvDataSegmentLength;
			}
			if (val_i > cur_val_i) {
				val_i = cur_val_i;
			}
			snprintf(in_val, ISCSI_TEXT_MAX_VAL_LEN, "%d", val_i);
			new_val = in_val;
			break;

		case ISPT_NUMERICAL_MAX:
			val_i = (int) strtol(param->val, NULL, 10);
			cur_val_i = (int) strtol(cur_val, NULL, 10);
			valid_next = valid_list;
			min_val = strsepq(&valid_next, ",");
			max_val = strsepq(&valid_next, ",");
			if (min_val != NULL) {
				min_i = (int) strtol(min_val, NULL, 10);
			} else {
				min_i = 0;
			}
			if (max_val != NULL) {
				max_i = (int) strtol(max_val, NULL, 10);
			} else {
				max_i = 0;
			}
			if (val_i < min_i || val_i > max_i) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"key %.64s reject\n",
					param->key);
				strlcpy(in_val, "Reject",
					ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
				goto add_val;
			}
			if (val_i < cur_val_i) {
				val_i = cur_val_i;
			}
			snprintf(in_val, ISCSI_TEXT_MAX_VAL_LEN, "%d", val_i);
			new_val = in_val;
			break;

		case ISPT_BOOLEAN_OR:
			if (strcasecmp(cur_val, "Yes") == 0) {
				/* YES || XXX */
				strlcpy(in_val, "Yes", ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
			} else {
				if (strcasecmp(param->val, "Yes") == 0
					|| strcasecmp(param->val, "No") == 0) {
					new_val = param->val;
				} else {
					/* unknown value */
					strlcpy(in_val, "Reject",
						ISCSI_TEXT_MAX_VAL_LEN);
					new_val = in_val;
					goto add_val;
				}
			}
			break;

		case ISPT_BOOLEAN_AND:
			if (strcasecmp(cur_val, "No") == 0) {
				/* No && XXX */
				strlcpy(in_val, "No", ISCSI_TEXT_MAX_VAL_LEN);
				new_val = in_val;
			} else {
				if (strcasecmp(param->val, "Yes") == 0
					|| strcasecmp(param->val, "No") == 0) {
					new_val = param->val;
				} else {
					/* unknown value */
					strlcpy(in_val, "Reject",
						ISCSI_TEXT_MAX_VAL_LEN);
					new_val = in_val;
					goto add_val;
				}
			}
			break;

		case ISPT_DECLARATIVE:
			strlcpy(in_val, param->val, ISCSI_TEXT_MAX_VAL_LEN);
			new_val = in_val;
			break;

		default:
			strlcpy(in_val, param->val, ISCSI_TEXT_MAX_VAL_LEN);
			new_val = in_val;
			break;
		}

	update_val:
		if (sw) {
			/* update session wide */
			SESS_MTX_LOCK(conn);
			istgt_iscsi_param_set(conn->sess->params, param->key,
				new_val);
			SESS_MTX_UNLOCK(conn);
		} else {
			/* update connection only */
			istgt_iscsi_param_set(conn->params, param->key,
				new_val);
		}
	add_val:
		if (cur_type != ISPT_DECLARATIVE) {
			if (alloc_len - total < 1) {
				ISTGT_ERRLOG("data space small %d\n",
					alloc_len);
				xfree(valid_list);
				xfree(in_val);
				xfree(cur_val);
				return (total);
			}
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "negotiated %s=%s\n",
				param->key, new_val);
			len = snprintf((char *) data + total,
				alloc_len - total, "%s=%s",
				param->key, new_val);
			total += len + 1;
		}
	}

	xfree(valid_list);
	xfree(in_val);
	xfree(cur_val);

	return (total);
}

static int
istgt_iscsi_append_text(CONN_Ptr conn __attribute__((__unused__)), const char *key, const char *val, uint8_t *data, int alloc_len, int data_len)
{
	int total;
	int len;

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
	len = snprintf((char *) data + total, alloc_len - total, "%s=%s",
		key, val);
	total += len + 1;

	return (total);
}

static int
istgt_iscsi_append_param(CONN_Ptr conn, const char *key, uint8_t *data, int alloc_len, int data_len)
{
	ISCSI_PARAM *param;
	int rc;

	param = istgt_iscsi_param_find(conn->params, key);
	if (param == NULL) {
		param = istgt_iscsi_param_find(conn->sess->params, key);
		if (param == NULL) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "no key %.64s\n",
				key);
			return (data_len);
		}
	}
	rc = istgt_iscsi_append_text(conn, param->key, param->val, data,
		alloc_len, data_len);
	return (rc);
}

int
istgt_chap_get_authinfo(ISTGT_CHAP_AUTH *auth, const char *authfile, const char *authuser, int ag_tag)
{
	CONFIG *config = NULL;
	CF_SECTION *sp;
	const char *val;
	const char *user, *muser;
	const char *secret, *msecret;
	int rc;
	int i;

	if (auth->user != NULL) {
		xfree(auth->user);
		xfree(auth->secret);
		xfree(auth->muser);
		xfree(auth->msecret);
		auth->user = auth->secret = NULL;
		auth->muser = auth->msecret = NULL;
	}

	/* read config files */
	config = istgt_allocate_config();
	rc = istgt_read_config(config, authfile);
	if (rc < 0) {
		ISTGT_ERRLOG("auth conf error\n");
		istgt_free_config(config);
		return (-1);
	}
	// istgt_print_config(config);

	sp = config->section;
	while (sp != NULL) {
		if (sp->type == ST_AUTHGROUP) {
			if (sp->num == 0) {
				ISTGT_ERRLOG("Group 0 is invalid\n");
				istgt_free_config(config);
				return (-1);
			}
			if (ag_tag != sp->num) {
				goto skip_ag_tag;
			}

			val = istgt_get_val(sp, "Comment");
			if (val != NULL) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"Comment %s\n", val);
			}
			for (i = 0; ; i++) {
				val = istgt_get_nval(sp, "Auth", i);
				if (val == NULL)
					break;
				user = istgt_get_nmval(sp, "Auth", i, 0);
				secret = istgt_get_nmval(sp, "Auth", i, 1);
				muser = istgt_get_nmval(sp, "Auth", i, 2);
				msecret = istgt_get_nmval(sp, "Auth", i, 3);
				if (!user || !secret || !muser || !msecret) {
					ISTGT_ERRLOG("Invalid argument\n");
					istgt_free_config(config);
					return (-1);
				}
				if (strcasecmp(authuser, user) == 0) {
					/* match user */
					auth->user = xstrdup(user);
					auth->secret = xstrdup(secret);
					auth->muser = xstrdup(muser);
					auth->msecret = xstrdup(msecret);
					istgt_free_config(config);
					return (0);
				}
			}
		}
	skip_ag_tag:
		sp = sp->next;
	}

	istgt_free_config(config);
	return (0);
}

static int
istgt_iscsi_get_authinfo(CONN_Ptr conn, const char *authuser)
{
	char *authfile = NULL;
	int ag_tag;
	int rc;

	SESS_MTX_LOCK(conn);
	if (conn->sess->lu != NULL) {
		ag_tag = conn->sess->lu->auth_group;
	} else {
		ag_tag = -1;
	}
	SESS_MTX_UNLOCK(conn);
	if (ag_tag < 0) {
		MTX_LOCK(&conn->istgt->mutex);
		ag_tag = conn->istgt->discovery_auth_group;
		MTX_UNLOCK(&conn->istgt->mutex);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "ag_tag=%d\n", ag_tag);

	MTX_LOCK(&conn->istgt->mutex);
	authfile = xstrdup(conn->istgt->authfile);
	MTX_UNLOCK(&conn->istgt->mutex);

	rc = istgt_chap_get_authinfo(&conn->auth, authfile, authuser, ag_tag);
	if (rc < 0) {
		ISTGT_ERRLOG("chap_get_authinfo() failed\n");
		xfree(authfile);
		return (-1);
	}
	xfree(authfile);
	return (0);
}

static int
istgt_iscsi_auth_params(CONN_Ptr conn, ISCSI_PARAM *params, const char *method, uint8_t *data, int alloc_len, int data_len)
{
	char *in_val;
	char *in_next;
	char *new_val;
	const char *val;
	const char *user;
	const char *response;
	const char *challenge;
	int total;
	int rc;

	if (conn == NULL || params == NULL || method == NULL) {
		return (-1);
	}
	if (strcasecmp(method, "CHAP") == 0) {
		/* method OK */
	} else {
		ISTGT_ERRLOG("unsupported AuthMethod %.64s\n", method);
		return (-1);
	}

	total = data_len;
	if (alloc_len < 1) {
		return (0);
	}
	if (total > alloc_len) {
		total = alloc_len;
		data[total - 1] = '\0';
		return (total);
	}

	/* for temporary store */
	in_val = xmalloc(ISCSI_TEXT_MAX_VAL_LEN + 1);

	/* CHAP method (RFC1994) */
	if ((val = ISCSI_GETVAL(params, "CHAP_A")) != NULL) {
		if (conn->auth.chap_phase != ISTGT_CHAP_PHASE_WAIT_A) {
			ISTGT_ERRLOG("CHAP sequence error\n");
			goto error_return;
		}

		/* CHAP_A is LIST type */
		strlcpy(in_val, val, ISCSI_TEXT_MAX_VAL_LEN);
		in_next = in_val;
		while ((new_val = strsepq(&in_next, ",")) != NULL) {
			if (strcasecmp(new_val, "5") == 0) {
				/* CHAP with MD5 */
				break;
			}
		}
		if (new_val == NULL) {
			strlcpy(in_val, "Reject", ISCSI_TEXT_MAX_VAL_LEN);
			new_val = in_val;
			total = istgt_iscsi_append_text(conn, "CHAP_A",
				new_val, data, alloc_len, total);
			goto error_return;
		}
		/* selected algorithm is 5 (MD5) */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "got CHAP_A=%s\n", new_val);
		total = istgt_iscsi_append_text(conn, "CHAP_A", new_val,
			data, alloc_len, total);

		/* Identifier is one octet */
		istgt_gen_random(conn->auth.chap_id, 1);
		snprintf(in_val, ISCSI_TEXT_MAX_VAL_LEN, "%d",
			(int) conn->auth.chap_id[0]);
		total = istgt_iscsi_append_text(conn, "CHAP_I", in_val,
			data, alloc_len, total);

		/* Challenge Value is a variable stream of octets */
		/* (binary length MUST not exceed 1024 bytes) */
		conn->auth.chap_challenge_len = ISTGT_CHAP_CHALLENGE_LEN;
		istgt_gen_random(conn->auth.chap_challenge,
			conn->auth.chap_challenge_len);
		istgt_bin2hex(in_val, ISCSI_TEXT_MAX_VAL_LEN,
			conn->auth.chap_challenge,
			conn->auth.chap_challenge_len);
		total = istgt_iscsi_append_text(conn, "CHAP_C", in_val,
			data, alloc_len, total);

		conn->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_NR;
	} else if ((val = ISCSI_GETVAL(params, "CHAP_N")) != NULL) {
		uint8_t resmd5[ISTGT_MD5DIGEST_LEN];
		uint8_t tgtmd5[ISTGT_MD5DIGEST_LEN];
		ISTGT_MD5CTX md5ctx;

		user = val;
		if (conn->auth.chap_phase != ISTGT_CHAP_PHASE_WAIT_NR) {
			ISTGT_ERRLOG("CHAP sequence error\n");
			goto error_return;
		}

		response = ISCSI_GETVAL(params, "CHAP_R");
		if (response == NULL) {
			ISTGT_ERRLOG("no response\n");
			goto error_return;
		}
		rc = istgt_hex2bin(resmd5, ISTGT_MD5DIGEST_LEN, response);
		if (rc < 0 || rc != ISTGT_MD5DIGEST_LEN) {
			ISTGT_ERRLOG("response format error\n");
			goto error_return;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "got CHAP_N/CHAP_R\n");

		rc = istgt_iscsi_get_authinfo(conn, val);
		if (rc < 0) {
			// ISTGT_ERRLOG("auth user or secret is missing\n");
			ISTGT_ERRLOG("iscsi_get_authinfo() failed\n");
			goto error_return;
		}
		if (conn->auth.user == NULL || conn->auth.secret == NULL) {
			// ISTGT_ERRLOG("auth user or secret is missing\n");
			ISTGT_ERRLOG("auth failed (user %.64s)\n", user);
			goto error_return;
		}

		istgt_md5init(&md5ctx);
		/* Identifier */
		istgt_md5update(&md5ctx, conn->auth.chap_id, 1);
		/* followed by secret */
		istgt_md5update(&md5ctx, conn->auth.secret,
			strlen(conn->auth.secret));
		/* followed by Challenge Value */
		istgt_md5update(&md5ctx, conn->auth.chap_challenge,
			conn->auth.chap_challenge_len);
		/* tgtmd5 is expecting Response Value */
		istgt_md5final(tgtmd5, &md5ctx);

		istgt_bin2hex(in_val, ISCSI_TEXT_MAX_VAL_LEN,
			tgtmd5, ISTGT_MD5DIGEST_LEN);

#if 0
		printf("tgtmd5=%s, resmd5=%s\n", in_val, response);
		istgt_dump("tgtmd5", tgtmd5, ISTGT_MD5DIGEST_LEN);
		istgt_dump("resmd5", resmd5, ISTGT_MD5DIGEST_LEN);
#endif

		/* compare MD5 digest */
		if (memcmp(tgtmd5, resmd5, ISTGT_MD5DIGEST_LEN) != 0) {
			/* not match */
			// ISTGT_ERRLOG("auth user or secret is missing\n");
			ISTGT_ERRLOG("auth failed (user %.64s)\n", user);
			goto error_return;
		}
		/* OK initiator's secret */
		conn->authenticated = 1;

		/* mutual CHAP? */
		val = ISCSI_GETVAL(params, "CHAP_I");
		if (val != NULL) {
			conn->auth.chap_mid[0] = (uint8_t) strtol(val, NULL, 10);
			challenge = ISCSI_GETVAL(params, "CHAP_C");
			if (challenge == NULL) {
				ISTGT_ERRLOG("CHAP sequence error\n");
				goto error_return;
			}
			rc = istgt_hex2bin(conn->auth.chap_mchallenge,
				ISTGT_CHAP_CHALLENGE_LEN,
				challenge);
			if (rc < 0) {
				ISTGT_ERRLOG("challenge format error\n");
				goto error_return;
			}
			conn->auth.chap_mchallenge_len = rc;
#if 0
			istgt_dump("MChallenge", conn->auth.chap_mchallenge,
				conn->auth.chap_mchallenge_len);
#endif
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"got CHAP_I/CHAP_C\n");

			if (conn->auth.muser == NULL || conn->auth.msecret == NULL) {
				// ISTGT_ERRLOG("mutual auth user or secret is missing\n");
				ISTGT_ERRLOG("auth failed (user %.64s)\n",
					user);
				goto error_return;
			}

			istgt_md5init(&md5ctx);
			/* Identifier */
			istgt_md5update(&md5ctx, conn->auth.chap_mid, 1);
			/* followed by secret */
			istgt_md5update(&md5ctx, conn->auth.msecret,
				strlen(conn->auth.msecret));
			/* followed by Challenge Value */
			istgt_md5update(&md5ctx, conn->auth.chap_mchallenge,
				conn->auth.chap_mchallenge_len);
			/* tgtmd5 is Response Value */
			istgt_md5final(tgtmd5, &md5ctx);

			istgt_bin2hex(in_val, ISCSI_TEXT_MAX_VAL_LEN,
				tgtmd5, ISTGT_MD5DIGEST_LEN);

			total = istgt_iscsi_append_text(conn, "CHAP_N",
				conn->auth.muser, data, alloc_len, total);
			total = istgt_iscsi_append_text(conn, "CHAP_R",
				in_val, data, alloc_len, total);
		} else {
			/* not mutual */
			if (conn->req_mutual) {
				ISTGT_ERRLOG("required mutual CHAP\n");
				goto error_return;
			}
		}

		conn->auth.chap_phase = ISTGT_CHAP_PHASE_END;
	} else {
		/* not found CHAP keys */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "start CHAP\n");
		conn->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_A;
	}

	xfree(in_val);
	return (total);

error_return:
	conn->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_A;
	xfree(in_val);
	return (-1);
}

static int
istgt_iscsi_reject(CONN_Ptr conn, ISCSI_PDU_Ptr pdu, int reason)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *data;
	int total_ahs_len;
	int data_len;
	int alloc_len;
	int rc;
	uint32_t l_SSN = conn->StatSN, l_CSN = 0, l_MCSN = 0;

	total_ahs_len = DGET8(&pdu->bhs.total_ahs_len);
	data_len = 0;
	alloc_len = ISCSI_BHS_LEN + (4 * total_ahs_len);
	if (conn->header_digest) {
		alloc_len += ISCSI_DIGEST_LEN;
	}
	data = xmalloc(alloc_len);
	memset(data, 0, alloc_len);

	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
		l_CSN = conn->sess->ExpCmdSN;
		l_MCSN = conn->sess->MaxCmdSN;
		SESS_MTX_UNLOCK(conn);
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"Reject PDU reason=%d StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x\n",
			reason, l_SSN, l_CSN, l_MCSN);
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"Reject PDU reason=%d StatSN=%x\n",
			reason, conn->StatSN);
	}

	memcpy(data, &pdu->bhs, ISCSI_BHS_LEN);
	data_len += ISCSI_BHS_LEN;
	if (total_ahs_len != 0) {
		memcpy(data + data_len, pdu->ahs, (4 * total_ahs_len));
		data_len += (4 * total_ahs_len);
	}
	if (conn->header_digest) {
		memcpy(data + data_len, pdu->header_digest, ISCSI_DIGEST_LEN);
		data_len += ISCSI_DIGEST_LEN;
	}

	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = data;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_REJECT;
	BDADD8W(&rsp[1], 1, 7, 1);
	rsp[2] = reason;
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], data_len); // DataSegmentLength

	DSET32(&rsp[16], 0xffffffffU);

	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);
	} else {
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		DSET32(&rsp[28], 1);
		DSET32(&rsp[32], 1);
	}
	DSET32(&rsp[36], 0); // DataSN/R2TSN

	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "PDU", rsp, ISCSI_BHS_LEN);

	rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}

	return (0);
}

static void
istgt_iscsi_copy_param2var(CONN_Ptr conn)
{
	const char *val;

	val = ISCSI_GETVAL(conn->params, "MaxRecvDataSegmentLength");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		"copy MaxRecvDataSegmentLength=%s\n", val);
	conn->MaxRecvDataSegmentLength = (int) strtol(val, NULL, 10);
	if (conn->sendbufsize != conn->MaxRecvDataSegmentLength) {
		xfree(conn->recvbuf);
		xfree(conn->sendbuf);
		if (conn->MaxRecvDataSegmentLength < 8192) {
			conn->recvbufsize = 8192;
			conn->sendbufsize = 8192;
		} else {
			conn->recvbufsize = conn->MaxRecvDataSegmentLength;
			conn->sendbufsize = conn->MaxRecvDataSegmentLength;
		}
		conn->recvbuf = xmalloc(conn->recvbufsize);
		conn->sendbuf = xmalloc(conn->sendbufsize);
	}
	val = ISCSI_GETVAL(conn->params, "HeaderDigest");
	if (strcasecmp(val, "CRC32C") == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set HeaderDigest=1\n");
		conn->header_digest = 1;
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set HeaderDigest=0\n");
		conn->header_digest = 0;
	}
	val = ISCSI_GETVAL(conn->params, "DataDigest");
	if (strcasecmp(val, "CRC32C") == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set DataDigest=1\n");
		conn->data_digest = 1;
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set DataDigest=0\n");
		conn->data_digest = 0;
	}

	SESS_MTX_LOCK(conn);
	val = ISCSI_GETVAL(conn->sess->params, "MaxConnections");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "copy MaxConnections=%s\n", val);
	conn->sess->MaxConnections = (int) strtol(val, NULL, 10);
	val = ISCSI_GETVAL(conn->sess->params, "MaxOutstandingR2T");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "copy MaxOutstandingR2T=%s\n", val);
	conn->sess->MaxOutstandingR2T = (int) strtol(val, NULL, 10);
	conn->MaxOutstandingR2T = conn->sess->MaxOutstandingR2T;
	val = ISCSI_GETVAL(conn->sess->params, "FirstBurstLength");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "copy FirstBurstLength=%s\n", val);
	conn->sess->FirstBurstLength = (int) strtol(val, NULL, 10);
	conn->FirstBurstLength = conn->sess->FirstBurstLength;
	val = ISCSI_GETVAL(conn->sess->params, "MaxBurstLength");
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "copy MaxBurstLength=%s\n", val);
	conn->sess->MaxBurstLength = (int) strtol(val, NULL, 10);
	conn->MaxBurstLength = conn->sess->MaxBurstLength;
	val = ISCSI_GETVAL(conn->sess->params, "InitialR2T");
	if (strcasecmp(val, "Yes") == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set InitialR2T=1\n");
		conn->sess->initial_r2t = 1;
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set InitialR2T=0\n");
		conn->sess->initial_r2t = 0;
	}
	val = ISCSI_GETVAL(conn->sess->params, "ImmediateData");
	if (strcasecmp(val, "Yes") == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set ImmediateData=1\n");
		conn->sess->immediate_data = 1;
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "set ImmediateData=0\n");
		conn->sess->immediate_data = 0;
	}
	SESS_MTX_UNLOCK(conn);
}

static int
istgt_iscsi_check_values(CONN_Ptr conn)
{
	SESS_MTX_LOCK(conn);
	if (conn->sess->FirstBurstLength > conn->sess->MaxBurstLength) {
		ISTGT_ERRLOG("FirstBurstLength(%d) > MaxBurstLength(%d)\n",
			conn->sess->FirstBurstLength,
			conn->sess->MaxBurstLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	if (conn->sess->MaxBurstLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxBurstLength(%d) > 0x00ffffff\n",
			conn->sess->MaxBurstLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	if (conn->TargetMaxRecvDataSegmentLength < 512) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) < 512\n",
			conn->TargetMaxRecvDataSegmentLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	if (conn->TargetMaxRecvDataSegmentLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) > 0x00ffffff\n",
			conn->TargetMaxRecvDataSegmentLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	if (conn->MaxRecvDataSegmentLength < 512) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) < 512\n",
			conn->MaxRecvDataSegmentLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	if (conn->MaxRecvDataSegmentLength > 0x00ffffff) {
		ISTGT_ERRLOG("MaxRecvDataSegmentLength(%d) > 0x00ffffff\n",
			conn->MaxRecvDataSegmentLength);
		SESS_MTX_UNLOCK(conn);
		return (-1);
	}
	SESS_MTX_UNLOCK(conn);
	return (0);
}

static int
istgt_iscsi_op_login(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	char buf[MAX_TMPBUF];
	ISTGT_LU_Ptr lu = NULL;
	ISCSI_PARAM *params = NULL;
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	uint8_t *data;
	const char *session_type;
	const char *auth_method;
	const char *val;
	uint64_t isid;
	uint16_t tsih;
	uint16_t cid;
	uint32_t task_tag;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	int T_bit, C_bit;
	int CSG, NSG;
	int VersionMin, VersionMax;
	int StatusClass, StatusDetail;
	int data_len;
	int alloc_len;
	int rc;

	/* Login is proceeding OK */
	/* https://www.iana.org/assignments/iscsi-parameters/iscsi-parameters.xhtml#iscsi-parameters-9 */
	StatusClass = 0x00;
	StatusDetail = 0x00;

	data_len = 0;

	if (conn->MaxRecvDataSegmentLength < 8192) {
		// Default MaxRecvDataSegmentLength - RFC3720(12.12)
		alloc_len = 8192;
	} else {
		alloc_len = conn->MaxRecvDataSegmentLength;
	}
	data = xmalloc(alloc_len);
	memset(data, 0, alloc_len);

	cp = (uint8_t *) &pdu->bhs;
	T_bit = BGET8(&cp[1], 7);
	C_bit = BGET8(&cp[1], 6);
	CSG = BGET8W(&cp[1], 3, 2);
	NSG = BGET8W(&cp[1], 1, 2);
	VersionMin = cp[2];
	VersionMax = cp[3];

	isid = DGET48(&cp[8]);
	tsih = DGET16(&cp[14]);
	cid = DGET16(&cp[20]);
	task_tag = DGET32(&cp[16]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);

#if 0
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "PDU", cp, ISCSI_BHS_LEN);
#endif

	if (conn->sess != NULL) {
		// SESS_MTX_LOCK(conn);
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				"op_login CSN:%x T=%d, C=%d, CSG=%d, NSG=%d, Min=%d, Max=%d, ITT=%x ExpStatSN=%x, StatSN=%x (session)\n",
				CmdSN, T_bit, C_bit, CSG, NSG, VersionMin, VersionMax, task_tag,
				ExpStatSN, conn->StatSN); // conn->sess->ExpCmdSN, conn->sess->MaxCmdSN);
		// SESS_MTX_UNLOCK(conn);
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				"op_login CSN:%x T=%d, C=%d, CSG=%d, NSG=%d, Min=%d, Max=%d, ITT=%x ExpStatSN=%x, StatSN=%x\n",
				CmdSN, T_bit, C_bit, CSG, NSG, VersionMin, VersionMax, task_tag,
				ExpStatSN, conn->StatSN);
	}

	if (T_bit && C_bit) {
		ISTGT_ERRLOG("transit error\n");
		xfree(data);
		return (-1);
	}
	if (VersionMin > ISCSI_VERSION || VersionMax < ISCSI_VERSION) {
		ISTGT_ERRLOG("unsupported version %d/%d\n", VersionMin, VersionMax);
		/* Unsupported version */
		StatusClass = 0x02;
		StatusDetail = 0x05;
		goto response;
	}

	/* store incoming parameters */
	rc = istgt_iscsi_parse_params(&params, pdu->data, pdu->data_segment_len);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_parse_params() failed\n");
	error_return:
		istgt_iscsi_param_free(params);
		xfree(data);
		return (-1);
	}

	/* set port identifiers and parameters */
	if (conn->login_phase == ISCSI_LOGIN_PHASE_NONE) {
		/* Initiator Name and Port */
		val = ISCSI_GETVAL(params, "InitiatorName");
		if (val == NULL) {
			ISTGT_ERRLOG("InitiatorName is empty\n");
			/* Missing parameter */
			StatusClass = 0x02;
			StatusDetail = 0x07;
			goto response;
		}
		snprintf(conn->initiator_name, sizeof (conn->initiator_name),
			"%s", val);
		snprintf(conn->initiator_port, sizeof (conn->initiator_port),
			"%s" ",i,0x" "%12.12" PRIx64, val, isid);
		/*
		 * We did this because of some normalization issue.
		 * But need to comment out this for logging issue.
		 * May be another solution will be needed in future if
		 * normalization is mandatory.
		 */
		 strlwr(conn->initiator_name);
		 strlwr(conn->initiator_port);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Initiator name: %s\n",
			conn->initiator_name);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Initiator port: %s\n",
			conn->initiator_port);

		/* Session Type */
		session_type = ISCSI_GETVAL(params, "SessionType");
		if (session_type == NULL) {
			if (tsih != 0) {
				session_type = "Normal";
			} else {
				ISTGT_ERRLOG("SessionType is empty\n");
				/* Missing parameter */
				StatusClass = 0x02;
				StatusDetail = 0x07;
				goto response;
			}
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Session Type: %s\n",
			session_type);

		/* Target Name and Port */
		if (strcasecmp(session_type, "Normal") == 0) {
			val = ISCSI_GETVAL(params, "TargetName");
			if (val == NULL) {
				ISTGT_ERRLOG("TargetName is empty\n");
				/* Missing parameter */
				StatusClass = 0x02;
				StatusDetail = 0x07;
				goto response;
			}
			snprintf(conn->target_name, sizeof (conn->target_name),
				"%s", val);
			snprintf(conn->target_port, sizeof (conn->target_port),
				"%s" ",t,0x" "%4.4x", val, conn->portal.tag);
			 strlwr(conn->target_name);
			 strlwr(conn->target_port);

			MTX_LOCK(&conn->istgt->mutex);
			lu = istgt_lu_find_target(conn->istgt,
				conn->target_name);
			if (lu == NULL) {
				MTX_UNLOCK(&conn->istgt->mutex);
				ISTGT_ERRLOG("lu_find_target() failed\n");
				/* Not found */
				StatusClass = 0x02;
				StatusDetail = 0x03;
				goto response;
			}
#ifdef REPLICATION
			ISTGT_LU_DISK *spec = NULL;
			spec = (ISTGT_LU_DISK *)(lu->lun[0].spec);
			if(spec == NULL || !spec->ready) {
				MTX_UNLOCK(&conn->istgt->mutex);
				ISTGT_ERRLOG("login failed, target not ready\n");
				/* Not Ready */
				StatusClass = 0x03;
				StatusDetail = 0x01;
				goto response;
			}
#endif
			rc = istgt_lu_access(conn, lu, conn->initiator_name,
				conn->initiator_addr);
			if (rc < 0) {
				MTX_UNLOCK(&conn->istgt->mutex);
				ISTGT_ERRLOG("lu_access() failed\n");
				/* Not found */
				StatusClass = 0x02;
				StatusDetail = 0x03;
				goto response;
			}
			if (rc == 0) {
				MTX_UNLOCK(&conn->istgt->mutex);
				ISTGT_ERRLOG("access denied\n");
				/* Not found */
				StatusClass = 0x02;
				StatusDetail = 0x03;
				goto response;
			}
			MTX_UNLOCK(&conn->istgt->mutex);

			/* check existing session */
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				"isid=%"PRIx64", tsih=%u, cid=%u\n",
				isid, tsih, cid);
			if (tsih != 0) {
				/* multiple connections */
				rc = istgt_append_sess(conn, isid, tsih, cid);
				if (rc < 0) {
					ISTGT_ERRLOG("isid=%"PRIx64", tsih=%u, cid=%u: "
						"append_sess() failed\n",
						isid, tsih, cid);
					/* Can't include in session */
					StatusClass = 0x02;
					StatusDetail = 0x08;
					goto response;
				}
			} else {
				/* new session, drop old sess by the initiator */
				istgt_iscsi_drop_old_conns(conn);
			}

			/* force target flags */
			MTX_LOCK(&lu->mutex);
			if (lu->no_auth_chap) {
				conn->req_auth = 0;
				rc = istgt_iscsi_param_del(&conn->params,
					"AuthMethod");
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"AuthMethod", "None", "None", ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			} else if (lu->auth_chap) {
				conn->req_auth = 1;
				rc = istgt_iscsi_param_del(&conn->params,
					"AuthMethod");
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"AuthMethod", "CHAP", "CHAP", ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			}
			if (lu->auth_chap_mutual) {
				conn->req_mutual = 1;
			}
			if (lu->header_digest) {
				rc = istgt_iscsi_param_del(&conn->params,
					"HeaderDigest");
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"HeaderDigest", "CRC32C", "CRC32C",
					ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			}
			if (lu->data_digest) {
				rc = istgt_iscsi_param_del(&conn->params,
					"DataDigest");
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"DataDigest", "CRC32C", "CRC32C",
					ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&lu->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			}
			MTX_UNLOCK(&lu->mutex);
		} else if (strcasecmp(session_type, "Discovery") == 0) {
			snprintf(conn->target_name, sizeof (conn->target_name),
				"%s", "dummy");
			snprintf(conn->target_port, sizeof (conn->target_port),
				"%s" ",t,0x" "%4.4x", "dummy", conn->portal.tag);
			lu = NULL;
			tsih = 0;

			/* force target flags */
			MTX_LOCK(&conn->istgt->mutex);
			if (conn->istgt->no_discovery_auth) {
				conn->req_auth = 0;
				rc = istgt_iscsi_param_del(&conn->params,
					"AuthMethod");
				if (rc < 0) {
					MTX_UNLOCK(&conn->istgt->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"AuthMethod", "None", "None", ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&conn->istgt->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			} else if (conn->istgt->req_discovery_auth) {
				conn->req_auth = 1;
				rc = istgt_iscsi_param_del(&conn->params,
					"AuthMethod");
				if (rc < 0) {
					MTX_UNLOCK(&conn->istgt->mutex);
					ISTGT_ERRLOG("iscsi_param_del() failed\n");
					goto error_return;
				}
				rc = istgt_iscsi_param_add(&conn->params,
					"AuthMethod", "CHAP", "CHAP", ISPT_LIST);
				if (rc < 0) {
					MTX_UNLOCK(&conn->istgt->mutex);
					ISTGT_ERRLOG("iscsi_param_add() failed\n");
					goto error_return;
				}
			}
			if (conn->istgt->req_discovery_auth_mutual) {
				conn->req_mutual = 1;
			}
			MTX_UNLOCK(&conn->istgt->mutex);
		} else {
			ISTGT_ERRLOG("unknown session type\n");
			/* Missing parameter */
			StatusClass = 0x02;
			StatusDetail = 0x07;
			goto response;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Target name: %s\n",
			conn->target_name);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Target port: %s\n",
			conn->target_port);

		conn->authenticated = 0;
		conn->auth.chap_phase = ISTGT_CHAP_PHASE_WAIT_A;
		conn->cid = cid;
		if (lu == NULL || lu->queue_depth == 0) {
			conn->queue_depth = ISCMDQ;
		} else {
			conn->queue_depth = lu->queue_depth;
		}
		conn->max_pending = (conn->queue_depth + 1) * 2;
#if 0
		/* override config setting */
		MTX_LOCK(&conn->r2t_mutex);
		if ((conn->max_r2t > 0)
			&& (conn->max_r2t < conn->max_pending)) {
			int i;
			xfree(conn->r2t_tasks);
			conn->max_r2t = conn->max_pending;
			conn->r2t_tasks = xmalloc(sizeof (*conn->r2t_tasks)
				* (conn->max_r2t + 1));
			for (i = 0; i < (conn->max_r2t + 1); i++) {
				conn->r2t_tasks[i] = NULL;
			}
		}
		MTX_UNLOCK(&conn->r2t_mutex);
#endif
		if (conn->sess == NULL) {
			/* new session */
			rc = istgt_create_sess(conn->istgt, conn, lu);
			if (rc < 0) {
				ISTGT_ERRLOG("create_sess() failed\n");
				goto error_return;
			}

			/* initialize parameters */
			SESS_MTX_LOCK(conn);
			conn->StatSN = ExpStatSN;
			conn->MaxOutstandingR2T
				= conn->sess->MaxOutstandingR2T;
			conn->isid = isid;
			conn->tsih = tsih;
			conn->sess->isid = isid;
			conn->sess->tsih = tsih;
			conn->sess->lu = lu;
			conn->sess->ExpCmdSN = CmdSN;
			conn->sess->MaxCmdSN = CmdSN + conn->queue_depth - 1;
			conn->sess->MaxCmdSN_local = conn->sess->MaxCmdSN;
			SESS_MTX_UNLOCK(conn);
		}

		/* limit conns on discovery session */
		if (strcasecmp(session_type, "Discovery") == 0) {
			SESS_MTX_LOCK(conn);
			conn->sess->MaxConnections = 1;
			rc = istgt_iscsi_param_set_int(conn->sess->params,
				"MaxConnections", conn->sess->MaxConnections);
			SESS_MTX_UNLOCK(conn);
			if (rc < 0) {
				ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
				goto error_return;
			}
		}

		/* declarative parameters */
		if (lu != NULL) {
			MTX_LOCK(&lu->mutex);
			if (lu->alias != NULL) {
				snprintf(buf, sizeof (buf), "%s", lu->alias);
			} else {
				snprintf(buf, sizeof (buf), "%s", "");
			}
			MTX_UNLOCK(&lu->mutex);
			SESS_MTX_LOCK(conn);
			rc = istgt_iscsi_param_set(conn->sess->params,
				"TargetAlias", buf);
			SESS_MTX_UNLOCK(conn);
			if (rc < 0) {
				ISTGT_ERRLOG("iscsi_param_set() failed\n");
				goto error_return;
			}
		}
		snprintf(buf, sizeof (buf), "%s:%s,%d",
			conn->portal.host, conn->portal.port, conn->portal.tag);
		SESS_MTX_LOCK(conn);
		rc = istgt_iscsi_param_set(conn->sess->params,
			"TargetAddress", buf);
		SESS_MTX_UNLOCK(conn);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_param_set() failed\n");
			goto error_return;
		}
		snprintf(buf, sizeof (buf), "%d", conn->portal.tag);
		SESS_MTX_LOCK(conn);
		rc = istgt_iscsi_param_set(conn->sess->params,
			"TargetPortalGroupTag", buf);
		SESS_MTX_UNLOCK(conn);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_param_set() failed\n");
			goto error_return;
		}

		/* write in response */
		if (lu != NULL) {
			SESS_MTX_LOCK(conn);
			val = ISCSI_GETVAL(conn->sess->params, "TargetAlias");
			if (val != NULL && strlen(val) != 0) {
				data_len = istgt_iscsi_append_param(conn,
					"TargetAlias", data, alloc_len, data_len);
			}
			if (strcasecmp(session_type, "Discovery") == 0) {
				data_len = istgt_iscsi_append_param(conn,
					"TargetAddress", data, alloc_len, data_len);
			}
			data_len = istgt_iscsi_append_param(conn,
				"TargetPortalGroupTag", data, alloc_len, data_len);
			SESS_MTX_UNLOCK(conn);
		}

		/* start login phase */
		conn->login_phase = ISCSI_LOGIN_PHASE_START;
	}

	/* negotiate parameters */
	data_len = istgt_iscsi_negotiate_params(conn, params,
		data, alloc_len, data_len);
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "Negotiated Params",
		data, data_len);

	switch (CSG) {
	case 0:
		/* SecurityNegotiation */
		auth_method = ISCSI_GETVAL(conn->params, "AuthMethod");
		if (auth_method == NULL) {
			ISTGT_ERRLOG("AuthMethod is empty\n");
			/* Missing parameter */
			StatusClass = 0x02;
			StatusDetail = 0x07;
			goto response;
		}
		if (strcasecmp(auth_method, "None") == 0) {
			conn->authenticated = 1;
		} else {
			rc = istgt_iscsi_auth_params(conn, params, auth_method,
				data, alloc_len, data_len);
			if (rc < 0) {
				ISTGT_ERRLOG("iscsi_auth_params() failed\n");
				/* Authentication failure */
				StatusClass = 0x02;
				StatusDetail = 0x01;
				goto response;
			}
			data_len = rc;
			if (conn->authenticated == 0) {
				/* not complete */
				T_bit = 0;
			} else {
				if (conn->auth.chap_phase != ISTGT_CHAP_PHASE_END) {
					ISTGT_WARNLOG("CHAP phase not complete");
				}
			}
#if 0
			ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
				"Negotiated Auth Params", data, data_len);
#endif
		}
		break;
	case 1:
		/* LoginOperationalNegotiation */
		if (conn->login_phase == ISCSI_LOGIN_PHASE_START) {
			if (conn->req_auth) {
				/* Authentication failure */
				StatusClass = 0x02;
				StatusDetail = 0x01;
				goto response;
			} else {
				/* AuthMethod=None */
				conn->authenticated = 1;
			}
		}
		if (conn->authenticated == 0) {
			ISTGT_ERRLOG("authentication error\n");
			/* Authentication failure */
			StatusClass = 0x02;
			StatusDetail = 0x01;
			goto response;
		}
		break;
	case 3:
		/* FullFeaturePhase */
		ISTGT_ERRLOG("XXX Login in FullFeaturePhase\n");
		/* Initiator error */
		StatusClass = 0x02;
		StatusDetail = 0x00;
		goto response;
	default:
		ISTGT_ERRLOG("unknown stage\n");
		/* Initiator error */
		StatusClass = 0x02;
		StatusDetail = 0x00;
		goto response;
	}

	if (T_bit) {
		switch (NSG) {
		case 0:
			/* SecurityNegotiation */
			conn->login_phase = ISCSI_LOGIN_PHASE_SECURITY;
			break;
		case 1:
			/* LoginOperationalNegotiation */
			conn->login_phase = ISCSI_LOGIN_PHASE_OPERATIONAL;
			break;
		case 3:
			/* FullFeaturePhase */
			conn->login_phase = ISCSI_LOGIN_PHASE_FULLFEATURE;
			rc = istgt_lu_add_nexus(conn->sess->lu, conn->initiator_port);
			if (rc == -1) {
				/* Ignore the error, Since during discovery there is no nexus formed */
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Failed to Add  the Nexus\n");
			} else if (rc == -2) {
				ISTGT_ERRLOG("Failed to Add  the Nexus\n");
				goto error_return;
			}

			if (ISCSI_EQVAL(conn->sess->params, "SessionType", "Normal")) {
				/* normal session */
				if (conn->sess->lu != NULL)
					MTX_LOCK(&conn->sess->lu->mutex);
				SESS_MTX_LOCK(conn);
				tsih = conn->sess->tsih;
				/* new tsih? */
				if (tsih == 0) {
					tsih = istgt_lu_allocate_tsih(conn->sess->lu,
						conn->initiator_port,
						conn->portal.tag);
					if (tsih == 0) {
						SESS_MTX_UNLOCK(conn);
						if (conn->sess->lu != NULL)
							MTX_UNLOCK(&conn->sess->lu->mutex);
						ISTGT_ERRLOG("lu_allocate_tsih() failed\n");
						goto error_return;
					}
					conn->sess->tsih = tsih;
				} else {
					/* multiple connection */
				}
				conn->sess->lu->conns++;
				snprintf(buf, sizeof (buf), "Login from %s (%s) on %s LU%d"
					" (%s:%s,%d), ISID=%"PRIx64", TSIH=%u,"
					" CID=%u, HeaderDigest=%s, DataDigest=%s\n",
					conn->initiator_name, conn->initiator_addr,
					conn->target_name, conn->sess->lu->num,
					conn->portal.host, conn->portal.port,
					conn->portal.tag,
					conn->sess->isid, conn->sess->tsih, conn->cid,
					(ISCSI_EQVAL(conn->params, "HeaderDigest", "CRC32C")
					? "on" : "off"),
					(ISCSI_EQVAL(conn->params, "DataDigest", "CRC32C")
					? "on" : "off"));
				ioctl_call(conn, TYPE_LOGIN);
				ISTGT_NOTICELOG("%s", buf);
				SESS_MTX_UNLOCK(conn);
				if (conn->sess->lu != NULL)
					MTX_UNLOCK(&conn->sess->lu->mutex);
				istgt_connection_status(conn, "SUCCESSFULL LOGIN");

			} else if (ISCSI_EQVAL(conn->sess->params, "SessionType", "Discovery")) {
				/* discovery session */
				/* new tsih */
				SESS_MTX_LOCK(conn);
				MTX_LOCK(&g_last_tsih_mutex);
				tsih = conn->sess->tsih;
				g_last_tsih++;
				tsih = g_last_tsih;
				if (tsih == 0) {
					g_last_tsih++;
					tsih = g_last_tsih;
				}
				conn->sess->tsih = tsih;
				MTX_UNLOCK(&g_last_tsih_mutex);

				snprintf(buf, sizeof (buf), "Login(discovery) from %s (%s) on"
					" (%s:%s,%d), ISID=%"PRIx64", TSIH=%u,"
					" CID=%u, HeaderDigest=%s, DataDigest=%s\n",
					conn->initiator_name, conn->initiator_addr,
					conn->portal.host, conn->portal.port,
					conn->portal.tag,
					conn->sess->isid, conn->sess->tsih, conn->cid,
					(ISCSI_EQVAL(conn->params, "HeaderDigest", "CRC32C")
					? "on" : "off"),
					(ISCSI_EQVAL(conn->params, "DataDigest", "CRC32C")
					? "on" : "off"));
				ISTGT_NOTICELOG("%s", buf);
				SESS_MTX_UNLOCK(conn);
			} else {
				ISTGT_ERRLOG("unknown session type\n");
				/* Initiator error */
				StatusClass = 0x02;
				StatusDetail = 0x00;
				goto response;
			}

			conn->full_feature = 1;
			break;
		default:
			ISTGT_ERRLOG("unknown stage\n");
			/* Initiator error */
			StatusClass = 0x02;
			StatusDetail = 0x00;
			goto response;
		}
	}

response:
	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	if (data_len == 0) {
		xfree(data);
		data = NULL;
	}
	rsp_pdu.data = data;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_LOGIN_RSP;
	BDADD8(&rsp[1], T_bit, 7);
	BDADD8(&rsp[1], C_bit, 6);
	BDADD8W(&rsp[1], CSG, 3, 2);
	BDADD8W(&rsp[1], NSG, 1, 2);
	rsp[2] = ISCSI_VERSION; // Version-max
	rsp[3] = ISCSI_VERSION; // Version-active
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], data_len); // DataSegmentLength

	DSET48(&rsp[8], isid);
	DSET16(&rsp[14], tsih);
	DSET32(&rsp[16], task_tag);

	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);
	} else {
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		DSET32(&rsp[28], CmdSN);
		DSET32(&rsp[32], CmdSN);
	}

	rsp[36] = StatusClass;
	rsp[37] = StatusDetail;

#if 0
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "PDU", rsp, ISCSI_BHS_LEN);
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "DATA", data, data_len);
#endif
	rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		istgt_iscsi_param_free(params);
		return (-1);
	}

	/* after send PDU digest on/off */
	if (conn->full_feature) {
		/* update internal variables */
		istgt_iscsi_copy_param2var(conn);
		/* check value */
		rc = istgt_iscsi_check_values(conn);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_check_values() failed\n");
			istgt_iscsi_param_free(params);
			if (data != NULL)
				xfree(data);
			return (-1);
		}
	}

	istgt_iscsi_param_free(params);
	return (0);
}

static int
istgt_iscsi_op_text(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	ISCSI_PARAM *params = NULL;
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	uint8_t *data;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t transfer_tag;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	const char *val;
	int I_bit, F_bit, C_bit;
	int data_len;
	int alloc_len;
	int rc;
	uint32_t sExpCmdSN = 0;
	uint32_t sMaxCmdSN = 0;
	uint32_t cStatSN = 0;
	int  step = 0;
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}

	data_len = 0;
	alloc_len = conn->sendbufsize;
	data = xmalloc(alloc_len); // (uint8_t *) conn->sendbuf;
	memset(data, 0, alloc_len);

	cp = (uint8_t *) &pdu->bhs;
	I_bit = BGET8(&cp[0], 7);
	F_bit = BGET8(&cp[1], 7);
	C_bit = BGET8(&cp[1], 6);

	lun = DGET64(&cp[8]);
	task_tag = DGET32(&cp[16]);
	transfer_tag = DGET32(&cp[20]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);

	SESS_MTX_LOCK(conn);
	sExpCmdSN = conn->sess->ExpCmdSN;
	sMaxCmdSN = conn->sess->MaxCmdSN;
	cStatSN = conn->StatSN;

	if (I_bit == 0) {
		if (SN32_LT(CmdSN, conn->sess->ExpCmdSN)
			|| SN32_GT(CmdSN, conn->sess->MaxCmdSN)) {
			SESS_MTX_UNLOCK(conn);
			ISTGT_ERRLOG("op_text CSN=%x ignore expCSN:%x-%x.  I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eSSN=%x, StatSN=%x\n",
				CmdSN, sExpCmdSN, sMaxCmdSN,
				I_bit, F_bit, C_bit, task_tag, transfer_tag, ExpStatSN, cStatSN);
			xfree(data);
			return (-1);
		}
	} else if (CmdSN != conn->sess->ExpCmdSN) {
		SESS_MTX_UNLOCK(conn);
		ISTGT_ERRLOG("op_text CSN=%x not expCSN:%x-%x.  I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eSSN=%x, StatSN=%x\n",
			CmdSN, sExpCmdSN, sMaxCmdSN,
			I_bit, F_bit, C_bit, task_tag, transfer_tag, ExpStatSN, cStatSN);
		xfree(data);
		return (-1);
	}
	if (SN32_GT(ExpStatSN, conn->StatSN)) {
		step = 1;
		conn->StatSN = ExpStatSN;
	}
	if (ExpStatSN != conn->StatSN) {
		/* StarPort have a bug */
		step = 2;
		conn->StatSN = ExpStatSN;
	}
	SESS_MTX_UNLOCK(conn);

	if (step == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"op_text CSN=%x SSN:%x-advancedto-eSSN:%x.  I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eCSN:%x-%x\n",
			CmdSN, cStatSN, ExpStatSN,
			I_bit, F_bit, C_bit, task_tag, transfer_tag, sExpCmdSN, sMaxCmdSN);
	} else if (step == 2) {
		ISTGT_WARNLOG("op_text CSN=%x SSN:%x-rewoundto-eSSN:%x.  I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eCSN:%x-%x\n",
			CmdSN, cStatSN, ExpStatSN,
			I_bit, F_bit, C_bit, task_tag, transfer_tag, sExpCmdSN, sMaxCmdSN);
	}

	if (F_bit && C_bit) {
		if (step != 2) { // we didn't log
			ISTGT_ERRLOG("op_text CSN=%x final_and_continue. I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eSSN=%x, StatSN=%x, eCSN=%x-%x\n",
				CmdSN, I_bit, F_bit, C_bit, task_tag, transfer_tag,
				ExpStatSN, cStatSN, sExpCmdSN, sMaxCmdSN);
		} else {
			ISTGT_ERRLOG("CSN=%x final and continue\n", CmdSN);
		}
		xfree(data);
		return (-1);
	}

	if (step == 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"op_text CSN=%x final_and_continue. I=%d, F=%d, C=%d, ITT=%x, TTT=%x, eSSN=%x, StatSN=%x, eCSN=%x-%x\n",
			CmdSN, I_bit, F_bit, C_bit, task_tag, transfer_tag,
			ExpStatSN, cStatSN, sExpCmdSN, sMaxCmdSN);
	}


	/* store incoming parameters */
	rc = istgt_iscsi_parse_params(&params, pdu->data,
		pdu->data_segment_len);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_parse_params() failed\n");
		istgt_iscsi_param_free(params);
		xfree(data);
		return (-1);
	}

	/* negotiate parameters */
	data_len = istgt_iscsi_negotiate_params(conn, params,
		data, alloc_len, data_len);
	/* sendtargets is special case */
	val = ISCSI_GETVAL(params, "SendTargets");
	if (val != NULL) {
		if (strcasecmp(val, "") == 0) {
			val = conn->target_name;
		}
		SESS_MTX_LOCK(conn);
		ISCSI_GETVAL(conn->sess->params,
			"InitiatorName");
		if (ISCSI_EQVAL(conn->sess->params,
			"SessionType", "Discovery")) {
			SESS_MTX_UNLOCK(conn);
			data_len = istgt_lu_sendtargets(conn,
				conn->initiator_name,
				conn->initiator_addr,
				val, data, alloc_len, data_len);
			SESS_MTX_LOCK(conn);
		} else {
			if (strcasecmp(val, "ALL") == 0) {
				/* not in discovery session */
				data_len = istgt_iscsi_append_text(conn, "SendTargets",
					"Reject", data, alloc_len, data_len);
			} else {
				SESS_MTX_UNLOCK(conn);
				data_len = istgt_lu_sendtargets(conn,
					conn->initiator_name,
					conn->initiator_addr,
					val, data, alloc_len, data_len);
				SESS_MTX_LOCK(conn);
			}
		}
		SESS_MTX_UNLOCK(conn);
	}
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "Negotiated Params",
		data, data_len);

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	if (data_len == 0) {
		xfree(data);
		data = NULL;
	}
	rsp_pdu.data = data;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_TEXT_RSP;
	BDADD8(&rsp[1], F_bit, 7);
	BDADD8(&rsp[1], C_bit, 6);
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], data_len); // DataSegmentLength

	DSET64(&rsp[8], lun);
	DSET32(&rsp[16], task_tag);
	if (F_bit) {
		DSET32(&rsp[20], 0xffffffffU);
	} else {
		transfer_tag = 1 + conn->id;
		DSET32(&rsp[20], transfer_tag);
	}

	SESS_MTX_LOCK(conn);
	DSET32(&rsp[24], conn->StatSN);
	conn->StatSN++;
	if (I_bit == 0) {
		conn->sess->ExpCmdSN++;
		conn->sess->MaxCmdSN++;
		conn->sess->MaxCmdSN_local++;
	}
	DSET32(&rsp[28], conn->sess->ExpCmdSN);
	DSET32(&rsp[32], conn->sess->MaxCmdSN);
	SESS_MTX_UNLOCK(conn);

	rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		istgt_iscsi_param_free(params);
		return (-1);
	}

	/* update internal variables */
	istgt_iscsi_copy_param2var(conn);
	/* check value */
	rc = istgt_iscsi_check_values(conn);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_check_values() failed\n");
		istgt_iscsi_param_free(params);
		return (-1);
	}

	istgt_iscsi_param_free(params);
	return (0);
}

static int
istgt_iscsi_op_logout(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	char buf[MAX_TMPBUF];
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	uint32_t task_tag;
	uint16_t cid;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	int reason;
	int response;
	int data_len;
	int rc;

	data_len = 0;
	cp = (uint8_t *) &pdu->bhs;
	reason = BGET8W(&cp[1], 6, 7);

	task_tag = DGET32(&cp[16]);
	cid = DGET16(&cp[20]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		"reason=%d, ITT=%x, cid=%d\n",
		reason, task_tag, cid);
	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"CmdSN=%u, ExpStatSN=%u, StatSN=%u, ExpCmdSN=%u, MaxCmdSN=%u\n",
			CmdSN, ExpStatSN, conn->StatSN, conn->sess->ExpCmdSN,
			conn->sess->MaxCmdSN);
		if (CmdSN != conn->sess->ExpCmdSN) {
			ISTGT_WARNLOG("CmdSN(%u) might have dropped\n", CmdSN);
			/* ignore error */
		}
		SESS_MTX_UNLOCK(conn);
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"CmdSN=%u, ExpStatSN=%u, StatSN=%u\n",
			CmdSN, ExpStatSN, conn->StatSN);
	}
	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
	}
	if (SN32_GT(ExpStatSN, conn->StatSN)) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "StatSN(%u) advanced\n",
			ExpStatSN);
		conn->StatSN = ExpStatSN;
	}
	if (ExpStatSN != conn->StatSN) {
		ISTGT_WARNLOG("StatSN(%u/%u) might have dropped\n",
			ExpStatSN, conn->StatSN);
		/* ignore error */
	}
	if (conn->sess != NULL) {
		SESS_MTX_UNLOCK(conn);
	}

	response = 0; // connection or session closed successfully

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = NULL;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_LOGOUT_RSP;
	BDADD8W(&rsp[1], 1, 7, 1);
	rsp[2] = response;
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], data_len); // DataSegmentLength

	DSET32(&rsp[16], task_tag);

	if (conn->sess != NULL) {
		SESS_MTX_LOCK(conn);
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		if (conn->sess->connections == 1) {
			conn->sess->ExpCmdSN++;
			conn->sess->MaxCmdSN++;
			conn->sess->MaxCmdSN_local++;
		}
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);
	} else {
		DSET32(&rsp[24], conn->StatSN);
		conn->StatSN++;
		DSET32(&rsp[28], CmdSN);
		DSET32(&rsp[32], CmdSN);
	}

	DSET16(&rsp[40], 0); // Time2Wait
	DSET16(&rsp[42], 0); // Time2Retain

	rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}

	SESS_MTX_LOCK(conn);
	if (ISCSI_EQVAL(conn->sess->params, "SessionType", "Normal")) {
		snprintf(buf, sizeof (buf), "Logout from %s (%s) on %s LU%d"
			" (%s:%s,%d), ISID=%"PRIx64", TSIH=%u,"
			" CID=%u, HeaderDigest=%s, DataDigest=%s\n",
			conn->initiator_name, conn->initiator_addr,
			conn->target_name, conn->sess->lu->num,
			conn->portal.host, conn->portal.port, conn->portal.tag,
			conn->sess->isid, conn->sess->tsih, conn->cid,
			(ISCSI_EQVAL(conn->params, "HeaderDigest", "CRC32C")
			? "on" : "off"),
			(ISCSI_EQVAL(conn->params, "DataDigest", "CRC32C")
			? "on" : "off"));
		ioctl_call(conn, TYPE_LOGOUT);
	} else {
		/* discovery session */
		snprintf(buf, sizeof (buf), "Logout(discovery) from %s (%s) on"
			" (%s:%s,%d), ISID=%"PRIx64", TSIH=%u,"
			" CID=%u, HeaderDigest=%s, DataDigest=%s\n",
			conn->initiator_name, conn->initiator_addr,
			conn->portal.host, conn->portal.port, conn->portal.tag,
			conn->sess->isid, conn->sess->tsih, conn->cid,
			(ISCSI_EQVAL(conn->params, "HeaderDigest", "CRC32C")
			? "on" : "off"),
			(ISCSI_EQVAL(conn->params, "DataDigest", "CRC32C")
			? "on" : "off"));
	}
	SESS_MTX_UNLOCK(conn);
	istgt_connection_status(conn, "SUCCESSFULL LOGOUT");

	ISTGT_NOTICELOG("%s", buf);

	conn->exec_logout = 1;
	return (0);
}

static int istgt_iscsi_transfer_in_internal(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);

static int
istgt_iscsi_transfer_in(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	int rc;

	// MTX_LOCK(&conn->wpdu_mutex);
	rc = istgt_iscsi_transfer_in_internal(conn, lu_cmd);
	// MTX_UNLOCK(&conn->wpdu_mutex);
	return (rc);
}

static int
istgt_iscsi_transfer_in_internal(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISCSI_PDU rsp_pdu = { 0 };
	uint8_t *rsp;
	uint8_t *data;
	uint32_t task_tag;
	uint32_t transfer_tag;
	uint32_t DataSN;
	int transfer_len;
	int data_len;
	int segment_len;
	int offset;
	int F_bit, O_bit, U_bit, S_bit;
	int residual_len;
	int sent_status;
	int len;
	int rc;
	const char *msg = "";
	int segs = 0;
	data = lu_cmd->data;
	transfer_len = lu_cmd->transfer_len;
	data_len = lu_cmd->data_len;
	segment_len = conn->MaxRecvDataSegmentLength;

	F_bit = O_bit = U_bit = S_bit = 0;
	if (data_len < transfer_len) {
		/* underflow */
		msg = "Underflow";
		residual_len = transfer_len - data_len;
		transfer_len = data_len;
		U_bit = 1;
	} else if (data_len > transfer_len) {
		/* overflow */
		msg = "Overflow";
		residual_len = data_len - transfer_len;
		O_bit = 1;
	} else {
		residual_len = 0;
	}

	task_tag = lu_cmd->task_tag;
	transfer_tag = 0xffffffffU;
	DataSN = 0;
	sent_status = 0;

	/* send data splitted by segment_len */
	for (offset = 0; offset < transfer_len; offset += segment_len) {
		len = DMIN32(segment_len, (transfer_len - offset));
		if (offset + len > transfer_len) {
			ISTGT_ERRLOG("transfer missing\n");
			return (-1);
		} else if (offset + len == transfer_len) {
			/* final PDU */
			F_bit = 1;
			S_bit = 0;
			if (lu_cmd->sense_data_len == 0
				&& (lu_cmd->status == ISTGT_SCSI_STATUS_GOOD
				|| lu_cmd->status == ISTGT_SCSI_STATUS_CONDITION_MET
				|| lu_cmd->status == ISTGT_SCSI_STATUS_INTERMEDIATE
				|| lu_cmd->status == ISTGT_SCSI_STATUS_INTERMEDIATE_CONDITION_MET)) {
				S_bit = 1;
				sent_status = 1;
			}
		} else {
			F_bit = 0;
			S_bit = 0;
		}

		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"sending:%d, from:%u %d bytes of(%u/%u /%u %s) StatSN=%u, DataSN=%u\n",
			segs, offset, len, transfer_len, data_len, residual_len, msg,
			conn->StatSN, DataSN);
		++segs;

		/* DATA PDU */
		rsp = (uint8_t *) &rsp_pdu.bhs;
		rsp_pdu.data = data + offset;
		// memset(rsp, 0, ISCSI_BHS_LEN);
		uint64_t *tptr = (uint64_t *)rsp;
		*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
		*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

		rsp[0] = ISCSI_OP_SCSI_DATAIN;
		BDADD8(&rsp[1], F_bit, 7);
		BDADD8(&rsp[1], 0, 6); // A_bit Acknowledge
		if (F_bit && S_bit)  {
			BDADD8(&rsp[1], O_bit, 2);
			BDADD8(&rsp[1], U_bit, 1);
		} else {
			BDADD8(&rsp[1], 0, 2);
			BDADD8(&rsp[1], 0, 1);
		}
		BDADD8(&rsp[1], S_bit, 0);
		if (S_bit) {
			rsp[3] = lu_cmd->status;
		} else {
			rsp[3] = 0; // Status or Rsvd
		}
		rsp[4] = 0; // TotalAHSLength
		DSET24(&rsp[5], len); // DataSegmentLength

		DSET32(&rsp[16], task_tag);
		DSET32(&rsp[20], transfer_tag);

		SESS_MTX_LOCK(conn);
		if (S_bit) {
			DSET32(&rsp[24], conn->StatSN);
			conn->StatSN++;
		} else {
			DSET32(&rsp[24], 0); // StatSN or Reserved
		}
		if (F_bit && S_bit && lu_cmd->I_bit == 0) {
			if (likely(lu_cmd->lu->limit_q_size == 0 || ((int)(conn->sess->MaxCmdSN - conn->sess->ExpCmdSN) < lu_cmd->lu->limit_q_size))) {
				conn->sess->MaxCmdSN++;
				conn->sess->MaxCmdSN_local++;
								if (unlikely((conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local) && (lu_cmd->lu->limit_q_size == 0))) {
					ISTGT_LOG("conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local in transfer internal\n");
										conn->sess->MaxCmdSN = conn->sess->MaxCmdSN_local;
				}
			 } else
				conn->sess->MaxCmdSN_local++;
		}
		DSET32(&rsp[28], conn->sess->ExpCmdSN);
		DSET32(&rsp[32], conn->sess->MaxCmdSN);
		SESS_MTX_UNLOCK(conn);

		DSET32(&rsp[36], DataSN);
		DataSN++;

		DSET32(&rsp[40], (uint32_t) offset);
		if (F_bit && S_bit)  {
			DSET32(&rsp[44], residual_len);
		} else {
			DSET32(&rsp[44], 0);
		}

		rc = istgt_iscsi_write_pdu_internal(conn, &rsp_pdu);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
			return (-1);
		}
	}

	if (sent_status) {
		return (1);
	}
	return (0);
}

static void
parse_scsi_cdb(ISTGT_LU_CMD_Ptr lu_cmd)
{
#define	setinf1()  {					 \
	if (dpo) lu_cmd->info[lu_cmd->infdx++] = 'd'; \
	if (fua) lu_cmd->info[lu_cmd->infdx++] = 'f'; \
	if (fua_nv) lu_cmd->info[lu_cmd->infdx++] = 'n'; \
}
	int sync_nv = 0, immed = 0;
	int sa = 0;
	int NOR = 0, NOW = 0;
	int dpo = 0, fua = 0, fua_nv = 0; // cdb[1] bits 4, 3, 1
	int bytchk = 0;	 // cdb[1] bits 1
	int anchor = 0, unmap = 0;  // cdb[1] bits 4,  3
	int pbdata = 0, lbdata = 0; // cdb[1] bits 2,  1
	uint64_t lba = 0;
	uint32_t len = 0;
	uint32_t transfer_len = 0; // uint32_t parameter_len;
	uint8_t *cdb  = lu_cmd->cdb;

	uint8_t sidx = istgt_cmd_table[cdb[0]].statidx;
	SCSIstat_rest[sidx].opcode = cdb[0];
	++SCSIstat_rest[sidx].req_start;

	lu_cmd->infdx = 0;
	lu_cmd->cdbflags = 0;
	lu_cmd->cdb0  = cdb[0];
	lu_cmd->lba   = 0;
	lu_cmd->lblen = 0;

	// ISTGT_LU_DISK *spec = NULL;
	// int lun_i = istgt_lu_islun2lun(lu_cmd->lun);
	// if (lun_i < lu_cmd->lu->maxlun)
	// 	spec = (ISTGT_LU_DISK *) (lu_cmd->lu->lun[lun_i].spec);

	switch (cdb[0]) {
		  // case SBC_READ_6:
		  // case SBC_WRITE_6:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) (DGET24(&lu_cmd->cdb[1]) & 0x001fffffU);
		  // lu_cmd->lblen = (uint32_t) DGET8(&lu_cmd->cdb[4]);
		  // break;
		  // case SBC_READ_10:
		  // case SBC_WRITE_10:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET32(&lu_cmd->cdb[2]);
		  // lu_cmd->lblen = (uint32_t) DGET16(&lu_cmd->cdb[7]);
		  // break;
		  // case SBC_WRITE_AND_VERIFY_10:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET32(&lu_cmd->cdb[2])
		  // lu_cmd->lblen = (uint32_t) DGET16(&lu_cmd->cdb[7]);
		  // break;
		  // case SBC_READ_12:
		  // case SBC_WRITE_12:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET32(&lu_cmd->cdb[2]);
		  // lu_cmd->lblen = (uint32_t) DGET32(&lu_cmd->cdb[6]);
		  // break;
		  // case SBC_WRITE_AND_VERIFY_12:
		  // lu_cmd->cdb0 = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET32(&lu_cmd->cdb[2]);
		  // lu_cmd->lblen = (uint32_t) DGET32(&lu_cmd->cdb[6]);
		  // break;
		  // case SBC_READ_16:
		  // case SBC_WRITE_16:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET64(&lu_cmd->cdb[2]);
		  // lu_cmd->lblen = (uint32_t) DGET32(&lu_cmd->cdb[10]);
		  // break;
		  // case SBC_WRITE_AND_VERIFY_16:
		  // lu_cmd->cdb0  = lu_cmd->cdb[0];
		  // lu_cmd->lba   = (uint64_t) DGET64(&lu_cmd->cdb[2]);
		  // lu_cmd->lblen = (uint32_t) DGET32(&lu_cmd->cdb[10]);
		  // break;

		case SBC_READ_6:
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			lba = (uint64_t) (DGET24(&cdb[1]) & 0x001fffffU);
			transfer_len = (uint32_t) DGET8(&cdb[4]);
			if (transfer_len == 0)
				transfer_len = 256;
			break;

		case SBC_READ_10:
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			break;

		case SBC_READ_12:
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[6]);
			break;

		case SBC_READ_16:
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			break;

		case SBC_WRITE_6:
			if (lu_cmd->W_bit == 0)
				NOW = 1; // WBit not set to 1
			lba = (uint64_t) (DGET24(&cdb[1]) & 0x001fffffU);
			transfer_len = (uint32_t) DGET8(&cdb[4]);
			if (transfer_len == 0)
				transfer_len = 256;
			break;

		case SBC_WRITE_10:
		case SBC_WRITE_AND_VERIFY_10:
			if (lu_cmd->W_bit == 0)
				NOW = 1; // WBit not set to 1
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			break;

		case SBC_WRITE_12:
		case SBC_WRITE_AND_VERIFY_12:
			if (lu_cmd->W_bit == 0)
				NOW = 1; // WBit not set to 1
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[6]);
			break;

		case SBC_WRITE_16:
		case SBC_WRITE_AND_VERIFY_16:
			if (lu_cmd->W_bit == 0)
				NOW = 1; // WBit not set to 1
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			break;

		case SBC_VERIFY_10:
			dpo = BGET8(&cdb[1], 4);
			bytchk = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET16(&cdb[7]);
			break;

		case SBC_VERIFY_12:
			dpo = BGET8(&cdb[1], 4);
			bytchk = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[6]);
			break;

		case SBC_VERIFY_16:
			dpo = BGET8(&cdb[1], 4);
			bytchk = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET64(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[10]);
			break;

		case SBC_WRITE_SAME_10:
			if (lu_cmd->W_bit == 0)
				NOW = 1;
			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			if (pbdata)
				lu_cmd->info[lu_cmd->infdx++] = 'P';
			if (lbdata)
				lu_cmd->info[lu_cmd->infdx++] = 'L';
			break;

		case SBC_WRITE_SAME_16:
			if (lu_cmd->W_bit == 0)
				NOW = 1;
			anchor = BGET8(&cdb[1], 4);
			unmap = BGET8(&cdb[1], 3);
			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			if (unmap)
				lu_cmd->info[lu_cmd->infdx++] = 'U';
			if (pbdata)
				lu_cmd->info[lu_cmd->infdx++] = 'P';
			if (lbdata)
				lu_cmd->info[lu_cmd->infdx++] = 'L';
			break;

		case SBC_COMPARE_AND_WRITE:
			if (lu_cmd->W_bit == 0)
				NOW = 1;
			dpo = BGET8(&cdb[1], 4);
			fua = BGET8(&cdb[1], 3);
			fua_nv = BGET8(&cdb[1], 1);
			setinf1()
			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET8(&cdb[13]);

			// maxlen = ISTGT_LU_WORK_ATS_BLOCK_SIZE / spec->blocklen;
			// if (maxlen > 0xff)
			// 	maxlen = 0xff;
			// if (transfer_len > maxlen)
			// 	inv = 1;
			break;

		case SBC_SYNCHRONIZE_CACHE_10:
			sync_nv = BGET8(&cdb[1], 2);
			immed = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET16(&cdb[7]);
			// if (len == 0)
			//	len = spec->blockcnt;
			break;

		case SBC_SYNCHRONIZE_CACHE_16:
			sync_nv = BGET8(&cdb[1], 2);
			immed = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET64(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[10]);
			// if (len == 0)
			//	len = spec->blockcnt;
			break;

		case SBC_READ_DEFECT_DATA_10:
		// case SBC_READ_DEFECT_DATA_16:
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			break;

		case SCC_MAINTENANCE_IN:
			sa = BGET8W(&cdb[1], 4, 5);
			if (sa == SPC_MI_REPORT_TARGET_PORT_GROUPS) {
				if (lu_cmd->R_bit == 0)
					NOR = 1;
				// alen = DGET32(&cdb[6]);
			}
			break;

		case SCC_MAINTENANCE_OUT:
			sa = BGET8W(&cdb[1], 4, 5);
			if (sa == SPC_MO_SET_TARGET_PORT_GROUPS) {
				if (lu_cmd->W_bit == 0)
					NOW = 1;
				// parameter_len = DGET32(&cdb[6]);
			}
			break;

		case SPC_PERSISTENT_RESERVE_IN:
			sa = BGET8W(&cdb[1], 4, 5);
			if (lu_cmd->R_bit == 0)
				NOR = 1;
			// alen = DGET16(&cdb[7]);
			break;

		case SPC_PERSISTENT_RESERVE_OUT:
			if (lu_cmd->W_bit == 0)
				NOW = 1;
			sa = BGET8W(&cdb[1], 4, 5);
			// parameter_len = DGET32(&cdb[5]);
			break;

		case SPC_EXTENDED_COPY:
			sa = BGET8W(&cdb[1], 4, 5); /* LID1  - 00h / LID4 - 01h  */
			break;
		case SPC2_RELEASE_6:
			break;
		case SPC2_RELEASE_10:
			break;
		case SPC2_RESERVE_6:
			break;
		case SPC2_RESERVE_10:
			break;
		case SBC_UNMAP:
			break;
		default:
			break;
	}
	// if (NOR == 0 && NOW == 0) {
	// }
	lu_cmd->lba = lba;
	if (transfer_len != 0)
		lu_cmd->lblen = transfer_len;
	else
		lu_cmd->lblen = len;

	lu_cmd->dpo = dpo == 1 ? 1 : 0;
	lu_cmd->fua = fua == 1 ? 1 : 0;
	lu_cmd->fua_nv = fua_nv == 1 ? 1 : 0;
	lu_cmd->bytchk = bytchk == 1 ? 1 : 0;
	lu_cmd->anchor = anchor == 1 ? 1 : 0;
	lu_cmd->unmap = unmap == 1 ? 1 : 0;
	lu_cmd->pbdata = pbdata == 1 ? 1 : 0;
	lu_cmd->lbdata = lbdata == 1 ? 1 : 0;
	lu_cmd->sync_nv = sync_nv == 1 ? 1 : 0;
	lu_cmd->immed = immed == 1 ? 1 : 0;
	if (NOW) {
		lu_cmd->info[lu_cmd->infdx++] = '-';
		lu_cmd->info[lu_cmd->infdx++] = 'W';
	} else if (NOR) {
		lu_cmd->info[lu_cmd->infdx++] = '-';
		lu_cmd->info[lu_cmd->infdx++] = 'R';
	}
	lu_cmd->info[lu_cmd->infdx] = '\0';
}

void
timediff(ISTGT_LU_CMD_Ptr p, char  ch, uint16_t line)
{
	p->_lst = p->_andx;
	int _inx = ++(p->_andx);
	struct timespec *_s = &(p->times[_inx-1]);
	struct timespec *_n = &(p->times[_inx]);
	struct timespec *_r = &(p->tdiff[_inx]);
	clock_gettime(clockid, _n);
	p->caller[_inx] = ch ? ch : '_';
	p->line[_inx] = line;
	if (p->_andx >= _PSZ-1) {
		p->times[0] = p->times[_inx];
		p->_andx = 0;
		++p->_roll_cnt;
	} else {
		p->caller[_inx+1] = '\0';
	}
	if ((_n->tv_nsec - _s->tv_nsec) < 0) {
		_r->tv_sec  = _n->tv_sec - _s->tv_sec-1;
		_r->tv_nsec = 1000000000 + _n->tv_nsec - _s->tv_nsec;
	} else {
		_r->tv_sec  = _n->tv_sec - _s->tv_sec;
		_r->tv_nsec = _n->tv_nsec - _s->tv_nsec;
	}
}

static int
istgt_iscsi_op_scsi(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	int istgt_state, lunum;
	ISTGT_LU_Ptr lu;
	ISTGT_LU_CMD lu_cmd;
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	// uint8_t *data;
	uint8_t *cdb;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t transfer_len;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	size_t bidi_residual_len;
	size_t residual_len;
	size_t data_len;
	int I_bit, F_bit, R_bit, W_bit, Attr_bit;
	int o_bit, u_bit, O_bit, U_bit;
	int rc;
	uint32_t c_StatSN, s_ExpCmdSN, s_MaxCmdSN;
	uint32_t QCmdSN;
	int s_conns;
	int operation_mode = 0;
	const char *msg = "", *que = "";
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}

	data_len = 0;

	cp = (uint8_t *) &pdu->bhs;
	I_bit = BGET8(&cp[0], 6);
	F_bit = BGET8(&cp[1], 7);
	R_bit = BGET8(&cp[1], 6);
	W_bit = BGET8(&cp[1], 5);
	Attr_bit = BGET8W(&cp[1], 2, 3);

	lun = DGET64(&cp[8]);
	task_tag = DGET32(&cp[16]);
	transfer_len = DGET32(&cp[20]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);

	cdb = &cp[32];
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "CDB", cdb, 16);
#if 0
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "PDU", cp, ISCSI_BHS_LEN);
#endif

	SESS_MTX_LOCK(conn);
	c_StatSN  = conn->StatSN;
	s_ExpCmdSN = conn->sess->ExpCmdSN;
	s_MaxCmdSN = conn->sess->MaxCmdSN;
	s_conns = conn->sess->connections;
	if (conn->sess->lu && conn->sess->lu->istgt)
		operation_mode = conn->sess->lu->istgt->OperationalMode;
	lu = conn->sess->lu;
	lunum = lu ? lu->num : -1;
	SESS_MTX_UNLOCK(conn);

	lu_cmd.cdb = cdb;
	parse_scsi_cdb(&lu_cmd);
	lu_cmd.task_tag = task_tag;
	lu_cmd.transfer_len = transfer_len;

	lu_cmd.I_bit = I_bit;
	lu_cmd.F_bit = F_bit;
	lu_cmd.R_bit = R_bit;
	lu_cmd.W_bit = W_bit;
	lu_cmd.Attr_bit = Attr_bit;
	lu_cmd.lun = lun;
	lu_cmd.CmdSN = CmdSN;
	lu_cmd.flags = 0;

	lu_cmd.times[0] = pdu->start;
	lu_cmd.caller[0] = 'i';
	lu_cmd._andx = 0;
	lu_cmd._lst = 0;
	lu_cmd._roll_cnt = 0;
	timediff(&lu_cmd, 'b', __LINE__);
	lu_cmd.lunum = lunum;

	lu_cmd.iobufindx = -1;
	lu_cmd.iobufsize = 0;
	lu_cmd.data = NULL; // data;
	lu_cmd.data_len = 0;
	lu_cmd.alloc_len = 0; // alloc_len;
	lu_cmd.status = 0;
	lu_cmd.sense_data = NULL; // xmalloc(conn->snsbufsize);
	lu_cmd.sense_alloc_len = 0; // conn->snsbufsize;
	lu_cmd.sense_data_len = 0;
	lu_cmd.connGone = 0;
#ifdef REPLICATION
	clock_gettime(CLOCK_MONOTONIC_RAW, &lu_cmd.start_rw_time);
#endif
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"LU%d: CSN:%x ITT:%x (%lu/%u)[0x%x %lx+%x] PG=0x%4.4x, LUN=0x%lx "
		"ExpStatSN=%x StatSN=%x ExpCmdSN=%x MaxCmdSN=%x "
		"%c%c%c%c Attr%d\n",
		lunum, CmdSN,
		task_tag, pdu->data_segment_len, transfer_len,
		lu_cmd.cdb0, lu_cmd.lba, lu_cmd.lblen,
		conn->portal.tag, lun, // lu->name,
		ExpStatSN, c_StatSN, s_ExpCmdSN, s_MaxCmdSN,
		I_bit ? 'I' : ' ', F_bit ? 'F' : ' ', R_bit ? 'R' : ' ', W_bit ? 'W' : ' ',
		Attr_bit);

	if (I_bit == 0) {
		/* XXX MCS reverse order? */
		if (SN32_GT(CmdSN, s_ExpCmdSN)) {
			if (s_conns > 1) {
				struct timespec abstime;
				time_t start, now;

				start = now = time(NULL);
				memset(&abstime, 0, sizeof (abstime));
				abstime.tv_sec = now + (MAX_MCSREVWAIT / 1000);
				abstime.tv_nsec = (MAX_MCSREVWAIT % 1000) * 1000000;

				rc = 0;
				SESS_MTX_LOCK(conn);
				while (SN32_GT(CmdSN, conn->sess->ExpCmdSN)) {
					conn->sess->req_mcs_cond++;
					rc = pthread_cond_timedwait(&conn->sess->mcs_cond,
						&conn->sess->mutex,
						&abstime);
					if (rc == ETIMEDOUT) {
						if (SN32_GT(CmdSN, conn->sess->ExpCmdSN)) {
							rc = -1;
							/* timeout */
							break;
						}
						/* OK cond */
						rc = 0;
						break;
					}
					if (rc != 0) {
						break;
					}
				}
				c_StatSN = conn->StatSN;
				s_ExpCmdSN = conn->sess->ExpCmdSN;
				s_MaxCmdSN = conn->sess->MaxCmdSN;
				s_conns = conn->sess->connections;
				if (conn->sess->lu && conn->sess->lu->istgt)
					operation_mode = conn->sess->lu->istgt->OperationalMode;
				if (I_bit == 0) {
					if (SN32_GT(CmdSN, s_ExpCmdSN))
						conn->sess->ExpCmdSN = CmdSN;
				} else if (CmdSN == s_ExpCmdSN) {
					if (SN32_GT(ExpStatSN, c_StatSN))
						conn->StatSN = ExpStatSN;
				}
				SESS_MTX_UNLOCK(conn);
				if (rc < 0) {
					now = time(NULL);
					ISTGT_ERRLOG("MCS: CmdSN(%u) error ExpCmdSN=%u "
						"(time=%f)\n",
						CmdSN, s_ExpCmdSN,
						difftime(now, start));
					return (-1);
				}
#if 0
				ISTGT_WARNLOG("MCS: reverse CmdSN=%u(retry=%d, yields=%d)\n",
					CmdSN, retry, try_yields);
#endif
			}
		}
	}

	if (I_bit == 0) {
		if (SN32_LT(CmdSN, s_ExpCmdSN)
			|| SN32_GT(CmdSN, s_MaxCmdSN)) {
			ISTGT_ERRLOG("CmdSN(%u) ignore (ExpCmdSN=%u, MaxCmdSN=%u)\n",
				CmdSN, s_ExpCmdSN, s_MaxCmdSN);
			return (-1);
		}
		if (SN32_GT(CmdSN, s_ExpCmdSN)) {
			ISTGT_WARNLOG("CmdSN(%u) > ExpCmdSN(%u)\n",
				CmdSN, s_ExpCmdSN);
		}
	} else if (CmdSN != s_ExpCmdSN) {
		ISTGT_ERRLOG("CmdSN(%u) error ExpCmdSN=%u\n",
			CmdSN, s_ExpCmdSN);
		return (-1);
	}
	if (SN32_GT(ExpStatSN, c_StatSN)) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "StatSN(%u) advanced\n",
			ExpStatSN);
	}


	SESS_MTX_LOCK(conn);
	if (conn->sess->lu && conn->sess->lu->istgt)
		operation_mode = conn->sess->lu->istgt->OperationalMode;
	if (I_bit == 0) {
		if (SN32_GT(CmdSN, s_ExpCmdSN))
			s_ExpCmdSN = conn->sess->ExpCmdSN = CmdSN;
	} else if (CmdSN == s_ExpCmdSN) {
		if (SN32_GT(ExpStatSN, c_StatSN))
			c_StatSN = conn->StatSN = ExpStatSN;
	}

	QCmdSN = s_MaxCmdSN - s_ExpCmdSN + 1 + conn->queue_depth;
	if (SN32_LT(ExpStatSN + QCmdSN, c_StatSN)) {
		SESS_MTX_UNLOCK(conn);
		ISTGT_ERRLOG("StatSN(%u/%u) QCmdSN(%u) error\n",
			ExpStatSN, c_StatSN, QCmdSN);
		return (-1);
	}

	lu_cmd.pdu = pdu;
	lu_cmd.lu = conn->sess->lu;
	if (I_bit == 0) {
		conn->sess->ExpCmdSN++;
		if (conn->sess->req_mcs_cond > 0) {
			conn->sess->req_mcs_cond--;
			rc = pthread_cond_broadcast(&conn->sess->mcs_cond);
			if (rc != 0) {
				SESS_MTX_UNLOCK(conn);
				ISTGT_ERRLOG("cond_broadcast() failed\n");
				return (-1);
			}
		}
	}
	SESS_MTX_UNLOCK(conn);

	if (R_bit != 0 && W_bit != 0) {
		ISTGT_ERRLOG("Bidirectional CDB is not supported\n");
		return (-1);
	}
	if (lu == NULL) {
		ISTGT_ERRLOG("lu not found\n");
		return (-1);
	}

	istgt_state = istgt_get_state(lu->istgt);
	if (istgt_state == ISTGT_STATE_EXITING || istgt_state == ISTGT_STATE_SHUTDOWN) {
		ISTGT_ERRLOG("istgt shutting down\n");
		rc = 0;
	} else if (lu->online == 0) {
		ISTGT_ERRLOG("LU%d: offline\n", lu->num);
		/* LOGICAL UNIT NOT READY, CAUSE NOT REPORTABLE */
		istgt_lu_scsi_build_sense_data(&lu_cmd,
					ISTGT_SCSI_SENSE_NOT_READY,
					0x04, 0x00);
		lu_cmd.status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		rc = 0;
	} else if (lu->type != ISTGT_LU_TYPE_DISK) {
		ISTGT_ERRLOG("LU%d: type:%d is not disk\n", lu->num, lu->type);
		/* LOGICAL UNIT NOT READY, CAUSE NOT REPORTABLE */
		istgt_lu_scsi_build_sense_data(&lu_cmd,
					ISTGT_SCSI_SENSE_NOT_READY,
					0x04, 0x00);
		lu_cmd.status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		rc = 0;
	} else {

		/* need R2T? */
		if (!operation_mode && (W_bit && F_bit) && (conn->max_r2t > 0)) {
			if (lu_cmd.pdu->data_segment_len < transfer_len) {
				rc = istgt_add_transfer_task(conn, &lu_cmd);
				if (rc < 0) {
					ISTGT_ERRLOG("add_transfer_task() failed\n");
					return (-1);
				}
			}
		}

		/* execute SCSI command */
		rc = istgt_lu_disk_queue(conn, &lu_cmd);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: lu_disk_queue() failed\n", lu->num);
			return (-1);
		}
		switch (rc) {
			case ISTGT_LU_TASK_RESULT_QUEUE_OK:
				return (0);
			case ISTGT_LU_TASK_RESULT_QUEUE_FULL:
				que =  "QueueFull ";
				ISTGT_WARNLOG("Queue Full\n");
				break;
			case ISTGT_LU_TASK_RESULT_IMMEDIATE:
				que =  "Immediate ";
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Immediate\n");
				break;
			default:
				ISTGT_ERRLOG("lu_disk_queue unknown rc=%d\n", rc);
				return (-1);
		}
	}

	/* transfer data from logical unit */
	/* (direction is view of initiator side) */
	if (lu_cmd.R_bit
		&& (lu_cmd.status == ISTGT_SCSI_STATUS_GOOD
			|| lu_cmd.sense_data_len != 0)) {
		rc = istgt_iscsi_transfer_in(conn, &lu_cmd);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_transfer_in() failed\n");
			return (-1);
		}
		if (rc > 0) {
			/* sent status by last DATAIN PDU */
			return (0);
		}
	}

	o_bit = u_bit = O_bit = U_bit = 0;
	bidi_residual_len = residual_len = 0;
	data_len = lu_cmd.data_len;
	if (transfer_len != 0
		&& lu_cmd.status == ISTGT_SCSI_STATUS_GOOD) {
		if (data_len < transfer_len) {
			/* underflow */
			msg = "Underflow";
			residual_len = transfer_len - data_len;
			U_bit = 1;
		} else if (data_len > transfer_len) {
			/* overflow */
			msg = "Overflow";
			residual_len = data_len - transfer_len;
			O_bit = 1;
		} else {
			msg = "Transfer";
		}
	}

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_SCSI_RSP;
	BDADD8(&rsp[1], 1, 7);
	BDADD8(&rsp[1], o_bit, 4);
	BDADD8(&rsp[1], u_bit, 3);
	BDADD8(&rsp[1], O_bit, 2);
	BDADD8(&rsp[1], U_bit, 1);
	rsp[2] = 0x00; // Command Completed at Target
	// rsp[2] = 0x01; // Target Failure
	rsp[3] = lu_cmd.status;
	rsp[4] = 0; // TotalAHSLength
	rsp_pdu.data = lu_cmd.sense_data;
	DSET24(&rsp[5], lu_cmd.sense_data_len); // DataSegmentLength
	rsp_pdu.data_segment_len = lu_cmd.sense_data_len;
	lu_cmd.sense_data = NULL; lu_cmd.sense_data_len = 0;
	DSET32(&rsp[16], task_tag);
	DSET32(&rsp[20], 0); // SNACK Tag

	SESS_MTX_LOCK(conn);
	DSET32(&rsp[24], conn->StatSN);
	conn->StatSN++;
	if (I_bit == 0) {
		if (likely((lu_cmd.lu->limit_q_size == 0) || ((int)(conn->sess->MaxCmdSN - conn->sess->ExpCmdSN) < lu_cmd.lu->limit_q_size))) {
			conn->sess->MaxCmdSN++;
			conn->sess->MaxCmdSN_local++;
			if (unlikely((conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local) && (lu_cmd.lu->limit_q_size == 0))) {
				ISTGT_LOG("conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local in op_scsi\n");
				conn->sess->MaxCmdSN = conn->sess->MaxCmdSN_local;
			}
		} else
			conn->sess->MaxCmdSN_local++;
	}
	DSET32(&rsp[28], conn->sess->ExpCmdSN);
	DSET32(&rsp[32], conn->sess->MaxCmdSN);
	SESS_MTX_UNLOCK(conn);

	DSET32(&rsp[36], 0); // ExpDataSN
	DSET32(&rsp[40], bidi_residual_len);
	DSET32(&rsp[44], residual_len);

	rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		if (lu_cmd.sense_data != NULL)
			xfree(lu_cmd.sense_data);
		if (lu_cmd.data != NULL)
			xfree(lu_cmd.data);
		return (-1);
	}

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"op_scsi_done: status:%d %s, CmdSN=%u, ExpStatSN=%u, StatSN=%u, ExpCmdSN=%u, MaxCmdSN=%u"
		"I=%d, F=%d, R=%d, W=%d, Attr=%d, ITT=%x, TL=%u (%s %lu)\n",
		lu_cmd.status, que, CmdSN, ExpStatSN, c_StatSN, s_ExpCmdSN, s_MaxCmdSN,
		I_bit, F_bit, R_bit, W_bit, Attr_bit,
		task_tag, transfer_len, msg, data_len);
	if (lu_cmd.sense_data != NULL)
		xfree(lu_cmd.sense_data);
	if (lu_cmd.data != NULL)
		xfree(lu_cmd.data);
	return (0);
}

static int
istgt_iscsi_task_response(CONN_Ptr conn, ISTGT_LU_TASK_Ptr lu_task)
{
	ISTGT_LU_CMD_Ptr lu_cmd;
	ISCSI_PDU rsp_pdu = { 0 };
	uint8_t *rsp;
	uint32_t task_tag;
	uint32_t transfer_len;
	uint32_t CmdSN;
	size_t residual_len;
	size_t data_len;
	int I_bit;
	int o_bit, u_bit, O_bit, U_bit;
	int bidi_residual_len;
	int rc;
	uint64_t *tptr;
	const  char *msg = NULL;
	lu_cmd = &lu_task->lu_cmd;
	ISTGT_SCSI_STATUS lstat = lu_cmd->status;
	transfer_len = lu_cmd->transfer_len;
	task_tag = lu_cmd->task_tag;
	I_bit = lu_cmd->I_bit;
	CmdSN = lu_cmd->CmdSN;

	uint8_t sidx = istgt_cmd_table[lu_cmd->cdb0].statidx;

	++SCSIstat_rest[sidx].req_finish;


	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "SCSI response CSN=%x/%x tt:%x tl:%x\n", CmdSN, lu_cmd->status, task_tag, transfer_len);

	/* transfer data from logical unit */
	/* (direction is view of initiator side) */
	if (lu_cmd->R_bit
		&& (lu_cmd->status == ISTGT_SCSI_STATUS_GOOD
		|| lu_cmd->sense_data_len != 0)) {
		if (lu_task->lock) {
			rc = istgt_iscsi_transfer_in_internal(conn, lu_cmd);
		} else {
			rc = istgt_iscsi_transfer_in(conn, lu_cmd);
		}
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_transfer_in() failed\n");
			return (-1);
		}
		if (rc > 0) {
			/* sent status by last DATAIN PDU */
			return (0);
		}
	}

	o_bit = u_bit = O_bit = U_bit = 0;
	bidi_residual_len = residual_len = 0;
	data_len = lu_cmd->data_len;
	if (transfer_len != 0
		&& lu_cmd->status == ISTGT_SCSI_STATUS_GOOD) {
		if (data_len < transfer_len) {
			/* underflow */
			msg = "underflow";
			residual_len = transfer_len - data_len;
			U_bit = 1;
		} else if (data_len > transfer_len) {
			/* overflow */
			msg = "overflow";
			residual_len = data_len - transfer_len;
			O_bit = 1;
		} else {
			msg = "transfer";
		}
	}

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = lu_cmd->sense_data;
	tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;
	rsp[0] = ISCSI_OP_SCSI_RSP;
	BDADD8(&rsp[1], 1, 7);
	BDADD8(&rsp[1], o_bit, 4);
	BDADD8(&rsp[1], u_bit, 3);
	BDADD8(&rsp[1], O_bit, 2);
	BDADD8(&rsp[1], U_bit, 1);
	rsp[2] = 0x00; // Command Completed at Target
	// rsp[2] = 0x01; // Target Failure
	rsp[3] = lu_cmd->status;
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], lu_cmd->sense_data_len); // DataSegmentLength
	rsp_pdu.data_segment_len = lu_cmd->sense_data_len;

	DSET32(&rsp[16], task_tag);
	DSET32(&rsp[20], 0); // SNACK Tag

	SESS_MTX_LOCK(conn);
	DSET32(&rsp[24], conn->StatSN);
	conn->StatSN++;
	if (I_bit == 0) {
		if (likely(lu_cmd->lu->limit_q_size == 0 || ((int)(conn->sess->MaxCmdSN - conn->sess->ExpCmdSN) < lu_cmd->lu->limit_q_size))) {
			conn->sess->MaxCmdSN++;
			conn->sess->MaxCmdSN_local++;
			if (unlikely((conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local) && (lu_cmd->lu->limit_q_size == 0))) {
				ISTGT_LOG("conn->sess->MaxCmdSN != conn->sess->MaxCmdSN_local\n");
				conn->sess->MaxCmdSN = conn->sess->MaxCmdSN_local;
			}
		}
		else
			conn->sess->MaxCmdSN_local++;
	}

	DSET32(&rsp[28], conn->sess->ExpCmdSN);
	DSET32(&rsp[32], conn->sess->MaxCmdSN);
	SESS_MTX_UNLOCK(conn);

	DSET32(&rsp[36], 0); // ExpDataSN
	DSET32(&rsp[40], bidi_residual_len);
	DSET32(&rsp[44], residual_len);

	if (lu_task->lock) {
		rc = istgt_iscsi_write_pdu_internal(conn, &rsp_pdu);
	} else {
		rc = istgt_iscsi_write_pdu(conn, &rsp_pdu);
	}
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}
	if (msg) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "SCSI response CSN=%x done, status:%x  %s %zu/%u\n",
				CmdSN, lstat, msg, data_len, transfer_len);
	}
	return (0);
}

static int
istgt_iscsi_op_task(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t ref_task_tag;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	uint32_t ref_CmdSN;
	int I_bit;
	int function;
	int response;
	int rc;
	int cleared = 0;
	uint32_t cStatSN, nStatSN = 0, sExpCmdSN, nExpCmdSN = 0, sMaxCmdSN;
	const char *msg = "";
	const char *fname = "";
	char initport[MAX_INITIATOR_NAME];
		ISTGT_LU_DISK *spec;
	int cmdqcount = 0;
	int blockedqcount = 0;

	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}
	initport[0] = '\0';
	initport[MAX_INITIATOR_NAME-2] = '\0';
	initport[MAX_INITIATOR_NAME-1] = '\0';

	cp = (uint8_t *) &pdu->bhs;
	I_bit = BGET8(&cp[0], 6);
	function = BGET8W(&cp[1], 6, 7);

	lun = DGET64(&cp[8]);
	if (lun != 0)
		lun = 0;
	task_tag = DGET32(&cp[16]);
	ref_task_tag = DGET32(&cp[20]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);
	ref_CmdSN = DGET32(&cp[32]);

	SESS_MTX_LOCK(conn);
	cStatSN = conn->StatSN;
	nStatSN = cStatSN;
	sExpCmdSN = conn->sess->ExpCmdSN;
	sMaxCmdSN = conn->sess->MaxCmdSN;
	if (CmdSN != conn->sess->ExpCmdSN) {
		msg = "dropped?";
		conn->sess->ExpCmdSN = CmdSN;
		nExpCmdSN  = CmdSN;
	}
	if (SN32_GT(ExpStatSN, conn->StatSN)) {
		msg = "StatSN-advanced!";
		conn->StatSN = ExpStatSN;
		nStatSN = ExpStatSN;
	}
#if 0
	/* not need */
	if (ExpStatSN != conn->StatSN) {
		ISTGT_WARNLOG("StatSN(%u/%u) might have dropped\n",
			ExpStatSN, conn->StatSN);
		conn->StatSN = ExpStatSN;
	}
#endif
	SESS_MTX_UNLOCK(conn);
	spec = (ISTGT_LU_DISK *)(conn->sess->lu->lun[0].spec);
	cmdqcount = istgt_queue_count(&spec->cmd_queue);
	blockedqcount = istgt_queue_count(&spec->blocked_queue);

	ISTGT_LOG("scsitask:%d start. CmdSN=0x%x, ExpStatSN=0x%x, StatSN=0x%x/0x%x, ExpCmdSN=0x%x/0x%x, MaxCmdSN=0x%x refCmdSN=0x%x (%s) "
		"I=%d, ITT=0x%x, ref TT=0x%x cmdqcount=%d, blockedqcount=%d inflight=%d dskIOPending: %d delayedFree: %d, LUN=0x%16.16lx\n",
		function, CmdSN, ExpStatSN, cStatSN, nStatSN, sExpCmdSN, nExpCmdSN,
		sMaxCmdSN, ref_CmdSN, msg,
		I_bit, task_tag, ref_task_tag, cmdqcount, blockedqcount, spec->inflight, conn->diskIoPending, conn->flagDelayedFree, lun);

	response = 0; // Function complete.
	switch (function) {
	case ISCSI_TASK_FUNC_ABORT_TASK:
		fname = "ABORT_TASK";
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_clear_task_ITLQ(conn, conn->sess->lu, lun,
			ref_CmdSN);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		if (cleared == 0 && send_abrt_resp == 1)
			response = 1;
		istgt_clear_transfer_task(conn, ref_CmdSN);
		break;
	case ISCSI_TASK_FUNC_ABORT_TASK_SET:
		fname = "ABORT_TASK_SET";
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_clear_task_ITL(conn, conn->sess->lu, lun);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		if (cleared == 0 && send_abrt_resp == 1)
			response = 1;
		istgt_clear_all_transfer_task(conn);
		break;
	case ISCSI_TASK_FUNC_CLEAR_ACA:
		fname = "CLEAR_ACA";
		break;
	case ISCSI_TASK_FUNC_CLEAR_TASK_SET:
		fname = "CLEAR_TASK_SET";
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_clear_task_ITL(conn, conn->sess->lu, lun);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		if (cleared == 0 && send_abrt_resp == 1)
			response = 1;
		istgt_clear_all_transfer_task(conn);
		break;
	case ISCSI_TASK_FUNC_LOGICAL_UNIT_RESET:
		memcpy(initport, conn->initiator_port, MAX_INITIATOR_NAME - 1);
		fname = "LOGICAL_UNIT_RESET";
		/* Ravi: Should not be dropping connections on LU_RESET */
		/* istgt_iscsi_drop_all_conns(conn); */
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_reset(conn->sess->lu, lun, ISTGT_UA_LUN_RESET);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		// conn->state = CONN_STATE_EXITING;
		break;
	case ISCSI_TASK_FUNC_TARGET_WARM_RESET:
		memcpy(initport, conn->initiator_port, MAX_INITIATOR_NAME - 1);
		fname = "TARGET_WARM_RESET";
		/* Ravi: Should not be dropping connections on TGT_RESET */
		/* istgt_iscsi_drop_all_conns(conn); */
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_reset(conn->sess->lu, lun, ISTGT_UA_TARG_RESET);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		// conn->state = CONN_STATE_EXITING;
		break;
	case ISCSI_TASK_FUNC_TARGET_COLD_RESET:
		memcpy(initport, conn->initiator_port, MAX_INITIATOR_NAME - 1);
		fname = "TARGET_COLD_RESET";
		/* This is a obsolete */
		/* Ravi: Should not be dropping connections on TGT_RESET */
		/* istgt_iscsi_drop_all_conns(conn); */
		if (conn->sess->lu != NULL)
			MTX_LOCK(&conn->sess->lu->mutex);
		SESS_MTX_LOCK(conn);
		cleared = istgt_lu_reset(conn->sess->lu, lun, ISTGT_UA_TARG_RESET);
		if (cleared <= 0) {
			ISTGT_ERRLOG("%s failed rc:%d\n", fname, cleared);
		} else {
			conn->sess->MaxCmdSN += cleared;
			conn->sess->MaxCmdSN_local += cleared;
			if (cleared >= 1)
				ISTGT_ERRLOG("%s cleared %d commands\n", fname, cleared);
		}
		SESS_MTX_UNLOCK(conn);
		if (conn->sess->lu != NULL)
			MTX_UNLOCK(&conn->sess->lu->mutex);
		// conn->state = CONN_STATE_EXITING;
		break;
	case ISCSI_TASK_FUNC_TASK_REASSIGN:
		fname = "TASK_REASSIGN";
		break;
	default:
		fname = "unsupported function";
		response = 255; // Function rejected.
		break;
	}

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = NULL;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_TASK_RSP;
	BDADD8(&rsp[1], 1, 7);
	rsp[2] = response;
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], 0); // DataSegmentLength

	DSET32(&rsp[16], task_tag);

	rc = istgt_iscsi_write_pdu_upd(conn, &rsp_pdu, I_bit);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed in iscsi_op_task\n");
		return (-1);
	}
	ISTGT_LOG("scsitask:%d:%s %s done. CSN=0x%x, ExpStatSN=0x%x, StatSN=0x%x/0x%x, ExpCmdSN=0x%x/0x%x, MaxCmdSN=0x%x (%s) "
		"I=%d, ITT=0x%x, ref TT=0x%x, LUN=0x%16.16lx\n",
		function, fname, initport, CmdSN, ExpStatSN, cStatSN, nStatSN, sExpCmdSN, nExpCmdSN,
		conn->sess->MaxCmdSN, msg,
		I_bit, task_tag, ref_task_tag, lun);
	return (0);
}

static int
istgt_iscsi_op_nopout(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint8_t *cp;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t transfer_tag;
	uint32_t CmdSN;
	uint32_t ExpStatSN;
	int I_bit;
	int ping_len;
	int data_len;
	int rc = 0;
	uint32_t QCmdSN;
	uint32_t s_ExpCmdSN, s_MaxCmdSN, lc_StatSN = 0;
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}

	data_len = 0;

	cp = (uint8_t *) &pdu->bhs;
	I_bit = BGET8(&cp[0], 6);
	ping_len = DGET24(&cp[5]);

	lun = DGET64(&cp[8]);
	task_tag = DGET32(&cp[16]);
	transfer_tag = DGET32(&cp[20]);
	CmdSN = DGET32(&cp[24]);
	ExpStatSN = DGET32(&cp[28]);

	SESS_MTX_LOCK(conn);
	s_ExpCmdSN = conn->sess->ExpCmdSN;
	s_MaxCmdSN = conn->sess->MaxCmdSN;
	lc_StatSN = conn->StatSN;
	SESS_MTX_UNLOCK(conn);

	if (I_bit == 0) {
		if (SN32_LT(CmdSN, s_ExpCmdSN)
			|| SN32_GT(CmdSN, s_MaxCmdSN)) {
			ISTGT_ERRLOG("Ignore CmdSN not in exp range "
					"CSN=%x, ExpStatSN=%x, StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x"
					"I=%d, ITT=%x, TTT=%x\n",
					CmdSN, ExpStatSN, conn->StatSN, s_ExpCmdSN,
					s_MaxCmdSN, I_bit, task_tag, transfer_tag);
			return (-1);
		}
	} else if (CmdSN != s_ExpCmdSN) {
		ISTGT_ERRLOG("CmdSN != ExpCmdSN "
					"CSN=%x, ExpStatSN=%x, StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x"
					"I=%d, ITT=%x, TTT=%x\n",
					CmdSN, ExpStatSN, conn->StatSN, s_ExpCmdSN,
					s_MaxCmdSN, I_bit, task_tag, transfer_tag);
		return (-1);
	}
	if (SN32_GT(ExpStatSN, conn->StatSN)) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "StatSN(%x->%x) advanced "
					"CSN=%x, ExpStatSN=%x, StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x"
					"I=%d, ITT=%x, TTT=%x\n",
					lc_StatSN, ExpStatSN,
					CmdSN, ExpStatSN, conn->StatSN, s_ExpCmdSN,
					s_MaxCmdSN, I_bit, task_tag, transfer_tag);
		lc_StatSN = ExpStatSN; // conn->StatSN = ExpStatSN;
	// use_lc = 1;
	}
	QCmdSN = s_MaxCmdSN - s_ExpCmdSN + 1 + conn->queue_depth;
	if (SN32_LT(ExpStatSN + QCmdSN, lc_StatSN)) {
		ISTGT_ERRLOG("StatSN(%x/%x) QCmdSN(%x) error "
				"CSN=%x, ExpStatSN=%x, StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x"
				"I=%d, ITT=%x, TTT=%x\n",
				ExpStatSN, lc_StatSN, QCmdSN,
				CmdSN, ExpStatSN, conn->StatSN, s_ExpCmdSN,
				s_MaxCmdSN, I_bit, task_tag, transfer_tag);
		return (-1);
	}

	if (task_tag == 0xffffffffU) {
		if (I_bit == 1) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"got NOPOUT ITT=0xffffffff\n");
			return (0);
		} else {
			ISTGT_ERRLOG("got NOPOUT ITT=0xffffffff, I=0\n");
			return (-1);
		}
	}

	transfer_tag = 0xffffffffU;

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	if (ping_len != 0) {
		/* response of NOPOUT */
		rsp_pdu.data = pdu->data;
		data_len = DMIN32(ping_len, conn->MaxRecvDataSegmentLength);
		if ((size_t)data_len != pdu->data_segment_len) {
			ISTGT_ERRLOG("got NOPOUT d_len:%d != rcvd_len:%lu\n", data_len, pdu->data_segment_len);
		}
		pdu->data = NULL;
	} else {
		rsp_pdu.data = NULL;
		data_len = 0;
	}
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_NOPIN;
	BDADD8(&rsp[1], 1, 7);
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], data_len); // DataSegmentLength

	DSET64(&rsp[8], lun);
	DSET32(&rsp[16], task_tag);
	DSET32(&rsp[20], transfer_tag);

	rc = istgt_iscsi_write_pdu_upd(conn, &rsp_pdu, I_bit);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}

	return (0);
}


static void
istgt_free_transfer_task(ISTGT_R2T_TASK_Ptr r2t_task)
{
	int ind = 0;
	if (r2t_task == NULL)
		return;
	if (r2t_task->iobufindx != -1) {
		while ((ind = r2t_task->iobufindx--) > -1) {
			xfree(r2t_task->iobuf[ind].iov_base);
		}
	}
	r2t_task->iobufsize = 0;
	xfree(r2t_task);
}

static ISTGT_R2T_TASK_Ptr
istgt_get_transfer_task(CONN_Ptr conn, uint32_t transfer_tag)
{
	ISTGT_R2T_TASK_Ptr r2t_task;
	int i;

	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t == 0) {
		MTX_UNLOCK(&conn->r2t_mutex);
		return (NULL);
	}
	for (i = 0; i < conn->pending_r2t; i++) {
		r2t_task = conn->r2t_tasks[i];
#if 0
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			"CmdSN=%d, TransferTag=%x/%x\n",
			r2t_task->CmdSN, r2t_task->transfer_tag, transfer_tag);
#endif
		if (r2t_task->transfer_tag == transfer_tag) {
			MTX_UNLOCK(&conn->r2t_mutex);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"Match index=%d, CmdSN=%d, TransferTag=%x\n",
				i, r2t_task->CmdSN, r2t_task->transfer_tag);
			return (r2t_task);
		}
	}
	MTX_UNLOCK(&conn->r2t_mutex);
	return (NULL);
}

static int
istgt_add_transfer_task(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_R2T_TASK_Ptr r2t_task;
	uint32_t transfer_len;
	uint32_t transfer_tag;
	size_t first_burst_len;
	size_t max_burst_len;
	size_t data_len;
	size_t offset = 0;
	int len;
	int idx;
	int rc;

	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t >= conn->max_r2t) {
		// no slot available, skip now...
		// ISTGT_WARNLOG("No R2T space available (%d/%d)\n",
		//	conn->pending_r2t, conn->max_r2t);
		MTX_UNLOCK(&conn->r2t_mutex);
		return (0);
	}
	MTX_UNLOCK(&conn->r2t_mutex);

	transfer_len = lu_cmd->transfer_len;
	transfer_tag = lu_cmd->task_tag;
	data_len = lu_cmd->pdu->data_segment_len;
	first_burst_len = conn->FirstBurstLength;
	max_burst_len = conn->MaxBurstLength;
	offset += data_len;
	if (offset >= first_burst_len) {
		len = DMIN32(max_burst_len, (transfer_len - offset));

		r2t_task = xmalloc(sizeof (*r2t_task));
		r2t_task->conn = conn;
		r2t_task->lu = lu_cmd->lu;
		r2t_task->lun = lu_cmd->lun;
		r2t_task->CmdSN = lu_cmd->CmdSN;
		r2t_task->task_tag = lu_cmd->task_tag;
		r2t_task->transfer_len = transfer_len;
		r2t_task->transfer_tag = transfer_tag;

		// r2t_task->iobuf = xmalloc(r2t_task->iobufsize);
		// memcpy(r2t_task->iobuf, lu_cmd->pdu->data, data_len);
		r2t_task->iobufindx = 0;
		r2t_task->iobufsize = data_len;
		r2t_task->iobuf[0].iov_base = lu_cmd->pdu->data;
		r2t_task->iobuf[0].iov_len = data_len;
		lu_cmd->pdu->data = NULL;
		lu_cmd->pdu->data_segment_len = 0;

		r2t_task->offset = offset;
		r2t_task->R2TSN = 0;
		r2t_task->DataSN = 0;
		r2t_task->F_bit = lu_cmd->F_bit;

		MTX_LOCK(&conn->r2t_mutex);
		idx = conn->pending_r2t++;
		conn->r2t_tasks[idx] = r2t_task;
		MTX_UNLOCK(&conn->r2t_mutex);

		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			"Send R2T(Offset=%d, Tag=%x)\n",
			r2t_task->offset, r2t_task->transfer_tag);
		rc = istgt_iscsi_send_r2t(conn, lu_cmd,
			r2t_task->offset, len, r2t_task->transfer_tag,
			&r2t_task->R2TSN);
		timediff(lu_cmd, 'R', __LINE__);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_send_r2t() failed\n");
			return (-1);
		}
	}
	return (0);
}

static void
istgt_del_transfer_task(CONN_Ptr conn, ISTGT_R2T_TASK_Ptr r2t_task)
{
	int found = 0;
	int i, removed = -1;

	if (r2t_task == NULL)
		return;

	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t == 0) {
		MTX_UNLOCK(&conn->r2t_mutex);
		return;
	}
	for (i = 0; i < conn->pending_r2t; i++) {
		if (conn->r2t_tasks[i] == r2t_task) {
			removed = i;
			found = 1;
			break;
		}
	}
	if (found) {
		for (; i < conn->pending_r2t; i++) {
			conn->r2t_tasks[i] = conn->r2t_tasks[i + 1];
		}
		conn->pending_r2t--;
		conn->r2t_tasks[conn->pending_r2t] = NULL;
	}
	MTX_UNLOCK(&conn->r2t_mutex);
	if (removed != -1) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"Removed R2T task conn id=%d, index=%d\n",
				conn->id, removed);
	}
}

static void
istgt_clear_transfer_task(CONN_Ptr conn, uint32_t CmdSN)
{
	int found = 0;
	int i;

	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t == 0) {
		MTX_UNLOCK(&conn->r2t_mutex);
		return;
	}
	for (i = 0; i < conn->pending_r2t; i++) {
		if (conn->r2t_tasks[i]->CmdSN == CmdSN) {
			istgt_free_transfer_task(conn->r2t_tasks[i]);
			conn->r2t_tasks[i] = NULL;
			found = 1;
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"Clearing R2T task conn id=%d, index=%d cmdsn:0x%x\n",
				conn->id, i, CmdSN);
			break;
		}
	}
	if (found) {
		for (; i < conn->pending_r2t; i++) {
			conn->r2t_tasks[i] = conn->r2t_tasks[i + 1];
		}
		conn->pending_r2t--;
		conn->r2t_tasks[conn->pending_r2t] = NULL;
	}
	MTX_UNLOCK(&conn->r2t_mutex);
}

static void
istgt_clear_all_transfer_task(CONN_Ptr conn)
{
	int i;

	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t == 0) {
		MTX_UNLOCK(&conn->r2t_mutex);
		return;
	}
	for (i = 0; i < conn->pending_r2t; i++) {
		istgt_free_transfer_task(conn->r2t_tasks[i]);
		conn->r2t_tasks[i] = NULL;
	}
	conn->pending_r2t = 0;
	MTX_UNLOCK(&conn->r2t_mutex);
}

static int
istgt_iscsi_op_data(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	ISTGT_R2T_TASK_Ptr r2t_task;
	uint8_t *cp;
	uint32_t current_task_tag;
	uint32_t current_transfer_tag;
	uint32_t ExpStatSN;
	uint32_t task_tag;
	uint32_t transfer_tag;
	uint32_t ExpDataSN;
	uint32_t DataSN;
	uint32_t buffer_offset;
	size_t data_len;
	size_t offset;
	int F_bit;
	int rc;
	int iondx;
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}
	MTX_LOCK(&conn->r2t_mutex);
	if (conn->pending_r2t == 0) {
		ISTGT_ERRLOG("No R2T task\n");
		MTX_UNLOCK(&conn->r2t_mutex);
	reject_return:
		rc = istgt_iscsi_reject(conn, pdu, 0x09);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_reject() failed\n");
			return (-1);
		}
		return (0);
	}
	MTX_UNLOCK(&conn->r2t_mutex);

	cp = (uint8_t *) &pdu->bhs;
	F_bit = BGET8(&cp[1], 7);
	data_len = DGET24(&cp[5]);

	task_tag = DGET32(&cp[16]);
	transfer_tag = DGET32(&cp[20]);
	ExpStatSN = DGET32(&cp[28]);
	DataSN = DGET32(&cp[36]);
	buffer_offset = DGET32(&cp[40]);

	r2t_task = istgt_get_transfer_task(conn, transfer_tag);
	if (r2t_task == NULL) {
		ISTGT_ERRLOG("Not found R2T task for transfer_tag=%x (pending_r2t:%d)\n",
			transfer_tag, conn->pending_r2t);
		goto reject_return;
	}

	current_task_tag = r2t_task->task_tag;
	current_transfer_tag = r2t_task->transfer_tag;
	offset = r2t_task->offset;
	ExpDataSN = r2t_task->DataSN;

	if (DataSN != ExpDataSN) {
		ISTGT_ERRLOG("DataSN(%x) error\n", DataSN);
		return (-1);
	}
	if (task_tag != current_task_tag) {
		ISTGT_ERRLOG("task_tag(%x/%x) error\n",
			task_tag, current_task_tag);
		return (-1);
	}
	if (transfer_tag != current_transfer_tag) {
		ISTGT_ERRLOG("transfer_tag(%x/%x) error\n",
			transfer_tag, current_transfer_tag);
		return (-1);
	}
	if (buffer_offset != offset) {
		ISTGT_ERRLOG("offset(%u) error\n", buffer_offset);
		return (-1);
	}
	// if (buffer_offset + data_len > alloc_len) {
	// 	ISTGT_ERRLOG("offset error\n");
	// 	return (-1);
	// }

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"copy pdu.data %lu to r2ttask.iobuf at %u; pending r2t=%d, StatSN=%x, ExpStatSN=%x, DataSN=%x\n",
		data_len, buffer_offset, conn->pending_r2t, conn->StatSN, ExpStatSN, DataSN);

	iondx = ++r2t_task->iobufindx;
	r2t_task->iobufsize += data_len;
	offset += data_len;  // we do check buffer_offest == offset
	r2t_task->iobuf[iondx].iov_base = pdu->data;
	r2t_task->iobuf[iondx].iov_len = data_len;
	// memcpy(data + buffer_offset, pdu->data, data_len);
	pdu->data = NULL;
	ExpDataSN++;

	r2t_task->offset = offset;
	r2t_task->DataSN = ExpDataSN;
	r2t_task->F_bit = F_bit;
	return (0);
}

uint8_t istgt_get_sleep_val(ISTGT_LU_DISK *spec) {
	int i, val, tot;
	val = random() % 100;
	tot = 0;

	if (spec->percent_count == 0)
		return (0);

	for (i = 0; i < spec->percent_count && i < 31; i++)
		if ((tot+spec->percent_val[i]) <= val)
			tot += spec->percent_val[i];
		else
			break;

	return (spec->percent_latency[i]);
}

static int
istgt_iscsi_send_r2t(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, int offset, int len, uint32_t transfer_tag, uint32_t *R2TSN)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	int rc;

	/* R2T PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = NULL;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;

	rsp[0] = ISCSI_OP_R2T;
	BDADD8(&rsp[1], 1, 7);
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], 0); // DataSegmentLength

	DSET64(&rsp[8], lu_cmd->lun);
	DSET32(&rsp[16], lu_cmd->task_tag);
	DSET32(&rsp[20], transfer_tag);

	DSET32(&rsp[36], *R2TSN);
	*R2TSN += 1;
	DSET32(&rsp[40], (uint32_t) offset);
	DSET32(&rsp[44], (uint32_t) len);

	rc = istgt_iscsi_write_pdu_upd(conn, &rsp_pdu, 0);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "sentR2T sn:%x  off:%u len:%u\n", *R2TSN, offset, len);

	return (0);
}

int
istgt_iscsi_transfer_out(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, size_t transfer_len)
{
	// uint8_t *data = lu_cmd->iobuf;
	// size_t alloc_len = lu_cmd->iobufsize;
	ISTGT_QUEUE_Ptr r_ptr = NULL;
	ISTGT_R2T_TASK_Ptr r2t_task;
	ISCSI_PDU data_pdu;
	uint8_t *cp;
	uint32_t current_task_tag;
	uint32_t current_transfer_tag;
	uint32_t ExpDataSN;
	uint32_t task_tag;
	uint32_t transfer_tag;
	uint32_t ExpStatSN;
	uint32_t wCmdSN;
	uint32_t DataSN;
	uint32_t buffer_offset;
	uint32_t R2TSN;
	size_t data_len;
	size_t segment_len;
	size_t first_burst_len;
	size_t max_burst_len;
	size_t offset;
	int opcode;
	int F_bit;
	int len;
	int r2t_flag;
	int r2t_sent;
	int rc, i;
	uint64_t *tptr;
	const char *msg;
	current_task_tag = lu_cmd->task_tag;
	current_transfer_tag = lu_cmd->task_tag;
	ExpDataSN = 0;
	segment_len = conn->MaxRecvDataSegmentLength;
	first_burst_len = conn->FirstBurstLength;
	max_burst_len = conn->MaxBurstLength;
	offset = 0;
	r2t_flag = 0;
	r2t_sent = 0;
	R2TSN = 0;

	cp = (uint8_t *) &lu_cmd->pdu->bhs;
	data_len = DGET24(&cp[5]);

	// if (transfer_len > alloc_len) {
	//	ISTGT_ERRLOG("transfer_len > alloc_len\n");
	//	return (-1);
	// }

start:
	if (lu_cmd->aborted == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d aborted CmdSN:0x%x Transfered=%zd, Offset=%zd\n",
				conn->id, lu_cmd->CmdSN, transfer_len, offset);
		return (0);
	}

	r2t_task = istgt_get_transfer_task(conn, current_transfer_tag);
	if (r2t_task != NULL) {
		current_task_tag = r2t_task->task_tag;
		current_transfer_tag = r2t_task->transfer_tag;
		offset = r2t_task->offset;
		R2TSN = r2t_task->R2TSN;
		ExpDataSN = r2t_task->DataSN;
		F_bit = r2t_task->F_bit;
		r2t_flag = 1;

		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"c#%d Using R2T(%x)trtag:%x offset=%zd, DataSN=%x. Transfer=%zd, First=%zd, Max=%zd, Segment=%zd\n",
			conn->id, R2TSN, current_transfer_tag, offset, ExpDataSN,
			transfer_len, data_len, max_burst_len, segment_len);

		data_len = 0;

		// memcpy(data, r2t_task->iobuf, offset);
		lu_cmd->iobufsize = r2t_task->iobufsize;
		lu_cmd->iobufindx = r2t_task->iobufindx;
		for (i = 0; i <= r2t_task->iobufindx; ++i) {
			lu_cmd->iobuf[i].iov_base = r2t_task->iobuf[i].iov_base;
			lu_cmd->iobuf[i].iov_len = r2t_task->iobuf[i].iov_len;
			r2t_task->iobuf[i].iov_base = NULL;
			r2t_task->iobuf[i].iov_len = 0;
		}
		r2t_task->iobufsize = 0;
		r2t_task->iobufindx = -1;
		istgt_del_transfer_task(conn, r2t_task);
		istgt_free_transfer_task(r2t_task);

		rc = istgt_queue_count(&conn->pending_pdus);
		if (rc > 0) {
			if (g_trace_flag) {
				ISTGT_WARNLOG("pending_pdus > 0\n");
			}
		}
		if (offset < transfer_len) {
			if (offset >= (first_burst_len + max_burst_len)) {
				/* need more data */
				r2t_flag = 0;
			}
			len = DMIN32(max_burst_len,
				(transfer_len - offset));
			// memset(&data_pdu.bhs, 0, ISCSI_BHS_LEN);
			tptr = (uint64_t *)&(data_pdu.bhs);
			*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
			*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;
			data_pdu.ahs = NULL;
			data_pdu.data = NULL;
			goto r2t_retry;
		} else if (offset == transfer_len) {
			if (F_bit == 0) {
				ISTGT_ERRLOG("c#%d F_bit not set on the last PDU\n", conn->id);
				return (-1);
			}
		}
		return (0);
	}

	if (data_len != 0) {
		if (data_len > first_burst_len) {
			ISTGT_ERRLOG("c#%d data_len > first_burst_len,  Transfer=%zd, First=%zd, Max=%zd, Segment=%zd\n",
					conn->id, transfer_len, data_len, max_burst_len, segment_len);
			return (-1);
		}
		if (lu_cmd->pdu->data) {
			i = ++lu_cmd->iobufindx;
			lu_cmd->iobufsize += data_len;
			lu_cmd->iobuf[i].iov_base = lu_cmd->pdu->data;
			lu_cmd->iobuf[i].iov_len = data_len;
			// memcpy(data + offset, lu_cmd->pdu->data, data_len);
			lu_cmd->pdu->data = NULL;
		} else {
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
					"c#%d nothing in R2T, Transfer=%zd, First=%zd, Max=%zd, Segment=%zd\n",
					conn->id, transfer_len, data_len, max_burst_len, segment_len);
		}
		offset += data_len;
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
			"c#%d no data in current pdu, or in R2T. Transfer=%zd, First=%zd, Max=%zd, Segment=%zd\n",
			conn->id, transfer_len, data_len, max_burst_len, segment_len);
	}

	if (offset < transfer_len) {
		len = DMIN32(first_burst_len, (transfer_len - offset));
		// memset(&data_pdu.bhs, 0, ISCSI_BHS_LEN);
		tptr = (uint64_t *)&(data_pdu.bhs);
		*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
		*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;
		data_pdu.ahs = NULL;
		data_pdu.data = NULL;
		do {

			if (lu_cmd->aborted == 1)
				goto start;

			/* send R2T if required */
			if (r2t_flag == 0
				&& (conn->sess->initial_r2t || offset >= first_burst_len)) {
				len = DMIN32(max_burst_len, (transfer_len - offset));
				rc = istgt_iscsi_send_r2t(conn, lu_cmd,
					offset, len, current_transfer_tag, &R2TSN);
				timediff(lu_cmd, '$', __LINE__);
				if (rc < 0) {
					ISTGT_ERRLOG("c#%d iscsi_send_r2t() failed\n", conn->id);
					goto error_return;
				}
				r2t_flag = 1;
				r2t_sent = 1;
				ExpDataSN = 0;
				ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
					"c#%d TransferIn=%zd, Offset=%zd, Len=%d, sent R2T (should avoid this)\n",
					conn->id, transfer_len, offset, len);
			} else {
				r2t_sent = 0;
				ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
					"c#%d TransferIn=%zd, Offset=%zd, Len=%d\n",
					conn->id, transfer_len, offset, len);
			}

			/* transfer by segment_len */
			rc = istgt_iscsi_read_pdu(conn, &data_pdu);
			if (rc < 0) {
				// ISTGT_ERRLOG("iscsi_read_pdu() failed\n");
				ISTGT_ERRLOG("c#%d iscsi_read_pdu() failed, r2t_sent=%d\n",
					conn->id, r2t_sent);
				goto error_return;
			}
			opcode = BGET8W(&data_pdu.bhs.opcode, 5, 6);

			cp = (uint8_t *) &data_pdu.bhs;
			F_bit = BGET8(&cp[1], 7);
			data_len = DGET24(&cp[5]);

			task_tag = DGET32(&cp[16]);
			transfer_tag = DGET32(&cp[20]);
			wCmdSN = DGET32(&cp[24]);
			ExpStatSN = DGET32(&cp[28]);
			DataSN = DGET32(&cp[36]);
			buffer_offset = DGET32(&cp[40]);

			/* current tag DATA? */
			msg = NULL;
			if (opcode == ISCSI_OP_SCSI_DATAOUT) {
				if (task_tag != current_task_tag) {
					msg = "not task_tag received";
				} else if (transfer_tag != current_transfer_tag) {
					msg = "not transfer_tag received";
				}
				if (msg) {
					ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
						"c#%d DATAOUT but %s [tsk:%x tran:%x]", conn->id, msg, task_tag,  transfer_tag);
					rc = istgt_iscsi_op_data(conn, &data_pdu);
					if (rc < 0) {
						ISTGT_ERRLOG("c#%d iscsi_op_data() failed\n", conn->id);
						goto error_return;
					}
					if (data_pdu.ahs != NULL) {
						xfree(data_pdu.ahs);
						data_pdu.ahs = NULL;
					}
					if (data_pdu.data != NULL) {
						xfree(data_pdu.data);
						data_pdu.data = NULL;
					}
					continue;
				}
			} else {
				ISCSI_PDU_Ptr save_pdu;

				rc = istgt_queue_count(&conn->pending_pdus);
				if (rc > conn->max_pending) {
					ISTGT_ERRLOG("c#%d pending queue(%d) is full\n", conn->id, conn->max_pending);
					goto error_return;
				}
				ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "c#%d non DATAOUT PDU, move to pending:%d/%d  OP=0x%x cmdsn:0x%x expstatsn:0x%x\n", conn->id, rc, conn->max_pending, opcode, wCmdSN, ExpStatSN);
				save_pdu = xmalloc(sizeof (*save_pdu));
				istgt_iscsi_copy_pdu(save_pdu, &data_pdu);
				r_ptr = istgt_queue_enqueue(&conn->pending_pdus, save_pdu);
				if (r_ptr == NULL) {
					ISTGT_ERRLOG("c#%d queue_enqueue() failed\n", conn->id);
					if (save_pdu->ahs != NULL) {
						xfree(save_pdu->ahs);
						save_pdu->ahs = NULL;
					}
					if (save_pdu->data != NULL) {
						xfree(save_pdu->data);
						save_pdu->data = NULL;
					}
					xfree(save_pdu);
					save_pdu = NULL;
					goto error_return;
				}
				data_pdu.ahs = NULL;
				data_pdu.data = NULL;
				data_pdu.opcode = 0;
				continue;
			}

			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				"c#%d read_from_net cmdSN=0x%x StatSN=%x, ExpStatSN=%x, DataSN=%x, Offset=%u, Data=%zd\n",
				conn->id, wCmdSN, conn->StatSN, ExpStatSN, DataSN, buffer_offset, data_len);
			if (DataSN != ExpDataSN) {
				ISTGT_ERRLOG("c#%d DataSN(%x) error\n", conn->id, DataSN);
				goto error_return;
			}
#if 0
			/* not check in DATAOUT */
			if (ExpStatSN != conn->StatSN) {
				ISTGT_ERRLOG("StatSN(%u) error\n",
					conn->StatSN);
				goto error_return;
			}
#endif

			if (buffer_offset != offset) {
				ISTGT_ERRLOG("c#%d offset(%u) error\n",
					conn->id, buffer_offset);
				goto error_return;
			}
			// if (buffer_offset + data_len > alloc_len) {
			//	ISTGT_ERRLOG("offset error\n");
			//	goto error_return;
			// }

			timediff(lu_cmd, '@', __LINE__);
			i = ++lu_cmd->iobufindx;
			lu_cmd->iobufsize += data_len;
			lu_cmd->iobuf[i].iov_base = data_pdu.data;
			lu_cmd->iobuf[i].iov_len = data_len;
			data_pdu.data = NULL; data_pdu.data_segment_len = 0;
			// memcpy(data + buffer_offset, data_pdu.data, data_len);
			offset += data_len;
			len -= data_len;
			ExpDataSN++;

			if (r2t_flag == 0 && (offset > first_burst_len)) {
				ISTGT_ERRLOG("c#%d data_len(%zd) > first_burst_length(%zd)",
				   conn->id,  offset, first_burst_len);
				goto error_return;
			}
			if (F_bit != 0 && len != 0) {
				if (offset < transfer_len) {
					r2t_flag = 0;
					goto r2t_retry;
				}
				ISTGT_ERRLOG("c#%d Expecting more data %d\n", conn->id, len);
				goto error_return;
			}
			if (F_bit == 0 && len == 0) {
				ISTGT_ERRLOG("c#%d F_bit not set on the last PDU\n", conn->id);
				goto error_return;
			}
			if (len == 0) {
				r2t_flag = 0;
			}
		r2t_retry:
			if (data_pdu.ahs == NULL) {
				xfree(data_pdu.ahs);
				data_pdu.ahs = NULL;
			}
			if (data_pdu.data != NULL) {
				xfree(data_pdu.data);
				data_pdu.data = NULL;
			}
		} while (offset < transfer_len);

		cp = (uint8_t *) &data_pdu.bhs;
		F_bit = BGET8(&cp[1], 7);
		if (F_bit == 0) {
			ISTGT_ERRLOG("c#%d F_bit not set on the last PDU\n", conn->id);
			return (-1);
		}
	} else {
		cp = (uint8_t *) &lu_cmd->pdu->bhs;
		F_bit = BGET8(&cp[1], 7);
		if (F_bit == 0) {
			ISTGT_ERRLOG("c#%d F_bit not set on the last PDU\n", conn->id);
			return (-1);
		}
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d Transfered=%zd, Offset=%zd\n",
		conn->id, transfer_len, offset);

	return (0);

error_return:
	if (data_pdu.ahs != NULL) {
		xfree(data_pdu.ahs);
		data_pdu.ahs = NULL;
	}
	if (data_pdu.data != NULL) {
		xfree(data_pdu.data);
		data_pdu.data = NULL;
	}
	return (-1);
}

static int
istgt_iscsi_send_nopin(CONN_Ptr conn)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t transfer_tag;
	int rc;
	uint64_t l_isid;
	uint16_t l_tsih;
	uint32_t s_ExpCmdSN, s_MaxCmdSN;
	if (conn->sess == NULL) {
		return (0);
	}
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}

	SESS_MTX_LOCK(conn);
	l_isid = conn->sess->isid;
	l_tsih = conn->sess->tsih;
	s_ExpCmdSN = conn->sess->ExpCmdSN;
	s_MaxCmdSN = conn->sess->MaxCmdSN;
	SESS_MTX_UNLOCK(conn);

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"send NOPIN isid=%lx, tsih=%u, cid=%u StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x\n",
		l_isid, l_tsih, conn->cid,
		conn->StatSN, s_ExpCmdSN, s_MaxCmdSN);

	/* without wanting NOPOUT */
	lun = 0;
	task_tag = 0xffffffffU;
	transfer_tag = 0xffffffffU;

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = NULL;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;
	rsp[0] = ISCSI_OP_NOPIN;
	BDADD8(&rsp[1], 1, 7);
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], 0); // DataSegmentLength

	DSET64(&rsp[8], lun);
	DSET32(&rsp[16], task_tag);
	DSET32(&rsp[20], transfer_tag);

	rc = istgt_iscsi_write_pdu_upd(conn, &rsp_pdu, 0);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}

	return (0);
}

int
istgt_iscsi_send_async(CONN_Ptr conn)
{
	ISCSI_PDU rsp_pdu;
	uint8_t *rsp;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t transfer_tag;
	int rc;
	ISTGT_NOTICELOG("SEND ASYNC EVENT for the Initiator");

	if (conn->sess == NULL) {
		return (0);
	}
	if (!conn->full_feature) {
		ISTGT_ERRLOG("before Full Feature\n");
		return (-1);
	}

	SESS_MTX_LOCK(conn);
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"StatSN=%x, ExpCmdSN=%x, MaxCmdSN=%x\n",
		conn->StatSN, conn->sess->ExpCmdSN,
		conn->sess->MaxCmdSN);
	SESS_MTX_UNLOCK(conn);

	/* without wanting NOPOUT */
	lun = 0;
	task_tag = 0xffffffffU;
	transfer_tag = 0xffffffffU;

	/* response PDU */
	rsp = (uint8_t *) &rsp_pdu.bhs;
	rsp_pdu.data = NULL;
	// memset(rsp, 0, ISCSI_BHS_LEN);
	uint64_t *tptr = (uint64_t *)rsp;
	*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0;
	*(tptr+3) = 0; *(tptr+4) = 0; *(tptr+5) = 0;
	rsp[0] = ISCSI_OP_ASYNC;
	BDADD8(&rsp[1], 1, 7);
	rsp[4] = 0; // TotalAHSLength
	DSET24(&rsp[5], 0); // DataSegmentLength

	DSET64(&rsp[8], lun);
	DSET32(&rsp[16], task_tag);
	DSET32(&rsp[20], transfer_tag);

	rc = istgt_iscsi_write_pdu_upd(conn, &rsp_pdu, 0);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_write_pdu() failed\n");
		return (-1);
	}
	istgt_iscsi_drop_all_conns(conn);

	return (0);
}

static int
istgt_iscsi_execute(CONN_Ptr conn, ISCSI_PDU_Ptr pdu)
{
	uint8_t opcode;
	int rc, ret = 0;
	const char *op = "--";

	if (pdu == NULL)
		return (-1);

	uint8_t bhsopcode = BGET8W(&pdu->bhs.opcode, 5, 6);
	opcode = pdu->opcode;
	if (bhsopcode != opcode) {
		ISTGT_ERRLOG("opcode changed? (%x->%x), isid=%"PRIx64", tsih=%u, cid=%u\n",
				opcode, bhsopcode, conn->isid, conn->tsih, conn->cid);
		pdu->opcode = bhsopcode;
		opcode = bhsopcode;
	}
	switch (opcode) {
	case ISCSI_OP_NOPOUT:
		op = "nopout";
		rc = istgt_iscsi_op_nopout(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_SCSI:
		op = "scsi";
		rc = istgt_iscsi_op_scsi(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_TASK:
		op = "task";
		rc = istgt_iscsi_op_task(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_LOGIN:
		op = "login";
		rc = istgt_iscsi_op_login(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_TEXT:
		op = "text";
		rc = istgt_iscsi_op_text(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_LOGOUT:
		op = "logout";
		rc = istgt_iscsi_op_logout(conn, pdu);
		if (rc < 0)
			goto error_out;
		ret = 1;
		break;

	case ISCSI_OP_SCSI_DATAOUT:
		op = "dataout";
		rc = istgt_iscsi_op_data(conn, pdu);
		if (rc < 0)
			goto error_out;
		break;

	case ISCSI_OP_SNACK:
		op = "snack";
	default:
		ISTGT_ERRLOG("unsupported opcode=%x:%s, isid=%"PRIx64", tsih=%u, cid=%u\n",
				opcode, op, conn->isid, conn->tsih, conn->cid);
		rc = istgt_iscsi_reject(conn, pdu, 0x04);
		if (rc < 0) {
			ISTGT_ERRLOG("iscsi_reject() failed\n");
			return (-1);
		}
		break;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "isid=%"PRIx64", tsih=%u, cid=%u, OP=%x:%s done\n",
		conn->isid, conn->tsih, conn->cid, opcode, op);
	return (ret);

	error_out:

	ISTGT_ERRLOG("failed:%d: op:%x:%s, isid=%"PRIx64", tsih=%u, cid=%u\n",
			rc, opcode, op, conn->isid, conn->tsih, conn->cid);
	return (-1);
}

static void
wait_all_task(CONN_Ptr conn)
{
	ISTGT_LU_TASK_Ptr lu_task;

	int epfd;
	struct epoll_event event, events;
	struct timespec ep_timeout;

	int msec = 30;
	int rc;

	if (conn->running_tasks == 0) {
		printf("c#%d running tasks are zero..\n", conn->id);
		return;
	}

	epfd = epoll_create1(0);
	if (epfd == -1) {
		ISTGT_ERRLOG("epoll_create1() failed\n");
		return;
	}
	event.data.fd = conn->task_pipe[0];
	event.events = EPOLLIN;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, conn->task_pipe[0], &event);
	if (rc == -1) {
		ISTGT_ERRLOG("epoll_ctl() failed\n");
		close(epfd);
		return;
	}

	/* wait all running tasks */
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		"waiting task start (%d) (left %d tasks)\n",
		conn->id, conn->running_tasks);
	while (1) {
		ep_timeout.tv_sec = msec;
		ep_timeout.tv_nsec = 0;

		rc = epoll_wait(epfd, &events, 1, ep_timeout.tv_sec*1000);
		// rc = kevent(kq, NULL, 0, &kev, 1, &kev_timeout);
		if (rc == -1 && errno == EINTR) {
			continue;
		}
		if (rc == -1) {
			ISTGT_ERRLOG("epoll_wait() failed\n");
			break;
		}
		if (rc == 0) {
			ISTGT_ERRLOG("waiting task timeout (left %d tasks)\n",
				conn->running_tasks);
			break;
		}

		if (events.data.fd == conn->task_pipe[0]) {
			// if (kev.flags & (EV_EOF|EV_ERROR)) {
			// 	break;
			// }
			char tmp[1];

			rc = read(conn->task_pipe[0], tmp, 1);
			if (rc < 0 || rc == 0 || rc != 1) {
				ISTGT_ERRLOG("read() failed\n");
				break;
			}

			MTX_LOCK(&conn->task_queue_mutex);
			lu_task = istgt_queue_dequeue(&conn->task_queue);
			MTX_UNLOCK(&conn->task_queue_mutex);
			if (lu_task != NULL) {
				if (lu_task->lu_cmd.W_bit) {
					/* write */
					if (lu_task->req_transfer_out != 0) {
						/* error transfer */
						lu_task->error = 1;
						lu_task->abort = 1;
						rc = pthread_cond_broadcast(&lu_task->trans_cond);
						if (rc != 0) {
							ISTGT_ERRLOG("cond_broadcast() failed\n");
							/* ignore error */
						}
					} else {
						if (lu_task->req_execute) {
							conn->running_tasks--;
							if (conn->running_tasks == 0) {
								ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
									"task cleanup finished\n");
								break;
							}
						}
						/* ignore response */
#if 0
						rc = istgt_lu_destroy_task(lu_task);
						if (rc < 0) {
							ISTGT_ERRLOG("lu_destroy_task() failed\n");
							/* ignore error */
						}
#endif
					}
				} else {
					/* read or no data */
					/* ignore response */
#if 0
					rc = istgt_lu_destroy_task(lu_task);
					if (rc < 0) {
						ISTGT_ERRLOG("lu_destroy_task() failed\n");
						/* ignore error */
					}
#endif
				}
			} else {
				ISTGT_ERRLOG("lu_task is NULL\n");
				break;
			}
		}
	}

	istgt_clear_all_transfer_task(conn);
	close(epfd);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		"waiting task end (%d) (left %d tasks)\n",
		conn->id, conn->running_tasks);
}


static void
snd_cleanup(void *arg)
{
	ISTGT_WARNLOG("snd:%p thread_exit\n", arg);
}

static void
worker_cleanup(void *arg)
{
	CONN_Ptr conn = (CONN_Ptr) arg;
	ISTGT_LU_Ptr lu;
	int rc;

	ISTGT_WARNLOG("conn:%d/%d/%d  %s/%s cleanup", conn->id, conn->epfd, ntohs(conn->iport), conn->thr, conn->sthr);

	/* cleanup */
	pthread_mutex_unlock(&conn->task_queue_mutex);
	pthread_mutex_unlock(&conn->result_queue_mutex);
	if (conn->sess != NULL) {
		if (conn->sess->lu != NULL) {
			pthread_mutex_unlock(&conn->sess->lu->mutex);
		}
		pthread_mutex_unlock(&conn->sess->mutex);
	}
	if (conn->exec_lu_task != NULL) {
		conn->exec_lu_task->error = 1;
		pthread_cond_broadcast(&conn->exec_lu_task->trans_cond);
		pthread_mutex_unlock(&conn->exec_lu_task->trans_mutex);
	}
	pthread_mutex_unlock(&conn->wpdu_mutex);
	pthread_mutex_unlock(&conn->r2t_mutex);
	pthread_mutex_unlock(&conn->istgt->mutex);
	pthread_mutex_unlock(&g_conns_mutex);
	pthread_mutex_unlock(&g_last_tsih_mutex);

	conn->state = CONN_STATE_EXITING;
	if (conn->sess != NULL) {
		lu = conn->sess->lu;
		if (lu != NULL)
			MTX_LOCK(&lu->mutex);
		SESS_MTX_LOCK(conn);
		if (lu != NULL && lu->queue_depth != 0) {
			rc = istgt_lu_clear_task_IT(conn, lu);
			if (rc < 0) {
				ISTGT_ERRLOG("lu_clear_task_IT() failed\n");
			}
			istgt_clear_all_transfer_task(conn);
		}
		SESS_MTX_UNLOCK(conn);
		if (lu != NULL)
			MTX_UNLOCK(&lu->mutex);
	}
	if (conn->pdu.ahs != NULL) {
		xfree(conn->pdu.ahs);
		conn->pdu.ahs = NULL;
	}
	if (conn->pdu.data != NULL) {
		xfree(conn->pdu.data);
		conn->pdu.data = NULL;
	}
	wait_all_task(conn);

	MTX_LOCK(&conn->result_queue_mutex);
	pthread_cond_broadcast(&conn->result_queue_cond);
	MTX_UNLOCK(&conn->result_queue_mutex);
	pthread_join(conn->sender_thread, NULL);
	close(conn->sock);
	close(conn->epfd);
	conn->epfd = -1;
#if 0
	sleep(1);
#endif

	/* cleanup conn & sess */
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "cancel cleanup LOCK\n");
	while (conn->inflight != 0)
		sleep(1);
	sleep(5);
	MTX_LOCK(&g_conns_mutex);
	g_conns[conn->id] = NULL;
	istgt_remove_conn(conn);
	MTX_UNLOCK(&g_conns_mutex);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "cancel cleanup UNLOCK\n");
}

const char lu_task_typ[4][12] = {
	"RESPONSE",
	"REQPDU",
	"REQUPDPDU",
	"unknown"
};

int g_logtimes = 0;
uint64_t g_logdelayns = 50000000; // 50ms
extern char scsi_ops[SCSI_ARYSZ + 1][20];

static inline void
prof_log(ISTGT_LU_CMD_Ptr p, const char *caller)
{
	if (p == NULL)
		return;
	int ind = 0;
	uint64_t len = 0;
	uint64_t x = 1;
	int base = 0, i, levels;
	int baseindx = (base >= 0 || base < _PSZ) ? base : 0;
	int _inx = p->_andx;
	struct timespec *_s = &(p->times[baseindx]);
	struct timespec *_n = &(p->times[_inx]);
	struct timespec _r;
	ISTGT_LU_DISK *spec;
	unsigned long secs, nsecs;

	if ((_n->tv_nsec - _s->tv_nsec) < 0) {
		_r.tv_sec  = _n->tv_sec - _s->tv_sec-1;
		_r.tv_nsec = 1000000000 + _n->tv_nsec - _s->tv_nsec;
	} else {
		_r.tv_sec  = _n->tv_sec - _s->tv_sec;
		_r.tv_nsec = _n->tv_nsec - _s->tv_nsec;
	}
	spec = NULL;
	if (p->lu) {
		spec = p->lu->lun[0].spec;
		if (spec != NULL) {
			if (p->lblen > 0) {
				len = (p->lblen * spec->blocklen)/ 1024;
				for (; (x < len && ind < 9); x *= 2, ind++);
			}
			else
				ind = 0;
			switch (p->cdb0) {
				case SBC_WRITE_6:
				case SBC_WRITE_10:
				case SBC_WRITE_12:
				case SBC_WRITE_16:
				case SBC_WRITE_AND_VERIFY_10:
				case SBC_WRITE_AND_VERIFY_12:
				case SBC_WRITE_AND_VERIFY_16:
					if ((spec->IO_size[ind].write.total_time.tv_sec < _r.tv_sec) || (spec->IO_size[ind].write.total_time.tv_sec == _r.tv_sec && spec->IO_size[ind].write.total_time.tv_nsec < _r.tv_nsec)) {
						spec->IO_size[ind].write.total_time.tv_sec = _r.tv_sec;
						spec->IO_size[ind].write.total_time.tv_nsec = _r.tv_nsec;
						spec->IO_size[ind].write.lba = p->lba;
						spec->IO_size[ind].write.lblen = p->lblen;
						for (i = 1; i < _PSZ; i++) {
							spec->IO_size[ind].write.caller[i] = p->caller[i];
							spec->IO_size[ind].write.tdiff[i].tv_sec = p->tdiff[i].tv_sec;
							spec->IO_size[ind].write.tdiff[i].tv_nsec = p->tdiff[i].tv_nsec;
						}
					}
					break;
				case SBC_READ_6:
				case SBC_READ_10:
				case SBC_READ_12:
				case SBC_READ_16:
					if ((spec->IO_size[ind].read.total_time.tv_sec < _r.tv_sec) || (spec->IO_size[ind].read.total_time.tv_sec == _r.tv_sec && spec->IO_size[ind].read.total_time.tv_nsec < _r.tv_nsec)) {
						spec->IO_size[ind].read.total_time.tv_sec = _r.tv_sec;
						spec->IO_size[ind].read.total_time.tv_nsec = _r.tv_nsec;
						spec->IO_size[ind].read.lba = p->lba;
						spec->IO_size[ind].read.lblen = p->lblen;
						for (i = 1; i < _PSZ; i++) {
							spec->IO_size[ind].read.caller[i] = p->caller[i];
							spec->IO_size[ind].read.tdiff[i].tv_sec = p->tdiff[i].tv_sec;
							spec->IO_size[ind].read.tdiff[i].tv_nsec = p->tdiff[i].tv_nsec;
						}
					}
					break;
				case SBC_COMPARE_AND_WRITE:
					if ((spec->IO_size[ind].cmp_n_write.total_time.tv_sec < _r.tv_sec) || (spec->IO_size[ind].cmp_n_write.total_time.tv_sec == _r.tv_sec && spec->IO_size[ind].cmp_n_write.total_time.tv_nsec < _r.tv_nsec)) {
						spec->IO_size[ind].cmp_n_write.total_time.tv_sec = _r.tv_sec;
						spec->IO_size[ind].cmp_n_write.total_time.tv_nsec = _r.tv_nsec;
						spec->IO_size[ind].cmp_n_write.lba = p->lba;
						spec->IO_size[ind].cmp_n_write.lblen = p->lblen;
						for (i = 1; i < _PSZ; i++) {
							spec->IO_size[ind].cmp_n_write.caller[i] = p->caller[i];
							spec->IO_size[ind].cmp_n_write.tdiff[i].tv_sec = p->tdiff[i].tv_sec;
							spec->IO_size[ind].cmp_n_write.tdiff[i].tv_nsec = p->tdiff[i].tv_nsec;
						}
					}
					break;
				case SBC_UNMAP:
					if ((spec->IO_size[ind].unmp.total_time.tv_sec < _r.tv_sec) || (spec->IO_size[ind].unmp.total_time.tv_sec == _r.tv_sec && spec->IO_size[ind].unmp.total_time.tv_nsec < _r.tv_nsec)) {
						spec->IO_size[ind].unmp.total_time.tv_sec = _r.tv_sec;
						spec->IO_size[ind].unmp.total_time.tv_nsec = _r.tv_nsec;
						spec->IO_size[ind].unmp.lba = p->lba;
						spec->IO_size[ind].unmp.lblen = p->lblen;
						for (i = 1; i < _PSZ; i++) {
							spec->IO_size[ind].unmp.caller[i] = p->caller[i];
							spec->IO_size[ind].unmp.tdiff[i].tv_sec = p->tdiff[i].tv_sec;
							spec->IO_size[ind].unmp.tdiff[i].tv_nsec = p->tdiff[i].tv_nsec;
						}
					}
					break;
				case SBC_WRITE_SAME_10:
				case SBC_WRITE_SAME_16:
					if ((spec->IO_size[ind].write_same.total_time.tv_sec < _r.tv_sec) || (spec->IO_size[ind].write_same.total_time.tv_sec == _r.tv_sec && spec->IO_size[ind].write_same.total_time.tv_nsec < _r.tv_nsec)) {
						spec->IO_size[ind].write_same.total_time.tv_sec = _r.tv_sec;
						spec->IO_size[ind].write_same.total_time.tv_nsec = _r.tv_nsec;
						spec->IO_size[ind].write_same.lba = p->lba;
						spec->IO_size[ind].write_same.lblen = p->lblen;
						for (i = 1; i < _PSZ; i++) {
							spec->IO_size[ind].write_same.caller[i] = p->caller[i];
							spec->IO_size[ind].write_same.tdiff[i].tv_sec = p->tdiff[i].tv_sec;
							spec->IO_size[ind].write_same.tdiff[i].tv_nsec = p->tdiff[i].tv_nsec;
						}
					}
					break;
			}
		}
	}
	levels = 8;
	if (unlikely(spec != NULL && spec->do_avg == 1)) {
		// if (p->caller[1] == 'q' && p->caller[2] == 'w' && p->caller[3] == 'D' && p->caller[4] == 'r' && p->caller[5] == 's')
		if (p->caller[2] == '1' && p->caller[3] == 'q' && p->caller[4] == 'w' && p->caller[5] == 'D' && p->caller[6] == 'r' && p->caller[7] == 's') {
			for (i = 2; i < levels; i++) {
				spec->avgs[i].count++;
				spec->avgs[i].tot_sec += p->tdiff[i].tv_sec;
				spec->avgs[i].tot_nsec += p->tdiff[i].tv_nsec;
				secs = spec->avgs[i].tot_nsec/1000000000;
				nsecs = spec->avgs[i].tot_nsec%1000000000;
				spec->avgs[i].tot_sec += secs;
				spec->avgs[i].tot_nsec = nsecs;
			}
		} {
			spec->avgs[levels].count++;
			spec->avgs[levels].tot_sec += (_r.tv_sec);
			spec->avgs[levels].tot_nsec += (_r.tv_nsec);
			secs = spec->avgs[levels].tot_nsec/1000000000;
			nsecs = spec->avgs[levels].tot_nsec%1000000000;
			spec->avgs[levels].tot_sec += secs;
			spec->avgs[levels].tot_nsec = nsecs;
		}
		// levels++;
		// spec->avgs[levels].count += istgt_queue_count(&spec->cmd_queue);
		// levels++;
		// spec->avgs[levels].count += istgt_queue_count(&spec->blocked_queue);
		// levels++;
		// levels++;
		// spec->avgs[levels].count += spec->inflight;
	}

		if (g_logtimes == 1 && (_r.tv_sec || (_r.tv_nsec > (long)g_logdelayns))) {
				syslog(LOG_NOTICE, "%-20.20s: %-4.4s: LU%d: CSN:0x%x TT:%x OP:%2.2x:%x:%s:%s(%lu+%u)"
						" %lds.%9.9ldns [%c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld"
						" %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld]%d flags:0x%x",
						tinfo, caller, p->lunum, p->CmdSN, p->task_tag, p->cdb0, p->status,
						scsi_ops[istgt_cmd_table[p->cdb0].statidx], p->info, p->lba, p->lblen,
						_r.tv_sec, _r.tv_nsec,
						p->caller[1] ? p->caller[1] : '9', p->tdiff[1].tv_sec, p->tdiff[1].tv_nsec,
						p->caller[2] ? p->caller[2] : '9', p->tdiff[2].tv_sec, p->tdiff[2].tv_nsec,
						p->caller[3] ? p->caller[3] : '9', p->tdiff[3].tv_sec, p->tdiff[3].tv_nsec,
						p->caller[4] ? p->caller[4] : '9', p->tdiff[4].tv_sec, p->tdiff[4].tv_nsec,
						p->caller[5] ? p->caller[5] : '9', p->tdiff[5].tv_sec, p->tdiff[5].tv_nsec,
						p->caller[6] ? p->caller[6] : '9', p->tdiff[6].tv_sec, p->tdiff[6].tv_nsec,
						p->caller[7] ? p->caller[7] : '9', p->tdiff[7].tv_sec, p->tdiff[7].tv_nsec,
						p->caller[8] ? p->caller[8] : '9', p->tdiff[8].tv_sec, p->tdiff[8].tv_nsec,
						p->_roll_cnt, p->flags);
		} else if (g_trace_flag & (ISTGT_TRACE_ISCSI | ISTGT_TRACE_PROF | ISTGT_TRACE_PROFX)) {
				if (_r.tv_sec == 0 && _r.tv_nsec < 8000000) {
						if (g_trace_flag & ISTGT_TRACE_PROFX)
								syslog(LOG_NOTICE, "%-20.20s: %-4.4s: LU%d: CSN:0x%x TT:%x OP:%2.2x:%x:%s:%s(%lu+%u)"
										"%ldns [%c %c %c %c %c %c %c %c]%d flags:0x%x",
										tinfo, caller, p->lunum, p->CmdSN, p->task_tag, p->cdb0, p->status,
										scsi_ops[istgt_cmd_table[p->cdb0].statidx], p->info, p->lba, p->lblen,
										_r.tv_nsec,
										p->caller[1] ? p->caller[1] : '9', p->caller[2] ? p->caller[2] : '9',
					p->caller[3] ? p->caller[3] : '9', p->caller[4] ? p->caller[4] : '9',
										p->caller[5] ? p->caller[5] : '9', p->caller[6] ? p->caller[6] : '9',
					p->caller[7] ? p->caller[7] : '9', p->caller[8] ? p->caller[8] : '9',
										p->_roll_cnt, p->flags);
				} else {
						syslog(LOG_NOTICE, "%-20.20s: %-4.4s: LU%d: CSN:0x%x TT:%x OP:%2.2x:%x:%s:%s(%lu+%u)"
										" %lds.%9.9ldns [%c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld"
										" %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld]%d flags:0x%x",
										tinfo, caller, p->lunum, p->CmdSN, p->task_tag, p->cdb0, p->status,
										scsi_ops[istgt_cmd_table[p->cdb0].statidx], p->info, p->lba, p->lblen,
										_r.tv_sec, _r.tv_nsec,
										p->caller[1] ? p->caller[1] : '9', p->tdiff[1].tv_sec, p->tdiff[1].tv_nsec,
										p->caller[2] ? p->caller[2] : '9', p->tdiff[2].tv_sec, p->tdiff[2].tv_nsec,
										p->caller[3] ? p->caller[3] : '9', p->tdiff[3].tv_sec, p->tdiff[3].tv_nsec,
										p->caller[4] ? p->caller[4] : '9', p->tdiff[4].tv_sec, p->tdiff[4].tv_nsec,
										p->caller[5] ? p->caller[5] : '9', p->tdiff[5].tv_sec, p->tdiff[5].tv_nsec,
										p->caller[6] ? p->caller[6] : '9', p->tdiff[6].tv_sec, p->tdiff[6].tv_nsec,
										p->caller[7] ? p->caller[7] : '9', p->tdiff[7].tv_sec, p->tdiff[7].tv_nsec,
										p->caller[8] ? p->caller[8] : '9', p->tdiff[8].tv_sec, p->tdiff[8].tv_nsec,
										p->_roll_cnt, p->flags);
				}
		}
}

#ifdef REPLICATION
static void
update_cummulative_rw_time(ISTGT_LU_TASK_Ptr lu_task)
{
	ISTGT_LU_DISK *spec = NULL;
	struct timespec endtime, diff;
	uint64_t ns = 0;

	switch (lu_task->lu_cmd.cdb0) {
		case SBC_WRITE_6:
		case SBC_WRITE_10:
		case SBC_WRITE_12:
		case SBC_WRITE_16:
		case SBC_WRITE_AND_VERIFY_10:
		case SBC_WRITE_AND_VERIFY_12:
		case SBC_WRITE_AND_VERIFY_16:
				spec = (ISTGT_LU_DISK *)
					lu_task->lu_cmd.lu->lun[0].spec;
				clock_gettime(CLOCK_MONOTONIC_RAW, &endtime);
				timesdiff(CLOCK_MONOTONIC_RAW,
					lu_task->lu_cmd.start_rw_time,
					endtime, diff);
				ns = diff.tv_sec*1000000000;
				ns += diff.tv_nsec;
				__sync_fetch_and_add(&spec->totalwritetime, ns);
				break;
		case SBC_READ_6:
		case SBC_READ_10:
		case SBC_READ_12:
		case SBC_READ_16:
				spec = (ISTGT_LU_DISK *)
					lu_task->lu_cmd.lu->lun[0].spec;
				clock_gettime(CLOCK_MONOTONIC_RAW, &endtime);
				timesdiff(CLOCK_MONOTONIC_RAW,
					lu_task->lu_cmd.start_rw_time,
					endtime, diff);
				ns = diff.tv_sec*1000000000;
				ns += diff.tv_nsec;
				__sync_fetch_and_add(&spec->totalreadtime, ns);
				break;
		}
}
#endif

static void *
sender(void *arg)
{
	CONN_Ptr conn = (CONN_Ptr) arg;
	ISTGT_LU_TASK_Ptr lu_task;
	struct timespec abstime;
	time_t now;
	int rc;
	ISCSI_PDU_Ptr pdu = NULL;
	pthread_t slf = pthread_self();
	snprintf(tinfo, sizeof (tinfo), "s#%d.%ld.%d", conn->id, (uint64_t)(((uint64_t *)slf)[0]), ntohs(conn->iport));
#ifdef HAVE_PTHREAD_SET_NAME_NP
	pthread_set_name_np(slf, tinfo);
#endif

	pthread_cleanup_push(snd_cleanup, (void *)conn);
	memset(&abstime, 0, sizeof (abstime));
	/* handle DATA-IN/SCSI status */
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "sender loop start (%d)\n", conn->id);
	// MTX_LOCK(&conn->sender_mutex);
	while (1) {
		if (conn->state != CONN_STATE_RUNNING) {
			break;
		}
		MTX_LOCK(&conn->result_queue_mutex);
	dequeue_result_queue:
		lu_task = istgt_queue_dequeue(&conn->result_queue);
		if (lu_task == NULL) {
			conn->sender_waiting = 1;
			now = time(NULL);
			abstime.tv_sec = now + conn->timeout;
			abstime.tv_nsec = 0;
			rc = pthread_cond_timedwait(&conn->result_queue_cond,
				&conn->result_queue_mutex, &abstime);
			conn->sender_waiting = 0;
			if (rc == ETIMEDOUT) {
				/* nothing */
			}
			if (conn->state != CONN_STATE_RUNNING) {
				MTX_UNLOCK(&conn->result_queue_mutex);
				break;
			} else {
				goto dequeue_result_queue;
			}
		}
		if (lu_task->lu_cmd.aborted == 1 || lu_task->lu_cmd.release_aborted == 1) {
			ISTGT_LOG("Aborted from result queue\n");
			MTX_UNLOCK(&conn->result_queue_mutex);
			rc = istgt_lu_destroy_task(lu_task);
			if (rc < 0)
				ISTGT_ERRLOG("lu_destroy_task failed\n");
			continue;
		}

		lu_task->lu_cmd.flags |= ISTGT_RESULT_Q_DEQUEUED;
		MTX_UNLOCK(&conn->result_queue_mutex);
		/* send all responses */
//		MTX_LOCK(&conn->wpdu_mutex);
		do {
			lu_task->lu_cmd.flags |= ISTGT_RESULT_Q_DEQUEUED;
			if (lu_task->lu_cmd.aborted == 1) {
				ISTGT_LOG("Aborted from result queue\n");
				rc = istgt_lu_destroy_task(lu_task);
				if (rc < 0)
					ISTGT_ERRLOG("lu_destroy_task failed\n");
				break;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				"CSN:%x sender type:%d 0x%x.%lu+%u",
				lu_task->lu_cmd.CmdSN, lu_task->type,
				lu_task->lu_cmd.cdb0, lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen);
			lu_task->lock = 1;
			timediff(&lu_task->lu_cmd, 'r', __LINE__);
			if (lu_task->type == ISTGT_LU_TASK_RESPONSE) {
#ifdef REPLICATION
				update_cummulative_rw_time(lu_task);
#endif

				/* send DATA-IN, SCSI status */
				rc = istgt_iscsi_task_response(conn, lu_task);
				if (rc < 0) {
					lu_task->error = 1;
					ISTGT_ERRLOG(
						"iscsi_task_response() CSN=%x failed"
						" on %s(%s)\n", lu_task->lu_cmd.CmdSN,
						conn->target_port, conn->initiator_port);
					rc = write(conn->task_pipe[1], "E", 1);
					if (rc < 0 || rc != 1) {
						ISTGT_ERRLOG("write() failed\n");
					}
					break;
				}
				timediff(&lu_task->lu_cmd, 's', __LINE__);
				prof_log(&lu_task->lu_cmd, "resp");
				if (lu_task->complete_queue_ptr != NULL)
					ISTGT_ERRLOG("complete_queue_ptr not NULL\n");
				rc = istgt_lu_destroy_task(lu_task);
				if (rc < 0) {
					ISTGT_ERRLOG("lu_destroy_task() failed\n");
					break;
				}
			} else if (lu_task->type == ISTGT_LU_TASK_REQPDU) {
			reqpdu:
				pdu = lu_task->lu_cmd.pdu;
				/* send PDU */
				rc = istgt_iscsi_write_pdu_internal(lu_task->conn,
					lu_task->lu_cmd.pdu);
				if (rc < 0) {
					lu_task->error = 1;
					ISTGT_ERRLOG(
						"iscsi_write_pdu() failed on %s(%s)\n",
						lu_task->conn->target_port,
						lu_task->conn->initiator_port);
					rc = write(conn->task_pipe[1], "E", 1);
					if (rc < 0 || rc != 1) {
						ISTGT_ERRLOG("write() failed\n");
					}
					break;
				}
				/* free allocated memory by caller */
				timediff(&lu_task->lu_cmd, 'S', __LINE__);
				prof_log(&lu_task->lu_cmd,
						(lu_task->type == ISTGT_LU_TASK_REQUPDPDU) ?  "requ" : "req ");
				if (pdu->data != NULL) {
					xfree(pdu->data);
					pdu->data = NULL;
				}
				if (pdu->ahs != NULL) {
					xfree(pdu->ahs);
					pdu->ahs = NULL;
				}
				if (lu_task->lu_cmd.data != NULL) {
					xfree(lu_task->lu_cmd.data);
					lu_task->lu_cmd.data = NULL;
				}
				if (lu_task->lu_cmd.sense_data != NULL) {
					xfree(lu_task->lu_cmd.sense_data);
					lu_task->lu_cmd.sense_data = NULL;
				}
				xfree(lu_task);
			} else if (lu_task->type == ISTGT_LU_TASK_REQUPDPDU) {
				rc = istgt_update_pdu(lu_task->conn, &lu_task->lu_cmd);
				if (rc < 0) {
					lu_task->error = 1;
					ISTGT_ERRLOG(
						"update_pdu() failed on %s(%s)\n",
						lu_task->conn->target_port,
						lu_task->conn->initiator_port);
					rc = write(conn->task_pipe[1], "E", 1);
					if (rc < 0 || rc != 1) {
						ISTGT_ERRLOG("write() failed\n");
					}
					break;
				}
				goto reqpdu;
			} else {
				ISTGT_ERRLOG("Unknown task type %x\n", lu_task->type);
				rc = -1;
			}
			// conn is running?
			if (conn->state != CONN_STATE_RUNNING) {
				// ISTGT_WARNLOG("exit thread\n");
				break;
			}
			MTX_LOCK(&conn->result_queue_mutex);
			lu_task = istgt_queue_dequeue(&conn->result_queue);
			MTX_UNLOCK(&conn->result_queue_mutex);
		} while (lu_task != NULL);
//		MTX_UNLOCK(&conn->wpdu_mutex);
	}
	// MTX_UNLOCK(&conn->sender_mutex);
	pthread_cleanup_pop(0);
	ISTGT_NOTICELOG("sender loop ended (%d:%d:%d)\n", conn->id, conn->epfd, ntohs(conn->iport));
	return (NULL);
}


static void *
worker(void *arg)
{
	CONN_Ptr conn = (CONN_Ptr) arg;
	ISTGT_LU_TASK_Ptr lu_task;
	ISTGT_LU_Ptr lu;
	ISCSI_PDU_Ptr pdu;
	sigset_t signew, sigold;
	int epfd;
	struct epoll_event events;
	struct timespec ep_timeout;

	// int kq;
	// struct kevent kev;
	// struct timespec kev_timeout;

	int rc;

	pthread_t slf = pthread_self();
	snprintf(tinfo, sizeof (tinfo), "c#%d.%ld.%d", conn->id, (uint64_t)(((uint64_t *)slf)[0]), ntohs(conn->iport));
#ifdef HAVE_PTHREAD_SET_NAME_NP
	pthread_set_name_np(slf, tinfo);
#endif
	epfd = epoll_create1(0);
	if (epfd == -1) {
		ISTGT_ERRLOG("epoll_create1() failed\n");
		return (NULL);
	}
	ISTGT_NOTICELOG("con:%d/%d [%x:%d->%s:%s,%d]",
		conn->id, epfd, conn->iaddr, ntohs(conn->iport),
		conn->portal.host, conn->portal.port, conn->portal.tag);
	conn->epfd = epfd;

//  TODO
// #if defined (ISTGT_USE_IOVEC) && defined (NOTE_LOWAT)
// 	ISTGT_EV_SET(&kev, conn->sock, EVFILT_READ, EV_ADD, NOTE_LOWAT, ISCSI_BHS_LEN, NULL);
// #else
// 	ISTGT_EV_SET(&kev, conn->sock, EVFILT_READ, EV_ADD, 0, 0, NULL);
// #endif

	events.data.fd = conn->sock;
		events.events = EPOLLIN;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, conn->sock, &events);
		if (rc == -1) {
		ISTGT_ERRLOG("epoll_ctl() failed\n");
		close(epfd);
		return (NULL);
		}
	events.data.fd = conn->task_pipe[0];
		events.events = EPOLLIN;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, conn->task_pipe[0], &events);
		if (rc == -1) {
		ISTGT_ERRLOG("epoll_ctl() failed\n");
		close(epfd);
		return (NULL);
		}


	// TODO
	// if (!conn->istgt->daemon) {
	// 	event.data.fd = SIGINT;
	// 	event.events = EPOLLIN;
	// 	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, SIGINT, &event);
	// 	if (rc == -1) {
	// 		ISTGT_ERRLOG("epoll_ctl() failed\n");
	// 		close(epfd);
	// 		return (NULL);
	// 	}
	// 	event.data.fd = SIGTERM;
	// 	event.events = EPOLLIN;
	// 	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, SIGTERM, &event);
	// 	if (rc == -1) {
	// 		ISTGT_ERRLOG("epoll_ctl() failed\n");
	// 		close(epfd);
	// 		return (NULL);
	// 	}
	// }



// 	kq = kqueue();
// 	if (kq == -1) {
// 		istgt_errlog("kqueue() failed\n");
// 		return (null);
// 	}
// 	istgt_noticelog("con:%d/%d [%x:%d->%s:%s,%d]",
// 		conn->id, kq, conn->iaddr, ntohs(conn->iport),
// 		conn->portal.host, conn->portal.port, conn->portal.tag);
// 	conn->kq = kq;
// #if defined (istgt_use_iovec) && defined (note_lowat)
// 	istgt_ev_set(&kev, conn->sock, evfilt_read, ev_add, note_lowat, iscsi_bhs_len, null);
// #else
// 	istgt_ev_set(&kev, conn->sock, evfilt_read, ev_add, 0, 0, null);
// #endif
// 	rc = kevent(kq, &kev, 1, null, 0, null);
// 	if (rc == -1) {
// 		istgt_errlog("kevent() failed\n");
// 		close(kq);
// 		return (null);
// 	}
// 	istgt_ev_set(&kev, conn->task_pipe[0], evfilt_read, ev_add, 0, 0, null);
// 	rc = kevent(kq, &kev, 1, null, 0, null);
// 	if (rc == -1) {
// 		istgt_errlog("kevent() failed\n");
// 		close(kq);
// 		return (null);
// 	}

// 	if (!conn->istgt->daemon) {
// 		istgt_ev_set(&kev, sigint, evfilt_signal, ev_add, 0, 0, null);
// 		rc = kevent(kq, &kev, 1, null, 0, null);
// 		if (rc == -1) {
// 			istgt_errlog("kevent() failed\n");
// 			close(kq);
// 			return (null);
// 		}
// 		istgt_ev_set(&kev, sigterm, evfilt_signal, ev_add, 0, 0, null);
// 		rc = kevent(kq, &kev, 1, null, 0, null);
// 		if (rc == -1) {
// 			istgt_errlog("kevent() failed\n");
// 			close(kq);
// 			return (null);
// 		}
// 	}

	conn->pdu.ahs = NULL;
	conn->pdu.data = NULL;
	conn->state = CONN_STATE_RUNNING;
	conn->exec_lu_task = NULL;
	lu_task = NULL;

	pthread_cleanup_push(worker_cleanup, conn);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	/* create sender thread */
	rc = pthread_create(&conn->sender_thread, &conn->istgt->attr,
			&sender, (void *)conn);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed\n");
		goto cleanup_exit;
	}
	snprintf(conn->sthr, sizeof (conn->sthr), "s#%d.%ld.%d", conn->id, (uint64_t)(((uint64_t *)(conn->sender_thread))[0]), ntohs(conn->iport));
	conn->wsock = conn->sock;

	sigemptyset(&signew);
	sigemptyset(&sigold);
	sigaddset(&signew, ISTGT_SIGWAKEUP);
	pthread_sigmask(SIG_UNBLOCK, &signew, &sigold);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "reader start (%d:%d) %s/%s\n", conn->id, conn->epfd, conn->thr, conn->sthr);
	while (1) {
		/* check exit request */
		if (conn->sess != NULL) {
			SESS_MTX_LOCK(conn);
			lu = conn->sess->lu;
			SESS_MTX_UNLOCK(conn);
		} else {
			lu = NULL;
		}
		if (lu != NULL) {
			if (istgt_lu_get_state(lu) != ISTGT_STATE_RUNNING) {
				conn->state = CONN_STATE_EXITING;
				break;
			}
		} else {
			if (istgt_get_state(conn->istgt) != ISTGT_STATE_RUNNING) {
				conn->state = CONN_STATE_EXITING;
				break;
			}
		}

		pthread_testcancel();
		if (conn->state != CONN_STATE_RUNNING) {
			break;
		}

		if (conn->nopininterval != 0) {
			ep_timeout.tv_sec = conn->nopininterval / 1000;
			ep_timeout.tv_nsec = (conn->nopininterval % 1000) * 1000000;
		} else {
			ep_timeout.tv_sec = DEFAULT_NOPININTERVAL;
			ep_timeout.tv_nsec = 0;
		}
		rc = epoll_wait(epfd, &events, 1, ep_timeout.tv_sec*1000);
		if (rc == -1 && errno == EINTR) {
			ISTGT_ERRLOG("EINTR event\n");
			continue;
		}
		if (rc == -1) {
			ISTGT_ERRLOG("epoll_wait() failed\n");
			break;
		}
		if (rc == 0) {
			/* idle timeout, send diagnosis packet */
			if (conn->nopininterval != 0) {
				rc = istgt_iscsi_send_nopin(conn);
				if (rc < 0) {
					ISTGT_ERRLOG("iscsi_send_nopin() failed\n");
					break;
				}
			}
			continue;
		}

		/* on socket */
		if (events.data.fd == conn->sock) {
			if ((events.events & EPOLLERR) ||
					(events.events & EPOLLHUP) ||
					(!(events.events & EPOLLIN))) {
				ISTGT_ERRLOG("close conn %d\n", errno);
				break;
			}

			rc = istgt_iscsi_read_pdu(conn, &conn->pdu);
			if (rc < 0) {
				if (errno == EAGAIN) {
					break;
				}
				if (conn->state != CONN_STATE_EXITING) {
					ISTGT_ERRLOG("conn->state = %d\n", conn->state);
				}
				if (conn->state != CONN_STATE_RUNNING) {
					if (errno == EINPROGRESS) {
						sleep(1);
						continue;
					}
					if (errno == ECONNRESET
						|| errno == ETIMEDOUT) {
						ISTGT_ERRLOG(
							"iscsi_read_pdu() RESET/TIMEOUT errno %d\n", errno);
					} else {
						ISTGT_ERRLOG(
							"iscsi_read_pdu() EOF\n");
					}
					break;
				}
				ISTGT_ERRLOG("iscsi_read_pdu() failed %d conn-state:%d\n", rc, conn->state);
				break;
			}
		execute_pdu:
			if (conn->state != CONN_STATE_RUNNING) {
				break;
			}

			rc = istgt_iscsi_execute(conn, &conn->pdu);
			if (rc < 0) {
				ISTGT_ERRLOG("iscsi_execute() failed on %s(%s)\n",
					conn->target_port, conn->initiator_port);
				break;
			} else if (rc == 1) { // means successful logout ISCSI_OP_LOGOUT
				ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "logout received\n");
				break;
			}

			if (conn->pdu.ahs != NULL) {
				xfree(conn->pdu.ahs);
				conn->pdu.ahs = NULL;
			}
			if (conn->pdu.data != NULL) {
				xfree(conn->pdu.data);
				conn->pdu.data = NULL;
			}

			/* execute pending PDUs */
			pdu = istgt_queue_dequeue(&conn->pending_pdus);
			if (pdu != NULL) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"execute pending PDU\n");
				istgt_iscsi_copy_pdu(&conn->pdu, pdu);
				xfree(pdu);
				goto execute_pdu;
			}

		}

		/* execute on task queue */
		if (events.data.fd == conn->task_pipe[0]) {
			if ((events.events & EPOLLERR) ||
					(events.events & EPOLLHUP) ||
					(!(events.events & EPOLLIN))) {
				ISTGT_LOG("close pipe %d\n", errno);
				break;
			}
			char tmp[1];


			rc = read(conn->task_pipe[0], tmp, 1);
			if (rc < 0 || rc == 0 || rc != 1) {
				ISTGT_ERRLOG("read() failed rc:%d (con:%d)\n", rc, conn->id);
				break;
			}
			if (tmp[0] == 'E') {
				ISTGT_NOTICELOG("exit request (%d)\n", conn->id);
				break;
			}

			/* DATA-IN/OUT */
			MTX_LOCK(&conn->task_queue_mutex);
			rc = istgt_queue_count(&conn->task_queue);
			lu_task = istgt_queue_dequeue(&conn->task_queue);
			MTX_UNLOCK(&conn->task_queue_mutex);
			if (lu_task != NULL) {
				if (conn->exec_lu_task != NULL) {
					ISTGT_ERRLOG("task is overlapped (CSN=%x, %x)\n",
						conn->exec_lu_task->lu_cmd.CmdSN,
						lu_task->lu_cmd.CmdSN);
					break;
				}
				conn->exec_lu_task = lu_task;
				if (lu_task->lu_cmd.W_bit) {
					/* write */
					if (lu_task->req_transfer_out == 0) {
						if (lu_task->req_execute) {
							if (conn->running_tasks > 0) {
								conn->running_tasks--;
							} else {
								ISTGT_ERRLOG("running no task\n");
							}
						}
						rc = istgt_iscsi_task_response(conn, lu_task);
						if (rc < 0) {
							lu_task->error = 1;
							ISTGT_ERRLOG("iscsi_task_response() failed on %s(%s)\n",
								conn->target_port,
								conn->initiator_port);
							break;
						}
						lu_task = NULL;
						conn->exec_lu_task = NULL;
					} else {
						rc = istgt_iscsi_transfer_out(conn, &(lu_task->lu_cmd),
								lu_task->lu_cmd.transfer_len);
						if (rc < 0) {
							lu_task->error = 1;
							ISTGT_ERRLOG("iscsi_task_transfer_out() failed on %s(%s)\n",
								conn->target_port,
								conn->initiator_port);
							break;
						}

						MTX_LOCK(&lu_task->trans_mutex);
						lu_task->req_transfer_out = 0;

						/* need response after execution */
						lu_task->req_execute = 1;

						rc = pthread_cond_broadcast(&lu_task->trans_cond);
						MTX_UNLOCK(&lu_task->trans_mutex);
						if (rc != 0) {
							ISTGT_ERRLOG("cond_broadcast() failed\n");
							break;
						}
						lu_task = NULL;
						conn->exec_lu_task = NULL;
					}
				} else {
					/* read or no data */
					rc = istgt_iscsi_task_response(conn, lu_task);
					if (rc < 0) {
						lu_task->error = 1;
						ISTGT_ERRLOG("iscsi_task_response() failed on %s(%s)\n",
							conn->target_port,
							conn->initiator_port);
						break;
					}
					lu_task = NULL;
					conn->exec_lu_task = NULL;
				}
			}
			/* XXX PDUs in DATA-OUT? */
			pdu = istgt_queue_dequeue(&conn->pending_pdus);
			if (pdu != NULL) {
				ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
					"pending in task\n");
				istgt_iscsi_copy_pdu(&conn->pdu, pdu);
				xfree(pdu);
				events.data.fd = -1;
				goto execute_pdu;
			}
		}
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "loop ended (%d)\n", conn->id);

	cleanup_exit:
;
	pthread_cleanup_pop(0);
	conn->state = CONN_STATE_EXITING;
	if (conn->sess != NULL) {
		lu = conn->sess->lu;
		if (lu != NULL)
			MTX_LOCK(&lu->mutex);
		SESS_MTX_LOCK(conn);
		rc = 0;
		if (lu != NULL && lu->queue_depth != 0) {
			rc = istgt_lu_clear_task_IT(conn, lu);
			istgt_clear_all_transfer_task(conn);
		}
		SESS_MTX_UNLOCK(conn);
		if (lu != NULL)
			MTX_UNLOCK(&lu->mutex);
		if (rc < 0) {
			ISTGT_ERRLOG("lu_clear_task_IT() failed\n");
		}
	}
	if (conn->pdu.ahs != NULL) {
		xfree(conn->pdu.ahs);
		conn->pdu.ahs = NULL;
	}
	if (conn->pdu.data != NULL) {
		xfree(conn->pdu.data);
		conn->pdu.data = NULL;
	}
	wait_all_task(conn);


	if (1) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "stop sender thread (%d)\n", conn->id);
		/* stop sender thread */
		MTX_LOCK(&conn->result_queue_mutex);
		rc = pthread_cond_broadcast(&conn->result_queue_cond);
		MTX_UNLOCK(&conn->result_queue_mutex);
		if (rc != 0) {
			ISTGT_ERRLOG("cond_broadcast() failed\n");
			/* ignore errors */
		}
		rc = pthread_join(conn->sender_thread, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("pthread_join() failed\n");
			/* ignore errors */
		}
	}

	close(conn->sock);
	close(epfd);
	conn->epfd = -1;
	ISTGT_NOTICELOG("worker %d/%d/%d end (%s/%s)", conn->id, conn->epfd, ntohs(conn->iport), conn->thr, conn->sthr);

	/* cleanup conn & sess */
	while (conn->inflight != 0)
		sleep(1);
	sleep(5);
	MTX_LOCK(&g_conns_mutex);
	g_conns[conn->id] = NULL;
	istgt_remove_conn(conn);
	MTX_UNLOCK(&g_conns_mutex);
	return (NULL);
}
int
istgt_create_conn(ISTGT_Ptr istgt, PORTAL_Ptr portal, int sock, struct sockaddr *sa, socklen_t salen __attribute__((__unused__)))
{
	char buf[MAX_TMPBUF];
	CONN_Ptr conn;
	int rc;
	int i;

	conn = xmalloc(sizeof (*conn));
	memset(conn, 0, sizeof (*conn));

	conn->istgt = istgt;
	MTX_LOCK(&istgt->mutex);
	conn->timeout = istgt->timeout;
	conn->nopininterval = istgt->nopininterval;
	conn->nopininterval *= 1000; /* sec. to msec. */
	conn->max_r2t = istgt->maxr2t;
	conn->TargetMaxRecvDataSegmentLength = istgt->MaxRecvDataSegmentLength;
	MTX_UNLOCK(&istgt->mutex);
	conn->MaxRecvDataSegmentLength = 8192; // RFC3720(12.12)
	if (conn->TargetMaxRecvDataSegmentLength
		< conn->MaxRecvDataSegmentLength) {
		conn->TargetMaxRecvDataSegmentLength
			= conn->MaxRecvDataSegmentLength;
	}
	conn->MaxOutstandingR2T = 1;
	conn->FirstBurstLength = DEFAULT_FIRSTBURSTLENGTH;
	conn->MaxBurstLength = DEFAULT_MAXBURSTLENGTH;

	conn->diskIoPending = 0;
	conn->flagDelayedFree = 0;

	conn->portal.label = xstrdup(portal->label);
	conn->portal.host = xstrdup(portal->host);
	conn->portal.port = xstrdup(portal->port);
	conn->portal.idx = portal->idx;
	conn->portal.tag = portal->tag;
	conn->portal.sock = -1;
	conn->sock = sock;
	conn->wsock = -1;
	conn->epfd = -1;

	conn->sess = NULL;
	conn->params = NULL;
	conn->state = CONN_STATE_INVALID;
	conn->exec_logout = 0;
	conn->max_pending = 0;
	conn->queue_depth = 0;
	conn->pending_r2t = 0;
	conn->header_digest = 0;
	conn->data_digest = 0;
	conn->full_feature = 0;
	conn->login_phase = ISCSI_LOGIN_PHASE_NONE;
	conn->auth.user = NULL;
	conn->auth.secret = NULL;
	conn->auth.muser = NULL;
	conn->auth.msecret = NULL;
	conn->authenticated = 0;
	conn->req_auth = 0;
	conn->req_mutual = 0;
	conn->inflight = 0;
	conn->sender_waiting = 0;
	istgt_queue_init(&conn->pending_pdus);
	conn->r2t_tasks = xmalloc((sizeof (conn->r2t_tasks))
		* (conn->max_r2t + 1));
	for (i = 0; i < (conn->max_r2t + 1); i++) {
		conn->r2t_tasks[i] = NULL;
	}
	conn->task_pipe[0] = -1;
	conn->task_pipe[1] = -1;
	conn->max_task_queue = MAX_LU_QUEUE_DEPTH;
	istgt_queue_init(&conn->task_queue);
	istgt_queue_init(&conn->result_queue);
	conn->exec_lu_task = NULL;
	conn->running_tasks = 0;

	// memset(conn->initiator_addr, 0, sizeof (conn->initiator_addr));
	// memset(conn->target_addr, 0, sizeof (conn->target_addr));

	switch (sa->sa_family) {
	case AF_INET6:
		conn->initiator_family = AF_INET6;
		rc = istgt_getaddr(sock, conn->target_addr,
			sizeof (conn->target_addr),
			conn->initiator_addr, sizeof (conn->initiator_addr), &conn->iaddr, &conn->iport);
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_getaddr() failed\n");
			goto error_return;
		}
		break;
	case AF_INET:
		conn->initiator_family = AF_INET;
		rc = istgt_getaddr(sock, conn->target_addr,
			sizeof (conn->target_addr),
			conn->initiator_addr, sizeof (conn->initiator_addr), &conn->iaddr, &conn->iport);
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_getaddr() failed\n");
			goto error_return;
		}
		break;
	default:
		ISTGT_ERRLOG("unsupported family\n");
		goto error_return;
	}
	// printf("sock=%d, addr=%s, peer=%s\n",
	//  sock, conn->target_addr,
	//  conn->initiator_addr);

	/* wildcard? */
	if (strcasecmp(conn->portal.host, "[::]") == 0
		|| strcasecmp(conn->portal.host, "[*]") == 0) {
		if (conn->initiator_family != AF_INET6) {
			ISTGT_ERRLOG("address family error\n");
			goto error_return;
		}
		snprintf(buf, sizeof (buf), "[%s]", conn->target_addr);
		xfree(conn->portal.host);
		conn->portal.host = xstrdup(buf);
	} else if (strcasecmp(conn->portal.host, "0.0.0.0") == 0
			   || strcasecmp(conn->portal.host, "*") == 0) {
		if (conn->initiator_family != AF_INET) {
			ISTGT_ERRLOG("address family error\n");
			goto error_return;
		}
		snprintf(buf, sizeof (buf), "%s", conn->target_addr);
		xfree(conn->portal.host);
		conn->portal.host = xstrdup(buf);
	}

	// memset(conn->initiator_name, 0, sizeof (conn->initiator_name));
	// memset(conn->target_name, 0, sizeof (conn->target_name));
	// memset(conn->initiator_port, 0, sizeof (conn->initiator_port));
	// memset(conn->target_port, 0, sizeof (conn->target_port));

	/* set timeout msec. */
	rc = istgt_set_recvtimeout(conn->sock, conn->timeout * 1000);
	if (rc != 0) {
		ISTGT_ERRLOG("istgt_set_recvtimeo() failed\n");
		goto error_return;
	}
	rc = istgt_set_sendtimeout(conn->sock, conn->timeout * 1000);
	if (rc != 0) {
		ISTGT_ERRLOG("istgt_set_sendtimeo() failed\n");
		goto error_return;
	}
#if defined(ISTGT_USE_IOVEC)
	/* set low water mark */
	rc = istgt_set_recvlowat(conn->sock, ISCSI_BHS_LEN);
	if (rc != 0) {
		ISTGT_ERRLOG("istgt_set_recvlowat() failed\n");
		goto error_return;
	}
#endif

	rc = pipe(conn->task_pipe);
	if (rc != 0) {
		ISTGT_ERRLOG("pipe() failed\n");
		conn->task_pipe[0] = -1;
		conn->task_pipe[1] = -1;
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->diskioflag_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->task_queue_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->result_queue_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_cond_init(&conn->result_queue_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->wpdu_mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_cond_init(&conn->wpdu_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->r2t_mutex, &istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_mutex_init(&conn->sender_mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	rc = pthread_cond_init(&conn->sender_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		goto error_return;
	}

	/* set default params */
	rc = istgt_iscsi_conn_params_init(&conn->params);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_conn_params_init() failed\n");
		goto error_return;
	}
	/* replace with config value */
	rc = istgt_iscsi_param_set_int(conn->params,
		"MaxRecvDataSegmentLength",
		conn->MaxRecvDataSegmentLength);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}

	if (conn->MaxRecvDataSegmentLength < 8192) {
		conn->recvbufsize = 8192;
		conn->sendbufsize = 8192;
	} else {
		conn->recvbufsize = conn->MaxRecvDataSegmentLength;
		conn->sendbufsize = conn->MaxRecvDataSegmentLength;
	}
	conn->recvbuf = xmalloc(conn->recvbufsize);
	conn->sendbuf = xmalloc(conn->sendbufsize);

	conn->worksize = 0;
	conn->workbuf = NULL;

	conn->StatSN = 0;
	conn->isid = 0;
	conn->tsih = 0;

	/* register global */
	rc = -1;
	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i < g_nconns; i++) {
		if (g_conns[i] == NULL) {
			g_conns[i] = conn;
			conn->id = i;
			if (i > g_max_connidx)
				g_max_connidx++;
			rc = 0;
			break;
		}
	}
	MTX_UNLOCK(&g_conns_mutex);
	if (rc < 0) {
		ISTGT_ERRLOG("no free conn slot available\n");
	error_return:
		if (conn->task_pipe[0] != -1)
			close(conn->task_pipe[0]);
		if (conn->task_pipe[1] != -1)
			close(conn->task_pipe[1]);
		istgt_iscsi_param_free(conn->params);
		istgt_queue_destroy(&conn->pending_pdus);
		istgt_queue_destroy(&conn->task_queue);
		istgt_queue_destroy(&conn->result_queue);
		xfree(conn->portal.label);
		xfree(conn->portal.host);
		xfree(conn->portal.port);
		xfree(conn->recvbuf);
		xfree(conn->sendbuf);
		xfree(conn);
		return (-1);
	}

	/* create new thread */
	rc = pthread_create(&conn->thread, &istgt->attr, &worker, (void *)conn);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create() failed\n");
		goto error_return;
	}
	snprintf(conn->thr, sizeof (conn->thr), "c#%d.%ld.%d", conn->id, (uint64_t)(((uint64_t *)(conn->thread))[0]), ntohs(conn->iport));
	rc = pthread_detach(conn->thread);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_detach() failed\n");
		goto error_return;
	}
	return (0);
}

int
istgt_create_sess(ISTGT_Ptr istgt, CONN_Ptr conn, ISTGT_LU_Ptr lu)
{
	SESS_Ptr sess;
	int rc;

	sess = xmalloc(sizeof (*sess));
	memset(sess, 0, sizeof (*sess));

	/* configuration values */
	MTX_LOCK(&istgt->mutex);
	if (lu != NULL) {
		MTX_LOCK(&lu->mutex);
	}
	sess->MaxConnections = istgt->MaxConnections;
	if (lu != NULL) {
		sess->MaxOutstandingR2T = lu->MaxOutstandingR2T;
	} else {
		sess->MaxOutstandingR2T = istgt->MaxOutstandingR2T;
	}
#if 0
	if (sess->MaxOutstandingR2T > conn->max_r2t) {
		if (conn->max_r2t > 0) {
			sess->MaxOutstandingR2T = conn->max_r2t;
		} else {
			sess->MaxOutstandingR2T = 1;
		}
	}
#else
	if (sess->MaxOutstandingR2T < 1) {
		sess->MaxOutstandingR2T = 1;
	}
	/* limit up to MaxOutstandingR2T */
	if (sess->MaxOutstandingR2T < conn->max_r2t) {
		conn->max_r2t = sess->MaxOutstandingR2T;
	}
#endif
	if (lu != NULL) {
		sess->DefaultTime2Wait = lu->DefaultTime2Wait;
		sess->DefaultTime2Retain = lu->DefaultTime2Retain;
		sess->FirstBurstLength = lu->FirstBurstLength;
		sess->MaxBurstLength = lu->MaxBurstLength;
		conn->MaxRecvDataSegmentLength
			= lu->MaxRecvDataSegmentLength;
		sess->InitialR2T = lu->InitialR2T;
		sess->ImmediateData = lu->ImmediateData;
		sess->DataPDUInOrder = lu->DataPDUInOrder;
		sess->DataSequenceInOrder = lu->DataSequenceInOrder;
		sess->ErrorRecoveryLevel = lu->ErrorRecoveryLevel;
	} else {
		sess->DefaultTime2Wait = istgt->DefaultTime2Wait;
		sess->DefaultTime2Retain = istgt->DefaultTime2Retain;
		sess->FirstBurstLength = istgt->FirstBurstLength;
		sess->MaxBurstLength = istgt->MaxBurstLength;
		conn->MaxRecvDataSegmentLength
			= istgt->MaxRecvDataSegmentLength;
		sess->InitialR2T = istgt->InitialR2T;
		sess->ImmediateData = istgt->ImmediateData;
		sess->DataPDUInOrder = istgt->DataPDUInOrder;
		sess->DataSequenceInOrder = istgt->DataSequenceInOrder;
		sess->ErrorRecoveryLevel = istgt->ErrorRecoveryLevel;
	}
	if (lu != NULL) {
		MTX_UNLOCK(&lu->mutex);
	}
	MTX_UNLOCK(&istgt->mutex);

	sess->initiator_port = xstrdup(conn->initiator_port);
	sess->target_name = xstrdup(conn->target_name);
	sess->tag = conn->portal.tag;

	sess->max_conns = sess->MaxConnections;
	sess->conns = xmalloc(sizeof (*sess->conns) * sess->max_conns);
	memset(sess->conns, 0, sizeof (*sess->conns) * sess->max_conns);
	sess->connections = 0;

	sess->conns[sess->connections] = conn;
	sess->connections++;

	sess->req_mcs_cond = 0;
	sess->params = NULL;
	sess->lu = NULL;
	sess->isid = 0;
	sess->tsih = 0;

	sess->initial_r2t = 0;
	sess->immediate_data = 0;

	rc = pthread_mutex_init(&sess->mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
	error_return:
		istgt_iscsi_param_free(sess->params);
		xfree(sess->initiator_port);
		xfree(sess->target_name);
		xfree(sess->conns);
		xfree(sess);
		conn->sess = NULL;
		return (-1);
	}
	rc = pthread_cond_init(&sess->mcs_cond, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("cond_init() failed\n");
		goto error_return;
	}

	/* set default params */
	rc = istgt_iscsi_sess_params_init(&sess->params);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_sess_params_init() failed\n");
		goto error_return;
	}
	/* replace with config value */
	rc = istgt_iscsi_param_set_int(sess->params,
		"MaxConnections",
		sess->MaxConnections);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"MaxOutstandingR2T",
		sess->MaxOutstandingR2T);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"DefaultTime2Wait",
		sess->DefaultTime2Wait);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"DefaultTime2Retain",
		sess->DefaultTime2Retain);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"FirstBurstLength",
		sess->FirstBurstLength);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"MaxBurstLength",
		sess->MaxBurstLength);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set(sess->params,
		"InitialR2T",
		sess->InitialR2T ? "Yes" : "No");
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set(sess->params,
		"ImmediateData",
		sess->ImmediateData ? "Yes" : "No");
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set(sess->params,
		"DataPDUInOrder",
		sess->DataPDUInOrder ? "Yes" : "No");
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set(sess->params,
		"DataSequenceInOrder",
		sess->DataSequenceInOrder ? "Yes" : "No");
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set() failed\n");
		goto error_return;
	}
	rc = istgt_iscsi_param_set_int(sess->params,
		"ErrorRecoveryLevel",
		sess->ErrorRecoveryLevel);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}

	/* realloc buffer */
	rc = istgt_iscsi_param_set_int(conn->params,
		"MaxRecvDataSegmentLength",
		conn->MaxRecvDataSegmentLength);
	if (rc < 0) {
		ISTGT_ERRLOG("iscsi_param_set_int() failed\n");
		goto error_return;
	}
	if (conn->MaxRecvDataSegmentLength != conn->recvbufsize) {
		xfree(conn->recvbuf);
		xfree(conn->sendbuf);
		if (conn->MaxRecvDataSegmentLength < 8192) {
			conn->recvbufsize = 8192;
			conn->sendbufsize = 8192;
		} else {
			conn->recvbufsize = conn->MaxRecvDataSegmentLength;
			conn->sendbufsize = conn->MaxRecvDataSegmentLength;
		}
		conn->recvbuf = xmalloc(conn->recvbufsize);
		conn->sendbuf = xmalloc(conn->sendbufsize);
	}

	/* sess for first connection of session */
	conn->sess = sess;
	return (0);
}

static int
istgt_append_sess(CONN_Ptr conn, uint64_t isid, uint16_t tsih, uint16_t cid)
{
	SESS_Ptr sess;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"append session: isid=%"PRIx64", tsih=%u, cid=%u\n",
		isid, tsih, cid);

	sess = NULL;
	rc = -1;
	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		if (g_conns[i] == NULL || g_conns[i]->sess == NULL)
			continue;
		sess = g_conns[i]->sess;
		MTX_LOCK(&sess->mutex);
		if (conn->portal.tag == sess->tag
			&& strcasecmp(conn->initiator_port, sess->initiator_port) == 0
			&& strcasecmp(conn->target_name, sess->target_name) == 0
			&& (isid == sess->isid && tsih == sess->tsih)) {
			/* match tag and initiator port and target */
			rc = 0;
			break;
		}
		MTX_UNLOCK(&sess->mutex);
	}
	if (rc < 0) {
		/* no match */
		MTX_UNLOCK(&g_conns_mutex);
		ISTGT_ERRLOG("no MCS session for isid=%"PRIx64", tsih=%d, cid=%d\n",
			isid, tsih, cid);
		return (-1);
	}
	/* sess is LOCK by loop */
	if (sess->connections >= sess->max_conns
		|| sess->connections >= sess->MaxConnections) {
		/* no slot for connection */
		MTX_UNLOCK(&sess->mutex);
		MTX_UNLOCK(&g_conns_mutex);
		ISTGT_ERRLOG("too many connections for isid=%"PRIx64
			", tsih=%d, cid=%d\n",
			isid, tsih, cid);
		return (-1);
	}
	printf("Connections(tsih %d): %d\n", sess->tsih, sess->connections);
	conn->sess = sess;
	sess->conns[sess->connections] = conn;
	sess->connections++;
	MTX_UNLOCK(&sess->mutex);
	MTX_UNLOCK(&g_conns_mutex);

	return (0);
}

static void
istgt_free_sess(SESS_Ptr sess)
{
	if (sess == NULL)
		return;
	(void) pthread_mutex_destroy(&sess->mutex);
	(void) pthread_cond_destroy(&sess->mcs_cond);
	istgt_iscsi_param_free(sess->params);
	xfree(sess->initiator_port);
	xfree(sess->target_name);
	xfree(sess->conns);
	xfree(sess);
}

void
istgt_close_conn(CONN_Ptr conn)
{
	if (conn == NULL)
		return;
	if (conn->task_pipe[0] != -1)
		close(conn->task_pipe[0]);
	if (conn->task_pipe[1] != -1)
		close(conn->task_pipe[1]);
	conn->task_pipe[0] = -1;
	conn->task_pipe[1] = -1;
	conn->closetime = time(NULL);
}

void
istgt_free_conn(CONN_Ptr conn)
{

	// if (conn == NULL)
	// 	return;
	// if (conn->task_pipe[0] != -1)
	// 	close(conn->task_pipe[0]);
	// if (conn->task_pipe[1] != -1)
	// 	close(conn->task_pipe[1]);

	(void) pthread_mutex_destroy(&conn->task_queue_mutex);
	(void) pthread_mutex_destroy(&conn->result_queue_mutex);
	(void) pthread_mutex_destroy(&conn->diskioflag_mutex);
	(void) pthread_cond_destroy(&conn->result_queue_cond);
	(void) pthread_mutex_destroy(&conn->wpdu_mutex);
	(void) pthread_cond_destroy(&conn->wpdu_cond);
	(void) pthread_mutex_destroy(&conn->r2t_mutex);
	(void) pthread_mutex_destroy(&conn->sender_mutex);
	(void) pthread_cond_destroy(&conn->sender_cond);
	istgt_iscsi_param_free(conn->params);
	istgt_queue_destroy(&conn->pending_pdus);
	istgt_queue_destroy(&conn->task_queue);
	istgt_queue_destroy(&conn->result_queue);
	xfree(conn->r2t_tasks);
	xfree(conn->portal.label);
	xfree(conn->portal.host);
	xfree(conn->portal.port);
	xfree(conn->auth.user);
	xfree(conn->auth.secret);
	xfree(conn->auth.muser);
	xfree(conn->auth.msecret);
	xfree(conn->recvbuf);
	xfree(conn->sendbuf);
	xfree(conn->workbuf);
	xfree(conn);
}

ISTGT_QUEUE closedconns;

static void
istgt_remove_conn(CONN_Ptr conn)
{
	SESS_Ptr sess;
	int clear = 0;
	ISTGT_LU_DISK *spec = NULL;
	int idx;
	int i, j;
	int delayedFree = 0;
	int sessConns = 0, ioPending = 0;
	int rc = 0;
	int lu_num;
	uint16_t tsih;

	if (conn->sess != NULL) {
		if (conn->sess->lu != NULL) {
			MTX_LOCK(&conn->sess->lu->mutex);
			conn->sess->lu->conns--;
			MTX_UNLOCK(&conn->sess->lu->mutex);
			clear = 1;
			spec = (ISTGT_LU_DISK *)conn->sess->lu->lun[0].spec;
		}
	}

	idx = -1;
	sess = conn->sess;
	conn->sess = NULL;
	if (sess == NULL) {
		istgt_close_conn(conn);
		istgt_free_conn(conn);
		return;
	}

	MTX_LOCK(&sess->mutex);
	for (i = 0; i < sess->connections; i++) {
		if (sess->conns[i] == conn) {
			idx = i;
			break;
		}
	}
	if (sess->connections < 1) {
		ISTGT_ERRLOG("zero connection\n");
		sess->connections = 0;
	} else {
		if (idx < 0) {
			ISTGT_ERRLOG("remove conn not found\n");
		} else {
			for (j = idx; j < sess->connections - 1; j++) {
				sess->conns[j] = sess->conns[j + 1];
			}
			sess->conns[sess->connections - 1] = NULL;
			sess->connections--;
		}
	}
	sessConns = sess->connections;
	MTX_UNLOCK(&sess->mutex);

	lu_num = (sess->lu == NULL)? 0 : sess->lu->num;
	tsih = sess->tsih;

	if (sessConns == 0) {
		if (clear == 1) {
			MTX_LOCK(&spec->pr_rsv_mutex);
			if (spec->spc2_reserved == 1 && clear_resv == 1) {
				if (strcmp(conn->initiator_port, spec->rsv_port) == 0) {
					/* release reservation by key */
					ISTGT_LOG("Clearing spc2 Reservations");
					xfree(spec->rsv_port);
					spec->rsv_port = NULL;
					spec->rsv_key = 0;
					spec->spc2_reserved = 0;
					spec->rsv_scope = 0;
					spec->rsv_type = 0;
					/* remove registrations */
					for (i = 0; i < spec->npr_keys; i++) {
						if (spec->pr_keys[i].registered_initiator_port == conn->initiator_port) {
							istgt_lu_disk_free_pr_key(&spec->pr_keys[i]);
							memset(&spec->pr_keys[i], 0, sizeof (spec->pr_keys[i]));
						}
					}
				}
			}
			MTX_UNLOCK(&spec->pr_rsv_mutex);
		}
		/* cleanup last connection */
		istgt_lu_free_tsih(sess->lu, sess->tsih, conn->initiator_port);
		rc = istgt_lu_remove_nexus(sess->lu, conn->initiator_port);
		if (rc < 0) {
			/* Ignore Error */
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Failed to remove the Nexus\n");
		}

		istgt_free_sess(sess);
	}
	MTX_LOCK(&conn->diskioflag_mutex);
	ioPending = conn->diskIoPending;
	if (conn->diskIoPending != 0) {
		conn->flagDelayedFree = 1;
		delayedFree = 1;
	} else {
		conn->flagDelayedFree = 0;
	}
	MTX_UNLOCK(&conn->diskioflag_mutex);

	ISTGT_NOTICELOG("remove_conn->initiator:%s(%s) Target: %s(%s LU%d) conn:%p:%d tsih:%d connections:%d %s IOPending=%d",
		conn->initiator_addr, conn->initiator_name, conn->target_addr, conn->target_name,
		lu_num, conn, conn->cid, tsih, sessConns,
		(delayedFree == 1) ? "delay_free" : "", ioPending);
	if (strcmp(conn->target_name, "dummy") && conn->exec_logout == 0)
		ioctl_call(conn, TYPE_CONNBRK);

	// if (delayedfree == 0)
	//	istgt_free_conn(conn);

	istgt_close_conn(conn);
	istgt_queue_enqueue(&closedconns, conn);
}

static int
istgt_iscsi_drop_all_conns(CONN_Ptr conn)
{
	CONN_Ptr xconn;
	int max_conns;
	int num;
	int rc;
	int i;

	MTX_LOCK(&conn->istgt->mutex);
	max_conns = conn->istgt->MaxConnections;
	MTX_UNLOCK(&conn->istgt->mutex);
	num = 0;
	MTX_LOCK(&g_conns_mutex);

	for (i = 0; i <= g_max_connidx; i++) {
		xconn = g_conns[i];
		if (xconn == NULL)
			continue;
		if (strcasecmp(conn->initiator_name, xconn->initiator_name) != 0) {
			continue;
		}
		if (strcasecmp(conn->target_name, xconn->target_name) == 0) {
			if (xconn->sess != NULL) {
				ISTGT_NOTICELOG("exiting conn by %s(%s), TSIH=%u, CID=%u\n",
					xconn->initiator_name,
					xconn->initiator_addr,
					xconn->sess->tsih, xconn->cid);
			} else {
				ISTGT_NOTICELOG("exiting conn by %s(%s), TSIH=xx, CID=%u\n",
					xconn->initiator_name,
					xconn->initiator_addr,
					xconn->cid);
			}
			xconn->state = CONN_STATE_EXITING;
			num++;
		}
	}

	if (num != 0) {
		istgt_yield();
		sleep(1);
		ISTGT_NOTICELOG("drop all %d connections %d %s by %s\n",
					num, conn->id, conn->target_name, conn->initiator_name);
	}

	if (num > max_conns + 1) {
		for (i = 0; i <= g_max_connidx; i++) {
			xconn = g_conns[i];
			if (xconn == NULL)
				continue;
			if (xconn == conn)
				continue;
			if (strcasecmp(conn->initiator_port, xconn->initiator_port) != 0) {
				continue;
			}
			if (strcasecmp(conn->target_name, xconn->target_name) == 0) {
				if (xconn->sess != NULL) {
					ISTGT_NOTICELOG("exiting conn by %s(%s), TSIH=%u, CID=%u\n",
						xconn->initiator_port,
						xconn->initiator_addr,
						xconn->sess->tsih, xconn->cid);
				} else {
					ISTGT_NOTICELOG("exiting conn by %s(%s), TSIH=xx, CID=%u\n",
						xconn->initiator_port,
						xconn->initiator_addr,
						xconn->cid);
				}
				rc = pthread_cancel(xconn->thread);
				if (rc != 0) {
					ISTGT_ERRLOG("pthread_cancel() failed rc=%d\n", rc);
				}
			}
		}
	}
	MTX_UNLOCK(&g_conns_mutex);
	return (0);
}

static int
istgt_iscsi_drop_old_conns(CONN_Ptr conn)
{
	CONN_Ptr xconn;
	int max_conns;
	int num;
	int rc;
	int i;

	MTX_LOCK(&conn->istgt->mutex);
	max_conns = conn->istgt->MaxConnections;
	MTX_UNLOCK(&conn->istgt->mutex);
	num = 0;

	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		xconn = g_conns[i];
		if (xconn == NULL)
			continue;
		if (xconn == conn)
			continue;
		if (strcasecmp(conn->initiator_port, xconn->initiator_port) != 0) {
			continue;
		}
		if (strcasecmp(conn->target_name, xconn->target_name) == 0) {
			if (xconn->sess != NULL) {
				printf("exiting conn by %s(%s), TSIH=%u, CID=%u\n",
					xconn->initiator_port,
					xconn->initiator_addr,
					xconn->sess->tsih, xconn->cid);
			} else {
				printf("exiting conn by %s(%s), TSIH=xx, CID=%u\n",
					xconn->initiator_port,
					xconn->initiator_addr,
					xconn->cid);
			}
			xconn->state = CONN_STATE_EXITING;
			num++;
		}
	}

	if (num != 0) {
		istgt_yield();
		sleep(1);
		ISTGT_NOTICELOG("drop all %d old connections %d %s by %s\n",
						num, conn->id, conn->target_name, conn->initiator_port);
	}

	if (num > max_conns + 1) {
		for (i = 0; i <= g_max_connidx; i++) {
			xconn = g_conns[i];
			if (xconn == NULL)
				continue;
			if (xconn == conn)
				continue;
			if (strcasecmp(conn->initiator_port, xconn->initiator_port) != 0) {
				continue;
			}
			if (strcasecmp(conn->target_name, xconn->target_name) == 0) {
				if (xconn->sess != NULL) {
					printf("exiting conn by %s(%s), TSIH=%u, CID=%u\n",
						xconn->initiator_port,
						xconn->initiator_addr,
						xconn->sess->tsih, xconn->cid);
				} else {
					printf("exiting conn by %s(%s), TSIH=xx, CID=%u\n",
						xconn->initiator_port,
						xconn->initiator_addr,
						xconn->cid);
				}
				rc = pthread_cancel(xconn->thread);
				if (rc != 0) {
					ISTGT_ERRLOG("pthread_cancel() failed rc=%d\n", rc);
				}
			}
		}
	}
	MTX_UNLOCK(&g_conns_mutex);
	return (0);
}

#ifdef __linux__
static void ioctl_call(CONN_Ptr conn __attribute__((__unused__)), enum iscsi_log log_type __attribute__((__unused__))) {}
#else
static void ioctl_call(CONN_Ptr conn, enum iscsi_log log_type)
{
		int fd;
		char ebuf[2048];
		char info[16];
		struct istgt_detail * idetail;

		switch (log_type) {
		case TYPE_LOGIN:
				snprintf(info, sizeof (info), "login");
				break;
		case TYPE_LOGOUT:
				snprintf(info, sizeof (info), "logout");
				break;
		case TYPE_CONNBRK:
				snprintf(info, sizeof (info), "logout_connbrk");
				break;
	default:
				snprintf(info, sizeof (info), "invalid");
				break;
		}

	// Send ioctl to kernel for logging purpose
		fd = open(ISCSI_SOCKET, O_WRONLY);
		if (fd == -1) {
				snprintf(ebuf, sizeof (ebuf),
						"fd opening failed: %s %s %s %s %s %s %s %s %s errno= %d",
						info, "Initiator IP:",
						conn->initiator_addr, "Name:", conn->initiator_name, "Target IP:",
						conn->target_addr, "Name:", conn->target_name, errno);
				ISTGT_NOTICELOG("%s", ebuf);
				return;
		}

	idetail = (struct istgt_detail *)malloc(sizeof (*idetail));
		if (idetail == (struct istgt_detail *)NULL) {
				snprintf(ebuf, sizeof (ebuf),
						"out of memory: %s %s %s %s %s %s %s %s %s errno= %d",
						info, "Initiator IP:",
						conn->initiator_addr, "Name:", conn->initiator_name, "Target IP:",
						conn->target_addr, "Name:", conn->target_name, errno);
				ISTGT_NOTICELOG("%s", ebuf);
				close(fd);
				return;
		}
		idetail->login_status = log_type;
		strncpy(idetail->initiator_addr, conn->initiator_addr, 64);
		strncpy(idetail->target_addr, conn->target_addr, 64);
		strncpy(idetail->initiator_name, conn->initiator_name, 256);
		strncpy(idetail->target_name, conn->target_name, 256);
		if (ioctl(fd, DIO_ISCSIWR, idetail) == -1) {
				snprintf(ebuf, sizeof (ebuf),
						"ioctl failed: %s %s %s %s %s %s %s %s %s errno= %d",
						info, "Initiator IP:",
						conn->initiator_addr, "Name:", conn->initiator_name, "Target IP:",
						conn->target_addr, "Name:", conn->target_name, errno);
				ISTGT_NOTICELOG("%s", ebuf);
		}
		free(idetail);
		close(fd);
}
#endif

void
istgt_lock_gconns(void)
{
	MTX_LOCK(&g_conns_mutex);
}

void
istgt_unlock_gconns(void)
{
	MTX_UNLOCK(&g_conns_mutex);
}

int
istgt_get_gnconns(void)
{
	return (g_nconns);
}

CONN_Ptr
istgt_get_gconn(int idx)
{
	if (idx >= g_nconns)
		return (NULL);
	return (g_conns[idx]);
}

int
istgt_get_active_conns(void)
{
	CONN_Ptr conn;
	int num = 0;
	int i;

	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		conn = g_conns[i];
		if (conn == NULL)
			continue;
		num++;
	}
	MTX_UNLOCK(&g_conns_mutex);
	return (num);
}

int
istgt_stop_conns(void)
{
	CONN_Ptr conn;
	char tmp[1];
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_stop_conns\n");
	tmp[0] = 'E';
	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		conn = g_conns[i];
		if (conn == NULL)
			continue;
		rc = write(conn->task_pipe[1], tmp, 1);
		if (rc < 0 || rc != 1) {
			ISTGT_ERRLOG("write() failed\n");
			/* ignore error */
		}
	}
	MTX_UNLOCK(&g_conns_mutex);
	return (0);
}

CONN_Ptr
istgt_find_conn(const char *initiator_port, const char *target_name, uint16_t tsih)
{
	CONN_Ptr conn;
	SESS_Ptr sess;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		"initiator_port=%s, target=%s, TSIH=%u",
		initiator_port, target_name, tsih);
	sess = NULL;
	rc = -1;
	// MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		conn = g_conns[i];
		if (conn == NULL || conn->sess == NULL)
			continue;
		sess = conn->sess;
		MTX_LOCK(&sess->mutex);
		if (strcasecmp(initiator_port, sess->initiator_port) == 0
			&& strcasecmp(target_name, sess->target_name) == 0
			&& (tsih == sess->tsih)) {
			/* match initiator port and target */
			rc = 0;
			break;
		}
		MTX_UNLOCK(&sess->mutex);
	}
	if (rc < 0) {
		// MTX_UNLOCK(&g_conns_mutex);
		return (NULL);
	}
	MTX_UNLOCK(&sess->mutex);
	// MTX_UNLOCK(&g_conns_mutex);
	return (conn);
}

int
istgt_iscsi_init(ISTGT_Ptr istgt)
{
	CF_SECTION *sp;
	int rc;
	int allocsize;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_iscsi_init\n");
	sp = istgt_find_cf_section(istgt->config, "Global");
	if (sp == NULL) {
		ISTGT_ERRLOG("find_cf_section failed()\n");
		return (-1);
	}

	rc = pthread_mutex_init(&g_conns_mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	rc = pthread_mutex_init(&g_last_tsih_mutex, NULL);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		return (-1);
	}
	g_max_connidx = -1;
	g_nconns = MAX_LOGICAL_UNIT * istgt->MaxSessions * istgt->MaxConnections;
	g_nconns += MAX_LOGICAL_UNIT * istgt->MaxConnections;
	// g_conns = xmalloc(sizeof (*g_conns * g_nconns));
	allocsize = ((sizeof (CONN_Ptr *)) * (g_nconns + 100));
	g_conns = xmalloc(allocsize);
	bzero(g_conns, allocsize);
	g_last_tsih = 0;

	return (0);
}

int
istgt_iscsi_shutdown(ISTGT_Ptr istgt __attribute__((__unused__)))
{
	CONN_Ptr conn;
	int retry = 10;
	int num;
	int rc;
	int i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_iscsi_shutdown\n");

	num = 0;
	MTX_LOCK(&g_conns_mutex);
	for (i = 0; i <= g_max_connidx; i++) {
		conn = g_conns[i];
		if (conn == NULL)
			continue;
		conn->state = CONN_STATE_EXITING;
		num++;
	}
	MTX_UNLOCK(&g_conns_mutex);

	if (num != 0) {
		/* check threads */
		while (retry > 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
				"check thread retry=%d\n",
				retry);
			sleep(1);
			num = 0;
			MTX_LOCK(&g_conns_mutex);
			for (i = 0; i <= g_max_connidx; i++) {
				conn = g_conns[i];
				if (conn == NULL)
					continue;
				num++;
			}
			MTX_UNLOCK(&g_conns_mutex);
			if (num == 0)
				break;
			retry--;
		}
	}

	rc = pthread_mutex_destroy(&g_last_tsih_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_destroy() failed\n");
		return (-1);
	}
	rc = pthread_mutex_destroy(&g_conns_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_destroy() failed\n");
		return (-1);
	}

	if (num == 0) {
		xfree(g_conns);
		g_conns = NULL;
	}

	return (0);
}
