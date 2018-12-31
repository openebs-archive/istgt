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

#ifndef ISTGT_ISCSI_H
#define	ISTGT_ISCSI_H

#include <stdint.h>
#include <pthread.h>
#include "istgt.h"
#include "istgt_iscsi_param.h"
#include "istgt_lu.h"
#include "istgt_queue.h"
#include <sys/uio.h>
/* Basic header segment length */
#define	ISCSI_BHS_LEN	48
#define	ISCSI_ALIGNMENT	4
/* support version - RFC3720(10.12.4) */
#define	ISCSI_VERSION	0x00

#define	ISCSI_ALIGN(SIZE) \
	(((SIZE) + (ISCSI_ALIGNMENT - 1)) & ~(ISCSI_ALIGNMENT - 1))

#define	ISCSI_TEXT_MAX_KEY_LEN 64
/* for authentication key (non encoded 1024bytes) RFC3720(5.1/11.1.4) */
#define	ISCSI_TEXT_MAX_VAL_LEN 8192

#define	SESS_MTX_LOCK(CONN) \
	do {								\
		int _rc_;						\
		if ((_rc_ = pthread_mutex_lock(&(CONN)->sess->mutex)) != 0) {	\
			ISTGT_ERRLOG("sess lock error:%d", _rc_);		\
			pthread_exit(NULL);				\
		}							\
	} while (0)
#define	SESS_MTX_UNLOCK(CONN) \
	do {								\
		int _rc_;						\
		if ((_rc_ = pthread_mutex_unlock(&(CONN)->sess->mutex)) != 0) {	\
			ISTGT_ERRLOG("sess unlock error:%d", _rc_);		\
			pthread_exit(NULL);				\
		}							\
	} while (0)

typedef enum {
	/* Initiator opcodes */
	ISCSI_OP_NOPOUT	=	0x00,
	ISCSI_OP_SCSI	=	0x01,
	ISCSI_OP_TASK	=	0x02,
	ISCSI_OP_LOGIN	=	0x03,
	ISCSI_OP_TEXT	=	0x04,
	ISCSI_OP_SCSI_DATAOUT	=	0x05,
	ISCSI_OP_LOGOUT	=	0x06,
	ISCSI_OP_SNACK	=	0x10,
	ISCSI_OP_VENDOR_1C	=	0x1c,
	ISCSI_OP_VENDOR_1D	=	0x1d,
	ISCSI_OP_VENDOR_1E	=	0x1e,

	/* Target opcodes */
	ISCSI_OP_NOPIN	=	0x20,
	ISCSI_OP_SCSI_RSP	=	0x21,
	ISCSI_OP_TASK_RSP	=	0x22,
	ISCSI_OP_LOGIN_RSP	=	0x23,
	ISCSI_OP_TEXT_RSP	=	0x24,
	ISCSI_OP_SCSI_DATAIN	=	0x25,
	ISCSI_OP_LOGOUT_RSP	=	0x26,
	ISCSI_OP_R2T	=	0x31,
	ISCSI_OP_ASYNC	=	0x32,
	ISCSI_OP_VENDOR_3C	=	0x3c,
	ISCSI_OP_VENDOR_3D	=	0x3d,
	ISCSI_OP_VENDOR_3E	=	0x3e,
	ISCSI_OP_REJECT	=	0x3f,
} ISCSI_OP;

typedef enum {
	ISCSI_TASK_FUNC_ABORT_TASK	=	1,
	ISCSI_TASK_FUNC_ABORT_TASK_SET	=	2,
	ISCSI_TASK_FUNC_CLEAR_ACA	=	3,
	ISCSI_TASK_FUNC_CLEAR_TASK_SET	=	4,
	ISCSI_TASK_FUNC_LOGICAL_UNIT_RESET	=	5,
	ISCSI_TASK_FUNC_TARGET_WARM_RESET	=	6,
	ISCSI_TASK_FUNC_TARGET_COLD_RESET	=	7,
	ISCSI_TASK_FUNC_TASK_REASSIGN	=	8,
} ISCSI_TASK_FUNC;


typedef struct iscsi_bhs_t {
	/* 0-3 */
	uint8_t opcode;
	uint8_t opcode_specific1[3];
	/* 4-7 */
	uint8_t total_ahs_len;
	uint8_t data_segment_len[3];
	/* 8-11 */
	union {
		uint8_t lun1[4];
		uint8_t opcode_specific2[4];
	} u1;
	/* 12-15 */
	union {
		uint8_t lun2[4];
		uint8_t opcode_specific3[4];
	} u2;
	/* 16-19 */
	uint8_t inititator_task_tag[4];
	/* 20-47 */
	uint8_t opcode_specific4[28];
} ISCSI_BHS;

typedef struct iscsi_ahs_t {
	/* 0-3 */
	uint8_t ahs_len[2];
	uint8_t ahs_type;
	uint8_t ahs_specific1;
	/* 4-x */
	uint8_t ahs_specific2[];
} ISCSI_AHS;

#define	ISCSI_DIGEST_LEN	4
typedef struct iscsi_pdu_t {
	ISCSI_BHS bhs;
	ISCSI_AHS *ahs;
	union {
		uint8_t header_digest[ISCSI_DIGEST_LEN];
		uint64_t header_digestX;
	};
	uint8_t *data;
	union {
		uint8_t data_digest[ISCSI_DIGEST_LEN];
		uint64_t data_digestX;
	};
	size_t total_ahs_len;
	size_t data_segment_len;
	uint8_t opcode;
	uint8_t scsi_op;
	uint16_t dummy;
	struct timespec start0;
	struct timespec start;
} ISCSI_PDU;
typedef ISCSI_PDU *ISCSI_PDU_Ptr;

typedef enum {
	CONN_STATE_INVALID	=	0,
	CONN_STATE_RUNNING	=	1,
	CONN_STATE_EXITING	=	2,
	CONN_STATE_SHUTDOWN	=	3,
} CONN_STATE;

typedef enum {
	ISCSI_LOGIN_PHASE_NONE	=	0,
	ISCSI_LOGIN_PHASE_START	=	1,
	ISCSI_LOGIN_PHASE_SECURITY	=	2,
	ISCSI_LOGIN_PHASE_OPERATIONAL	=	3,
	ISCSI_LOGIN_PHASE_FULLFEATURE	=	4,
} ISCSI_LOGIN_PHASE;

typedef enum {
	ISTGT_CHAP_PHASE_NONE	=	0,
	ISTGT_CHAP_PHASE_WAIT_A	=	1,
	ISTGT_CHAP_PHASE_WAIT_NR	=	2,
	ISTGT_CHAP_PHASE_END	=	3,
} ISTGT_CHAP_PHASE;

#define	ISTGT_CHAP_CHALLENGE_LEN	1024
typedef struct istgt_chap_auth_t {
	ISTGT_CHAP_PHASE chap_phase;

	char *user;
	char *secret;
	char *muser;
	char *msecret;

	uint8_t chap_id[1];
	uint8_t chap_mid[1];
	int chap_challenge_len;
	uint8_t chap_challenge[ISTGT_CHAP_CHALLENGE_LEN];
	int chap_mchallenge_len;
	uint8_t chap_mchallenge[ISTGT_CHAP_CHALLENGE_LEN];
} ISTGT_CHAP_AUTH;

typedef struct istgt_r2t_task_t {
	struct istgt_conn_t *conn;
	ISTGT_LU_Ptr lu;
	uint64_t lun;
	uint32_t CmdSN;
	uint32_t task_tag;
	uint32_t transfer_len;
	uint32_t transfer_tag;

	uint64_t iobufsize;
	int iobufindx;
	struct iovec iobuf[40];
	//int iobufoff[20]; int iobufsize[20]; uint8_t *iobuf[20];
	uint32_t R2TSN;
	uint32_t DataSN;
	int F_bit;
	int offset;
} ISTGT_R2T_TASK;
typedef ISTGT_R2T_TASK *ISTGT_R2T_TASK_Ptr;

typedef struct istgt_conn_t {
	int id;

	ISTGT_Ptr istgt;
	PORTAL portal;
	int sock;
	int wsock;
	int epfd;
//#ifdef ISTGT_USE_KQUEUE
//	int kq;
//#endif /* ISTGT_USE_KQUEUE */
	char thr[20];
	char sthr[20];
	pthread_t thread;
	pthread_t sender_thread;
	pthread_mutex_t sender_mutex;
	pthread_cond_t sender_cond;
	struct istgt_sess_t *sess;

	CONN_STATE state;
	int exec_logout;

	int max_pending;
	int queue_depth;
	pthread_mutex_t wpdu_mutex;
	pthread_cond_t wpdu_cond;
	ISTGT_QUEUE pending_pdus;
	ISCSI_PDU pdu;

	int max_r2t;
	int pending_r2t;
	pthread_mutex_t r2t_mutex;
	ISTGT_R2T_TASK_Ptr *r2t_tasks;

	int task_pipe[2];
	int max_task_queue;
	pthread_mutex_t task_queue_mutex;
	ISTGT_QUEUE task_queue;
	pthread_mutex_t result_queue_mutex;
	pthread_cond_t result_queue_cond;
	ISTGT_QUEUE result_queue;
	ISTGT_LU_TASK_Ptr exec_lu_task;
	int running_tasks;

	uint16_t cid;
	uint16_t iport;
	uint32_t iaddr;
	/* IP address */
	int initiator_family;
	char initiator_addr[MAX_INITIATOR_ADDR];
	char target_addr[MAX_TARGET_ADDR];

	int  diskIoPending;
	int  flagDelayedFree;
	pthread_mutex_t diskioflag_mutex;
	/* Initiator/Target port binds */
	char initiator_name[MAX_INITIATOR_NAME];
	char target_name[MAX_TARGET_NAME];
	char initiator_port[MAX_INITIATOR_NAME];
	char target_port[MAX_TARGET_NAME];
	uint8_t inn_len;
	uint8_t inp_len;
	uint8_t tn_len;
	uint8_t tp_len;
	/* for fast access */
	int header_digest;
	int data_digest;
	int full_feature;

	ISCSI_PARAM *params;
	ISCSI_LOGIN_PHASE login_phase;
	ISTGT_CHAP_AUTH auth;
	int authenticated;
	int req_auth;
	int req_mutual;

	int timeout;
	int nopininterval;

	int TargetMaxRecvDataSegmentLength;
	int MaxRecvDataSegmentLength;
	int MaxOutstandingR2T;
	int FirstBurstLength;
	int MaxBurstLength;

	int recvbufsize;
	int sendbufsize;
	uint8_t *recvbuf;
	uint8_t *sendbuf;

	int worksize;
	uint8_t *workbuf;

	uint32_t StatSN;

	uint64_t isid;
	uint16_t tsih;

	time_t closetime;
	int inflight;
	int sender_waiting;
} CONN;
typedef CONN *CONN_Ptr;

typedef struct istgt_sess_t {
	int connections;
	int max_conns;
	CONN_Ptr *conns;

	pthread_mutex_t mutex;
	pthread_cond_t mcs_cond;
	int req_mcs_cond;

	char *initiator_port;
	char *target_name;
	int tag;

	uint64_t isid;
	uint16_t tsih;
	ISTGT_LU_Ptr lu;

	ISCSI_PARAM *params;

	int MaxConnections;
	int MaxOutstandingR2T;
	int DefaultTime2Wait;
	int DefaultTime2Retain;
	int FirstBurstLength;
	int MaxBurstLength;
	int InitialR2T;
	int ImmediateData;
	int DataPDUInOrder;
	int DataSequenceInOrder;
	int ErrorRecoveryLevel;

	int initial_r2t;
	int immediate_data;

	uint32_t ExpCmdSN;
	uint32_t MaxCmdSN;
	uint32_t MaxCmdSN_local;
} SESS;
typedef SESS *SESS_Ptr;

int istgt_iscsi_send_async(CONN_Ptr conn);
void istgt_free_conn(CONN_Ptr conn);
void istgt_close_conn(CONN_Ptr conn);
void istgt_lu_disk_free_pr_key(ISTGT_LU_PR_KEY *prkey);

#endif /* ISTGT_ISCSI_H */
