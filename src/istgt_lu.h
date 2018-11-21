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

#ifndef ISTGT_LU_H
#define ISTGT_LU_H

#include <pthread.h>
#include <time.h>
#ifdef HAVE_UUID_H
#include <uuid.h>
#endif
#include "istgt.h"
#include "istgt_queue.h"

#ifdef	REPLICATION
#include "replication.h"
#include "ring_mempool.h"
#endif

#ifdef __linux__
#include <x86_64-linux-gnu/sys/queue.h>
#endif
#include <stdbool.h>
#ifdef __FreeBSD__
#include <sys/queue.h>
/* IOCTL to Write/Read persistent Reservation and Registration data to/from zvol ZAP attribute */
#define DIOCGWRRSV _IOW('d', 144, struct scsi_pr_data) /* Write pr data */
#define DIOCGRDRSV _IOR('d', 145, struct scsi_pr_data) /* Read pr data */
#endif

#include <sys/uio.h>
#define MAX_LU_LUN 64
#define MAX_LU_LUN_SLOT 8
#define MAX_LU_TSIH 256
#define MAX_LU_MAP 256
#define MAX_LU_SERIAL_STRING 32
#define MAX_LU_RESERVE 256
#define MAX_LU_ZAP_RESERVE 8
#define MAX_LU_RESERVE_IPT 256
#define MAX_LU_QUEUE_DEPTH 256

#define USE_LU_TAPE_DLT8000

#define DEFAULT_LU_BLOCKLEN 512
#define DEFAULT_LU_BLOCKLEN_DISK DEFAULT_LU_BLOCKLEN
#define DEFAULT_LU_BLOCKLEN_DVD 2048
#define DEFAULT_LU_BLOCKLEN_TAPE DEFAULT_LU_BLOCKLEN
#define DEFAULT_LU_QUEUE_DEPTH 32
#define DEFAULT_LU_ROTATIONRATE 7200	/* 7200 rpm */
#define DEFAULT_LU_FORMFACTOR 0x02	/* 3.5 inch */

#if defined (__linux__)
#define DEFAULT_LU_VENDOR "linux"
#elif defined (__FreeBSD__)
#define DEFAULT_LU_VENDOR "FreeBSD"
#elif defined (__NetBSD__)
#define DEFAULT_LU_VENDOR "NetBSD"
#elif defined (__OpenBSD__)
#define DEFAULT_LU_VENDOR "OpenBSD"
#else
#define DEFAULT_LU_VENDOR "FreeBSD"
#endif

#define DEFAULT_LU_VENDOR_DISK DEFAULT_LU_VENDOR
#define DEFAULT_LU_VENDOR_DVD  DEFAULT_LU_VENDOR
#ifndef USE_LU_TAPE_DLT8000
#define DEFAULT_LU_VENDOR_TAPE DEFAULT_LU_VENDOR
#else
#define DEFAULT_LU_VENDOR_TAPE "QUANTUM"
#endif /* !USE_LU_TAPE_DLT8000 */
#define DEFAULT_LU_PRODUCT      "iSCSI UNIT"
#define DEFAULT_LU_PRODUCT_DISK "iSCSI DISK"
#define DEFAULT_LU_PRODUCT_DVD  "iSCSI DVD"
#ifndef USE_LU_TAPE_DLT8000
#define DEFAULT_LU_PRODUCT_TAPE "iSCSI TAPE"
#else
#define DEFAULT_LU_PRODUCT_TAPE "DLT8000"
#endif /* !USE_LU_TAPE_DLT8000 */
#define DEFAULT_LU_REVISION "0001"
#define DEFAULT_LU_REVISION_DISK DEFAULT_LU_REVISION
#define DEFAULT_LU_REVISION_DVD  DEFAULT_LU_REVISION
#ifndef USE_LU_TAPE_DLT8000
#define DEFAULT_LU_REVISION_TAPE DEFAULT_LU_REVISION
#else
#define DEFAULT_LU_REVISION_TAPE "C001"
#endif /* !USE_LU_TAPE_DLT8000 */
#define MAX_INQUIRY_SERIAL 16

#define ISTGT_LU_WORK_BLOCK_SIZE (1ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_WORK_ATS_BLOCK_SIZE (1ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_MAX_WRITE_CACHE_SIZE (8ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_MEDIA_SIZE_MIN (1ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_MEDIA_EXTEND_UNIT (256ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_1GB (1ULL * 1024ULL * 1024ULL * 1024ULL)
#define ISTGT_LU_1MB (1ULL * 1024ULL * 1024ULL)

/*
 * CloudByte - additional definitions for multiple luworkers
 */
#define ISTGT_MAX_NUM_LUWORKERS 128
#define ISTGT_NUM_LUWORKERS_DEFAULT 6

typedef enum {
	ISTGT_LU_FLAG_MEDIA_READONLY = 0x00000001,
	ISTGT_LU_FLAG_MEDIA_AUTOSIZE = 0x00000002,
	ISTGT_LU_FLAG_MEDIA_EXTEND   = 0x00000010,
	ISTGT_LU_FLAG_MEDIA_DYNAMIC  = 0x00000020,
} ISTGT_LU_FLAG;

/* 
 * Value for max retry attempts by luworkers that get spawned,
 * esp for reload (kill -HUP) case to detect lu state in 
 * RUNNING
 */
#define ISTGT_MAX_LU_RUNNING_STATE_RETRY_COUNT 45

typedef enum {
	ISTGT_LU_TYPE_NONE = 0,
	ISTGT_LU_TYPE_PASS = 1,
	ISTGT_LU_TYPE_DISK = 2,
	ISTGT_LU_TYPE_DVD = 3,
	ISTGT_LU_TYPE_TAPE = 4,
} ISTGT_LU_TYPE;

typedef enum {
	ISTGT_LU_LUN_TYPE_NONE = 0,
	ISTGT_LU_LUN_TYPE_DEVICE = 1,
	ISTGT_LU_LUN_TYPE_STORAGE = 2,
	ISTGT_LU_LUN_TYPE_REMOVABLE = 3,
	ISTGT_LU_LUN_TYPE_SLOT = 4,
} ISTGT_LU_LUN_TYPE;

typedef struct istgt_lu_device_t {
	char *file;
} ISTGT_LU_DEVICE;

typedef struct istgt_lu_storage_t {
	int fd;
	char *file;
	uint64_t size;
	uint32_t rsize;
	uint32_t rshift;
	uint32_t rshiftreal;
} ISTGT_LU_STORAGE;

typedef struct istgt_lu_removable_t {
	int type;
	int id;
	int flags;
	int fd;
	char *file;
	uint64_t size;
} ISTGT_LU_REMOVABLE;

typedef struct istgt_lu_slot_t {
	int maxslot;
	int present[MAX_LU_LUN_SLOT];
	int flags[MAX_LU_LUN_SLOT];
	char *file[MAX_LU_LUN_SLOT];
	uint64_t size[MAX_LU_LUN_SLOT];
} ISTGT_LU_SLOT;

typedef struct istgt_lu_lun_t {
	int type;
	union {
		ISTGT_LU_DEVICE device;
		ISTGT_LU_STORAGE storage;
		ISTGT_LU_REMOVABLE removable;
		ISTGT_LU_SLOT slot;
	} u;
	int rotationrate;
	int formfactor;
	int opt_tlen;
	union {
		struct {
			int readcache : 1;
			int writecache : 1;
			int unmap : 1;
			int ats : 1;
			int xcopy: 1;
			int wsame : 1;
			int dpofua : 1;
			int wzero : 1;
		};
		int lunflags;
	};
	char *serial;
	void *spec;
} ISTGT_LU_LUN;
typedef ISTGT_LU_LUN *ISTGT_LU_LUN_Ptr;

typedef struct istgt_lu_tsih_t {
	int tag;
	uint16_t tsih;
	char *initiator_port;
} ISTGT_LU_TSIH;

typedef enum {
	AAS_ACTIVE_OPTIMIZED = 0x00,
	AAS_ACTIVE_NON_OPTIMIZED = 0x01,
	AAS_STANDBY = 0x02,
	AAS_UNAVAILABLE = 0x03,
	AAS_TRANSITIONING = 0x0F,

	AAS_STATUS_NO = 0x0000,
	AAS_STATUS_STPG = 0x0100,
	AAS_STATUS_IMPLICIT = 0x0200,
} ISTGT_LU_AAS;

typedef struct istgt_lu_map_t {
	int pg_tag;
	int pg_aas;
	int ig_tag;
} ISTGT_LU_MAP;

typedef enum {
	ISTGT_UA_NONE			=	0X0000,
	ISTGT_UA_POWERON		=	0X0001,
	ISTGT_UA_BUS_RESET		=	0X0002,
	ISTGT_UA_TARG_RESET		=	0X0004,
	ISTGT_UA_LUN_RESET		=	0X0008,
	ISTGT_UA_LUN_CHANGE		=	0X0010,
	ISTGT_UA_MODE_CHANGE		=	0X0020,
	ISTGT_UA_LOG_CHANGE		=	0X0040,
	ISTGT_UA_LVD			=	0X0080,
	ISTGT_UA_SE			=	0X0100,
	ISTGT_UA_RES_PREEMPT		=	0X0200,
	ISTGT_UA_RES_RELEASE		=	0X0400,
	ISTGT_UA_REG_PREEMPT		=	0X0800,
	ISTGT_UA_ASYM_ACC_CHANGE	=	0X1000,
	ISTGT_UA_CAPACITY_CHANGED	=	0X2000
} istgt_ua_type;

typedef enum {
	ISTGT_RSV_NONE 	=	0X0000,
	ISTGT_RSV_READ		=	0X0001,
} istgt_rsv_pending;

typedef struct istgt_lu_disk_nexus {
	istgt_ua_type ua_pending;
	pthread_mutex_t nexus_mutex;
	const char *initiator_port;
	TAILQ_ENTRY (istgt_lu_disk_nexus) nexus_next;
}IT_NEXUS;

typedef struct istgt_lu_t {
	int num;
	char *name;
	char *volname;
	char *alias;

	char *inq_vendor;
	char *inq_product;
	char *inq_revision;
	char *inq_serial;

	ISTGT_Ptr istgt;
	ISTGT_STATE state;
	pthread_mutex_t mutex;
	pthread_mutex_t state_mutex;
	pthread_mutex_t queue_mutex;
	pthread_cond_t queue_cond;
	pthread_t luthread[ISTGT_MAX_NUM_LUWORKERS];
	pthread_t schdler_thread;
	pthread_t maintenance_thread;
	int		luworkers;
	int		luworkersActive;
	uint16_t last_tsih;

	int no_auth_chap;
	int auth_chap;
	int auth_chap_mutual;
	int auth_group;
	int header_digest;
	int data_digest;

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

	int type;
	int online;
	int readonly;
	int blocklen;
	int recordsize;
	int rshift;
	int queue_depth;
	int limit_q_size;
	int queue_check;

	int maxlun;
	ISTGT_LU_LUN lun[MAX_LU_LUN];
	int maxtsih;
	ISTGT_LU_TSIH tsih[MAX_LU_TSIH];
	int maxmap;
	ISTGT_LU_MAP map[MAX_LU_MAP];
	int conns;
#ifdef REPLICATION
	uint8_t replication_factor;
	uint8_t consistency_factor;
#endif
} ISTGT_LU;
typedef ISTGT_LU *ISTGT_LU_Ptr;

typedef enum {
	ISTGT_TAG_UNTAGGED,
	ISTGT_TAG_SIMPLE,
	ISTGT_TAG_ORDERED,
	ISTGT_TAG_HEAD_OF_QUEUE,
	ISTGT_TAG_ACA
} istgt_tag_type;

#define _PSZ 16

enum {
ISTGT_ADD_TO_MAINT            = 0x00000001,
ISTGT_ADD_TO_CMD              = 0x00000002,
ISTGT_ADD_TO_BLOCKED_CMD      = 0x00000004,
ISTGT_ADD_TO_BLOCKED_MAINT    = 0x00000008,
ISTGT_UNBLOCKED               = 0x00000010,
ISTGT_MAINT_WORKER_PICKED     = 0x00000020,
ISTGT_SCHEDULED               = 0x00000040,
ISTGT_WORKER_PICKED           = 0x00000080,
ISTGT_DISK_EXEC               = 0x00000100,
ISTGT_COMPLETED_EXEC          = 0x00000200,
ISTGT_RESULT_Q_ENQUEUED       = 0x00000400,
ISTGT_RESULT_Q_DEQUEUED       = 0x00000800
};

typedef struct istgt_lu_cmd_t {

	struct iscsi_pdu_t *pdu;
	ISTGT_LU_Ptr lu;

	uint64_t lba;
	uint32_t lblen;
	uint8_t  cdb0;
	uint8_t  infdx;
	union {
		struct {
			uint16_t dpo : 1;
			uint16_t fua : 1;
			uint16_t fua_nv : 1;
			uint16_t bytchk : 1;
			uint16_t anchor : 1;
			uint16_t unmap  : 1;
			uint16_t pbdata : 1;
			uint16_t lbdata : 1;
			uint16_t sync_nv: 1;
			uint16_t immed  : 1;
			uint16_t wprot  : 3;
		};
		uint16_t cdbflags;
	};
	union {
	char      info[8];
	uint64_t  infcpy;
	};

	int I_bit;
	int F_bit;
	int R_bit;
	int W_bit;
	istgt_tag_type  Attr_bit;
	uint64_t lun;
	uint32_t task_tag;
	uint32_t flags;
	uint32_t transfer_len;
	uint32_t CmdSN;
	uint8_t *cdb;

	uint64_t iobufsize;
	int iobufindx;
	struct iovec iobuf[40];
	//int iobufoff[20]; int iobufsize[20]; uint8_t *iobuf[20];

	uint8_t *data;
	size_t data_len;
	size_t alloc_len;

	int status;
	uint8_t *sense_data;
	size_t sense_data_len;
	size_t sense_alloc_len;

	int        lunum;

	struct timespec times[_PSZ];
	struct timespec tdiff[_PSZ];
	char       caller[_PSZ];
	uint16_t   line[_PSZ];
	uint8_t    _andx;
	uint8_t    _lst;
	uint8_t    _roll_cnt;
	uint8_t    connGone;
	uint8_t    aborted;
	uint8_t	   release_aborted;
#ifdef REPLICATION
	uint32_t   luworkerindx;
	struct timespec start_rw_time;
#endif
} ISTGT_LU_CMD;
typedef ISTGT_LU_CMD *ISTGT_LU_CMD_Ptr;

void timediff(ISTGT_LU_CMD_Ptr p, char  ch, uint16_t line);

typedef enum {
	__TUR = 0,	// "TEST_UNIT_READY"
	__RQSNS,	// "REQ_SENSE"
	__FMT,		// "FMT_UNIT"
	__RBL,		// "READ_BLOCK_LIMITS"
	__RD6,		// "READ6"
	__WR6,		// "WRITE6"
	__INQ,		// "INQUIRY"
	__MSEL,		// "MODE_SELECT"
	__RSV6,		// "RESERVE6"
	__REL6,		// "RELEASE6"
	__MSN6,		// "MODE SENSE6"
	__START,	// "START STOP UNIT"
	__RCAP10,	// "READCAP10"
	__RD10,		// "READ10"
	__WR10,		// "WRITE10"
	__WV10,		// "WRITE_AND_VERIFY10"
	__PREFTCH10,// "PREFETCH10"
	__SYC10,	// "SYNC_CACHE10"
	__RDDEF,	// "READ_DEFECT_DATA"
	__WBF,		// "WRITE_BUFFER"
	__WS10,		// "WRITE_SAME10"
	__UNMAP,	// "UNMAP"
	__XDW,		// "XDxx"
	__MSEL10,	// "MODE_SELECT10"
	__RSV10,	// "RESERVE10"
	__REL10,	// "RELEASE10"
	__MSN10,	// "MODE_SENSE10"
	__PRIN,		// "PR_IN"
	__PROUT,	// "PR_OUT"
	__XCPY,		// "XCOPY"
	__RCVCPY,	// "RECV_COPY_RESULTS"
	__RD16,		// "READ16"
	__ATS,		// "ATS"
	__WR16,		// "WRITE16"
	__WV16,		// "WRITE_AND_VERIFY16"
	__SYC16,	// "SYNC_CACHE16"
	__WS16,		// "WRITE_SAME16"
	__RDCAP,	// "RDCAP_SAIN16"
	__RLUN,		// "REPORT LUNS"
	__M_IN,		// "MAINTENANCE_IN"
	__RD12,		// "READ12"
	__WR12,		// "WRITE12"
	__WRV12,	// "WRITE_VERIFY12"
	__ISMx,		// "ISMXX"
	SCSI_ARYSZ,
	__UNK = SCSI_ARYSZ
} scsi_stat_idx;

typedef enum {
	NOPOUT = 0, SCSI, TASK, LOGIN, TXT, DATAOUT, LOGOUT, SNACK, VEN_1C, VEN_1D, VEN_1E,
	NOPIN, SCSI_RSP, TASK_RSP, LOGIN_RSP, TXT_RSP, DATAIN, LOGOUT_RSP, R2T, ASYNC, VEN_3C, VEN_3D, VEN_3E, REJECT,
	ISCSI_ARYSZ,
	UNDEF = ISCSI_ARYSZ
} isciOpInd;

typedef struct _istat {
	uint8_t opcode;
	uint32_t pdu_read;
	uint32_t pdu_sent;
} _verb_istat;

typedef struct _stat {
	uint8_t opcode;
	uint32_t req_start;
	uint32_t req_finish;
	//uint32_t req_inflight;
	//uint64_t req_avgtime;
} _verb_stat;

typedef enum {
	ISTGT_SERIDX_TUR = 0,
	ISTGT_SERIDX_READ,
	ISTGT_SERIDX_WRITE,
	ISTGT_SERIDX_UNMAP,
	ISTGT_SERIDX_MD_SNS,
	ISTGT_SERIDX_MD_SEL,
	ISTGT_SERIDX_RQ_SNS,
	ISTGT_SERIDX_INQ,
	ISTGT_SERIDX_RD_CAP,
	ISTGT_SERIDX_RESV,
	ISTGT_SERIDX_REL,
	ISTGT_SERIDX_LOG_SNS,
	ISTGT_SERIDX_FORMAT,
	ISTGT_SERIDX_START,
	ISTGT_SERIDX_PRES_IN,
	ISTGT_SERIDX_PRES_OUT,
	ISTGT_SERIDX_MAIN_IN,
	ISTGT_SERIDX_COUNT,
	ISTGT_SERIDX_INVLD = ISTGT_SERIDX_COUNT
} istgt_seridx;

typedef enum {
	ISTGT_CMD_FLAG_NONE               = 0x0000,
	ISTGT_CMD_FLAG_NO_SENSE           = 0x0010,
	ISTGT_CMD_FLAG_OK_ON_ALL_LUNS     = 0x0020,
	ISTGT_CMD_FLAG_ALLOW_ON_RESV      = 0x0040,
	ISTGT_CMD_FLAG_OK_ON_PROC         = 0x0100,
	ISTGT_CMD_FLAG_OK_ON_SLUN         = 0x0200,
	ISTGT_CMD_FLAG_OK_ON_BOTH         = 0x0300,
	ISTGT_CMD_FLAG_OK_ON_STOPPED      = 0x0400,
	ISTGT_CMD_FLAG_OK_ON_INOPERABLE   = 0x0800,
	ISTGT_CMD_FLAG_OK_ON_OFFLINE      = 0x1000,
	ISTGT_CMD_FLAG_OK_ON_SECONDARY    = 0x2000,
	ISTGT_CMD_FLAG_ALLOW_ON_PR_RESV   = 0x4000
} istgt_cmd_flags;

/* This may not be needed for control but looking at ctl
 * operational state we will decide on if we need to use this
 * or not
 */

typedef enum {
	ISTGT_FLAG_NONE           = 0x00000000,   /* no flags */
	ISTGT_FLAG_DATA_IN        = 0x00000001,   /* DATA IN */
	ISTGT_FLAG_DATA_OUT       = 0x00000002,   /* DATA OUT */
	ISTGT_FLAG_DATA_NONE      = 0x00000003,   /* no data */
	ISTGT_FLAG_DATA_MASK      = 0x00000003,
	ISTGT_FLAG_KDPTR_SGLIST   = 0x00000008,   /* kern_data_ptr is S/G list*/
	ISTGT_FLAG_EDPTR_SGLIST   = 0x00000010,   /* ext_data_ptr is S/G list */
	ISTGT_FLAG_DO_AUTOSENSE   = 0x00000020,   /* grab sense info */
	ISTGT_FLAG_USER_REQ       = 0x00000040,   /* request came from userland */
	ISTGT_FLAG_CONTROL_DEV    = 0x00000080,   /* processor device */
	ISTGT_FLAG_ALLOCATED      = 0x00000100,   /* data space allocated */
	ISTGT_FLAG_BLOCKED        = 0x00000200,   /* on the blocked queue */
	ISTGT_FLAG_ABORT          = 0x00000800,   /* this I/O should be aborted */
	ISTGT_FLAG_DMA_INPROG     = 0x00001000,   /* DMA in progress */
	ISTGT_FLAG_NO_DATASYNC    = 0x00002000,   /* don't cache flush data */
	ISTGT_FLAG_DELAY_DONE     = 0x00004000,   /* delay injection done */
	ISTGT_FLAG_INT_COPY       = 0x00008000,   /* internal copy, no done call*/
	ISTGT_FLAG_SENT_2OTHER_SC = 0x00010000,
	ISTGT_FLAG_FROM_OTHER_SC  = 0x00020000,
	ISTGT_FLAG_IS_WAS_ON_RTR  = 0x00040000,   /* Don't rerun cmd on failover*/
	ISTGT_FLAG_BUS_ADDR       = 0x00080000,   /* ctl_sglist contains BUS
						   addresses, not virtual ones*/
	ISTGT_FLAG_IO_CONT        = 0x00100000,   /* Continue I/O instead of
						   completing */
	ISTGT_FLAG_AUTO_MIRROR    = 0x00200000,   /* Automatically use memory
						   from the RC cache mirrored
						   address area. */
#if 0
	ISTGT_FLAG_ALREADY_DONE   = 0x00200000    /* I/O already completed */
#endif
	ISTGT_FLAG_NO_DATAMOVE    = 0x00400000,
	ISTGT_FLAG_DMA_QUEUED     = 0x00800000,   /* DMA queued but not started*/
	ISTGT_FLAG_STATUS_QUEUED  = 0x01000000,   /* Status queued but not sent*/

	ISTGT_FLAG_REDIR_DONE     = 0x02000000,   /* Redirection has already
						   been done. */
	ISTGT_FLAG_FAILOVER       = 0x04000000,   /* Killed by a failover */
	ISTGT_FLAG_IO_ACTIVE      = 0x08000000,   /* I/O active on this SC */
	ISTGT_FLAG_RDMA_MASK      = ISTGT_FLAG_NO_DATASYNC | ISTGT_FLAG_BUS_ADDR |
		ISTGT_FLAG_AUTO_MIRROR | ISTGT_FLAG_REDIR_DONE
		/* Flags we care about for
		   remote DMA */
} istgt_io_flags;

struct istgt_cmd_entry {
	//istgt_opfunc	*execute; /* NOT inuse */
	istgt_seridx	seridx;
	uint64_t	flags;  /* istgt_io_flags + istgt cmd flags */
	scsi_stat_idx   statidx;
//	istgt_lun_error_pattern	pattern;
};

enum {
	ISTGT_LU_TASK_RESULT_IMMEDIATE = 0,
	ISTGT_LU_TASK_RESULT_QUEUE_OK = 1,
	ISTGT_LU_TASK_RESULT_QUEUE_FULL = 2,
} ISTGT_LU_TASK_RESULT;

enum {
	ISTGT_LU_TASK_RESPONSE = 0,
	ISTGT_LU_TASK_REQPDU = 1,
	ISTGT_LU_TASK_REQUPDPDU = 2,
} ISTGT_LU_TASK_TYPE;

typedef enum {
	ISTGT_SER_BLOCK,
	ISTGT_SER_EXTENT,
	ISTGT_SER_PASS,
	ISTGT_SER_SKIP
} istgt_serialize_action;

typedef enum {
	ISTGT_TASK_BLOCK,
	ISTGT_TASK_OVERLAP,
	ISTGT_TASK_OVERLAP_TAG,
	ISTGT_TASK_BLOCK_SUSPECT,
	ISTGT_TASK_PASS,
	ISTGT_TASK_SKIP,
	ISTGT_TASK_ERROR
} istgt_task_action;

typedef struct istgt_lu_task_t {
	uint16_t type;
	uint16_t  cdb0;
	uint32_t lblen;
	uint64_t lba;
	uint64_t lbE;

	uint16_t in_plen;
	char     in_port[MAX_INITIATOR_NAME];

	struct istgt_conn_t *conn;
	ISTGT_LU_CMD lu_cmd;
	int lun;
	pthread_t thread;
	int use_cond;
	pthread_mutex_t trans_mutex;
	pthread_cond_t trans_cond;
	pthread_cond_t exec_cond;

	int condwait;

	int dup_iobuf;
	size_t alloc_len;

	int offset;
	int req_execute;
	int req_transfer_out;
	int error;
	int abort;
	int execute;
	int complete;
	int lock;
	void *complete_queue_ptr;//Pointer to the task in Complete queue
	ISTGT_QUEUE_Ptr blocked_by;// Pointer to the last task in complete queue blocking the current task

	int flags;
} ISTGT_LU_TASK;
typedef ISTGT_LU_TASK *ISTGT_LU_TASK_Ptr;

/* lu_disk.c */
typedef struct istgt_lu_pr_key_t {
	uint64_t key;

	/* transport IDs */
	char *registered_initiator_port;
	char *registered_target_port;
	/* PERSISTENT RESERVE OUT received from */
	int pg_idx; /* relative target port */
	int pg_tag; /* target port group */

	int ninitiator_ports;
	char **initiator_ports;
	int all_tpg;
} ISTGT_LU_PR_KEY;

typedef enum {
	ISTGT_LUN_ONLINE,
	ISTGT_LUN_BUSY
}istgt_lun_state;

typedef enum {
	ISTGT_LUN_OPEN_PENDING,  /* ask one worker to start opening the file */
	ISTGT_LUN_OPEN_INPROGRESS,  /* one worker is in the process of opening */
	ISTGT_LUN_OPEN,
	ISTGT_LUN_CLOSE_PENDING, /* one worker has to pick and  start the spec sync and close */
	ISTGT_LUN_CLOSE_INPROGRESS, /*one worker has already started the close */
	ISTGT_LUN_CLOSE,
	ISTGT_LUN_NOTYET
}istgt_lun_ex_state;

struct timeavgs
{
	unsigned int count;
	unsigned long tot_sec;
	unsigned long tot_nsec;
};

struct IO_info
{
	struct timespec total_time;
	struct timespec tdiff[_PSZ];
	char       caller[_PSZ];
	uint8_t    _andx;
	uint64_t lba;
	uint64_t lblen;
};

struct IO_types
{
	struct IO_info write;
	struct IO_info read;
	struct IO_info cmp_n_write;
	struct IO_info unmp;
	struct IO_info write_same;
};

struct istgt_lu_disk_t;

struct luworker_exit_args {
	struct istgt_lu_disk_t *spec;
	int workerid;
	ISTGT_LU_TASK_Ptr lu_task;
};

#define HOLD_SCHEDULER		0x1
#define WRITE_INFLIGHT_ISCSI	0x2
#define WRITE_INFLIGHT_ONDISK	0x4
#define READ_INFLIGHT_ISCSI	0x8
#define READ_INFLIGHT_ONDISK	0x10
#define R2T_TASK_BEFORE_WAIT	0x20
#define R2T_TASK_AFTER_WAIT	0x40
#define SEND_R2T		0x80
#define SEND_TASK_RSP		0x100

typedef struct istgt_lu_disk_t {
	ISTGT_LU_Ptr lu;
	int num;
	int lun;
	int inflight;
	int persist;
#ifdef	REPLICATION
	char *volname;
#endif
	int fd;
	const char *file;
	const char *disktype;
	void *exspec;
	uint64_t fsize;
	uint64_t foffset;
	uint64_t size;
	uint64_t blocklen;
	uint64_t blockcnt;
	uint32_t rsize;
	uint32_t rshift;
	uint32_t rshiftreal;

#ifdef	REPLICATION
	/* inflight write IOs at spec layer */
	uint64_t inflight_write_io_cnt;
	/* inflight read IOs at spec layer*/
	uint64_t inflight_read_io_cnt;
	/* inflight sync IOs at spec layer */
	uint64_t inflight_sync_io_cnt;
#endif

	uint32_t max_unmap_sectors;
	struct IO_types IO_size[10];
#ifdef REPLICATION
	uint64_t writes;
	uint64_t reads;
	uint64_t readbytes;
	uint64_t writebytes;
	uint64_t totalreadtime;
	uint64_t totalwritetime;
	uint64_t totalreadblockcount;
	uint64_t totalwriteblockcount;
#endif
	/* modify lun */
	int dofake;
	pthread_t diskmod_thr;

#ifdef HAVE_UUID_H
	uuid_t uuid;
#endif /* HAVE_UUID_H */

	uint32_t lb_per_rec;
	uint16_t opt_tlen;

	/* thin provisioning */
	union {
		struct {
			uint16_t readcache : 1;
			uint16_t writecache : 1;
			uint16_t unmap : 1;
			uint16_t ats : 1;
			uint16_t xcopy : 1;
			uint16_t wsame : 1;
			uint16_t dpofua : 1;
			uint16_t wzero : 1;
			uint16_t delay_reserve : 1;
			uint16_t delay_release : 1;
			uint16_t exit_lu_worker : 1;
		};
		uint16_t lunflags;
	};

	uint32_t error_inject;
	int32_t inject_cnt;

	/* for ats */
	uint8_t *watsbuf;

	/* for clone */
	pthread_mutex_t clone_mutex;

	int		luworkers;            //we have one-to-one mapping between LU and LUN (spec)
	int		luworkersActive;
	int queue_depth;
	ISTGT_LU_TASK_Ptr inflight_io[ISTGT_MAX_NUM_LUWORKERS];
	pthread_cond_t cmd_queue_cond;
	int disk_modify_work_pending;
	pthread_cond_t maint_cmd_queue_cond;
	ISTGT_QUEUE maint_cmd_queue;
	ISTGT_QUEUE maint_blocked_queue;
	ISTGT_QUEUE cmd_queue;
	ISTGT_QUEUE blocked_queue;

#ifdef REPLICATION
	TAILQ_ENTRY(istgt_lu_disk_t)  spec_next;
	TAILQ_HEAD(, rcommon_cmd_s) rcommon_waitq; //Contains IOs waiting for acks from atleast n(consistency level) replicas
	rte_smempool_t rcommon_deadlist;	// Contains completed IOs
	TAILQ_HEAD(, replica_s) rq; //Queue of replicas connected to this spec(volume)
	TAILQ_HEAD(, replica_s) rwaitq; //Queue of replicas completed handshake, and yet to have data connection to this spec(volume)
	TAILQ_HEAD(, known_replica_s) identified_replica;	/* List of replicas known to spec */
	int replication_factor;
	int consistency_factor;
	int healthy_rcount;
	int degraded_rcount;
	bool ready;
	struct {
		struct replica_s *dw_replica;
		void *healthy_replica;
		bool rebuild_in_progress;
	} rebuild_info;

	/*Common for both the above queues,
	Since same cmd is part of both the queues*/
	pthread_mutex_t rq_mtx; 
	pthread_mutex_t rcommonq_mtx; 
	pthread_mutex_t luworker_rmutex[ISTGT_MAX_NUM_LUWORKERS];
	pthread_cond_t luworker_rcond[ISTGT_MAX_NUM_LUWORKERS];

	/* stats */
	struct {
		uint64_t	used;
		struct timespec	updated_stats_time;
	} stats;
#endif

	/*Queue containing all the tasks. Instead of going to separate 
	queues (Cmd Queue, blocked queue, maint_cmd_que, maint_blocked_queue, 
	inflight)to check for blockage, we will check it in just this queue.*/
	ISTGT_QUEUE complete_queue;
	pthread_mutex_t complete_queue_mutex;

	pthread_mutex_t schdler_mutex;
	pthread_mutex_t sleep_mutex;
	pthread_cond_t schdler_cond;
	uint8_t schdler_waiting;
	uint8_t schdler_cmd_waiting;
	uint8_t maint_thread_waiting;
	uint8_t error_count;
	pthread_mutex_t luworker_mutex[ISTGT_MAX_NUM_LUWORKERS];
	pthread_cond_t luworker_cond[ISTGT_MAX_NUM_LUWORKERS];
	uint8_t luworker_waiting[ISTGT_MAX_NUM_LUWORKERS];
	uint32_t lu_free_matrix[(ISTGT_MAX_NUM_LUWORKERS/32) + 1];

	pthread_mutex_t lu_tmf_mutex[ISTGT_MAX_NUM_LUWORKERS];
	pthread_cond_t lu_tmf_cond[ISTGT_MAX_NUM_LUWORKERS];
	uint8_t lu_tmf_wait[ISTGT_MAX_NUM_LUWORKERS];

	pthread_mutex_t wait_lu_task_mutex;
	ISTGT_LU_TASK_Ptr wait_lu_task[ISTGT_MAX_NUM_LUWORKERS];

	struct luworker_exit_args luworker_exit_arg[ISTGT_MAX_NUM_LUWORKERS];
	pthread_mutex_t pr_rsv_mutex;
	/* PERSISTENT RESERVE */
	int npr_keys;
	ISTGT_LU_PR_KEY pr_keys[MAX_LU_RESERVE];
	uint32_t pr_generation;

	char *rsv_port;
	uint64_t rsv_key;
	int rsv_scope;
	int rsv_type;

	istgt_rsv_pending rsv_pending;

	TAILQ_HEAD (, istgt_lu_disk_nexus) nexus;

	int spc2_reserved;

	/* SCSI sense code */
	volatile int sense;

	pthread_mutex_t state_mutex;
	int             ludsk_ref;
	int             open_waiting4close;
	int             close_waiting4open;
	struct timespec  close_started;
	struct timespec  open_started;
	istgt_lun_ex_state ex_state;
	istgt_lun_state state;
	struct timeavgs avgs[32];
	int do_avg;
	int fderr;
	int fderrno;
	uint8_t percent_count;
	uint8_t percent_val[32];
	uint8_t percent_latency[32];
	uint64_t io_seq;
#ifdef	REPLICATION
	int quiesce;
#endif

	/* entry */
	int (*open)(struct istgt_lu_disk_t *spec, int flags, int mode);
	int (*close)(struct istgt_lu_disk_t *spec);
	int64_t (*seek)(struct istgt_lu_disk_t *spec, uint64_t offset);

	int64_t (*sync)(struct istgt_lu_disk_t *spec, uint64_t offset, uint64_t nbytes);
	int (*allocate)(struct istgt_lu_disk_t *spec);
	int (*setcache)(struct istgt_lu_disk_t *spec);
} ISTGT_LU_DISK;

#ifdef CB_COMPILE
typedef struct scsi_pr_key  SCSI_PR_KEY;
typedef struct scsi_pr_data  SCSI_PR_DATA;
#else
/* To store Persistent Registrations */ 
typedef struct scsi_pr_key {
        uint64_t key;
        char registered_initiator_port[256];
        char registered_target_port[256];
        int pg_idx;
        int pg_tag;
        int all_tpg;
} SCSI_PR_KEY;

/* To store Persistent Reservation */
typedef struct scsi_pr_data {
        int npr_keys;
        int rsv_scope;
        int rsv_type;
        uint32_t pr_generation;
        uint64_t rsv_key;
        char rsv_port[256];
        struct scsi_pr_key keys[MAX_LU_ZAP_RESERVE];
} SCSI_PR_DATA;
#endif

void *timerfn(void *);
extern ISTGT_QUEUE closedconns;


typedef enum {
    ACTION_CLOSE,
    ACTION_OPEN
} istgt_action;

int istgt_lu_disk_signal_action(ISTGT_LU_Ptr lu, int i, struct timespec *now, istgt_action act);
int istgt_lu_disk_close(ISTGT_LU_Ptr lu, int i);
int istgt_lu_disk_open(ISTGT_LU_Ptr lu, int i);
int istgt_lu_disk_post_open(ISTGT_LU_DISK *spec);

extern struct istgt_cmd_entry istgt_cmd_table[] ;
extern istgt_serialize_action istgt_serialize_table[ISTGT_SERIDX_COUNT + 1][ISTGT_SERIDX_COUNT + 1];

int64_t
replicate(ISTGT_LU_DISK *, ISTGT_LU_CMD_Ptr, uint64_t, uint64_t);
int
istgt_lu_disk_update_raw(ISTGT_LU_Ptr lu, int i, int dofake);
 
int istgt_lu_print_q(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_print_reservation(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_close_raw(ISTGT_LU_DISK *spec);
int istgt_lu_disk_get_reservation(ISTGT_LU_DISK *spec);

#ifndef likely
#define likely(x)      __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x)    __builtin_expect(!!(x), 0)
#endif

#endif /* ISTGT_LU_H */

