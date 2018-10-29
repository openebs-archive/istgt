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
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/eventfd.h>
#ifdef HAVE_UUID_H
#include <uuid.h>
#endif

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_conf.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_crc32c.h"
#include "istgt_md5.h"
#include "istgt_iscsi.h"
#include "istgt_iscsi_xcopy.h"
#include "istgt_lu.h"
#include "istgt_proto.h"
#include "istgt_scsi.h"
#include "istgt_queue.h"
#ifdef	REPLICATION
#include "istgt_integration.h"
#include "replication.h"
#include "ring_mempool.h"
#endif

#ifdef __FreeBSD__
#include <sys/disk.h>
#endif

#include <assert.h>

#if !defined(__GNUC__)
#undef __attribute__
#define __attribute__(x)
#endif

#ifndef O_FSYNC
#define O_FSYNC O_SYNC
#endif

extern clockid_t clockid;

//#define ISTGT_TRACE_DISK

#ifdef	REPLICATION
#define	IS_SPEC_BUSY(_spec)						\
		(_spec->state == ISTGT_LUN_BUSY ||			\
		    _spec->ready == false)
#else
#define	IS_SPEC_BUSY(_spec)	\
		(_spec->state == ISTGT_LUN_BUSY)
#endif

#define MAX_DIO_WAIT 30   //loop for 30 seconds
//#define enterblockingcall(lu_cmd, conn)
#define enterblockingcall(macroname)				   \
{                                          \
	lu_cmd->connGone = 0;                  \
	if(pthread_mutex_lock(&spec->state_mutex) != 0)	\
	{	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	if (IS_SPEC_BUSY(spec)) {  \
		lu_cmd->data_len  = 0;             \
		lu_cmd->status = ISTGT_SCSI_STATUS_BUSY; \
		if(pthread_mutex_unlock(&spec->state_mutex) != 0)	\
		{	\
			markedForReturn = 1;	\
			goto macroname;	\
		}	\
		markedForReturn = 2;	\
		ISTGT_ERRLOG("Spec is busy!");	\
		goto macroname;	\
	}                                      \
	++(spec->ludsk_ref);				   \
	if(pthread_mutex_unlock(&spec->state_mutex) != 0)	\
	{	\
		--(spec->ludsk_ref);				   \
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	if(pthread_mutex_lock(&conn->diskioflag_mutex) != 0)	\
	{	\
		--(spec->ludsk_ref);	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	++(conn->diskIoPending);               \
	if(pthread_mutex_unlock(&conn->diskioflag_mutex) != 0)	\
	{	\
		--(conn->diskIoPending);               \
		--(spec->ludsk_ref);	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
}	\
macroname:

//#define exitblockingcall(lu_cmd, conn, diskIoPendingL, markedForFree)
#define exitblockingcall(macroname)                 \
{                                          \
	if(pthread_mutex_lock(&spec->state_mutex) != 0)	\
	{	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	--(spec->ludsk_ref);                   \
	if(pthread_mutex_unlock(&spec->state_mutex) != 0)	\
	{	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	if(pthread_mutex_lock(&conn->diskioflag_mutex) != 0)	\
	{	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
	--(conn->diskIoPending);               \
	diskIoPendingL = conn->diskIoPending;  \
	if (conn->flagDelayedFree == 1)        \
		markedForFree = 1;                 \
	if(pthread_mutex_unlock(&conn->diskioflag_mutex) != 0)	\
	{	\
		markedForReturn = 1;	\
		goto macroname;	\
	}	\
}	\
macroname:

#define create_time  times[0]

#define errlog(lu_cmd, fmt, ...)						\
{														\
	syslog(LOG_ERR, "%-18.18s:%4d: %-8.8s: [%u.%x lba%lu+%u %c.%ld.%ld %c.%ld.%ld]" fmt,		   \
			__func__, __LINE__, tinfo, lu_cmd->CmdSN, lu_cmd->cdb[0], lu_cmd->lba, lu_cmd->lblen,  \
			lu_cmd->caller[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0],        \
			lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_sec,  \
			lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_nsec, \
			lu_cmd->caller[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0],        \
			lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_sec,  \
			lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_nsec, \
			##__VA_ARGS__);								\
}

typedef enum {
	ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE = 0x01,
	ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS = 0x03,
	ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY = 0x05,
	ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY = 0x06,
	ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS = 0x07,
	ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS = 0x08,
} ISTGT_LU_PR_TYPE;

#define PR_ALLOW(WE,EA,ALLRR,WERR,EARR) \
	((((WE)&1) << 4) | (((EA)&1) << 3) | (((ALLRR)&1) << 2) \
	 | (((WERR)&1) << 1) | (((EARR)&1) << 0))
#define PR_ALLOW_WE    0x0010
#define PR_ALLOW_EA    0x0008
#define PR_ALLOW_ALLRR 0x0004
#define PR_ALLOW_WERR  0x0002
#define PR_ALLOW_EARR  0x0001

#define BUILD_SENSE(SK,ASC,ASCQ)  istgt_lu_scsi_build_sense_data(lu_cmd,ISTGT_SCSI_SENSE_ ## SK, (ASC), (ASCQ))
#define BUILD_SENSE2(SK,ASC,ASCQ) istgt_lu_scsi_build_sense_data2(lu_cmd, ISTGT_SCSI_SENSE_ ## SK, (ASC), (ASCQ))

static int istgt_lu_disk_queue_abort_ITL(ISTGT_LU_DISK *spec, const char *initiator_port);
 static const char *
istgt_get_disktype_by_ext(const char *file);
static int istgt_lu_disk_unmap(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint8_t *data, int pllen);
int istgt_lu_disk_copy_reservation(ISTGT_LU_DISK *spec_bkp, ISTGT_LU_DISK *spec);
/**
 * find_first_bit - find the first set bit in a memory region
 * @addr: The address to start the search at
 * @size: The maximum size to search
 *
 * Returns the bit-number of the first set bit
 */

static inline long
find_first_bit(ISTGT_LU_CMD_Ptr lu_cmd)
{
	long d0, d1;
	long res, res_final=0;
	int iovcnt, i;
	uint64_t nbytes;
	unsigned long nbits = 0;
	const unsigned long * addr;
	iovcnt = lu_cmd->iobufindx+1;
	for (i=0; i < iovcnt; ++i) {
		addr = (const unsigned long *)lu_cmd->iobuf[i].iov_base;
		nbytes = lu_cmd->iobuf[i].iov_len;
		nbits = nbytes << 3;
		nbits += 63;
		nbits >>= 6;
		if (!nbits)
			continue;
		asm volatile(
				"   repe; scasq\n"
				"   jz 1f\n"
				"   subq $8,%%rdi\n"
				"   bsfq (%%rdi),%%rax\n"
				"1: subq %[addr],%%rdi\n"
				"   shlq $3,%%rdi\n"
				"   addq %%rdi,%%rax"
				:"=a" (res), "=&c" (d0), "=&D" (d1)
				:"0" (0ULL), "1" (nbits), "2" (addr),
				[addr] "r" (addr) : "memory");
		res_final +=res;
		if((unsigned long)res != nbits)
			return res_final;
	}	
	return res_final;
}
static int
istgt_lu_disk_open_raw(ISTGT_LU_DISK *spec, int flags, int mode)
{
	int rc;
	errno = 0;
#ifdef	REPLICATION
	rc = 0;
#else
	rc = open(spec->file, flags, mode);
#endif
	spec->fderr = rc;
	spec->fderrno = errno;
	if (rc < 0) {
		return -1;
	}
	spec->fd = rc;
	spec->foffset = 0;
	return 0;
}

int
istgt_lu_disk_close_raw(ISTGT_LU_DISK *spec)
{
#ifndef	REPLICATION
	int rc;

	if (spec->fd == -1)
		return 0;
	rc = close(spec->fd);
	if (rc < 0) {
		return -1;
	}
#endif
	spec->fd = -1;
	spec->foffset = 0;
	return 0;
}

static int64_t
istgt_lu_disk_seek_raw(ISTGT_LU_DISK *spec, uint64_t offset)
{
#ifndef	REPLICATION
	off_t rc;
	rc = lseek(spec->fd, (off_t) offset, SEEK_SET);
	if (rc < 0) {
		return -1;
	}
#endif
	spec->foffset = offset;
	return 0;
}

static int64_t
istgt_lu_disk_sync_raw(ISTGT_LU_DISK *spec, uint64_t offset, uint64_t nbytes)
{
	int64_t rc = 0;

#ifndef	REPLICATION
	rc = (int64_t) fsync(spec->fd);
	if (rc < 0) {
		return -1;
	}
#endif
	spec->foffset = offset + nbytes;
	return rc;
}

int
istgt_lu_disk_signal_action(ISTGT_LU_Ptr lu, int i, struct timespec *now, istgt_action act)
{
	ISTGT_LU_DISK *spec;
	int rc;
	int signal_worker = 0;
	const char *action = (act == ACTION_CLOSE ) ? "close" : "open";
	istgt_lun_state st, st1;
	istgt_lun_ex_state exst, exst1;
	const char *path = "";

	if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
				lu->num, i);
		lu->lun[i].spec = NULL;
		return -1;
	}
	if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return -1;
	}

	spec = lu->lun[i].spec;
	if (spec == NULL) {
		ISTGT_NOTICELOG("LU%d: LUN%d spec is found\n", lu->num, i);
		return -1;
	}

	MTX_LOCK(&spec->state_mutex);
	st = spec->state;
	exst = spec->ex_state;
	if (act == ACTION_CLOSE) {
		spec->state = ISTGT_LUN_BUSY;
		if (spec->ex_state < ISTGT_LUN_CLOSE_PENDING) {
			spec->ex_state = ISTGT_LUN_CLOSE_PENDING;
			spec->close_started.tv_sec = now->tv_sec;
			spec->close_started.tv_nsec = now->tv_nsec;
			signal_worker = 1;
		} else if ((spec->ex_state == ISTGT_LUN_CLOSE_PENDING) || (spec->ludsk_ref != 0)) {
			signal_worker = 1;
		}
	} else if (act == ACTION_OPEN) {
		if (spec->ex_state > ISTGT_LUN_OPEN) {
			if (spec->ex_state < ISTGT_LUN_CLOSE) {
				spec->open_waiting4close = 1; //open after the pending close
				path = "open_after_pending_close";
			} else {
				spec->ex_state = ISTGT_LUN_OPEN_PENDING;
			}
			spec->open_started.tv_sec = now->tv_sec;
			spec->open_started.tv_nsec = now->tv_nsec;
			signal_worker = 1;
		} else if (spec->ex_state == ISTGT_LUN_OPEN_PENDING) {
			signal_worker = 1;
		}
	}
	st1 = spec->state;
	exst1 = spec->ex_state;
	MTX_UNLOCK(&spec->state_mutex);
	lu->limit_q_size=0;

	if (signal_worker == 1) {
		/* notify LUN thread */
                MTX_LOCK(&spec->complete_queue_mutex);
		spec->disk_modify_work_pending = 1;
                rc = pthread_cond_signal(&spec->maint_cmd_queue_cond);
                MTX_UNLOCK(&spec->complete_queue_mutex);

		if (rc != 0) {
			ISTGT_ERRLOG("cond signal failed ,lun %d, rc = %d \n",
                        	lu->num, rc);
			return -1;
		}
	}
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d %s %s %s state:%d/%d  %d/%d \n",
			lu->num, i, action,
			signal_worker == 1 ? "signal" : "skip", path,
			st, st1, exst, exst1);
	return signal_worker;
}

int
istgt_lu_disk_close(ISTGT_LU_Ptr lu, int i)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	int rc;
	int disk_ref = 0;
	int workers_signaled = 0;
	struct timespec  _s1, _s2, _s3, _wrk, _wrkx, cur_time;

	if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n", lu->num, i);
		lu->lun[i].spec = NULL;
		return -1;
	}
	if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return -1;
	}
	spec = lu->lun[i].spec;
	if (spec == NULL) {
		ISTGT_NOTICELOG("LU%d: LUN%d spec is not found\n", lu->num, i);
		return -1;
	}

	MTX_LOCK(&spec->state_mutex);
	spec->state = ISTGT_LUN_BUSY;
	if (spec->ludsk_ref == 0)
		workers_signaled = 1;
	if (workers_signaled == 0) {
		clock_gettime(clockid, &cur_time);
		if(cur_time.tv_sec - spec->close_started.tv_sec < 170) {
			MTX_UNLOCK(&spec->state_mutex);
			return -1;
		}
	}
	spec->ex_state = ISTGT_LUN_CLOSE_INPROGRESS;
	MTX_UNLOCK(&spec->state_mutex);
	disk_ref = spec->ludsk_ref;
	
	timesdiff(clockid, spec->close_started, _wrkx, _s1)
#ifndef	REPLICATION
	if (!spec->lu->readonly) {
		rc = spec->sync(spec, 0, spec->size);
		timesdiff(clockid, _wrkx, _wrk, _s2)
		if (rc < 0) { 
			/* Ignore error */
			ISTGT_ERRLOG("LU%d: LUN%d: failed to sync\n", lu->num, i);
		}
	} else
#endif
	{
		_wrk.tv_sec = _wrkx.tv_sec;
		_wrk.tv_nsec = _wrkx.tv_nsec;
		_s2.tv_sec = 0; _s2.tv_nsec = 0;
	}
	rc = spec->close(spec);
	timesdiff(clockid, _wrk, _wrkx, _s3)
	if (rc < 0) {
		/* Ignore error */
		ISTGT_ERRLOG("LU%d: LUN%d failed to close device\n", lu->num, i);
	}
	lu->lun[i].u.storage.fd = -1;
	spec->fd = -1;

	MTX_LOCK(&spec->pr_rsv_mutex);
	/* release reservation */
	xfree(spec->rsv_port);
	spec->rsv_port = NULL;
	spec->rsv_key = 0;
	spec->spc2_reserved = 0;
	spec->rsv_scope = 0;
	spec->rsv_type = 0;

	/* remove all registrations */
	for (i = 0; i < spec->npr_keys; i++) {
		istgt_lu_disk_free_pr_key(&spec->pr_keys[i]);
	}
	spec->npr_keys = 0;
	for (i = 0; i < MAX_LU_RESERVE; i++) {
		memset(&spec->pr_keys[i], 0, sizeof(spec->pr_keys[i]));
	} 
	MTX_UNLOCK(&spec->pr_rsv_mutex);

	TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
		if (nexus == NULL)
			break;
		nexus->ua_pending &= ISTGT_UA_NONE;
	}
	MTX_LOCK(&spec->state_mutex);
	spec->ex_state = ISTGT_LUN_CLOSE;
	MTX_UNLOCK(&spec->state_mutex);
	lu->limit_q_size = 0;

	ISTGT_LOG("LU%d: LUN%d %s: storage_offline, holds:%d [%ld.%ld - %ld.%ld - %ld.%ld]\n",
			lu->num, i, lu->name ? lu->name  : "-", disk_ref,
			_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec, _s3.tv_sec, _s3.tv_nsec);
	return 0;
}

int
istgt_lu_disk_post_open(ISTGT_LU_DISK *spec)
{
	ISTGT_LU_Ptr lu;
	int i;
	int rc = 0;
	int errorno = 0;

	lu = spec->lu;
	i = spec->lun;
#ifndef	REPLICATION
	if (!lu->readonly) {
		rc = spec->allocate(spec);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: LUN%d: allocate error\n",
						lu->num, i);
			errorno = errno;
			goto error_return;
		}
	}
#endif

	if (spec->rsize == 0) {
		//TODO
#ifndef	REPLICATION
		#ifdef __FreeBSD__	
		off_t rsize = 0;
		uint32_t lbPerRecord = 0;
		rc = ioctl(spec->fd, DIOCGSTRIPESIZE, &rsize);
		if (rc != -1) {
			ISTGT_ERRLOG("LU%d: LUN%d: ioctl-blksize:%lu \n", lu->num, i, rsize);
			spec->rsize = rsize;
			if (spec->rsize > spec->blocklen) {
				lbPerRecord = spec->rsize / ((uint32_t)(spec->blocklen));
				spec->lb_per_rec = lbPerRecord;
				spec->rshiftreal = fls(lbPerRecord) - 1;
				if (lbPerRecord & (lbPerRecord-1)) {
					ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen:%lu zfsrecordSize:%u\n",
								lu->num, i, spec->blocklen, spec->rsize);
				}
			}
		} else {
			ISTGT_ERRLOG("LU%d: LUN%d: ioctl-blksize:%lu failed:%d %d\n", lu->num, i, rsize, rc, errno);
		}
		#endif
#endif
	}

	rc = spec->setcache(spec);
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: LUN%d: setcache error\n", lu->num, i);
		errorno = errno;
		goto error_return;
	}

	if(spec->persist){
		ISTGT_LOG("READING RESERVATIONS FROM ZAP");
		MTX_LOCK(&spec->pr_rsv_mutex);
		rc = istgt_lu_disk_get_reservation(spec);
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_lu_disk_get_reservation() failed errno %d\n", rc);
			errorno = errno;
			goto error_return;
		}
	}

	MTX_LOCK(&spec->state_mutex);
	spec->state = ISTGT_LUN_ONLINE;
	spec->ex_state = ISTGT_LUN_OPEN;
	MTX_UNLOCK(&spec->state_mutex);
	ISTGT_LOG("Completed successfully post open for LU%d\n", lu->num);
error_return:
	return errorno;
}

int
istgt_lu_disk_open(ISTGT_LU_Ptr lu, int i)
{
	ISTGT_LU_DISK *spec;
	uint64_t gb_size;
	uint64_t mb_size;
	int flags;
	int rc;
	uint32_t lbPerRecord = 0;
	uint32_t old_rsize;
	uint64_t old_size;
	uint64_t old_blockcnt;
	uint64_t old_blocklen;
	uint32_t old_lb_per_rec, old_rshift;
	uint16_t old_opt_tlen;
	struct timespec  _s1, _s2, _wrk, _wrkx;
	int rpm;
	if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
			lu->num, i);
		lu->lun[i].spec = NULL;
		return -1;
	}
	if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
		return -1;
	}

	spec = lu->lun[i].spec;
	if (spec == NULL) {
		ISTGT_NOTICELOG("LU%d: LUN%d spec is found\n", lu->num, i);
		return -1;
	}

	_wrk.tv_sec = spec->open_started.tv_sec;
	_wrk.tv_nsec = spec->open_started.tv_nsec;

	timesdiff(clockid, _wrk, _wrkx, _s1)
	old_rsize = spec->rsize;
	old_size = spec->size;
	old_blocklen = spec->blocklen;
	old_blockcnt = spec->blockcnt;
	old_lb_per_rec = spec->lb_per_rec;
	old_rshift = spec->rshiftreal;
	old_opt_tlen = spec->opt_tlen;
	spec->luworkers = lu->luworkers;
	spec->luworkersActive = lu->luworkersActive;
	spec->file = lu->lun[i].u.storage.file;
	spec->size = lu->lun[i].u.storage.size;
	spec->rsize = lu->lun[i].u.storage.rsize;
	spec->disktype = istgt_get_disktype_by_ext(spec->file);
	spec->blocklen = 0;
	spec->blockcnt = 0;
	spec->lb_per_rec = 1;
	spec->opt_tlen = (uint16_t)lu->lun[i].opt_tlen;
	spec->rshift = (uint32_t)lu->rshift;
	spec->rshiftreal = 0;
	rpm = lu->lun[i].rotationrate;
	if (strcasecmp(spec->disktype, "RAW") == 0) {
		spec->blocklen = lu->blocklen;
		if (spec->blocklen != 512     &&
		    spec->blocklen != 1024    &&
		    spec->blocklen != 2048    &&
		    spec->blocklen != 4096    &&
		    spec->blocklen != 8192    &&
		    spec->blocklen != 16384   &&
		    spec->blocklen != 32768   &&
		    spec->blocklen != 65536   &&
		    spec->blocklen != 131072  &&
		    spec->blocklen != 262144  &&
		    spec->blocklen != 524288) {
			ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen %"PRIu64"\n",
				lu->num, i, spec->blocklen);
			goto error_return;
		}
		spec->blockcnt = spec->size / spec->blocklen;
		if (spec->blockcnt == 0) {
			ISTGT_ERRLOG("LU%d: LUN%d: size zero\n", lu->num, i);
			goto error_return;
		}
		if (spec->rsize > spec->blocklen) {
			lbPerRecord = spec->rsize / ((uint32_t)(spec->blocklen));
			spec->lb_per_rec = lbPerRecord;
			spec->rshiftreal = fls(lbPerRecord) - 1;
			if (lbPerRecord & (lbPerRecord-1)) {
				ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen:%lu zfsrecordSize:%u\n",
						lu->num, i, spec->blocklen, spec->rsize);
			}
		}

		if (spec->fd > -1){
			MTX_LOCK(&spec->state_mutex);
			spec->state = ISTGT_LUN_BUSY;
			MTX_UNLOCK(&spec->state_mutex);
			rc = spec->close(spec);
			if (rc < 0) {
				/* Ignore error */
				ISTGT_ERRLOG("LU%d: LUN%d failed to close device\n", lu->num, i);
				goto error_return;
			} //else {
			//	MTX_LOCK(&spec->state_mutex);
			//	spec->ex_state = ISTGT_LUN_CLOSE;
			//	MTX_UNLOCK(&spec->state_mutex);
			//}
	
		}

		spec->fd = -1;
		lu->lun[i].u.storage.fd = -1;
		lu->limit_q_size = 0;

		flags = lu->readonly ? O_RDONLY : O_RDWR;
		rc = spec->open(spec, flags, 0666);
		timesdiff(clockid, _wrkx, _wrk, _s2)
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: LUN%d: open error(errno=%d/%d)[%s]\n",
						lu->num, i, spec->fderr, spec->fderrno, spec->file);
			flags = lu->readonly ? O_RDONLY : (O_CREAT | O_EXCL | O_RDWR);
			rc = spec->open(spec, flags, 0666);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: LUN%d: open error(errno=%d/%d)[%s]\n",
						lu->num, i, spec->fderr, spec->fderrno, spec->file);
				MTX_LOCK(&spec->state_mutex);
				spec->state = ISTGT_LUN_BUSY;
				spec->ex_state = ISTGT_LUN_NOTYET;
				MTX_UNLOCK(&spec->state_mutex);
				goto error_return;
			}
		}
		timesdiff(clockid, _wrkx, _wrk, _s2)

		MTX_LOCK(&spec->pr_rsv_mutex);
		spec->rsv_pending |= ISTGT_RSV_READ;
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		MTX_LOCK(&spec->state_mutex);
		spec->state = ISTGT_LUN_BUSY;
		spec->ex_state = ISTGT_LUN_OPEN;
		MTX_UNLOCK(&spec->state_mutex);

		gb_size = spec->size / ISTGT_LU_1GB;
		mb_size = (spec->size % ISTGT_LU_1GB) / ISTGT_LU_1MB;
 		printf("LU%d: LUN%d %s: storage_onlinex %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u/%u %s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d [%ld.%ld, %ld.%ld]\n",
				lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
				spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift, spec->rshiftreal,
				spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
				spec->ats ? " ATS" : "", spec->unmap ? " UNMAP" : "",
				spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
				rpm, spec->queue_depth, spec->luworkers, spec->luworkersActive,
				_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec);
 		ISTGT_LOG("LU%d: LUN%d %s: storage_onlinex %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u/%u %s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d [%ld.%ld, %ld.%ld]\n",
				lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
 				spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift, spec->rshiftreal,
				spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
				spec->ats ? " ATS" : "", spec->unmap ? " UNMAP" : "",
				spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
				rpm, spec->queue_depth, spec->luworkers, spec->luworkersActive,
				_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec);

	} else {
		ISTGT_ERRLOG("LU%d: LUN%d: unsupported format\n", lu->num, i);
		goto error_return;
	}

	lu->lun[i].spec = spec;
	return 0;
error_return:
	spec->rsize = old_rsize;
	spec->size = old_size;
	spec->blockcnt = old_blockcnt;
	spec->blocklen = old_blocklen;
	spec->lb_per_rec = old_lb_per_rec;
	spec->rshiftreal = old_rshift;
	spec->opt_tlen = old_opt_tlen;
	return -1;
}

static int
istgt_lu_disk_allocate_raw(ISTGT_LU_DISK *spec)
{
#ifdef	REPLICATION
	return 0;
#endif

	uint8_t *data;
	uint64_t fsize;
	uint64_t size;
	uint64_t blocklen;
	uint64_t offset;
	uint64_t nbytes;
	int64_t rc;

	size = spec->size;
	blocklen = spec->blocklen;
	nbytes = blocklen;
	data = xmalloc(nbytes);
	memset(data, 0, nbytes);

	fsize = istgt_lu_get_filesize(spec->file);
	if (fsize > size)  { /* case when quota is reduced */
		xfree(data);
		return 0;
	}
	spec->fsize = fsize;

	offset = size - nbytes;

	rc = pread(spec->fd, data, nbytes, offset);
	/* EOF is OK */
	if (rc < 0) {
		ISTGT_ERRLOG("lu_disk_read() failed:errno:%d\n", errno);
		xfree(data);
		return -1;
	}
	
	/* allocate complete size */
	/*This can be avoided since thin provisioned volume of this size has already been created*/
	/*
	rc = pwrite(spec->fd, data, nbytes, offset);
	if (rc < 0 || (uint64_t) rc != nbytes) {
		ISTGT_ERRLOG("lu_disk_write() failed:errno:%d written:%ld\n", errno, rc);
		xfree(data);
		return -1;
	}
	*/
	spec->foffset = size;

	xfree(data);
	return 0;
}

static int
istgt_lu_disk_setcache_raw(ISTGT_LU_DISK *spec)
{
#ifdef	REPLICATION
	return 0;
#endif
	int flags;
	int rc;
	int fd;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_setcache\n");

	fd = spec->fd;
	if (spec->readcache) {
		/* not implement */
	} else {
		/* not implement */
	}
	flags = fcntl(fd , F_GETFL, 0);
	if (flags != -1) {
		if (spec->writecache) {
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "write cache enable\n");
			rc = fcntl(fd, F_SETFL, (flags & ~O_FSYNC));
			spec->writecache = 1;
		} else {
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "write cache disable\n");
			rc = fcntl(fd, F_SETFL, (flags | O_FSYNC));
			spec->writecache = 0;
		}
		if (rc == -1) {
#if 0
			ISTGT_ERRLOG("LU%d: LUN%d: fcntl(F_SETFL) failed(errno=%d)\n",
			    spec->num, spec->lun, errno);
#endif
		}
	} else {
		ISTGT_ERRLOG("LU%d: LUN%d: fcntl(F_GETFL) failed(errno=%d)\n",
		    spec->num, spec->lun, errno);
	}
	return 0;
}

static const char *
istgt_get_disktype_by_ext(const char *file)
{
	size_t n;

	if (file == NULL || file[0] == '\n')
		return "RAW";

	n = strlen(file);
	if (n > 4 && strcasecmp(file + (n - 4), ".vdi") == 0)
		return "VDI";
	if (n > 4 && strcasecmp(file + (n - 4), ".vhd") == 0)
		return "VHD";
	if (n > 5 && strcasecmp(file + (n - 5), ".vmdk") == 0)
		return "VMDK";

	if (n > 5 && strcasecmp(file + (n - 5), ".qcow") == 0)
		return "QCOW";
	if (n > 6 && strcasecmp(file + (n - 6), ".qcow2") == 0)
		return "QCOW";
	if (n > 4 && strcasecmp(file + (n - 4), ".qed") == 0)
		return "QED";
	if (n > 5 && strcasecmp(file + (n - 5), ".vhdx") == 0)
		return "VHDX";

	return "RAW";
}

int
istgt_lu_disk_init(ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	uint64_t gb_size;
	uint64_t mb_size;
#ifdef HAVE_UUID_H
	uint32_t status;
#endif /* HAVE_UUID_H */
	int flags, rpm;
	int rc;
	int i, j, k;
	uint32_t lbPerRecord =  0;

	printf("LU%d HDD UNIT\n", lu->num);
	ISTGT_NOTICELOG("lu_disk_init LU%d TargetName=%s",
	    lu->num, lu->name);
	
	for (i = 0; i < lu->maxlun; i++) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
			    lu->num, i);
			lu->lun[i].spec = NULL;
			continue;
		}
		if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
			ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
			return -1;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d storage\n",
		    lu->num, i);

		spec = xmalloc(sizeof *spec);
		memset(spec, 0, sizeof *spec);
		spec->lu = lu;
#ifdef	REPLICATION
		spec->volname = xstrdup(spec->lu->volname);
#endif
		spec->num = lu->num;
		spec->lun = i;
		spec->do_avg = 0;
		spec->inflight = 0;
		spec->fd = -1;
		spec->ludsk_ref = 0;
#ifdef	REPLICATION
		spec->quiesce = 0;
#endif
		spec->max_unmap_sectors = 4096;
		spec->persist = is_persist_enabled();
		spec->luworkers = lu->luworkers;
		spec->luworkersActive = lu->luworkersActive;

		spec->readcache = spec->lu->lun[i].readcache;
		spec->writecache = spec->lu->lun[i].writecache;
		spec->unmap = spec->lu->lun[i].unmap;
		spec->wzero = spec->lu->lun[i].wzero;
		spec->ats = spec->lu->lun[i].ats;
		spec->xcopy = spec->lu->lun[i].xcopy;
		spec->wsame = spec->lu->lun[i].wsame;
		spec->dpofua = spec->lu->lun[i].dpofua;
		rpm = spec->lu->lun[i].rotationrate;

		spec->watsbuf = NULL;

		spec->queue_depth = lu->queue_depth;

		spec->disk_modify_work_pending = 0;

		rc = pthread_cond_init(&spec->cmd_queue_cond, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: cond_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		istgt_queue_init(&spec->cmd_queue);

		rc = pthread_cond_init(&spec->maint_cmd_queue_cond, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: maint_cond_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		istgt_queue_init(&spec->maint_cmd_queue);
		istgt_queue_init(&spec->maint_blocked_queue);

		rc = pthread_mutex_init(&spec->clone_mutex, &istgt->mutex_attr);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: clone_mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		rc = pthread_mutex_init(&spec->complete_queue_mutex, &istgt->mutex_attr);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: comp_q_mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}
		istgt_queue_init(&spec->complete_queue);

		rc = pthread_mutex_init(&spec->wait_lu_task_mutex, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: wait_mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		rc = pthread_mutex_init(&spec->schdler_mutex, &istgt->mutex_attr);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: scheduler mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		rc = pthread_mutex_init(&spec->sleep_mutex, &istgt->mutex_attr);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: sleep mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		rc = pthread_cond_init(&spec->schdler_cond, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d:scheduler cond_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
			spec->inflight_io[k] = NULL;
			spec->wait_lu_task[k] = NULL;
			spec->lu_tmf_wait[k] = 0;

			rc = pthread_mutex_init(&spec->luworker_mutex[k], &istgt->mutex_attr);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: luworker %d mutex_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}
        
			rc = pthread_cond_init(&spec->luworker_cond[k], NULL);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: luworker %d cond_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}

#ifdef	REPLICATION
			rc = pthread_mutex_init(&spec->luworker_rmutex[k], &istgt->mutex_attr);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: luworker %d mutex_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}
        
			rc = pthread_cond_init(&spec->luworker_rcond[k], NULL);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: luworker %d cond_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}
#endif

			rc = pthread_mutex_init(&spec->lu_tmf_mutex[k], &istgt->mutex_attr);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: lu_tmf_mutex %d mutex_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}
        
			rc = pthread_cond_init(&spec->lu_tmf_cond[k], NULL);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: lu_tmf_cond %d cond_init() failed errno:%d\n", lu->num, k, errno);
				return -1;
			}
		}
		istgt_queue_init(&spec->blocked_queue);
#ifdef REPLICATION
		rc = initialize_volume(spec, spec->lu->replication_factor, spec->lu->consistency_factor);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: persistent reservation mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}
#endif
		memset(&spec->lu_free_matrix, 0, ((ISTGT_MAX_NUM_LUWORKERS/32)+1)*4);
		memset(&spec->luworker_waiting, 0, sizeof(spec->luworker_waiting));
		spec->schdler_waiting = 0;
		spec->schdler_cmd_waiting = 0;
		spec->error_count = 0;
		spec->maint_thread_waiting = 0;
	
		rc = pthread_mutex_init(&spec->pr_rsv_mutex, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: persistent reservation mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		rc = pthread_mutex_init(&spec->state_mutex, NULL);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: mutex_init() failed errno:%d\n", lu->num, errno);
			return -1;
		}

		spec->npr_keys = 0;
		for (j = 0; j < MAX_LU_RESERVE; j++) {
			spec->pr_keys[j].registered_initiator_port = NULL;
		}
		spec->pr_generation = 0;
		spec->rsv_port = NULL;
		spec->rsv_key = 0;
		spec->rsv_scope = 0;
		spec->rsv_type = 0;
		spec->rsv_pending = ISTGT_RSV_NONE;

		TAILQ_INIT(&spec->nexus);

#ifdef HAVE_UUID_H
		uuid_create(&spec->uuid, &status);
		if (status != uuid_s_ok) {
			ISTGT_ERRLOG("LU%d: LUN%d: uuid_create() failed\n", lu->num, i);
			(void) pthread_mutex_destroy(&spec->wait_lu_task_mutex);
			(void) pthread_mutex_destroy(&spec->complete_queue_mutex);
			(void) pthread_mutex_destroy(&spec->pr_rsv_mutex);
			(void) pthread_mutex_destroy(&spec->state_mutex);
			(void) pthread_mutex_destroy(&spec->schdler_mutex);
			(void) pthread_mutex_destroy(&spec->sleep_mutex);
			(void) pthread_cond_destroy(&spec->schdler_cond);
			(void) pthread_cond_destroy(&spec->cmd_queue_cond);
			(void) pthread_cond_destroy(&spec->maint_cmd_queue_cond);
			istgt_queue_destroy(&spec->cmd_queue);
			istgt_queue_destroy(&spec->blocked_queue);
			istgt_queue_destroy(&spec->maint_cmd_queue);
			istgt_queue_destroy(&spec->maint_blocked_queue);
			for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
				(void) pthread_mutex_destroy(&spec->luworker_mutex[k]);
				(void) pthread_cond_destroy(&spec->luworker_cond[k]);
			}
			xfree(spec);
			return -1;
		}
#endif /* HAVE_UUID_H */

		spec->file = lu->lun[i].u.storage.file;
		spec->size = lu->lun[i].u.storage.size;
		spec->rsize = lu->lun[i].u.storage.rsize;
		spec->disktype = istgt_get_disktype_by_ext(spec->file);
		if (strcasecmp(spec->disktype, "VDI") == 0
		    || strcasecmp(spec->disktype, "VHD") == 0
		    || strcasecmp(spec->disktype, "VMDK") == 0
		    || strcasecmp(spec->disktype, "QCOW") == 0
		    || strcasecmp(spec->disktype, "QED") == 0
		    || strcasecmp(spec->disktype, "VHDX") == 0) {
			rc = istgt_lu_disk_vbox_lun_init(spec, istgt, lu);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: LUN%d: lu_disk_vbox_lun_init() failed\n",
				    lu->num, i);
				goto error_return;
			}
		} else if (strcasecmp(spec->disktype, "RAW") == 0) {
			spec->open = istgt_lu_disk_open_raw;
			spec->close = istgt_lu_disk_close_raw;
			spec->seek = istgt_lu_disk_seek_raw;
			spec->sync = istgt_lu_disk_sync_raw;
			spec->allocate = istgt_lu_disk_allocate_raw;
			spec->setcache = istgt_lu_disk_setcache_raw;

			spec->rshift = lu->rshift;
			spec->blocklen = lu->blocklen;
			if (spec->blocklen != 512
			    && spec->blocklen != 1024
			    && spec->blocklen != 2048
			    && spec->blocklen != 4096
			    && spec->blocklen != 8192
			    && spec->blocklen != 16384
			    && spec->blocklen != 32768
			    && spec->blocklen != 65536
			    && spec->blocklen != 131072
			    && spec->blocklen != 262144
			    && spec->blocklen != 524288) {
				ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen %"PRIu64"\n",
				    lu->num, i, spec->blocklen);
error_return:
				(void) pthread_mutex_destroy(&spec->wait_lu_task_mutex);
				(void) pthread_mutex_destroy(&spec->complete_queue_mutex);
				(void) pthread_mutex_destroy(&spec->pr_rsv_mutex);
				(void) pthread_mutex_destroy(&spec->state_mutex);
				(void) pthread_mutex_destroy(&spec->schdler_mutex);
				(void) pthread_mutex_destroy(&spec->sleep_mutex);
				(void) pthread_cond_destroy(&spec->schdler_cond);
				(void) pthread_cond_destroy(&spec->cmd_queue_cond);
				(void) pthread_cond_destroy(&spec->maint_cmd_queue_cond);
				istgt_queue_destroy(&spec->cmd_queue);
				istgt_queue_destroy(&spec->blocked_queue);
				istgt_queue_destroy(&spec->maint_cmd_queue);
				istgt_queue_destroy(&spec->maint_blocked_queue);
				for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
					(void) pthread_mutex_destroy(&spec->luworker_mutex[k]);
					(void) pthread_cond_destroy(&spec->luworker_cond[k]);
				}
				xfree(spec);
				return -1;
			}
			spec->blockcnt = spec->size / spec->blocklen;
			if (spec->blockcnt == 0) {
				ISTGT_ERRLOG("LU%d: LUN%d: size zero\n", lu->num, i);
				goto error_return;
			}
			spec->lb_per_rec = 1;
			spec->rshiftreal = 0;
			if (spec->rsize > spec->blocklen) {
				lbPerRecord = spec->rsize / ((uint32_t)(spec->blocklen));
				spec->lb_per_rec = lbPerRecord;
				spec->rshiftreal = fls(lbPerRecord) - 1;
				if (lbPerRecord & (lbPerRecord-1)) {
					ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen:%lu zfsrecordSize:%u\n",
							lu->num, i, spec->blocklen, spec->rsize);
				}
			}
#if 0
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "LU%d: LUN%d file=%s, size=%"PRIu64"\n",
			    lu->num, i, spec->file, spec->size);
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "LU%d: LUN%d %"PRIu64" blocks, %"
			    PRIu64" bytes/block\n",
			    lu->num, i, spec->blockcnt, spec->blocklen);
#endif

			if (lu->istgt->OperationalMode) {
				MTX_LOCK(&spec->state_mutex);
				spec->state = ISTGT_LUN_BUSY;
				spec->ex_state = ISTGT_LUN_NOTYET;
				MTX_UNLOCK(&spec->state_mutex);

				gb_size = spec->size / ISTGT_LU_1GB;
				mb_size = (spec->size % ISTGT_LU_1GB) / ISTGT_LU_1MB;
				printf("LU%d: LUN%d %s: storage_offline %s [%s, size=%"PRIu64"GB %"PRIu64"MB %"PRIu64" blocks, %"PRIu64" bytes/block %u]\n",
					lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
					spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rsize);
				ISTGT_LOG("LU%d: LUN%d %s: storage_offline %s [%s, size=%"PRIu64"GB %"PRIu64"MB %"PRIu64" blocks, %"PRIu64" bytes/block %u]\n",
					lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
					spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rsize);
			} else {
				struct timespec  _s1, _s2, _wrk, _wrkx;
				flags = lu->readonly ? O_RDONLY : O_RDWR;
				clock_gettime(clockid, &_wrk);

				rc = spec->open(spec, flags, 0666);
				timesdiff(clockid, _wrk, _wrkx, _s1)
				if (rc < 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: open error(errno=%d/%d)[%s]\n",
						    lu->num, i, spec->fderr, spec->fderrno, spec->file);
					flags = lu->readonly ? O_RDONLY : (O_CREAT | O_EXCL | O_RDWR);
					rc = spec->open(spec, flags, 0666);
					if (rc < 0) {
						ISTGT_ERRLOG("LU%d: LUN%d: open error(errno=%d/%d)[%s]\n",
						    lu->num, i, spec->fderr, spec->fderrno, spec->file);
						goto error_return;
					}
				}

#ifndef	REPLICATION
				if (!lu->readonly) {
					rc = spec->allocate(spec);
					if (rc < 0) {
						ISTGT_ERRLOG("LU%d: LUN%d: allocate error\n",
						    lu->num, i);
						rc = spec->close(spec);
						if(rc < 0 ) {
							ISTGT_ERRLOG("LU%d:LUN%d: close error\n", lu->num, i);
							/* Ignore error */
						}
						goto error_return;
					}
				}
#endif
				timesdiff(clockid, _wrk, _wrkx, _s1)
				/*
				if (spec->rsize == 0) {
					off_t rsize = 0;
					rc = ioctl(spec->fd, DIOCGSTRIPESIZE, &rsize);
					if (rc != -1) {
						ISTGT_ERRLOG("LU%d: LUN%d: ioctl-blksize:%lu\n", lu->num, i, rsize);
						spec->rsize = rsize;
						if (spec->rsize > spec->blocklen) {
							lbPerRecord = spec->rsize / ((uint32_t)(spec->blocklen));
							spec->lb_per_rec = lbPerRecord;
							spec->rshiftreal = fls(lbPerRecord) - 1;
							if (lbPerRecord & (lbPerRecord-1)) {
								ISTGT_ERRLOG("LU%d: LUN%d: invalid blocklen:%lu zfsrecordSize:%u\n",
										lu->num, i, spec->blocklen, spec->rsize);
							}
						}
					} else {
						ISTGT_ERRLOG("LU%d: LUN%d: ioctl-blksize:%lu failed:%d %d\n", lu->num, i, rsize, rc, errno);
					}
				}
				*/
#ifndef	REPLICATION
				rc = spec->setcache(spec);
				if (rc < 0) {
					ISTGT_ERRLOG("LU%d: LUN%d: setcache error\n", lu->num, i);
					rc = spec->close(spec);
						if(rc < 0 ) {
							ISTGT_ERRLOG("LU%d:LUN%d: close error\n", lu->num, i);
							/* Ignore error */
						}
					goto error_return;
				}

                                if(spec->persist){
                                        ISTGT_LOG("READING RESERVATION FROM ZAP");
                                        MTX_LOCK(&spec->pr_rsv_mutex);
					spec->rsv_pending |= ISTGT_RSV_READ;
                                        //rc = istgt_lu_disk_get_reservation(spec);
                                        MTX_UNLOCK(&spec->pr_rsv_mutex);
					spec->state = ISTGT_LUN_BUSY;
                        		spec->ex_state = ISTGT_LUN_OPEN;
                                        //if (rc < 0) {
                                        //       ISTGT_ERRLOG("istgt_lu_disk_get_reservation() failed\n");
                                        //}
                                } else {
                                	MTX_LOCK(&spec->state_mutex);
                                	spec->state = ISTGT_LUN_ONLINE;
                                	spec->ex_state = ISTGT_LUN_OPEN;
                               		MTX_UNLOCK(&spec->state_mutex);
				}
#endif
				timesdiff(clockid, _wrkx, _wrk, _s2)

				gb_size = spec->size / ISTGT_LU_1GB;
				mb_size = (spec->size % ISTGT_LU_1GB) / ISTGT_LU_1MB;
				printf("LU%d: LUN%d %s: storage_online %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u/%u %s%s%s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d [%ld.%ld, %ld.%ld]\n",
					lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
					spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift, spec->rshiftreal,
					spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
					spec->ats ? " ATS" : "", spec->xcopy ? " XCOPY" : "", spec->unmap ? " UNMAP" : "",
					spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
					spec->wzero ? " WZERO" : "",
					rpm, spec->queue_depth, spec->luworkers, spec->luworkersActive,
					_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec);
				ISTGT_LOG("LU%d: LUN%d %s: storage_online %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u/%u %s%s%s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d [%ld.%ld, %ld.%ld]\n",
					lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
					spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift, spec->rshiftreal,
					spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
					spec->ats ? " ATS" : "",spec->xcopy ? " XCOPY" : "", spec->unmap ? " UNMAP" : "",
					spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
					spec->wzero ? " WZERO" : "",
					rpm, spec->queue_depth, spec->luworkers, spec->luworkersActive,
					_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec);
			}
		} else {
			ISTGT_ERRLOG("LU%d: LUN%d: unsupported format\n", lu->num, i);
			goto error_return;
		}

		printf("LU%d: LUN%d serial:%s\n", lu->num, i,
			spec->lu->lun[i].serial != NULL ?  spec->lu->lun[i].serial : spec->lu->inq_serial);
		ISTGT_LOG("LU%d: LUN%d serial:%s\n", lu->num, i,
			spec->lu->lun[i].serial != NULL ?  spec->lu->lun[i].serial : spec->lu->inq_serial);


		/* Needs a thot */
		TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
			if (nexus == NULL)
				break;
			nexus->ua_pending |= ISTGT_UA_RES_PREEMPT;
		}

		lu->lun[i].spec = spec;
	}

	return 0;
}

int
istgt_lu_disk_shutdown(ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	ISTGT_LU_PR_KEY *prkey;
	int rc;
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d TargetName=%s istgt_lu_disk_shutdown\n",
	    lu->num, lu->name);
	for (i = 0; i < lu->maxlun; i++) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
			    lu->num, i);
			continue;
		}
		if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
			ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
			return -1;
		}
		spec = (ISTGT_LU_DISK *) lu->lun[i].spec;

		/* CloudByte: Spec can be null when there is a LUN initialization failure. */
		if (spec == NULL)
			continue;

		if (strcasecmp(spec->disktype, "VDI") == 0
		    || strcasecmp(spec->disktype, "VHD") == 0
		    || strcasecmp(spec->disktype, "VMDK") == 0
		    || strcasecmp(spec->disktype, "QCOW") == 0
		    || strcasecmp(spec->disktype, "QED") == 0
		    || strcasecmp(spec->disktype, "VHDX") == 0) {
			rc = istgt_lu_disk_vbox_lun_shutdown(spec, istgt, lu);
			if (rc < 0) {
				ISTGT_ERRLOG("LU%d: lu_disk_vbox_lun_shutdown() failed\n",
				    lu->num);
				/* ignore error */
			}
		} else if (strcasecmp(spec->disktype, "RAW") == 0) {
			int workers_signaled = 0, loop = 0;
			struct timespec  _s1, _s2, _s3, _wrk, _wrkx;

			clock_gettime(clockid, &_wrk);
			do {
				MTX_LOCK(&spec->state_mutex);
				spec->state = ISTGT_LUN_BUSY;
				if (spec->ludsk_ref == 0)
					workers_signaled = 1;
				spec->ex_state = ISTGT_LUN_CLOSE_INPROGRESS;
				MTX_UNLOCK(&spec->state_mutex);
				if (workers_signaled == 0)
					sleep(1);
			} while ((workers_signaled == 0) && (++loop  < 10));

			timesdiff(clockid, _wrk, _wrkx, _s1)
#ifndef	REPLICATION
			if (!spec->lu->readonly) {
				rc = spec->sync(spec, 0, spec->size);
				if (rc < 0) {
					//ISTGT_ERRLOG("LU%d: lu_disk_sync() failed\n", lu->num);
					/* ignore error */
				}
			}
#endif
			timesdiff(clockid, _wrkx, _wrk, _s2)
			rc = spec->close(spec);
			if (rc < 0) {
				//ISTGT_ERRLOG("LU%d: lu_disk_close() failed\n", lu->num);
				/* ignore error */
			}
			MTX_LOCK(&spec->state_mutex);
			spec->ex_state = ISTGT_LUN_CLOSE;
			MTX_UNLOCK(&spec->state_mutex);
			timesdiff(clockid, _wrk, _wrkx, _s3)
			ISTGT_LOG("LU%d: LUN%d %s: storage_offlinex %s [%s %"PRIu64" blocks, %"PRIu64" bytes/block] [%ld.%ld %ld.%ld %ld.%ld]\n",
					lu->num, i, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
					spec->file, spec->blockcnt, spec->blocklen,
					_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec, _s3.tv_sec, _s3.tv_nsec);
		} else {
			ISTGT_ERRLOG("LU%d: LUN%d: unsupported format\n", lu->num, i);
			return -1;
		}

		for (j = 0; j < spec->npr_keys; j++) {
			prkey = &spec->pr_keys[j];
			istgt_lu_disk_free_pr_key(prkey);
		}
		if (spec->rsv_key != 0) {
			xfree(spec->rsv_port);
			spec->rsv_port = NULL;
		}

		lu->limit_q_size = 0;

		if (lu->queue_depth != 0) {
			for ( i=0; i < lu->luworkers; i++ ) {
				MTX_LOCK(&spec->luworker_mutex[i]);	
				rc = pthread_cond_broadcast(&spec->luworker_cond[i]);
				if (rc != 0) {
					ISTGT_ERRLOG("LU%d: %d luthread cond_broadcast() failed:%d\n", lu->num, i, rc);
				}	
				MTX_UNLOCK(&spec->luworker_mutex[i]);	
			}

			MTX_LOCK(&spec->schdler_mutex);	
			rc = pthread_cond_broadcast(&spec->schdler_cond);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: schdler_cond_broadcast() failed:%d\n", lu->num, rc);
			}
			MTX_UNLOCK(&spec->schdler_mutex);	

			MTX_LOCK(&spec->complete_queue_mutex);	
			rc = pthread_cond_broadcast(&spec->cmd_queue_cond);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: cmd_q cond_broadcast() failed:%d\n", lu->num, rc);
			}

			rc = pthread_cond_broadcast(&spec->maint_cmd_queue_cond);
			if (rc != 0) {
				ISTGT_ERRLOG("LU%d: maint_q cond_broadcast() failed:%d\n", lu->num, rc);
			}
			MTX_UNLOCK(&spec->complete_queue_mutex);

			for ( i=0; i < lu->luworkers; i++ ) {
				if (lu->luthread[i] == 0)
					continue;
				rc = pthread_join(lu->luthread[i], NULL);
				if (rc != 0) {
					ISTGT_ERRLOG("LU%d: pthread_join() failed for thread join op %d\n", lu->num, i);
				} else {
					--lu->luworkersActive;
				}
			}
			rc = pthread_join(lu->schdler_thread, NULL);
			if (rc != 0) 
				ISTGT_ERRLOG("LU%d: pthread_join() failed for schdler thread join\n", lu->num);
			rc = pthread_join(lu->maintenance_thread, NULL);
			if (rc != 0) 
				ISTGT_ERRLOG("LU%d: pthread_join() failed for maint thread join\n", lu->num);
		}
		istgt_queue_destroy(&spec->cmd_queue);
		istgt_queue_destroy(&spec->maint_cmd_queue);
		istgt_queue_destroy(&spec->maint_blocked_queue);
		istgt_queue_destroy(&spec->complete_queue);
		rc = pthread_mutex_destroy(&spec->complete_queue_mutex);
		rc = pthread_cond_destroy(&spec->maint_cmd_queue_cond);

		rc = pthread_mutex_destroy(&spec->schdler_mutex);
		rc = pthread_mutex_destroy(&spec->sleep_mutex);
		rc = pthread_cond_destroy(&spec->schdler_cond);
		rc = pthread_cond_destroy(&spec->cmd_queue_cond);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
			/* ignore error */
		}
		istgt_queue_destroy(&spec->blocked_queue);

#ifdef	REPLICATION
		destroy_volume(spec);
#endif
		for ( i=0; i < lu->luworkers; i++ ) {
			rc = pthread_mutex_destroy(&spec->luworker_mutex[i]);
			rc = pthread_cond_destroy(&spec->luworker_cond[i]);
#ifdef	REPLICATION
			rc = pthread_mutex_destroy(&spec->luworker_rmutex[i]);
			rc = pthread_cond_destroy(&spec->luworker_rcond[i]);
#endif
			pthread_mutex_destroy(&spec->lu_tmf_mutex[i]);
			pthread_cond_destroy(&spec->lu_tmf_cond[i]);
		}

		rc = pthread_mutex_destroy(&spec->wait_lu_task_mutex);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
			/* ignore error */
		}
		rc = pthread_mutex_destroy(&spec->clone_mutex);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: clone_mutex_destroy() failed\n", lu->num);
			/* ignore error */
		}
		rc = pthread_mutex_destroy(&spec->pr_rsv_mutex);
		if (rc != 0) {
			ISTGT_ERRLOG("LU%d: mutex_destroy() failed\n", lu->num);
			/* ignore error */
		}
		rc = pthread_mutex_destroy(&spec->state_mutex);
		if (rc != 0){
			/* ignore error */
		}
		xfree(spec->watsbuf);
		while ((nexus = TAILQ_FIRST(&spec->nexus))) {
			TAILQ_REMOVE(&spec->nexus, nexus, nexus_next);
			xfree(nexus);
		}
#ifdef	REPLICATION
		xfree(spec->volname);
#endif
		xfree(spec);
		lu->lun[i].spec = NULL;
	}

	return 0;
}

void
istgt_scsi_dump_cdb(uint8_t *cdb)
{
	int group;
	int cdblen = 0;
	int i;

	if (cdb == NULL)
		return;

	group = (cdb[0] >> 5) & 0x07;
	switch (group) {
	case 0x00:
		/* 6byte commands */
		cdblen = 6;
		break;
	case 0x01:
		/* 10byte commands */
		cdblen = 10;
		break;
	case 0x02:
		/* 10byte commands */
		cdblen = 10;
		break;
	case 0x03:
		/* reserved */
		if (cdb[0] == 0x7f) {
			/* variable length */
			cdblen = 8 + (cdb[7] & 0xff);
		} else {
			/* XXX */
			cdblen = 6;
		}
		break;
	case 0x04:
		/* 16byte commands */
		cdblen = 16;
		break;
	case 0x05:
		/* 12byte commands */
		cdblen = 12;
		break;
	case 0x06:
	case 0x07:
		/* vendor specific */
		cdblen = 6;
		break;
	}

	printf("CDB=");
	for (i = 0; i < cdblen; i++) {
		printf("%2.2x ", cdb[i]);
	}
	printf("\n");
}

void
istgt_strcpy_pad(uint8_t *dst, size_t size, const char *src, int pad)
{
	size_t len;

	len = strlen(src);
	if (len < size) {
		memcpy(dst, src, len);
		memset(dst + len, pad, (size - len));
	} else {
		memcpy(dst, src, size);
	}
}

#ifdef HAVE_UUID_H
uint64_t
istgt_uuid2uint64(uuid_t *uuid)
{
	uint64_t low, mid, hi;
	uint64_t r;

	low = (uint64_t) uuid->time_low;
	mid = (uint64_t) uuid->time_mid;
	hi  = (uint64_t) uuid->time_hi_and_version;
	r = (hi & 0xffffULL) << 48;
	r |= (mid & 0xffffULL) << 32;
	r |= (low & 0xffffffffULL);
	return r;
}
#endif /* HAVE_UUID_H */

uint64_t
istgt_get_lui(const char *name, int lun)
{
	char buf[MAX_TMPBUF];
	uint32_t crc32c;
	uint64_t r;

	if (lun >= 0) {
		snprintf(buf, sizeof buf, "%s,%d",
		    name, lun);
	} else {
		snprintf(buf, sizeof buf, "%s",
		    name);
	}
	crc32c = istgt_crc32c((uint8_t *) buf, strlen(buf));
	r = (uint64_t) crc32c;
	return r;
}

uint64_t
istgt_get_rkey(const char *initiator_name, uint64_t lui)
{
	ISTGT_MD5CTX md5ctx;
	uint8_t rkeymd5[ISTGT_MD5DIGEST_LEN];
	char buf[MAX_TMPBUF];
	uint64_t rkey;
	int idx;
	int i;

	snprintf(buf, sizeof buf, "%s,%16.16" PRIx64,
	    initiator_name, lui);

	istgt_md5init(&md5ctx);
	istgt_md5update(&md5ctx, buf, strlen(buf));
	istgt_md5final(rkeymd5, &md5ctx);

	rkey = 0U;
	idx = ISTGT_MD5DIGEST_LEN - 8;
	if (idx < 0) {
		ISTGT_WARNLOG("missing MD5 length\n");
		idx = 0;
	}
	for (i = idx; i < ISTGT_MD5DIGEST_LEN; i++) {
		rkey |= (uint64_t) rkeymd5[i];
		rkey = rkey << 8;
	}
	return rkey;
}

/* XXX */
#define COMPANY_ID 0xACDE48U // 24bits

int
istgt_lu_set_lid(uint8_t *buf, uint64_t vid)
{
	uint64_t naa;
	uint64_t enc;
	int total;

	naa = 0x3; // Locally Assigned

	/* NAA + LOCALLY ADMINISTERED VALUE */
	enc = (naa & 0xfULL) << (64-4); // 4bits
	enc |= vid & 0xfffffffffffffffULL; //60bits
	DSET64(&buf[0], enc);

	total = 8;
	return total;
}

int
istgt_lu_set_id(uint8_t *buf, uint64_t vid)
{
	uint64_t naa;
	uint64_t cid;
	uint64_t enc;
	int total;

	naa = 0x5; // IEEE Registered
	cid = COMPANY_ID; //IEEE COMPANY_ID

	/* NAA + COMPANY_ID + VENDOR SPECIFIC IDENTIFIER */
	enc = (naa & 0xfULL) << (64-4); // 4bits
	enc |= (cid & 0xffffffULL) << (64-4-24); // 24bits
	enc |= vid & 0xfffffffffULL; //36bits
	DSET64(&buf[0], enc);

	total = 8;
	return total;
}

int
istgt_lu_set_extid(uint8_t *buf, uint64_t vid, uint64_t vide)
{
	uint64_t naa;
	uint64_t cid;
	uint64_t enc;
	int total;

	naa = 0x6; // IEEE Registered Extended
	cid = COMPANY_ID; //IEEE COMPANY_ID

	/* NAA + COMPANY_ID + VENDOR SPECIFIC IDENTIFIER */
	enc = (naa & 0xfULL) << (64-4); // 4bits
	enc |= (cid & 0xffffffULL) << (64-4-24); // 24bits
	enc |= vid & 0xfffffffffULL; //36bits
	DSET64(&buf[0], enc);
	/* VENDOR SPECIFIC IDENTIFIER EXTENSION */
	DSET64(&buf[8], vide);

	total = 16;
	return total;
}

static int
istgt_lu_disk_scsi_report_luns(ISTGT_LU_Ptr lu, CONN_Ptr conn __attribute__((__unused__)), uint8_t *cdb __attribute__((__unused__)), int sel, uint8_t *data, int alloc_len)
{
	uint64_t fmt_lun, lun, method;
	int hlen = 0, len = 0;
	int i;

	if (alloc_len < 8) {
		return -1;
	}

	if (sel == 0x00) {
		/* logical unit with addressing method */
	} else if (sel == 0x01) {
		/* well known logical unit */
	} else if (sel == 0x02) {
		/* logical unit */
	} else {
		return -1;
	}

	/* LUN LIST LENGTH */
	DSET32(&data[0], 0);
	/* Reserved */
	DSET32(&data[4], 0);
	hlen = 8;

	for (i = 0; i < lu->maxlun; i++) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
#if 0
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
			    lu->num, i);
#endif
			continue;
		}
		if (alloc_len - (hlen + len) < 8) {
			return -1;
		}
		lun = (uint64_t) i;
		if (lu->maxlun <= 0x0100) {
			/* below 256 */
			method = 0x00U;
			fmt_lun = (method & 0x03U) << 62;
			fmt_lun |= (lun & 0x00ffU) << 48;
		} else if (lu->maxlun <= 0x4000) {
			/* below 16384 */
			method = 0x01U;
			fmt_lun = (method & 0x03U) << 62;
			fmt_lun |= (lun & 0x3fffU) << 48;
		} else {
			/* XXX */
			fmt_lun = 0;
		}
		/* LUN */
		DSET64(&data[hlen + len], fmt_lun);
		len += 8;
	}
	/* LUN LIST LENGTH */
	DSET32(&data[0], len);
	return hlen + len;
}

//extern int enable_xcopy;
extern int enable_oldBL;

static int
istgt_lu_disk_scsi_inquiry(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, uint8_t *data, int alloc_len)
{
	uint64_t LUI;
	uint8_t *cp, *cp2;
	uint32_t blocks;
	int hlen = 0, len = 0, plen, plen2;
	int pc;
	int pq, pd;
	int rmb;
	int evpd;
	int pg_tag;
	int i, j;
	uint64_t *tptr;
	if (alloc_len < 0xff) {
		return -1;
	}

	pq = 0x00;
	pd = SPC_PERIPHERAL_DEVICE_TYPE_DISK;
	rmb = 0;

#if 0
	LUI = istgt_uuid2uint64(&spec->uuid);
#else
	LUI = istgt_get_lui(spec->lu->name, spec->lun & 0xffffU);
#endif

	pc = cdb[2];
	evpd = BGET8(&cdb[1], 0);
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d INQUIRY\n", conn->id);
	if (evpd) {
		/* Vital product data */
		switch (pc) {
		case SPC_VPD_SUPPORTED_VPD_PAGES:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* Reserved */
			data[2] = 0;
			/* PAGE LENGTH */
			data[3] = 0;
			hlen = 4;

			data[4] = SPC_VPD_SUPPORTED_VPD_PAGES;      /* 0x00 */
			data[5] = SPC_VPD_UNIT_SERIAL_NUMBER;       /* 0x80 */
			data[6] = SPC_VPD_DEVICE_IDENTIFICATION;    /* 0x83 */
			data[7] = SPC_VPD_MANAGEMENT_NETWORK_ADDRESSES; /* 0x85 */
			data[8] = SPC_VPD_EXTENDED_INQUIRY_DATA;    /* 0x86 */
			data[9] = SPC_VPD_MODE_PAGE_POLICY;         /* 0x87 */
			data[10]= SPC_VPD_SCSI_PORTS;               /* 0x88 */
			data[11]= 0xb0; /* SBC Block Limits */
			data[12]= 0xb1; /* SBC Block Device Characteristics */
			len = 13 - hlen;
			if (spec->unmap) {
				data[13]= 0xb2; /* SBC Thin Provisioning */
				len++;
			}

			/* PAGE LENGTH */
			data[3] = len;
			break;

		case SPC_VPD_UNIT_SERIAL_NUMBER:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* Reserved */
			data[2] = 0;
			/* PAGE LENGTH */
			data[3] = 0;
			hlen = 4;

			/* PRODUCT SERIAL NUMBER */
			if (spec->lu->lun[spec->lun].serial != NULL) {
				len = strlen(spec->lu->lun[spec->lun].serial);
				if (len > MAX_LU_SERIAL_STRING) {
					len = MAX_LU_SERIAL_STRING;
				}
				istgt_strcpy_pad(&data[4], len,
				    spec->lu->lun[spec->lun].serial, ' ');
			} else {
				len = strlen(spec->lu->inq_serial);
				if (len > MAX_LU_SERIAL_STRING) {
					len = MAX_LU_SERIAL_STRING;
				}
				istgt_strcpy_pad(&data[4], len,
				    spec->lu->inq_serial, ' ');
			}

			/* PAGE LENGTH */
			data[3] = len;
			break;

		case SPC_VPD_DEVICE_IDENTIFICATION:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* Identification descriptor 1 */
			/* Logical Unit */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], 0, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_BINARY, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 0, 7, 1); /* PIV=0 */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_LOGICAL_UNIT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_NAA, 3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			DSET8(&data[3], 8);
			//cp[3] = 0;

			/* IDENTIFIER */
#if 0
			/* 16bytes ID */
			plen = istgt_lu_set_extid(&cp[4], 0, LUI);
#else
			plen = istgt_lu_set_lid(&cp[4], LUI);


			cp[3] = plen;
			len += 4 + plen;
#endif
			/* Identification descriptor 2 */
			/* T10 VENDOR IDENTIFICATION */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], 0, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_UTF8, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 0, 7, 1); /* PIV=0 */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_LOGICAL_UNIT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_T10_VENDOR_ID, 3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			/* T10 VENDOR IDENTIFICATION */
			istgt_strcpy_pad(&cp[4], 8, spec->lu->inq_vendor, ' ');
			plen = 8;
			/* VENDOR SPECIFIC IDENTIFIER */
			/* PRODUCT IDENTIFICATION */
			istgt_strcpy_pad(&cp[12], 16, spec->lu->inq_product, ' ');
			/* PRODUCT SERIAL NUMBER */
			if (spec->lu->lun[spec->lun].serial != NULL) {
				istgt_strcpy_pad(&cp[28], MAX_LU_SERIAL_STRING,
				    spec->lu->lun[spec->lun].serial, ' ');
			} else {
				istgt_strcpy_pad(&cp[28], MAX_LU_SERIAL_STRING,
				    spec->lu->inq_serial, ' ');
			}
			plen += 16 + MAX_LU_SERIAL_STRING;

			cp[3] = plen;
			len += 4 + plen;

			/* Identification descriptor 3 */
			/* Target Device */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_UTF8, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 1, 7, 1); /* PIV */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_TARGET_DEVICE, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_SCSI_NAME, 3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			plen = snprintf((char *) &cp[4], MAX_TARGET_NAME,
			    "%s", spec->lu->name);
			cp[3] = plen;
			len += 4 + plen;

			/* Identification descriptor 4 */
			/* Target Port */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_UTF8, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 1, 7, 1); /* PIV */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_TARGET_PORT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_SCSI_NAME, 3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			plen = snprintf((char *) &cp[4], MAX_TARGET_NAME,
			    "%s"",t,0x""%4.4x", spec->lu->name, conn->portal.tag);
			cp[3] = plen;
			len += 4 + plen;

			/* Identification descriptor 5 */
			/* Relative Target Port */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_BINARY, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 1, 7, 1); /* PIV */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_TARGET_PORT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_RELATIVE_TARGET_PORT,
			    3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			/* Obsolete */
			DSET16(&cp[4], 0);
			/* Relative Target Port Identifier */
			//DSET16(&cp[6], 1); /* port1 as port A */
			//DSET16(&cp[6], 2); /* port2 as port B */
			DSET16(&cp[6], (uint16_t) (1 + conn->portal.idx));
			plen = 4;

			cp[3] = plen;
			len += 4 + plen;

			/* Identification descriptor 6 */
			/* Target port group */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_BINARY, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 1, 7, 1); /* PIV */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_TARGET_PORT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_TARGET_PORT_GROUP,
			    3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			/* Reserved */
			DSET16(&cp[4], 0);
			/* TARGET PORT GROUP */
			DSET16(&cp[6], (uint16_t) (conn->portal.tag));
			plen = 4;

			cp[3] = plen;
			len += 4 + plen;

			/* Identification descriptor 7 */
			/* Logical unit group */
			cp = &data[hlen + len];

			/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
			BDSET8W(&cp[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
			BDADD8W(&cp[0], SPC_VPD_CODE_SET_BINARY, 3, 4);
			/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
			BDSET8W(&cp[1], 1, 7, 1); /* PIV */
			BDADD8W(&cp[1], SPC_VPD_ASSOCIATION_TARGET_PORT, 5, 2);
			BDADD8W(&cp[1], SPC_VPD_IDENTIFIER_TYPE_LOGICAL_UNIT_GROUP,
			    3, 4);
			/* Reserved */
			cp[2] = 0;
			/* IDENTIFIER LENGTH */
			cp[3] = 0;

			/* IDENTIFIER */
			/* Reserved */
			DSET16(&cp[4], 0);
			/* LOGICAL UNIT GROUP */
			DSET16(&cp[6], (uint16_t) (spec->lu->num));
			plen = 4;

			cp[3] = plen;
			len += 4 + plen;

			/* PAGE LENGTH */
			if (len > 0xffff) {
				len = 0xffff;
			}
			DSET16(&data[2], len);
			break;

		case SPC_VPD_EXTENDED_INQUIRY_DATA:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* Reserved */
			data[2] = 0;
			/* PAGE LENGTH */
			data[3] = 0;
			hlen = 4;

			/* RTO(3) GRD_CHK(2) APP_CHK(1) REF_CHK(0) */
			data[4] = 0;
			/* GROUP_SUP(4) PRIOR_SUP(3) HEADSUP(2) ORDSUP(1) SIMPSUP(0) */
			data[5] = 0;
			if (spec->queue_depth != 0) {
				BDADD8(&data[5], 1, 2);     /* HEADSUP */
				//BDADD8(&data[5], 1, 1);     /* ORDSUP */
				BDADD8(&data[5], 1, 0);     /* SIMPSUP */
			}
			/* NV_SUP(1) V_SUP(0) */
			data[6] = 0;
			/* Reserved[7-63] */
			//memset(&data[7], 0, (64 - 7));
			data[7] = 0;
			tptr = (uint64_t *)&(data[8]);
			*tptr = 0; *(tptr+1) = 0; *(tptr+2) = 0; *(tptr+3) = 0;
			*(tptr+4) = 0; *(tptr+5) = 0; *(tptr+6) = 0;

			len = 64 - hlen;

			/* PAGE LENGTH */
			data[3] = len;
			break;

		case SPC_VPD_MANAGEMENT_NETWORK_ADDRESSES:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

#if 0
			/* Network services descriptor N */
			cp = &data[hlen + len];

			/* ASSOCIATION(6-5) SERVICE TYPE(4-0) */
			BDSET8W(&cp[0], 0x00, 6, 2);
			BDADD8W(&cp[0], 0x00, 4, 5);
			/* Reserved */
			cp[1] = 0;
			/* NETWORK ADDRESS LENGTH */
			DSET16(&cp[2], 0);
			/* NETWORK ADDRESS */
			cp[4] = 0;
			/* ... */
			plen = 0;
			DSET16(&cp[2], plen);
			len += 4 + plen;
#endif

			/* PAGE LENGTH */
			if (len > 0xffff) {
				len = 0xffff;
			}
			DSET16(&data[2], len);
			break;

		case SPC_VPD_MODE_PAGE_POLICY:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* Mode page policy descriptor 1 */
			cp = &data[hlen + len];

			/* POLICY PAGE CODE(5-0) */
			BDSET8W(&cp[0], 0x3f, 5, 6);    /* all page code */
			/* POLICY SUBPAGE CODE */
			cp[1] = 0xff;                   /* all sub page */
			/* MLUS(7) MODE PAGE POLICY(1-0) */
			//BDSET8(&cp[2], 1, 7); /* multiple logical units share */
			BDSET8(&cp[2], 0, 7); /* own copy */
			BDADD8W(&cp[2], 0x00, 1, 2); /* Shared */
			//BDADD8W(&cp[2], 0x01, 1, 2); /* Per target port */
			//BDADD8W(&cp[2], 0x02, 1, 2); /* Per initiator port */
			//BDADD8W(&cp[2], 0x03, 1, 2); /* Per I_T nexus */
			/* Reserved */
			cp[3] = 0;
			len += 4;

			/* PAGE LENGTH */
			if (len > 0xffff) {
				len = 0xffff;
			}
			DSET16(&data[2], len);
			break;

		case SPC_VPD_SCSI_PORTS:
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* Identification descriptor list */
			for (i = 0; i < spec->lu->maxmap; i++) {
				pg_tag = spec->lu->map[i].pg_tag;
				/* skip same pg_tag */
				for (j = 0; j < i; j++) {
					if (spec->lu->map[j].pg_tag == pg_tag) {
						goto skip_pg_tag;
					}
				}

				/* Identification descriptor N */
				cp = &data[hlen + len];

				/* Reserved */
				DSET16(&cp[0], 0);
				/* RELATIVE PORT IDENTIFIER */
				DSET16(&cp[2], (uint16_t) (1 + pg_tag));
				/* Reserved */
				DSET16(&cp[4], 0);
				/* INITIATOR PORT TRANSPORTID LENGTH */
				DSET16(&cp[6], 0);
				/* Reserved */
				DSET16(&cp[8], 0);
				/* TARGET PORT DESCRIPTORS LENGTH */
				DSET16(&cp[10], 0);
				len += 12;

				plen2 = 0;
				/* Target port descriptor 1 */
				cp2 = &data[hlen + len + plen2];

				/* PROTOCOL IDENTIFIER(7-4) CODE SET(3-0) */
				BDSET8W(&cp2[0], SPC_PROTOCOL_IDENTIFIER_ISCSI, 7, 4);
				BDADD8W(&cp2[0], SPC_VPD_CODE_SET_UTF8, 3, 4);
				/* PIV(7) ASSOCIATION(5-4) IDENTIFIER TYPE(3-0) */
				BDSET8W(&cp2[1], 1, 7, 1); /* PIV */
				BDADD8W(&cp2[1], SPC_VPD_ASSOCIATION_TARGET_PORT, 5, 2);
				BDADD8W(&cp2[1], SPC_VPD_IDENTIFIER_TYPE_SCSI_NAME, 3, 4);
				/* Reserved */
				cp2[2] = 0;
				/* IDENTIFIER LENGTH */
				cp2[3] = 0;

				/* IDENTIFIER */
				plen = snprintf((char *) &cp2[4], MAX_TARGET_NAME,
				    "%s"",t,0x""%4.4x", spec->lu->name, pg_tag);
				cp2[3] = plen;
				plen2 += 4 + plen;

				/* TARGET PORT DESCRIPTORS LENGTH */
				DSET16(&cp[10], plen2);
				len += plen2;
			skip_pg_tag:
				;
			}

			/* PAGE LENGTH */
			if (len > 0xffff) {
				len = 0xffff;
			}
			DSET16(&data[2], len);
			break;

		case 0xb0: /* SBC Block Limits */
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* WSNZ(0) */
			BDSET8(&data[4], 0, 0); /* support zero length in WRITE SAME */

			/* MAXIMUM COMPARE AND WRITE LENGTH */
			if (spec->lu->lun[0].ats) {
				blocks = ISTGT_LU_WORK_ATS_BLOCK_SIZE / (uint32_t) spec->blocklen;
				if (blocks > 0xff)
					blocks = 0xff;
				data[5] = (uint8_t) blocks;
			}
			if (spec->lu->istgt->swmode == ISTGT_SWMODE_TRADITIONAL) {
				/* no support compare and write */
				data[5] = 0;
			}

			if (enable_oldBL == 1) {
				/* force align to 4KB */
				if (spec->blocklen < 4096)
					blocks = 4096 / (uint32_t) spec->blocklen;
				else
					blocks = 1;
			} else {
				blocks = spec->lb_per_rec;
				if (blocks == 0) //this would not happen
					blocks = 1;
			}
			/* OPTIMAL TRANSFER LENGTH GRANULARITY */
			DSET16(&data[6], blocks);
			/* MAXIMUM TRANSFER LENGTH in blocks Blocksize = spec->blocklen, max length = spec->blocklen*max_blocks*/
			uint32_t max_blocks = conn->MaxBurstLength/(uint32_t)spec->blocklen + 1;
			DSET32(&data[8], max_blocks);/* While changing this value keep track of iobuf boundaries */
			/* OPTIMAL TRANSFER LENGTH */
			if (spec->opt_tlen == 0 || spec->rsize == 0) {
				blocks = ISTGT_LU_WORK_BLOCK_SIZE / (uint32_t) spec->blocklen;
			} else {
				blocks = spec->lb_per_rec * spec->opt_tlen;
				if (blocks == 0) //this would not happen
					blocks = 1;
				if ((blocks * (uint32_t)spec->blocklen) > ISTGT_LU_WORK_BLOCK_SIZE) {
					blocks = ISTGT_LU_WORK_BLOCK_SIZE / (uint32_t) spec->blocklen;
				}
			}
			DSET32(&data[12], blocks);
			/* MAXIMUM PREFETCH XDREAD XDWRITE TRANSFER LENGTH */
			DSET32(&data[16], 0);

			if (spec->unmap) {
				/* MAXIMUM UNMAP LBA COUNT */
				unsigned int max_unmap_sectors = spec->max_unmap_sectors;
				DSET32(&data[20], max_unmap_sectors);
				/* MAXIMUM UNMAP BLOCK DESCRIPTOR COUNT */
				data[24] = 0xFF;
				data[25] = 0xFF;
				data[26] = 0xFF;
				data[27] = 0xFF;

				/* OPTIMAL UNMAP GRANULARITY */
				DSET32(&data[28], spec->lb_per_rec);

				/* UNMAP GRANULARITY ALIGNMENT &  UGAVALID(7) */
				data[32] = 0x80;
				data[33] = 0; data[34] = 0; data[35] = 0;

				/* MAXIMUM WRITE SAME LENGTH */
				DSET64(&data[36], 0); /* no limit */
				/* Reserved */
				memset(&data[44], 0x00, 64-44);
				len = 64 - hlen;
			} else {
				/* MAXIMUM UNMAP LBA COUNT */
				DSET32(&data[20], 0); /* not implement UNMAP */
				/* MAXIMUM UNMAP BLOCK DESCRIPTOR COUNT */
				DSET32(&data[24], 0); /* not implement UNMAP */
				/* OPTIMAL UNMAP GRANULARITY */
				DSET32(&data[28], 0); /* not specified */
				/* UNMAP GRANULARITY ALIGNMENT */
				DSET32(&data[32], (0 & 0x7fffffffU));
				/* UGAVALID(7) */
				BDADD8(&data[32], 0, 7); /* not valid ALIGNMENT */
				/* MAXIMUM WRITE SAME LENGTH */
				DSET64(&data[36], 0); /* no limit */
				/* Reserved */
				memset(&data[44], 0x00, 64-44);
				len = 64 - hlen;
			}

			DSET16(&data[2], len);
			break;

		case 0xb1: /* SBC Block Device Characteristics */
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* MEDIUM ROTATION RATE */
			//DSET16(&data[4], 0x0000); /* not reported */
			//DSET16(&data[4], 0x0001); /* Non-rotating medium (solid state) */
			//DSET16(&data[4], 5400); /* rotation rate (5400rpm) */
			//DSET16(&data[4], 7200); /* rotation rate (7200rpm) */
			//DSET16(&data[4], 10000); /* rotation rate (10000rpm) */
			//DSET16(&data[4], 15000); /* rotation rate (15000rpm) */
			if (spec->unmap) {
				DSET16(&data[4], 0x0001);
			} else {
				DSET16(&data[4], spec->lu->lun[spec->lun].rotationrate);
			}
			/* Reserved */
			data[6] = 0;
			/* NOMINAL FORM FACTOR(3-0) */
			//BDSET8W(&data[7], 0x00, 3, 4); /* not reported */
			//BDSET8W(&data[7], 0x01, 3, 4); /* 5.25 inch */
			//BDSET8W(&data[7], 0x02, 3, 4); /* 3.5 inch */
			//BDSET8W(&data[7], 0x03, 3, 4); /* 2.5 inch */
			//BDSET8W(&data[7], 0x04, 3, 4); /* 1.8 inch */
			//BDSET8W(&data[7], 0x05, 3, 4); /* less 1.8 inch */
			BDSET8W(&data[7], spec->lu->lun[spec->lun].formfactor, 3, 4);
			/* Reserved */
			memset(&data[8], 0x00, 64-8);

			len = 64 - hlen;
			DSET16(&data[2], len);
			break;

		case 0xb2: /* SBC Thin Provisioning */
			if (!spec->unmap) {
				ISTGT_ERRLOG("c#%d unsupported INQUIRY VPD page 0x%x\n", conn->id, pc);
				return -1;
			}

			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], pq, 7, 3);
			BDADD8W(&data[0], pd, 4, 5);
			/* PAGE CODE */
			data[1] = pc;
			/* PAGE LENGTH */
			DSET16(&data[2], 0);
			hlen = 4;

			/* THRESHOLD EXPONENT */
			data[4] = 0;
			/* DP(0) */
			//BDSET8(&data[5], 0, 0);
			//if (spec->unmap && spec->wsame)
			//		data[5] = 0xE0;  //LBPU unmap command; LBPWS, LBPWS10- write_same with unmap bit
			//else
			if (spec->unmap)
				data[5] = 0x80;
			else
				data[5] = 0;

			/* Reserved */
			data[6] = 0x02; //010: thin provisioned, 001: resource provisioned
			data[7] = 0;
			len = 8 - hlen;
#if 0
			/* XXX not yet */
			/* PROVISIONING GROUP DESCRIPTOR ... */
			DSET16(&data[8], 0);
			len = 8 - hlen;
#endif

			DSET16(&data[2], len);
			break;

		default:
			if (pc >= 0xc0 && pc <= 0xff) {
				ISTGT_WARNLOG("c#%d Vendor specific INQUIRY VPD page 0x%x\n", conn->id, pc);
			} else {
				ISTGT_ERRLOG("unsupported INQUIRY VPD page 0x%x\n", pc);
			}
			return -1;
		}
	} else {
		/* Standard INQUIRY data */
		/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
		BDSET8W(&data[0], pq, 7, 3);
		BDADD8W(&data[0], pd, 4, 5);
		/* RMB(7) */
		BDSET8W(&data[1], rmb, 7, 1);
		/* VERSION */
		/* See SPC3/SBC2/MMC4/SAM2 for more details */
		data[2] = SPC_VERSION_SPC3;
		/* NORMACA(5) HISUP(4) RESPONSE DATA FORMAT(3-0) */
		BDSET8W(&data[3], 2, 3, 4);		/* format 2 */
		BDADD8(&data[1], 1, 4);         /* hierarchical support */
		/* ADDITIONAL LENGTH */
		data[4] = 0;
		hlen = 5;

		/* SCCS(7) ACC(6) TPGS(5-4) 3PC(3) PROTECT(0) */
		data[5] = 0;
		//BDADD8W(&data[5], 1, 7, 1); /* storage array controller */
		BDADD8W(&data[5], 0x00, 5, 2); /* Not support TPGS */
		if (spec->xcopy == 1) {
			BDADD8(&data[5], 1, 3); /* support 3PC EXTETNDED COPY */
		}
		//BDADD8W(&data[5], 0x01, 5, 2); /* Only implicit */
		//BDADD8W(&data[5], 0x02, 5, 2); /* Only explicit */
		//BDADD8W(&data[5], 0x03, 5, 2); /* Both explicit and implicit */
		/* BQUE(7) ENCSERV(6) VS(5) MULTIP(4) MCHNGR(3) ADDR16(0) */
		data[6] = 0;
		BDADD8W(&data[6], 1, 4, 1); /* MULTIP */
		/* WBUS16(5) SYNC(4) LINKED(3) CMDQUE(1) VS(0) */
		data[7] = 0;
		if (spec->queue_depth != 0) {
			BDADD8(&data[7], 1, 1);     /* CMDQUE */
		}
		/* T10 VENDOR IDENTIFICATION */
		istgt_strcpy_pad(&data[8], 8, spec->lu->inq_vendor, ' ');
		/* PRODUCT IDENTIFICATION */
		istgt_strcpy_pad(&data[16], 16, spec->lu->inq_product, ' ');
		/* PRODUCT REVISION LEVEL */
		istgt_strcpy_pad(&data[32], 4, spec->lu->inq_revision, ' ');
		/* Vendor specific */
		memset(&data[36], 0x20, 20);
		/* CLOCKING(3-2) QAS(1) IUS(0) */
		data[56] = 0;
		/* Reserved */
		data[57] = 0;
		/* VERSION DESCRIPTOR 1-8 */
		DSET16(&data[58], 0x0960); /* iSCSI (no version claimed) */
		DSET16(&data[60], 0x0300); /* SPC-3 (no version claimed) */
		DSET16(&data[62], 0x0320); /* SBC-2 (no version claimed) */
		DSET16(&data[64], 0x0040); /* SAM-2 (no version claimed) */
		DSET16(&data[66], 0x0000);
		DSET16(&data[68], 0x0000);
		DSET16(&data[70], 0x0000);
		DSET16(&data[72], 0x0000);
		/* Reserved[74-95] */
		memset(&data[74], 0, (96 - 74));
		/* Vendor specific parameters[96-n] */
		//data[96] = 0;
		len = 96 - hlen;

		/* ADDITIONAL LENGTH */
		data[4] = len;
	}

	return hlen + len;
}

#define MODE_SENSE_PAGE_INIT(B,L,P,SP)					\
	do {								\
		memset((B), 0, (L));					\
		if ((SP) != 0x00) {					\
			(B)[0] = (P) | 0x40; /* PAGE + SPF=1 */		\
			(B)[1] = (SP);					\
			DSET16(&(B)[2], (L) - 4);			\
		} else {						\
			(B)[0] = (P);					\
			(B)[1] = (L) - 2;				\
		}							\
	} while (0)

static int
istgt_lu_disk_scsi_mode_sense_page(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, int pc, int page, int subpage, uint8_t *data, int alloc_len)
{
	uint8_t *cp;
	int len = 0;
	int plen;
	int i;

#if 0
	printf("pc=%d, page=%2.2x, subpage=%2.2x\n", pc, page, subpage);
#endif
#if 0
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "MODE_SENSE: pc=%d, page=%2.2x, subpage=%2.2x\n", pc, page, subpage);
#endif

	if (pc == 0x00) {
		/* Current values */
	} else if (pc == 0x01) {
		/* Changeable values */
		if (page != 0x08) {
			/* not supported */
			return -1;
		}
	} else if (pc == 0x02) {
		/* Default values */
	} else {
		/* Saved values */
	}

	cp = &data[len];
	switch (page) {
	case 0x00:
		/* Vendor specific */
		break;
	case 0x01:
		/* Read-Write Error Recovery */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Read-Write Error Recovery\n", conn->id);
		if (subpage != 0x00)
			break;
		plen = 0x0a + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x02:
		/* Disconnect-Reconnect */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Disconnect-Reconnect\n", conn->id);
		if (subpage != 0x00)
			break;
		plen = 0x0e + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x03:
		/* Obsolete (Format Device) */
		break;
	case 0x04:
		/* Obsolete (Rigid Disk Geometry) */
		break;
	case 0x05:
		/* Obsolete (Rigid Disk Geometry) */
		break;
	case 0x06:
		/* Reserved */
		break;
	case 0x07:
		/* Verify Error Recovery */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Verify Error Recovery\n", conn->id);
		if (subpage != 0x00)
			break;
		plen = 0x0a + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x08:
		/* Caching */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Caching\n", conn->id);
		if (subpage != 0x00)
			break;

		plen = 0x12 + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		BDADD8(&cp[0], 1, 7); /* PS */
		if (pc == 0x01) {
			// Changeable values
			BDADD8(&cp[2], 1, 2); /* WCE */
			BDADD8(&cp[2], 1, 0); /* RCD */
			len += plen;
			break;
		}
		BDADD8(&cp[2], 1, 2); /* WCE */
		//BDADD8(&cp[2], 1, 0); /* RCD */
#ifndef	REPLICATION
		{
			int fd, rc;
			fd = spec->fd;
			rc = fcntl(fd , F_GETFL, 0);
			if (rc != -1 && !(rc & O_FSYNC)) {
				BDADD8(&cp[2], 1, 2); /* WCE=1 */
			} else {
				BDADD8(&cp[2], 0, 2); /* WCE=0 */
			}
		}
#endif
		if (spec->readcache) {
			BDADD8(&cp[2], 0, 0); /* RCD=0 */
		} else {
			BDADD8(&cp[2], 1, 0); /* RCD=1 */
		}
		len += plen;
		break;
	case 0x09:
		/* Obsolete */
		break;
	case 0x0a:
		switch (subpage) {
		case 0x00:
			/* Control */
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Control\n", conn->id);
			plen = 0x0a + 2;
			MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
			len += plen;
			break;
		case 0x01:
			/* Control Extension */
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Control Extension\n", conn->id);
			plen = 0x1c + 4;
			MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
			len += plen;
			break;
		case 0xff:
			/* All subpages */
			len += istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, page, 0x00, &data[len], alloc_len);
			len += istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, page, 0x01, &data[len], alloc_len);
			break;
		default:
			/* 0x02-0x3e: Reserved */
			break;
		}
		break;
	case 0x0b:
		/* Obsolete (Medium Types Supported) */
		break;
	case 0x0c:
		/* Obsolete (Notch And Partitio) */
		break;
	case 0x0d:
		/* Obsolete */
		break;
	case 0x0e:
	case 0x0f:
		/* Reserved */
		break;
	case 0x10:
		/* XOR Control */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE XOR Control\n", conn->id);
		if (subpage != 0x00)
			break;
		plen = 0x16 + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x11:
	case 0x12:
	case 0x13:
		/* Reserved */
		break;
	case 0x14:
		/* Enclosure Services Management */
		break;
	case 0x15:
	case 0x16:
	case 0x17:
		/* Reserved */
		break;
	case 0x18:
		/* Protocol-Specific LUN */
#if 0
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "MODE_SENSE Protocol-Specific LUN\n");
		if (subpage != 0x00)
			break;
		plen = 0x04 + 0x00 + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
#endif
		break;
	case 0x19:
		/* Protocol-Specific Port */
#if 0
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "MODE_SENSE Protocol-Specific Port\n");
		if (subpage != 0x00)
			break;
		plen = 0x04 + 0x00 + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
#endif
		break;
	case 0x1a:
		/* Power Condition */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Power Condition\n", conn->id);
		if (subpage != 0x00)
			break;
		plen = 0x0a + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x1b:
		/* Reserved */
		break;
	case 0x1c:
		/* Informational Exceptions Control */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SENSE Informational Exceptions Control\n", conn->id);
		if (subpage != 0x00)
			break;

		plen = 0x0a + 2;
		MODE_SENSE_PAGE_INIT(cp, plen, page, subpage);
		len += plen;
		break;
	case 0x1d:
	case 0x1e:
	case 0x1f:
		/* Reserved */
		break;
	case 0x20:
	case 0x21:
	case 0x22:
	case 0x23:
	case 0x24:
	case 0x25:
	case 0x26:
	case 0x27:
	case 0x28:
	case 0x29:
	case 0x2a:
	case 0x2b:
	case 0x2c:
	case 0x2d:
	case 0x2e:
	case 0x2f:
	case 0x30:
	case 0x31:
	case 0x32:
	case 0x33:
	case 0x34:
	case 0x35:
	case 0x36:
	case 0x37:
	case 0x38:
	case 0x39:
	case 0x3a:
	case 0x3b:
	case 0x3c:
	case 0x3d:
	case 0x3e:
		/* Vendor-specific */
		break;
	case 0x3f:
		switch (subpage) {
		case 0x00:
			/* All mode pages */
			for (i = 0x00; i < 0x3e; i ++) {
				len += istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, i, 0x00, &cp[len], alloc_len);
			}
			break;
		case 0xff:
			/* All mode pages and subpages */
			for (i = 0x00; i < 0x3e; i ++) {
				len += istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, i, 0x00, &cp[len], alloc_len);
			}
			for (i = 0x00; i < 0x3e; i ++) {
				len += istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, i, 0xff, &cp[len], alloc_len);
			}
			break;
		default:
			/* 0x01-0x3e: Reserved */
			break;
		}
	}

	return len;
}

static int
istgt_lu_disk_scsi_mode_sense6(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, int dbd, int pc, int page, int subpage, uint8_t *data, int alloc_len)
{
	uint8_t *cp;
	int hlen = 0, len = 0, plen;
	int total;
	int llbaa = 0;

	data[0] = 0;                    /* Mode Data Length */
	data[1] = 0;                    /* Medium Type */
	data[2] = 0;                    /* Device-Specific Parameter */
	if (spec->dpofua) {
		BDADD8(&data[2], 1, 4);     /* DPOFUA */
	}
	if (spec->lu->readonly) {
		BDADD8(&data[2], 1, 7);     /* WP */
	}
	data[3] = 0;                    /* Block Descripter Length */
	hlen = 4;

	cp = &data[4];
	if (dbd) {                      /* Disable Block Descripters */
		len = 0;
	} else {
		if (llbaa) {
			/* Number of Blocks */
			DSET64(&cp[0], spec->blockcnt);
			/* Reserved */
			DSET32(&cp[8], 0);
			/* Block Length */
			DSET32(&cp[12], (uint32_t) spec->blocklen);
			len = 16;
		} else {
			/* Number of Blocks */
			if (spec->blockcnt > 0xffffffffULL) {
				DSET32(&cp[0], 0xffffffffUL);
			} else {
				DSET32(&cp[0], (uint32_t) spec->blockcnt);
			}
			/* Block Length */
			DSET32(&cp[4], (uint32_t) spec->blocklen);
			len = 8;
		}
		cp += len;
	}
	data[3] = len;                  /* Block Descripter Length */

	plen = istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, page, subpage, &cp[0], alloc_len);
	if (plen < 0) {
		return -1;
	}
	cp += plen;

	total = hlen + len + plen;
	data[0] = total - 1;            /* Mode Data Length */

	return total;
}

static int
istgt_lu_disk_scsi_mode_sense10(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, int llbaa, int dbd, int pc, int page, int subpage, uint8_t *data, int alloc_len)
{
	uint8_t *cp;
	int hlen = 0, len = 0, plen;
	int total;

	DSET16(&data[0], 0);            /* Mode Data Length */
	data[2] = 0;                    /* Medium Type */
	data[3] = 0;                    /* Device-Specific Parameter */
	if (spec->dpofua) {
		BDADD8(&data[3], 1, 4);		/* DPOFUA */
	}
	if (spec->lu->readonly) {
		BDADD8(&data[3], 1, 7);     /* WP */
	}
	if (llbaa) {
		BDSET8(&data[4], 1, 1);      /* Long LBA */
	} else {
		BDSET8(&data[4], 0, 1);      /* Short LBA */
	}
	data[5] = 0;                    /* Reserved */
	DSET16(&data[6], 0);		    /* Block Descripter Length */
	hlen = 8;

	cp = &data[8];
	if (dbd) {                      /* Disable Block Descripters */
		len = 0;
	} else {
		if (llbaa) {
			/* Number of Blocks */
			DSET64(&cp[0], spec->blockcnt);
			/* Reserved */
			DSET32(&cp[8], 0);
			/* Block Length */
			DSET32(&cp[12], (uint32_t) spec->blocklen);
			len = 16;
		} else {
			/* Number of Blocks */
			if (spec->blockcnt > 0xffffffffULL) {
				DSET32(&cp[0], 0xffffffffUL);
			} else {
				DSET32(&cp[0], (uint32_t) spec->blockcnt);
			}
			/* Block Length */
			DSET32(&cp[4], (uint32_t) spec->blocklen);
			len = 8;
		}
		cp += len;
	}
	DSET16(&data[6], len);          /* Block Descripter Length */

	plen = istgt_lu_disk_scsi_mode_sense_page(spec, conn, cdb, pc, page, subpage, &cp[0], alloc_len);
	if (plen < 0) {
		return -1;
	}
	cp += plen;

	total = hlen + len + plen;
	DSET16(&data[0], total - 2);	/* Mode Data Length */

	return total;
}

int
istgt_lu_disk_transfer_data(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, size_t len)
{
	int rc;

	if (lu_cmd->lu->queue_depth == 0) {
		rc = istgt_iscsi_transfer_out(conn, lu_cmd, len);
		if (rc < 0) {
			ISTGT_ERRLOG("c#%d iscsi_transfer_out()\n", conn->id);
			return -1;
		}
	}
	return 0;
}

static int
istgt_lu_disk_scsi_mode_select_page(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, int pf, int sp, uint8_t *data, size_t len)
{
	size_t hlen, plen;
	int spf, page, subpage;
	int rc;
	const char *msgwce = NULL;
	const char *msgrcd = NULL;
	if (pf == 0) {
		/* vendor specific */
		return 0;
	}

	if (len < 1)
		return 0;
	spf = BGET8(&data[0], 6);
	page = data[0] & 0x3f;
	if (spf) {
		/* Sub_page mode page format */
		hlen = 4;
		if (len < hlen)
			return 0;
		subpage = data[1];

		plen = DGET16(&data[2]);
	} else {
		/* Page_0 mode page format */
		hlen = 2;
		if (len < hlen)
			return 0;
		subpage = 0;
		plen = data[1];
	}
	plen += hlen;
	if (len < plen)
		return 0;

#if 0
	printf("ps=%d, page=%2.2x, subpage=%2.2x\n", ps, page, subpage);
#endif
	switch (page) {
	case 0x08:
		/* Caching */
		{
			int wce, rcd;

			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SELECT Caching\n", conn->id);
			if (subpage != 0x00)
				break;
			if (plen != 0x12 + hlen) {
				/* unknown format */
				break;
			}
			wce = BGET8(&data[2], 2); /* WCE */
			rcd = BGET8(&data[2], 0); /* RCD */
			(void) wce;
#ifndef	REPLICATION
			{
				int fd;
				fd = spec->fd;
				rc = fcntl(fd , F_GETFL, 0);
				if (rc != -1) {
					if (wce) {
						msgwce = "WCE";
						rc = fcntl(fd, F_SETFL, (rc & ~O_FSYNC));
						spec->writecache = 1;
					} else {
						msgwce = "WCE-off";
						rc = fcntl(fd, F_SETFL, (rc | O_FSYNC));
						spec->writecache = 0;
					}
					if (rc == -1) {
						/* XXX */
						//ISTGT_ERRLOG("fcntl(F_SETFL) failed\n");
					}
				}
			}
#endif
			if (rcd) {
				msgrcd = "RCD";
				spec->readcache = 0;
			} else {
				msgrcd = "RCD-off";
				spec->readcache = 1;
			}
		}
		break;
	default:
		/* not supported */
		break;
	}

	len -= plen;
	if (len != 0) {
		rc = istgt_lu_disk_scsi_mode_select_page(spec, conn, cdb,  pf, sp, &data[plen], len);
		if (rc < 0) {
			if (msgwce || msgrcd) {
				ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SELECT %s %s (error..)\n", conn->id, msgwce, msgrcd);
			}
			return rc;
		}
	}
	if (msgwce || msgrcd) {
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE_SELECT %s %s\n", conn->id, msgwce, msgrcd);
	}
	return 0;
}

static int
istgt_lu_disk_scsi_read_defect10(ISTGT_LU_DISK *spec __attribute__((__unused__)), CONN_Ptr conn __attribute__((__unused__)), uint8_t *cdb __attribute__((__unused__)), int req_plist, int req_glist, int list_format, uint8_t *data, int alloc_len)
{
	int hlen = 0, len = 0;
	int total;

	if (alloc_len < 4) {
		return -1;
	}

	data[0] = 0;				/* Reserved */
	data[1] = 0;
	if (req_plist) {
		BDADD8(&data[1], 1, 4);		/* PLISTV */
	}
	if (req_glist) {
		BDADD8(&data[1], 1, 3);		/* GLISTV */
	}
	BDADD8W(&data[1], list_format, 2, 3);	/* DEFECT LIST FORMAT */
	DSET16(&data[2], 0);			/* DEFECT LIST LENGTH */
	hlen = 4;

	/* defect list (if any) */
	len = 0;

	total = hlen + len;
	DSET16(&data[2], total - hlen);		/* DEFECT LIST LENGTH */
	return total;
}

static int
istgt_lu_disk_scsi_read_defect12(ISTGT_LU_DISK *spec __attribute__((__unused__)), CONN_Ptr conn __attribute__((__unused__)), uint8_t *cdb __attribute__((__unused__)), int req_plist, int req_glist, int list_format, uint8_t *data, int alloc_len)
{
	int hlen = 0, len = 0;
	int total;

	if (alloc_len < 8) {
		return -1;
	}

	data[0] = 0;				/* Reserved */
	data[1] = 0;
	if (req_plist) {
		BDADD8(&data[1], 1, 4);		/* PLISTV */
	}
	if (req_glist) {
		BDADD8(&data[1], 1, 3);		/* GLISTV */
	}
	BDADD8W(&data[1], list_format, 2, 3);	/* DEFECT LIST FORMAT */
	data[2] = 0;				/* Reserved */
	data[3] = 0;				/* Reserved */
	DSET32(&data[4], 0);			/* DEFECT LIST LENGTH */
	hlen = 8;

	/* defect list (if any) */
	len = 0;

	total = hlen + len;
	DSET32(&data[4], total - hlen);		/* DEFECT LIST LENGTH */
	return total;
}

static int
istgt_lu_disk_scsi_report_target_port_groups(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb __attribute__((__unused__)), uint8_t *data, int alloc_len)
{
	ISTGT_Ptr istgt;
	ISTGT_LU_Ptr lu;
	uint8_t *cp;
	uint8_t *cp_count;
	int hlen = 0, len = 0, plen;
	int total;
	int pg_tag;
	int nports;
	int i, j, k;
	int ridx;

	if (alloc_len < 0xfff) {
		return -1;
	}

	istgt = conn->istgt;
	lu = spec->lu;

	/* RETURN DATA LENGTH */
	DSET32(&data[0], 0);
	hlen = 4;

	MTX_LOCK(&istgt->mutex);
	for (i = 0; i < lu->maxmap; i++) {
		pg_tag = lu->map[i].pg_tag;
		/* skip same pg_tag */
		for (j = 0; j < i; j++) {
			if (lu->map[j].pg_tag == pg_tag) {
				goto skip_pg_tag;
			}
		}

		/* Target port group descriptor N */
		cp = &data[hlen + len];

		/* PREF(7) ASYMMETRIC ACCESS STATE(3-0) */
		cp[0] = 0;
		BDSET8(&cp[0], 1, 7); /* PREF */
		switch (lu->map[j].pg_aas & 0x0f) {
		case AAS_ACTIVE_OPTIMIZED:
			BDADD8W(&cp[0], AAS_ACTIVE_OPTIMIZED, 3, 4);
			break;
		case AAS_ACTIVE_NON_OPTIMIZED:
			BDADD8W(&cp[0], AAS_ACTIVE_NON_OPTIMIZED, 3, 4);
			break;
		case AAS_STANDBY:
			BDADD8W(&cp[0], AAS_STANDBY, 3, 4);
			break;
		case AAS_UNAVAILABLE:
			BDADD8W(&cp[0], AAS_UNAVAILABLE, 3, 4);
			break;
		case AAS_TRANSITIONING:
			BDADD8W(&cp[0], AAS_TRANSITIONING, 3, 4);
			break;
		default:
			ISTGT_ERRLOG("c#%d unsupported AAS\n", conn->id);
			break;
		}
		/* T_SUP(7) U_SUP(3) S_SUP(2) S_SUP AN_SUP(1) AO_SUP(0) */
		cp[1] = 0;
		//BDADD8(&cp[1], 1, 7); /* transitioning supported */
		//BDADD8(&cp[1], 1, 3); /* unavailable supported */
		//BDADD8(&cp[1], 1, 2); /* standby supported */
		BDADD8(&cp[1], 1, 1); /* active/non-optimized supported */
		BDADD8(&cp[1], 1, 0); /* active/optimized supported */
		/* TARGET PORT GROUP */
		DSET16(&cp[2], pg_tag);
		/* Reserved */
		cp[4] = 0;
		/* STATUS CODE */
		if (lu->map[j].pg_aas & AAS_STATUS_IMPLICIT) {
			cp[5] = 0x02; /* by implicit */
		} else if (lu->map[j].pg_aas & AAS_STATUS_STPG) {
			cp[5] = 0x01; /* by SET TARGET PORT GROUPS */
		} else {
			cp[5] = 0;    /* No status */
		}
		/* Vendor specific */
		cp[6] = 0;
		/* TARGET PORT COUNT */
		cp[7] = 0;
		cp_count = &cp[7];
		plen = 8;
		len += plen;

		nports = 0;
		ridx = 0;

		for (j = 0; j < istgt->nportal_group; j++) {
			if (istgt->portal_group[j].tag == pg_tag) {
				for (k = 0; k < istgt->portal_group[j].nportals; k++) {
					/* Target port descriptor(s) */
					cp = &data[hlen + len];
					/* Obsolete */
					DSET16(&cp[0], 0);
					/* RELATIVE TARGET PORT IDENTIFIER */
					DSET16(&cp[2], (uint16_t) (1 + ridx));
					plen = 4;
					len += plen;
					nports++;
					ridx++;
				}
			} else {
				ridx += istgt->portal_group[j].nportals;
			}
		}

		if (nports > 0xff) {
			ISTGT_ERRLOG("c#%d too many portals in portal group\n", conn->id);
			MTX_UNLOCK(&istgt->mutex);
			return -1;
		}

		/* TARGET PORT COUNT */
		cp_count[0] = nports;

	skip_pg_tag:
		;
	}
	MTX_UNLOCK(&istgt->mutex);

	total = hlen + len;
	if (total > alloc_len) {
		ISTGT_ERRLOG("c#%d alloc_len(%d) too small\n", conn->id, alloc_len);
		return -1;
	}

	/* RETURN DATA LENGTH */
	DSET32(&data[0], total - 4);

	return total;
}

static int
istgt_lu_disk_scsi_set_target_port_groups(ISTGT_LU_DISK *spec, CONN_Ptr conn, uint8_t *cdb, uint8_t *data, int len)
{
	ISTGT_LU_Ptr lu;
	int pg_tag;
	int aas;
	int pg;
	int rc;
	int i;

	if (len < 4) {
		return -1;
	}

	lu = spec->lu;

	aas = BGET8W(&data[0], 3, 4);
	pg = DGET16(&data[2]);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d AAS=0x%x, PG=0x%4.4x\n", conn->id, aas, pg);

	for (i = 0; i < lu->maxmap; i++) {
		pg_tag = lu->map[i].pg_tag;
		if (pg != pg_tag)
			continue;

		switch (aas) {
		case AAS_ACTIVE_OPTIMIZED:
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d Active/optimized\n", conn->id);
			break;
		case AAS_ACTIVE_NON_OPTIMIZED:
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d Active/non-optimized\n", conn->id);
			break;
#if 0
		case AAS_STANDBY:
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Standby\n");
			break;
		case AAS_UNAVAILABLE:
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Unavailable\n");
			break;
#endif
		case AAS_TRANSITIONING:
			return -1;
		default:
			ISTGT_ERRLOG("c#%d nsupported AAS 0x%x\n", conn->id, aas);
			return -1;
		}
		lu->map[i].pg_aas = aas;
		lu->map[i].pg_aas |= AAS_STATUS_STPG;
	}

	len -=4;
	if (len != 0) {
		rc = istgt_lu_disk_scsi_set_target_port_groups(spec, conn, cdb, data, len);
		if (rc < 0) {
			return rc;
		}
	}
	return 0;
}

int
istgt_lu_disk_add_nexus(ISTGT_LU_Ptr lu, int lun, const char *initiator_port)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	int rc = 0;

	spec = lu->lun[lun].spec;
	if (spec == NULL)
		return 0;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_add_nexus: LU%d, LUN%d, Initiator %s\n", lu->num, lun, initiator_port);

	/* Find the nexus */
	TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
		if (nexus == NULL)
			break;
		/* Retrun if nexus is already added */
		if (strcasecmp(nexus->initiator_port, initiator_port) == 0){
			return 0;
		}
	}

	nexus = xmalloc(sizeof *nexus);
	if (nexus == NULL) {
		ISTGT_ERRLOG("istgt_lu_disk_add_nexus: ENOMEM\n");
		goto error_return;
	}
	nexus->initiator_port = initiator_port;
	nexus->ua_pending &= ISTGT_UA_NONE;
	rc = pthread_mutex_init(&nexus->nexus_mutex, &lu->istgt->mutex_attr);
	if (rc != 0) {
		ISTGT_ERRLOG("mutex_init() failed\n");
		goto error_return;
	}
	TAILQ_INSERT_TAIL(&spec->nexus, nexus, nexus_next);
	
	return 0;
	error_return:
		return -2;
}

int
istgt_lu_disk_remove_nexus(ISTGT_LU_Ptr lu, int lun, const char *initiator_port)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus, *tmp_nexus;
	int rc = 0;

	spec = lu->lun[lun].spec;
	if (spec == NULL)
		return 0;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_remove_nexus: LU%d LUN%d Initiator %s\n", lu->num, lun, initiator_port);

	TAILQ_FOREACH_SAFE(nexus, &spec->nexus, nexus_next, tmp_nexus) {
		if (nexus == NULL)
			break;
		if (strcasecmp(nexus->initiator_port, initiator_port)  == 0) {
			TAILQ_REMOVE(&spec->nexus, nexus, nexus_next);
			rc = pthread_mutex_destroy(&nexus->nexus_mutex);
			if (rc != 0) {
				/* Ignore Error */
				ISTGT_ERRLOG("mutex_destroy() failed\n");
			}
			xfree(nexus);
			return 0;
		}
	}
	return 0;
}

void
istgt_lu_disk_free_pr_key(ISTGT_LU_PR_KEY *prkey)
{
	int i;

	if (prkey == NULL)
		return;
	xfree(prkey->registered_initiator_port);
	prkey->registered_initiator_port = NULL;
	xfree(prkey->registered_target_port);
	prkey->registered_target_port = NULL;
	prkey->pg_idx = 0;
	prkey->pg_tag = 0;
	for (i = 0; i < prkey->ninitiator_ports; i++) {
		xfree(prkey->initiator_ports[i]);
		prkey->initiator_ports[i] = NULL;
	}
	xfree(prkey->initiator_ports);
	prkey->initiator_ports = NULL;
	prkey->all_tpg = 0;
	prkey->ninitiator_ports = 0;
}

static ISTGT_LU_PR_KEY *
istgt_lu_disk_find_pr_key(ISTGT_LU_DISK *spec, const char *initiator_port, const char *target_port, uint64_t key)
{
	ISTGT_LU_PR_KEY *prkey;
	int i;

	/* return pointer if I_T nexus is registered */
#ifdef ISTGT_TRACE_DISK
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "find prkey=0x%16.16"PRIx64", port=%s\n",
	    key, ((initiator_port != NULL) ? initiator_port : "N/A"));
#endif /* ISTGT_TRACE_DISK */

	if (initiator_port == NULL)
		return NULL;
	for (i = 0; i < spec->npr_keys; i++) {
		prkey = &spec->pr_keys[i];
		if (prkey == NULL)
			continue;
#ifdef ISTGT_TRACE_DISK
		if (key != 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "prkey=0x%16.16"PRIx64"\n",
			    prkey->key);
		}
#endif /* ISTGT_TRACE_DISK */
		if ((key != 0 && prkey->key != key) || (prkey->key == 0))
			continue;

#ifdef ISTGT_TRACE_DISK
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "pript=%s, ipt=%s\n",
		    prkey->registered_initiator_port,
		    initiator_port);
#endif /* ISTGT_TRACE_DISK */
		if (strcmp(prkey->registered_initiator_port,
			initiator_port) == 0) {
#ifdef ISTGT_TRACE_DISK
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "prtpt=%s, tpt=%s\n",
			    prkey->registered_target_port,
			    target_port);
#endif /* ISTGT_TRACE_DISK */
			if (prkey->all_tpg != 0
			    || target_port == NULL
			    || strcmp(prkey->registered_target_port,
				target_port) == 0) {
				return prkey;
			}
		}
	}
	return NULL;
}

static int
istgt_lu_disk_remove_other_pr_key(ISTGT_LU_DISK *spec, CONN_Ptr conn __attribute__((__unused__)), const char *initiator_port, const char *target_port, uint64_t key)
{
	ISTGT_LU_PR_KEY *prkey, *prkey1, *prkey2;
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "remove other prkey=0x%16.16"PRIx64", port=%s\n",
	    key, ((initiator_port != NULL) ? initiator_port : "N/A"));

	for (i = 0; i < spec->npr_keys; ) {
		prkey = &spec->pr_keys[i];
		if (prkey == NULL){
			i++;
			continue;
		}
		if (key == 0 || prkey->key == key){
			i++;	
			continue;
		}
		if (initiator_port == NULL ||
		    strcasecmp(prkey->registered_initiator_port,
			initiator_port) == 0){
			i++;
			continue;
		}
		if (prkey->all_tpg != 0
		    || target_port == NULL
		    || strcasecmp(prkey->registered_target_port,
			target_port) == 0) {
			i++;
			continue;
		}

		istgt_lu_disk_free_pr_key(prkey);
		for (j = i; j < spec->npr_keys - 1; j++) {
			prkey1 = &spec->pr_keys[j];
			prkey2 = &spec->pr_keys[j+1];

			prkey1->key = prkey2->key;
			prkey2->key = 0;
			prkey1->registered_initiator_port
				= prkey2->registered_initiator_port;
			prkey2->registered_initiator_port = NULL;
			prkey1->registered_target_port
				= prkey2->registered_target_port;
			prkey2->registered_target_port = NULL;
			prkey1->pg_idx = prkey2->pg_idx;
			prkey2->pg_idx = 0;
			prkey1->pg_tag = prkey2->pg_tag;
			prkey2->pg_tag = 0;
			prkey1->ninitiator_ports = prkey2->ninitiator_ports;
			prkey2->ninitiator_ports = 0;
			prkey1->initiator_ports = prkey2->initiator_ports;
			prkey2->initiator_ports = NULL;
			prkey1->all_tpg = prkey2->all_tpg;
			prkey2->all_tpg = 0;
		}
		spec->npr_keys--;
	}
	return 0;
}

static int
istgt_lu_disk_remove_pr_key(ISTGT_LU_DISK *spec, CONN_Ptr conn __attribute__((__unused__)), const char *initiator_port, const char *target_port, uint64_t key)
{
	ISTGT_LU_PR_KEY *prkey, *prkey1, *prkey2;
	int i, j;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "remove prkey=0x%16.16"PRIx64", port=%s\n",
	    key, ((initiator_port != NULL) ? initiator_port : "N/A"));

	for (i = 0; i < spec->npr_keys; ) {
		prkey = &spec->pr_keys[i];
		if (prkey == NULL || prkey->key == 0){
			i++;
			continue;
		}
		if (key != 0 && prkey->key != key){
			i++;
			continue;
		}
		if (initiator_port != NULL
		    && strcasecmp(prkey->registered_initiator_port,
			initiator_port) != 0){
			i++;
			continue;
		}
		if (prkey->all_tpg == 0
		    && target_port != NULL
		    && strcasecmp(prkey->registered_target_port,
			target_port) != 0){
			i++;
			continue;
		}

		istgt_lu_disk_free_pr_key(prkey);
		for (j = i; j < spec->npr_keys - 1; j++) {
			prkey1 = &spec->pr_keys[j];
			prkey2 = &spec->pr_keys[j+1];

			prkey1->key = prkey2->key;
			prkey2->key = 0;
			prkey1->registered_initiator_port
				= prkey2->registered_initiator_port;
			prkey2->registered_initiator_port = NULL;
			prkey1->registered_target_port
				= prkey2->registered_target_port;
			prkey2->registered_target_port = NULL;
			prkey1->pg_idx = prkey2->pg_idx;
			prkey2->pg_idx = 0;
			prkey1->pg_tag = prkey2->pg_tag;
			prkey2->pg_tag = 0;
			prkey1->ninitiator_ports = prkey2->ninitiator_ports;
			prkey2->ninitiator_ports = 0;
			prkey1->initiator_ports = prkey2->initiator_ports;
			prkey2->initiator_ports = NULL;
			prkey1->all_tpg = prkey2->all_tpg;
			prkey2->all_tpg = 0;
		}
		spec->npr_keys--;
	}
	return 0;
}

static int
istgt_lu_parse_transport_id(char **tid, uint8_t *data, int len)
{
	int fc, pi;
	int hlen, plen;

	if (tid == NULL)
		return -1;
	if (data == NULL)
		return -1;

	fc = BGET8W(&data[0], 7, 2);
	pi = BGET8W(&data[0], 3, 4);
	if (fc != 0) {
		ISTGT_ERRLOG("FORMAT CODE != 0\n");
		return -1;
	}
	if (pi != SPC_VPD_IDENTIFIER_TYPE_SCSI_NAME) {
		ISTGT_ERRLOG("PROTOCOL IDENTIFIER != ISCSI\n");
		return -1;
	}

	/* PROTOCOL IDENTIFIER = 0x05 */
	hlen = 4;
	/* ADDITIONAL LENGTH */
	plen = DGET16(&data[2]);
	if (plen > len) {
		ISTGT_ERRLOG("invalid length %d (expected %d)\n",
		    plen, len);
		return -1;
	}
	if (plen > MAX_ISCSI_NAME) {
		ISTGT_ERRLOG("invalid length %d (expected %d)\n",
		    plen, MAX_ISCSI_NAME);
		return -1;
	}

	/* ISCSI NAME */
	*tid = xmalloc(plen + 1);
	memcpy(*tid, data, plen);
	(*tid)[plen] = '\0';
	strlwr(*tid);

	return hlen + plen;
}

#ifndef	REPLICATION
static int
istgt_lu_disk_scsi_persistent_reserve_in(ISTGT_LU_DISK *spec, CONN_Ptr conn __attribute__((__unused__)), ISTGT_LU_CMD_Ptr lu_cmd, int sa, uint8_t *data, int alloc_len __attribute__((__unused__)))
{
	ISTGT_LU_PR_KEY *prkey;
	size_t hlen = 0, len = 0, plen;
	uint8_t *cp;
	int total;
	int i;

	cp = &data[hlen + len];
	total = 0;
	switch (sa) {
	case 0x00: /* READ KEYS */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ KEYS\n", conn->id);

		/* PRGENERATION */
		DSET32(&data[0], spec->pr_generation);
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], 0);
		hlen = 8;

		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			/* reservation key N */
			cp = &data[hlen + len];
			DSET64(&cp[0], prkey->key);
			len += 8;
		}
		total = hlen + len;
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], total - hlen);
		break;

	case 0x01: /* READ RESERVATION */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ RESERVATION\n", conn->id);

		/* PRGENERATION */
		DSET32(&data[0], spec->pr_generation);
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], 0);
		hlen = 8;

		if (spec->rsv_key != 0) {
			/* RESERVATION KEY */
			DSET64(&data[8], spec->rsv_key);
			/* Obsolete */
			DSET32(&data[16], 0);
			/* Reserved */
			data[20] = 0;
			/* SCOPE(7-4) TYPE(3-0) */
			BDSET8W(&data[21], spec->rsv_scope, 7, 4);
			BDADD8W(&data[21], spec->rsv_type, 3, 4);
			/* Obsolete */
			DSET16(&data[22], 0);
			len = 24 - hlen;
		}

		total = hlen + len;
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], total - hlen);
		break;

	case 0x02: /* REPORT CAPABILITIES */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d REPORT CAPABILITIES\n", conn->id);

		/* LENGTH */
		DSET16(&data[0], 0x0008);
		/* CRH(4) SIP_C(3) ATP_C(2) PTPL_C(0) */
		data[2] = 0;
		//BDADD8(&data[2], 1, 4); /* Compatible Reservation Handling */
		BDADD8(&data[2], 1, 3); /* Specify Initiator Ports Capable */
		BDADD8(&data[2], 1, 2); /* All Target Ports Capable */
		//BDADD8(&data[2], 1, 0); /* Persist Through Power Loss Capable */
		/* TMV(7) PTPL_A(0) */
		data[3] = 0;
		//BDADD8(&data[2], 1, 7); /* Type Mask Valid */
		//BDADD8(&data[2], 1, 0); /* Persist Through Power Loss Activated */
		/* PERSISTENT RESERVATION TYPE MASK */
		DSET16(&data[4], 0);
		/* Reserved */
		DSET16(&data[6], 0);
		hlen = 8;

		total = hlen + len;
		break;

	case 0x03: /* READ FULL STATUS */
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ FULL STATUS\n", conn->id);

		/* PRGENERATION */
		DSET32(&data[0], spec->pr_generation);
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], 0);
		hlen = 8;

		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			/* Full status descriptors N */
			cp = &data[hlen + len];

			/* RESERVATION KEY */
			DSET64(&cp[0], prkey->key);
			/* Reserved */
			DSET64(&cp[8], 0);
			/* ALL_TG_PT(1) R_HOLDER(0) */
			cp[12] = 0;
			if (prkey->all_tpg) {
				BDADD8(&cp[12], 1, 1);
			}
			/* SCOPE(7-4) TYPE(3-0) */
			cp[13] = 0;
			if (spec->rsv_key != 0) {
				if (spec->rsv_key == prkey->key) {
					BDADD8(&cp[12], 1, 0);
					BDADD8W(&cp[13], spec->rsv_scope & 0x0f, 7, 4);
					BDADD8W(&cp[13], spec->rsv_type & 0x0f, 3, 4);
				}
			}
			/* Reserved */
			DSET32(&cp[14], 0);
			/* RELATIVE TARGET PORT IDENTIFIER */
			DSET16(&cp[18], 1 + prkey->pg_idx);
			/* ADDITIONAL DESCRIPTOR LENGTH */
			DSET32(&cp[20], 0);

			/* TRANSPORTID */
			plen = snprintf((char *) &cp[24], MAX_INITIATOR_NAME,
			    "%s",
			    prkey->registered_initiator_port);

			/* ADDITIONAL DESCRIPTOR LENGTH */
			DSET32(&cp[20], plen);
			len += 24 + plen;
		}

		total = hlen + len;
		/* ADDITIONAL LENGTH  */
		DSET32(&data[4], total - hlen);
		break;

	default:
		ISTGT_ERRLOG("c#%d unsupported service action 0x%x\n", conn->id, sa);
		/* INVALID FIELD IN CDB */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}

	lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
	return total;
}
#endif

static int
istgt_lu_disk_update_reservation(ISTGT_LU_DISK *spec)
{
	int i= 0, rsv_idx = 0;
	int ret = 0;
	SCSI_PR_DATA *pr_reservation = NULL;
	ISTGT_LU_PR_KEY *prkey = NULL;

	if (spec == NULL)
		return -1;
	int luni = (spec->lu != NULL) ? spec->lu->num : -1;

	/* Lun is in the fake mode, Spurious Reservation commands are not allowed */
	if (spec->fd == -1 || spec->state == ISTGT_LUN_BUSY) {
		ret = -1;
		ISTGT_ERRLOG("LU%d:RSV-UPD %s (rsvkey?:%lx)",
				luni, spec->state == ISTGT_LUN_BUSY ? "FAKE" : "fd invalid", spec->rsv_key);
		goto exit;
	}
	
	pr_reservation = xmalloc(sizeof  *pr_reservation);
	memset(pr_reservation, 0, sizeof *pr_reservation);

	if (spec->npr_keys > MAX_LU_ZAP_RESERVE) { 
		pr_reservation->npr_keys = MAX_LU_ZAP_RESERVE;
	} else {
		pr_reservation->npr_keys = spec->npr_keys;
	}
	pr_reservation->rsv_scope = spec->rsv_scope;
	pr_reservation->rsv_type = spec->rsv_type;
	pr_reservation->pr_generation =  spec->pr_generation;
	pr_reservation->rsv_key =  spec->rsv_key;
	if(spec->rsv_port)
		strncpy(pr_reservation->rsv_port, spec->rsv_port, sizeof(pr_reservation->rsv_port) - 1);
	pr_reservation->rsv_port[sizeof(pr_reservation->rsv_port) - 1] = '\0';
	for(i = 0; i < spec->npr_keys; i++) {
		if(spec->pr_keys[i].key == spec->rsv_key) {
			rsv_idx = i;
		}
	}
	
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "LU%d:RSV-UPD  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
			luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
			spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);

	/* Update all the npr_keys */
	for (i = 0; i < spec->npr_keys; i++) {
		if (i >= MAX_LU_ZAP_RESERVE) {
			break;
		}
		if ((i == MAX_LU_ZAP_RESERVE - 1) && (rsv_idx > MAX_LU_ZAP_RESERVE - 1)) {
			prkey = &spec->pr_keys[rsv_idx];
		} else {
			prkey = &spec->pr_keys[i];
		}
		if (prkey != NULL ) { 
			/* copy the string */
			pr_reservation->keys[i].key = prkey->key;
			strncpy(pr_reservation->keys[i].registered_initiator_port, prkey->registered_initiator_port,
				((strlen(prkey->registered_initiator_port) + 1) > MAX_INITIATOR_NAME ? MAX_INITIATOR_NAME : (strlen(prkey->registered_initiator_port) + 1)));
			strncpy(pr_reservation->keys[i].registered_target_port, prkey->registered_target_port,
				((strlen(prkey->registered_target_port) + 1) > MAX_TARGET_NAME ? MAX_TARGET_NAME : (strlen(prkey->registered_target_port) + 1)));
			pr_reservation->keys[i].pg_idx= prkey->pg_idx;
			pr_reservation->keys[i].pg_tag = prkey->pg_tag;
			pr_reservation->keys[i].all_tpg = prkey->all_tpg;
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "LU%d:RSV-UPD.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
					luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
					prkey->ninitiator_ports);
		}
	}

#ifdef	REPLICATION
	// TODO Write the Persistent Reservation data to the device as a ZAP attribute 
	#ifdef __FreeBSD__
	if (ioctl(spec->fd, DIOCGWRRSV, pr_reservation) == -1) {
		if (errno != ENOENT){
			ISTGT_ERRLOG("LU%d:RSV-UPD  failed:%d  (rsvkey:%lx)",
				luni, errno, spec->rsv_key);
			ret = -1;
		}
		goto exit;
	}
	#endif
#endif

exit:
	if (pr_reservation)
		xfree(pr_reservation);
	return ret;

}

int
istgt_lu_disk_get_reservation(ISTGT_LU_DISK *spec)
{
	int  ret = 0, i = 0, rsv_idx = -1;
	SCSI_PR_DATA  *pr_reservation = NULL;
	SCSI_PR_KEY *prkey = NULL;
	if(spec == NULL)
		return -1;
	int luni = (spec->lu != NULL) ? spec->lu->num : -1;

	pr_reservation = xmalloc(sizeof *pr_reservation);
	memset(pr_reservation, 0, sizeof *pr_reservation);

	/* The Lun is in fake mode, Fail here only */
	if (spec->fd == -1) {
		ISTGT_NOTICELOG("LU%d:RSV-READ  %s (rsvkey?:%lx)",
				luni, spec->state == ISTGT_LUN_BUSY ? "FAKE" : "fd invalid", spec->rsv_key);
		ret = -1;
		spec->rsv_pending |= ISTGT_RSV_READ;
		goto exit;
	}

	spec->rsv_pending &= ~ISTGT_RSV_READ;

#ifdef	REPLICATION
	// TODO Read the Persistent Reservation data from the device
	#ifdef __FreeBSD__
	if (ioctl(spec->fd, DIOCGRDRSV, pr_reservation) == -1) {
		if (errno != ENOENT) {
			ISTGT_ERRLOG("LU%d:RSV-READ  failed:%d  (rsvkey?:%lx)",
				luni, errno, spec->rsv_key);
			ret = -1;
			spec->rsv_pending |= ISTGT_RSV_READ;
		}
		goto exit;
	}
	#endif
#endif

	spec->rsv_pending &= ~ISTGT_RSV_READ;
	pr_reservation = (SCSI_PR_DATA *)pr_reservation;

	/* Update the spec with the Reservation data */
	if (pr_reservation->npr_keys > MAX_LU_ZAP_RESERVE) {
		spec->npr_keys = MAX_LU_ZAP_RESERVE;
	} else {
		spec->npr_keys = pr_reservation->npr_keys;
	}
	for(i = 0; i < spec->npr_keys; i++) {
		if( pr_reservation->keys[i].key == pr_reservation->rsv_key){
			rsv_idx = i;
		}
	}
	if(rsv_idx == -1) {
		spec->npr_keys = 0;
		goto exit;
	}
	spec->rsv_scope = pr_reservation->rsv_scope;
	spec->rsv_type = pr_reservation->rsv_type;
	spec->pr_generation = pr_reservation->pr_generation;
	spec->rsv_key = pr_reservation->rsv_key;
			
	if (spec->rsv_port != NULL)
		xfree(spec->rsv_port);
	spec->rsv_port = xstrdup(trim_string(pr_reservation->rsv_port));
	
	ISTGT_NOTICELOG("LU%d:RSV-READ  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
			luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
			spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);

	for (i = 0; i < pr_reservation->npr_keys; i++) {
		if (i >= MAX_LU_ZAP_RESERVE) {
			break;
		}
		prkey = &pr_reservation->keys[i];
		istgt_lu_disk_free_pr_key(&spec->pr_keys[i]);
		if(prkey != NULL) {
			spec->pr_keys[i].key = prkey->key;
			spec->pr_keys[i].registered_initiator_port = xstrdup(trim_string(prkey->registered_initiator_port));
			spec->pr_keys[i].registered_target_port = xstrdup(trim_string(prkey->registered_target_port));
			spec->pr_keys[i].pg_idx = prkey->pg_idx;
			spec->pr_keys[i].pg_tag = prkey->pg_tag;
			spec->pr_keys[i].all_tpg = prkey->all_tpg;
			ISTGT_NOTICELOG("LU%d:RSV-READ.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d ",
					luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag);
		}
	}

exit:
	if(pr_reservation)
		xfree(pr_reservation);
	return ret;
}

int
istgt_lu_disk_copy_reservation(ISTGT_LU_DISK *spec_bkp, ISTGT_LU_DISK *spec)
{
	int i = 0, j = 0;
	ISTGT_LU_PR_KEY *prkey_bkp;
	ISTGT_LU_PR_KEY *prkey;
	
	if (spec_bkp->rsv_port != NULL)
		xfree(spec_bkp->rsv_port);

	spec_bkp->rsv_port = xstrdup(spec->rsv_port);
	spec_bkp->rsv_key = spec->rsv_key;
	spec_bkp->spc2_reserved = spec->spc2_reserved;
	spec_bkp->rsv_scope = spec->rsv_scope;
	spec_bkp->rsv_type = spec->rsv_type;
	spec_bkp->pr_generation = spec->pr_generation;
	spec_bkp->npr_keys = spec->npr_keys;

	for(i = 0; i < spec->npr_keys;i++) {
		prkey = &spec->pr_keys[i];
		prkey_bkp = &spec_bkp->pr_keys[i];
		if(prkey_bkp == NULL) {
			prkey = NULL;
			continue;
		}
		istgt_lu_disk_free_pr_key(prkey_bkp);
		prkey_bkp->all_tpg = prkey->all_tpg;
		prkey_bkp->key = prkey->key;
		prkey_bkp->registered_initiator_port
				= xstrdup(prkey->registered_initiator_port);
		prkey_bkp->registered_target_port
				= xstrdup(prkey->registered_target_port);
		prkey_bkp->pg_idx = prkey->pg_idx;
		prkey_bkp->pg_tag = prkey->pg_tag;
		prkey_bkp->ninitiator_ports = prkey->ninitiator_ports;
		if(prkey->initiator_ports != NULL) {
			prkey_bkp->initiator_ports = xmalloc(sizeof (char *) * MAX_LU_RESERVE_IPT);
			memset(prkey_bkp->initiator_ports, 0, sizeof (char *) * MAX_LU_RESERVE_IPT);
			for(j =0; j < prkey_bkp->ninitiator_ports; j++) {
				if (prkey->initiator_ports[j] == NULL)
					continue;
				prkey_bkp->initiator_ports[j]= xstrdup(prkey->initiator_ports[j]);
			}
		}
		
	}
	return 0;	
}



int
istgt_lu_disk_print_reservation(ISTGT_LU_Ptr lu, int lun)
{
	int i = 0, j = 0;
	ISTGT_LU_PR_KEY *prkey;
	ISTGT_LU_DISK *spec;

	if (lu == NULL)
		return -1;
	if (lun >= lu->maxlun)
		return -1;
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;
	if(spec->persist==0){
                ISTGT_LOG("PERSIST NOT ENABLED, won't read from ZAP");
                return -1;
        }

	uint64_t gb_size = spec->size / ISTGT_LU_1GB;
	uint64_t mb_size = (spec->size % ISTGT_LU_1GB) / ISTGT_LU_1MB;

	ISTGT_NOTICELOG("LU%d:RSV %s %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u %s%s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d inflight:%d\n",
				lu->num, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
				spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift,
				spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
				spec->ats ? " ATS" : "",spec->xcopy ? " XCOPY" : "", spec->unmap ? " UNMAP" : "",
				spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
				lu->lun[lun].rotationrate,
				spec->queue_depth, spec->luworkers, spec->luworkersActive, spec->ludsk_ref);
	ISTGT_NOTICELOG("LU%d:RSV  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
			lu->num, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
			spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);
	for (i = 0; i < spec->npr_keys; i++) {
		prkey = &spec->pr_keys[i];
		ISTGT_NOTICELOG("LU%d:RSV.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
			lu->num, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
			prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
			prkey->ninitiator_ports);
		for (j =0; j < prkey->ninitiator_ports; j++) {
			ISTGT_NOTICELOG("LU%d:RSV.%2.2d.%d   I:%s", lu->num, i, j, prkey->initiator_ports[j]);
		}
	}
	return 0;
}

enum prout_sa {
	PROUTSA_REGISTER = 0,
	PROUTSA_RESERVE,
	PROUTSA_RELEASE,
	PROUTSA_CLEAR,
	PPROUTSA_REEMPT,
	PPROUTSA_REEMPT_AND_ABORT,
	PROUTSA_REGISTER_AND_IGNORE_EXISTING_KEY,
	PROUTSA_REGISTER_AND_MOVE,
	PROUTSA_UNK
};
const char prout_sa[PROUTSA_UNK+2][50] = {
	"PROUT_REGISTER",
	"PROUT_RESERVE",
	"PROUT_RELEASE",
	"PROUT_CLEAR",
	"PROUT_PREEMPT",
	"PROUT_PREEMPT_AND_ABORT",
	"PROUT_REGISTER_AND_IGNORE_EXISTING_KEY",
	"PROUT_REGISTER_AND_MOVE",
	"PROUT_UNK"
};

static int
istgt_lu_disk_scsi_persistent_reserve_out(ISTGT_LU_DISK *spec,
		CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd,
		int sa, int scope, int type, uint8_t *data, int len, int spc2)
{
	ISTGT_LU_PR_KEY *prkey;
	ISTGT_LU_DISK *spec_bkp = NULL;
	char *old_rsv_port = NULL;
	char **initiator_ports = NULL;
	int maxports = 0, nports;
	int plen, total;
	uint64_t rkey;
	uint64_t sarkey;
	int bkp_success = 1;
	int spec_i_pt, all_tg_pt, aptpl;
	int task_abort;
	int idx;
	int rc;
	int ret = 0;
	int i;
	int change_rsv = 0;

	spec_bkp = xmalloc(sizeof(*spec_bkp));
	memset(spec_bkp, 0, sizeof(*spec_bkp));
	
	rkey = DGET64(&data[0]);
	sarkey = DGET64(&data[8]);
	spec_i_pt = BGET8(&data[20], 3);
	all_tg_pt = BGET8(&data[20], 2);
	aptpl = BGET8(&data[20], 0);

	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
		conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
		rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);

	(void) istgt_lu_disk_copy_reservation(spec_bkp, spec);

	switch (sa) {
	case 0x00: /* REGISTER */
		change_rsv = 0;

		if (aptpl != 0) {
			/* Activate Persist Through Power Loss */
			ISTGT_ERRLOG("c#%d unsupport Activate Persist Through Power Loss\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INVALID FIELD IN PARAMETER LIST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret =  -1;
			goto cleanup_return;
		}
		/* lost reservations if daemon restart */

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* unregistered port */
			if (rkey != 0) {
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				ret =  -1;
				goto cleanup_return;
			}
			if (sarkey != 0) {
				/* XXX check spec_i_pt */
			}
		} else {
			/* registered port */
			if (spec_i_pt) {
				/* INVALID FIELD IN CDB */
				ISTGT_ERRLOG("c#%d Invalid field in CDB\n", conn->id);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret =  -1;
				goto cleanup_return;
			}

			prkey = istgt_lu_disk_find_pr_key(spec,
			    conn->initiator_port, conn->target_port, rkey);
			if (prkey == NULL) {
				ISTGT_ERRLOG("c#%d key not found initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				/* not found key */
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				ret =  -1;
				goto cleanup_return;
			}
			/* table 33 REGISTER service action in spc3r23 */
			/*if ((spec->rsv_key != 0) && (rkey != 0)) {
				if (spec->rsv_key != rkey) {
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					return -1;
				}
				if (sarkey != 0)
					change_rsv = 1;
			}*/
			if ((sarkey != 0) && (spec_i_pt == 0)) {
				ISTGT_ERRLOG("c#%d change_rsv REGISTER rsvkey:%lx rkey %lx sakey:0x%16.16lx\n", conn->id, spec->rsv_key, rkey, sarkey);
				change_rsv = 1;
			}

			/* remove existing keys */
			rc = istgt_lu_disk_remove_pr_key(spec, conn,
			    conn->initiator_port, conn->target_port, 0);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				/* INTERNAL TARGET FAILURE */
				BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}
		}

		/* unregister? */
		if (sarkey == 0) {
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ret =  0;
			goto cleanup_return;
		}

		goto do_register;

	case 0x01: /* RESERVE */

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* unregistered port */
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret = -1;
			goto cleanup_return;
		}

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, rkey);
		if (prkey == NULL) {
			/* not found key */
			ISTGT_ERRLOG("c#%d key not found initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret  =  -1;
			goto cleanup_return;
		}
		if (spec->rsv_key == 0) {
			/* no reservation */
		} else {
			if (prkey->key != spec->rsv_key) {
				ISTGT_ERRLOG("c#%d prkey->key != spec->rsv_key initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				ret =  -1;
				goto cleanup_return;
			}
			if (strcasecmp(spec->rsv_port, conn->initiator_port) != 0) {
				ISTGT_ERRLOG("c#%d spec->rsv_port != conn->initiator_port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				ret =  -1;
				goto cleanup_return;
			}
			if (g_trace_flag) {
				ISTGT_WARNLOG("LU%d: duplicate reserve\n", spec->lu->num);
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ret = 0;
			goto cleanup_return;
		}

		if (scope != 0x00) { // !LU_SCOPE
			/* INVALID FIELD IN CDB */
			ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		if (type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS) {
			ISTGT_ERRLOG("c#%d unsupported type 0x%x\n", conn->id, type);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INVALID FIELD IN CDB */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}

		/* establish reservation by key */
		xfree(spec->rsv_port);
		spec->rsv_port = xstrdup(conn->initiator_port);
		strlwr(spec->rsv_port);
		spec->rsv_key = rkey;
		spec->spc2_reserved = 0;
		spec->rsv_scope = scope;
		spec->rsv_type = type;

		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		    "c#%d LU%d: reserved (scope=%d, type=%d) by key=0x%16.16lx\n",
		    conn->id, spec->lu->num, scope, type, rkey);
		break;

	case 0x02: /* RELEASE */

		if (spec->rsv_key == 0) {
			/* no reservation */
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ret =  0;
			goto cleanup_return;
		}
		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			/* unregistered port */
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret = -1;
			goto cleanup_return;
		}

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, rkey);
		if (prkey == NULL) {
			/* not found key */
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret = -1;
			goto cleanup_return;
		}
		if (prkey->key != spec->rsv_key) {
			/* INVALID RELEASE OF PERSISTENT RESERVATION */
			ISTGT_ERRLOG("c#%d prkey->key != spec->rsv_key initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x04);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret =  -1;
			goto cleanup_return;
		}
		if (strcasecmp(spec->rsv_port, conn->initiator_port) != 0) {
			/* INVALID RELEASE OF PERSISTENT RESERVATION */
			ISTGT_ERRLOG("c#%d spec->rsv_port != conn->initiator_port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x04);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}

		if (scope != 0x00) { // !LU_SCOPE
			/* INVALID FIELD IN CDB */
			ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		if (type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS) {
			ISTGT_ERRLOG("c#%d unsupported type 0x%x\n", conn->id, type);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INVALID FIELD IN CDB */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		if (spec->rsv_scope != scope || spec->rsv_type != type) {
			/* INVALID RELEASE OF PERSISTENT RESERVATION */
			ISTGT_ERRLOG("c#%d spec->rsv_scope != scope || spec->rsv_type != type initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x04);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}

		/* release reservation by key */
		xfree(spec->rsv_port);
		spec->rsv_port = NULL;
		spec->spc2_reserved = 0;
		spec->rsv_scope = 0;
		spec->rsv_type = 0;

		ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		    "c#%d LU%d: released 0x%16.16lx (scope=%d, type=%d) by key=0x%16.16lx\n",
		    conn->id, spec->lu->num, spec->rsv_key, scope, type, rkey);
		spec->rsv_key = 0;
		break;

	case 0x03: /* CLEAR */

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			/* unregistered port */
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret = -1;
			goto cleanup_return;
		}

		/* release reservation */
		xfree(spec->rsv_port);
		spec->rsv_port = NULL;
		spec->rsv_key = 0;
		spec->rsv_scope = 0;
		spec->rsv_type = 0;

		/* remove all registrations */
		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			istgt_lu_disk_free_pr_key(prkey);
		}
		spec->npr_keys = 0;
		break;

	case 0x04: /* PREEMPT */

		task_abort = 0;
	do_preempt:
		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			ISTGT_ERRLOG("c#%d unregistered port initiator_port: %s target_port: %s rsv_port: %s\n", conn->id, conn->initiator_port, conn->target_port, spec->rsv_port);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* unregistered port */
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			ret = -1;
			goto cleanup_return;
		}

		if (spec->rsv_key == 0) {
			/* no reservation */
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "no reservation\n");
			/* remove registration */
			rc = istgt_lu_disk_remove_pr_key(spec, conn,
			    NULL, NULL, sarkey);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				/* INTERNAL TARGET FAILURE */
				BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}

			/* update generation */
			spec->pr_generation++;

			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "rsv_key=0x%16.16"PRIx64"\n",
		    spec->rsv_key);

		if (spec->rsv_type == ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS
		    || spec->rsv_type == ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS) {
			if (sarkey != 0) {
				/* remove registration */
				rc = istgt_lu_disk_remove_pr_key(spec, conn,
				    NULL, NULL, sarkey);
				if (rc < 0) {
					ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					/* INTERNAL TARGET FAILURE */
					BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret =  -1;
					goto cleanup_return;
				}

				/* update generation */
				spec->pr_generation++;

				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			} else {
				/* remove other registrations */
				rc = istgt_lu_disk_remove_other_pr_key(spec, conn,
				    conn->initiator_port,
				    conn->target_port,
				    rkey);
				if (rc < 0) {
					ISTGT_ERRLOG("c#%d lu_disk_remove_other_pr_key() failed\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					/* INTERNAL TARGET FAILURE */
					BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}

				if (scope != 0x00) { // !LU_SCOPE
					/* INVALID FIELD IN CDB */
					ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}
				if (type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE
				    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS
				    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY
				    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY
				    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS
				    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS) {
					ISTGT_ERRLOG("c#%d unsupported type 0x%x\n", conn->id, type);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					/* INVALID FIELD IN CDB */
					BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}

				/* release reservation */
				//xfree(spec->rsv_port);
				old_rsv_port = spec->rsv_port;
				spec->rsv_port = NULL;
				spec->rsv_key = 0;
				spec->rsv_scope = 0;
				spec->rsv_type = 0;
				/* establish new reservation */
				spec->rsv_port = xstrdup(conn->initiator_port);
				strlwr(spec->rsv_port);
				spec->rsv_key = rkey;
				spec->rsv_scope = scope;
				spec->rsv_type = type;

				ISTGT_NOTICELOG("c#%d LU%d: reserved (scope=%d, type=%d) by key=0x%16.16"PRIx64" rsv:%s (preempt:%s)\n",
				    conn->id, spec->lu->num, scope, type, rkey, spec->rsv_port,
					task_abort && (old_rsv_port != NULL) ? old_rsv_port: "none");

				/* update generation */
				spec->pr_generation++;

				/* XXX TODO fix */
				if (task_abort) {
					/* abort all tasks for preempted I_T nexus */
					if (old_rsv_port != NULL) {
						rc = istgt_lu_disk_queue_abort_ITL(spec, old_rsv_port);
						xfree(old_rsv_port);
						old_rsv_port = NULL;
						if (rc < 0) {
							/* INTERNAL TARGET FAILURE */
							ISTGT_ERRLOG("c#%d INTERNAL TARGET FAILURE\n", conn->id);
							ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
								conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
								rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
							BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
							lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
							ret = -1;
							goto cleanup_return;
						}
					}
				}
				if (old_rsv_port != NULL) {
					xfree(old_rsv_port);
					old_rsv_port = NULL;
				}

				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
		}

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, rkey);

		if (prkey == NULL) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "prkey == NULL\n");
		} else {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "prkey key=%16.16"PRIx64"\n",
			    prkey->key);
		}

		if (prkey == NULL
		    || sarkey != spec->rsv_key) {
			if (sarkey != 0) {
				/* remove registration */
				rc = istgt_lu_disk_remove_pr_key(spec, conn,
				    NULL, NULL, sarkey);
				if (rc < 0) {
					ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					/* INTERNAL TARGET FAILURE */
					BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret =  -1;
					goto cleanup_return;
				}
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			} else {
				/* INVALID FIELD IN PARAMETER LIST */
				ISTGT_ERRLOG("c#%d INVALID FIELD IN PARAMETER LIST\n", conn->id);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}
		}

		/* remove registration */
		rc = istgt_lu_disk_remove_pr_key(spec, conn,
		    NULL, NULL, sarkey);
		if (rc < 0) {
			ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INTERNAL TARGET FAILURE */
			BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret =  -1;
			goto cleanup_return;
		}

		if (scope != 0x00) { // !LU_SCOPE
			/* INVALID FIELD IN CDB */
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		if (type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY
		    && type != ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS
		    && type != ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS) {
			ISTGT_ERRLOG("c#%d unsupported type 0x%x\n", conn->id, type);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INVALID FIELD IN CDB */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret =  -1;
			goto cleanup_return;
		}

		/* release reservation */
		//xfree(spec->rsv_port);
		old_rsv_port = spec->rsv_port;
		spec->rsv_port = NULL;
		spec->rsv_key = 0;
		spec->rsv_scope = 0;
		spec->rsv_type = 0;
		/* establish new reservation */
		spec->rsv_port = xstrdup(conn->initiator_port);
		strlwr(spec->rsv_port);
		spec->rsv_key = rkey;
		spec->rsv_scope = scope;
		spec->rsv_type = type;

		ISTGT_NOTICELOG("c#%d LU%d: reserved (scope=%d, type=%d) by key=0x%16.16"PRIx64" rsv:%s (preempt:%s).\n",
		    conn->id, spec->lu->num, scope, type, rkey, spec->rsv_port,
			task_abort && (old_rsv_port != NULL) ? old_rsv_port: "none");

		/* update generation */
		spec->pr_generation++;

		/* XXX TODO fix */
		if (task_abort) {
			/* abort all tasks for preempted I_T nexus */
			if (old_rsv_port != NULL) {
				rc = istgt_lu_disk_queue_abort_ITL(spec, old_rsv_port);
				xfree(old_rsv_port);
				old_rsv_port = NULL;
				if (rc < 0) {
					/* INTERNAL TARGET FAILURE */
					ISTGT_ERRLOG("c#%d INTERNAL TARGET FAILURE\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}
			}
		}
		if (old_rsv_port != NULL) {
			xfree(old_rsv_port);
			old_rsv_port = NULL;
		}

		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;

	case 0x05: /* PREEMPT AND ABORT */

		task_abort = 1;
		goto do_preempt;

	case 0x06: /* REGISTER AND IGNORE EXISTING KEY */

		if (aptpl != 0) {
			/* Activate Persist Through Power Loss */
			ISTGT_ERRLOG("c#%d unsupport Activate Persist Through Power Loss\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INVALID FIELD IN PARAMETER LIST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		/* lost reservations if daemon restart */

		prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
		    conn->target_port, 0);
		if (prkey == NULL) {
			/* unregistered port */
			if (sarkey != 0) {
				if (spec_i_pt) {
					/* INVALID FIELD IN CDB */
					ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}
			}
			/* unregister? */
			if (sarkey == 0) {
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				ret = 0;
				goto cleanup_return;
			}
		} else {
			/* registered port */
			if (spec_i_pt) {
				/* INVALID FIELD IN CDB */
				ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}
		}

		/* remove existing keys */
		rc = istgt_lu_disk_remove_pr_key(spec, conn,
		    conn->initiator_port,
		    conn->target_port, 0);
		if (rc < 0) {
			ISTGT_ERRLOG("c#%d lu_disk_remove_pr_key() failed\n", conn->id);
			ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
				conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
				rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
			/* INTERNAL TARGET FAILURE */
			BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}

		/* unregister? */
		if (sarkey == 0) {
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ret = 0;
			goto cleanup_return;
		}

do_register:
		/* specified port? */
		nports = 0;
		initiator_ports = NULL;
		if (spec_i_pt) {
			if (len < 28) {
				/* INVALID FIELD IN PARAMETER LIST */
				ISTGT_ERRLOG("c#%d INVALID FIELD IN PARAMETER LIST\n", conn->id);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}

			/* TRANSPORTID PARAMETER DATA LENGTH */
			plen = DGET32(&data[24]);
			if (28 + plen > len) {
				ISTGT_ERRLOG("c#%d invalid length %d (expect %d)\n",
				    conn->id, len, 28 + plen);
				ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
					conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
					rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
				/* INVALID FIELD IN PARAMETER LIST */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}

			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "c#%d TransportID parameter data length %d\n",
			    conn->id, plen);
			if (plen != 0) {
				maxports = MAX_LU_RESERVE_IPT;
				initiator_ports = xmalloc(sizeof (char *) * maxports);
				memset(initiator_ports, 0, sizeof (char *) * maxports);
				nports = 0;
				total = 0;
				while (total < plen) {
					if (nports >= MAX_LU_RESERVE_IPT) {
						ISTGT_ERRLOG("maximum transport IDs\n");
						/* INSUFFICIENT REGISTRATION RESOURCES */
						BUILD_SENSE(ILLEGAL_REQUEST, 0x55, 0x04);
						lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
						ret = -1;
						goto cleanup_return;
					}
					rc = istgt_lu_parse_transport_id
						(&initiator_ports[nports],
						 &data[24] + total, plen - total);
					if (rc < 0) {
						/* INVALID FIELD IN PARAMETER LIST */
						ISTGT_ERRLOG("c#%d INVALID FIELD IN PARAMETER LIST\n", conn->id);
						ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
							conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
							rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
						BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
						lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
						ret = -1;
						goto cleanup_return;
					}
					ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d got TransportID %s\n",
					    conn->id, initiator_ports[nports]);
					total += rc;
					nports++;
				}
			}
			/* check all port unregistered? */
			for (i = 0; i < nports; i++) {
				prkey = istgt_lu_disk_find_pr_key(spec,
				    initiator_ports[i], NULL, 0);
				if (prkey != NULL) {
					/* registered port */
					/* INVALID FIELD IN CDB */
					ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
					ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
						conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
						rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
					BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
					ret = -1;
					goto cleanup_return;
				}
			}
			/* OK, all port unregistered */
			idx = spec->npr_keys;
			if (idx + nports >= MAX_LU_RESERVE) {
				/* INSUFFICIENT REGISTRATION RESOURCES */
				ISTGT_ERRLOG("c#%d INSUFFICIENT REGISTRATION RESOURCES\n", conn->id);
				BUILD_SENSE(ILLEGAL_REQUEST, 0x55, 0x04);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ret = -1;
				goto cleanup_return;
			}
			/* register each I_T nexus */
			for (i = 0; i < nports; i++) {
				prkey = &spec->pr_keys[idx + i];

				/* register new key */
				prkey->key = sarkey;

				/* command received port */
				prkey->registered_initiator_port
					= xstrdup(conn->initiator_port);
				strlwr(prkey->registered_initiator_port);
				prkey->registered_target_port
					= xstrdup(conn->target_port);
				strlwr(prkey->registered_target_port);
				prkey->pg_idx = conn->portal.idx;
				prkey->pg_tag = conn->portal.tag;

				/* specified ports */
				prkey->ninitiator_ports = 0;
				prkey->initiator_ports = NULL;
				prkey->all_tpg = (all_tg_pt) ? 1 : 0;
			}
			spec->npr_keys = idx + nports;
		}

		idx = spec->npr_keys;
		if (idx >= MAX_LU_RESERVE) {
			/* INSUFFICIENT REGISTRATION RESOURCES */
			ISTGT_ERRLOG("c#%d INSUFFICIENT REGISTRATION RESOURCES\n", conn->id);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x55, 0x04);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ret = -1;
			goto cleanup_return;
		}
		prkey = &spec->pr_keys[idx];

		/* register new key */
		prkey->key = sarkey;

		/* command received port */
		prkey->registered_initiator_port = xstrdup(conn->initiator_port);
		strlwr(prkey->registered_initiator_port);
		prkey->registered_target_port = xstrdup(conn->target_port);
		strlwr(prkey->registered_target_port);
		prkey->pg_idx = conn->portal.idx;
		prkey->pg_tag = conn->portal.tag;

		/* specified ports */
		prkey->ninitiator_ports = nports;
		prkey->initiator_ports = initiator_ports;
		initiator_ports = NULL;
		prkey->all_tpg = (all_tg_pt) ? 1 : 0;

		if (change_rsv == 1)
		{
			spec->rsv_key = sarkey;
			ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
				    "c#%d LU%d: reservation (scope=%d, type=%d) changed to "
				    "by key=0x%16.16"PRIx64"\n",
				    conn->id, spec->lu->num, scope, type, spec->rsv_key);
		}
		/* count up keys */
		idx++;
		spec->npr_keys = idx;

		/* update generation */
		spec->pr_generation++;
		break;

	case 0x07: /* REGISTER AND MOVE */
		/* INVALID FIELD IN CDB */
		ISTGT_ERRLOG("c#%d INVALID FIELD IN CDB\n", conn->id);
		ISTGT_LOG("c#%d %s:0x%2.2x key=0x%16.16lx sakey=0x%16.16lx ipt=%d tgpt=%d aptpl=%d iport:%s [0x%16.16lx]\n",
			conn->id, prout_sa[(sa > -1 && sa < PROUTSA_UNK) ? sa : PROUTSA_UNK], sa,
			rkey, sarkey, spec_i_pt, all_tg_pt, aptpl, conn->initiator_port, spec->rsv_key);
		BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		ret = -1;
		goto cleanup_return;

	default:
		ISTGT_ERRLOG("c#%d unsupported service action 0x%x\n", conn->id, sa);
		/* INVALID FIELD IN CDB */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		ret = -1;
		goto cleanup_return;
	}
	/* write reservation to file */
        if(spec->persist && spc2 == 0){

                ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "c#%d Writing Reservations to disk", conn->id);
                rc = istgt_lu_disk_update_reservation(spec);
                if (rc < 0) {
                        ISTGT_ERRLOG("c#%d istgt_lu_disk_update_reservation() failed\n", conn->id);
                        /* Copy only if the backup was successful */
                        if (bkp_success == 1) {
                                rc = istgt_lu_disk_copy_reservation(spec, spec_bkp);
                                if (rc < 0) {
                                        ISTGT_ERRLOG("c#%d istgt_lu_disk_copy_reservation() failed\n", conn->id);
                                }
                        }
                        /* INTERNAL TARGET FAILURE */
                        BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
                        lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
                        ret = -1;
                        goto cleanup_return;
                }
        }
        lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
        ret = 0;

cleanup_return:
	/* Do a cleanup */
	if (spec_bkp) {
		/* remove all registrations */
		for (i = 0; i < spec_bkp->npr_keys; i++) {
			prkey = &spec_bkp->pr_keys[i];
			istgt_lu_disk_free_pr_key(prkey);
		}
		if(spec_bkp->rsv_port)
			xfree(spec_bkp->rsv_port);
		xfree(spec_bkp);
	}
	if (ret == -1 && initiator_ports) {
		for (i = 0; i < maxports; i++) {
			if (initiator_ports[i])
				xfree(initiator_ports[i]);
		}
		xfree(initiator_ports);
	}
	return ret;
}

int
istgt_lu_disk_check_pr(ISTGT_LU_DISK *spec, CONN_Ptr conn, int pr_allow)
{
	ISTGT_LU_PR_KEY *prkey;

#ifdef ISTGT_TRACE_DISK
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "RSV_KEY=0x%16.16"PRIx64", RSV_TYPE=0x%x, PR_ALLOW=0x%x\n",
	    spec->rsv_key, spec->rsv_type, pr_allow);
#endif /* ISTGT_TRACE_DISK */

	prkey = istgt_lu_disk_find_pr_key(spec, conn->initiator_port,
	    conn->target_port, 0);
	if (prkey != NULL) {
#ifdef ISTGT_TRACE_DISK
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "PRKEY(0x%16.16"PRIx64") found for %s\n",
		    prkey->key, conn->initiator_port);
#endif /* ISTGT_TRACE_DISK */

		if (spec->rsv_key == prkey->key) {
			/* reservation holder */
			return 0;
		}

		switch (spec->rsv_type) {
		case ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS:
			if (pr_allow & PR_ALLOW_ALLRR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS:
			if (pr_allow & PR_ALLOW_ALLRR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY:
			if (pr_allow & PR_ALLOW_ALLRR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY:
			if (pr_allow & PR_ALLOW_ALLRR)
				return 0;
			return -1;
		}
	} else {
#ifdef ISTGT_TRACE_DISK
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "PRKEY not found for %s\n",
		    conn->initiator_port);
#endif /* ISTGT_TRACE_DISK */

		switch (spec->rsv_type) {
		case ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_ALL_REGISTRANTS:
			if (pr_allow & PR_ALLOW_WERR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE_REGISTRANTS_ONLY:
			if (pr_allow & PR_ALLOW_WERR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_ALL_REGISTRANTS:
			if (pr_allow & PR_ALLOW_EARR)
				return 0;
			return -1;
		case ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS_REGISTRANTS_ONLY:
			if (pr_allow & PR_ALLOW_EARR)
				return 0;
			return -1;
		}
	}

#ifdef ISTGT_TRACE_DISK
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "non registrans type\n");
#endif /* ISTGT_TRACE_DISK */
	/* any I_T nexus */
	switch (spec->rsv_type) {
	case ISTGT_LU_PR_TYPE_WRITE_EXCLUSIVE:
		if (pr_allow & PR_ALLOW_WE)
			return 0;
		return -1;
	case ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS:
		if (pr_allow & PR_ALLOW_EA)
			return 0;
		return -1;
	}

	/* NG */
	return -1;
}

static int
istgt_lu_disk_scsi_release(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_LU_CMD lu_cmd2;
	uint64_t LUI;
	uint64_t rkey;
	uint8_t cdb[10] = {0};
	uint8_t PRO_data[24] = {0};
	int parameter_len;
	int rc;
	int luni = (spec->lu != NULL) ? spec->lu->num : -1;
	int i = 0;
	ISTGT_LU_PR_KEY *prkey = NULL;


	memset(&lu_cmd2, 0, sizeof lu_cmd2);
	//memset(&cdb, 0, sizeof cdb);
	lu_cmd2.sense_data = xmalloc(ISTGT_SNSBUFSIZE);
	lu_cmd2.sense_alloc_len = ISTGT_SNSBUFSIZE;
	lu_cmd2.sense_data_len = 0;
	lu_cmd->sense_data_len = 0;
	parameter_len = sizeof PRO_data;

	LUI = istgt_get_lui(spec->lu->name, spec->lun & 0xffffU);
	rkey = istgt_get_rkey(conn->initiator_name, LUI);

	/* issue release action of PERSISTENT RESERVE OUT */
	cdb[0] = SPC_PERSISTENT_RESERVE_OUT;
	BDSET8W(&cdb[1], 0x02, 4, 5); /* RELEASE */
	BDSET8W(&cdb[2], 0x00, 7, 4); /* LU_SCOPE */
	BDADD8W(&cdb[2], 0x03, 3, 4); /* ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS */
	cdb[3] = 0;
	cdb[4] = 0;
	DSET32(&cdb[5], parameter_len);
	cdb[9] = 0;
	lu_cmd2.cdb = &cdb[0];

	//memset(&PRO_data, 0, sizeof PRO_data);
	DSET64(&PRO_data[0], rkey); // RESERVATION KEY
	DSET64(&PRO_data[8], 0);

	rc = istgt_lu_disk_scsi_persistent_reserve_out(spec, conn, &lu_cmd2,
	    0x02, 0x00, 0x03,
	    PRO_data, parameter_len, 1);
	if (rc < 0) {
		ISTGT_LOG("c#%d LU%d:RSV-UPD  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
					conn->id, luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
					spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);
		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			if (prkey != NULL ) {
				ISTGT_LOG("c#%d LU%d:RSV-UPD.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
					conn->id, luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
					prkey->ninitiator_ports);
			}
		}
		lu_cmd->status = lu_cmd2.status;
		if (lu_cmd->status == ISTGT_SCSI_STATUS_RESERVATION_CONFLICT) {
			ISTGT_ERRLOG("c#%d Reservation Conflict while releasing initiator_port:%s\n", conn->id, conn->initiator_port);
			xfree(lu_cmd2.sense_data);
			return -1;
		}
		/* INTERNAL TARGET FAILURE */
		BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		xfree(lu_cmd2.sense_data);
		return -1;
	}

	/* issue unregister action of PERSISTENT RESERVE OUT */
	cdb[0] = SPC_PERSISTENT_RESERVE_OUT;
	BDSET8W(&cdb[1], 0x06, 4, 5); /* REGISTER AND IGNORE EXISTING KEY */
	cdb[2] = 0;
	cdb[3] = 0;
	cdb[4] = 0;
	DSET32(&cdb[5], parameter_len);
	cdb[9] = 0;
	lu_cmd2.cdb = &cdb[0];

	memset(&PRO_data, 0, sizeof PRO_data);
	DSET64(&PRO_data[0], rkey); // RESERVATION KEY
	DSET64(&PRO_data[8], 0); // unregister

	rc = istgt_lu_disk_scsi_persistent_reserve_out(spec, conn, &lu_cmd2,
	    0x06, 0, 0,
	    PRO_data, parameter_len, 1);
	if (rc < 0) {
		ISTGT_LOG("c#%d LU%d:RSV-UPD  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
					conn->id, luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
					spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);
		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			if (prkey != NULL ) {
				ISTGT_LOG("c#%d LU%d:RSV-UPD.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
					conn->id, luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
					prkey->ninitiator_ports);
			}
		}
		lu_cmd->status = lu_cmd2.status;
		if (lu_cmd->status == ISTGT_SCSI_STATUS_RESERVATION_CONFLICT) {
			ISTGT_ERRLOG("c#%d Reservation Conflict while releasing initiator_port:%s\n", conn->id, conn->initiator_port);
			xfree(lu_cmd2.sense_data);
			return -1;
		}
		/* INTERNAL TARGET FAILURE */
		BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		xfree(lu_cmd2.sense_data);
		return -1;
	}

	spec->spc2_reserved = 0;

	lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
	xfree(lu_cmd2.sense_data);
	return 0;
}

#define timediffw(lu_cmd, ch) timediff(lu_cmd, ch, __LINE__)

#define getdata2(data, lu_cmd) {		\
	if (lu_cmd->iobufindx == -1) {		\
		data = NULL;					\
		ISTGT_ERRLOG("data null\n");	\
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;\
		return -1;							\
	} else {							\
		/*?? malloc and copy over all data to data = lu_c*/\
	    uint64_t _nb = 0; uint8_t *dptr; int i;				\
	    for (i = 0; i <= lu_cmd->iobufindx; i++)  \
		        _nb += lu_cmd->iobuf[i].iov_len; \
		dptr = data = xmalloc(_nb);		\
	    for (i = 0; i <= lu_cmd->iobufindx; (dptr += lu_cmd->iobuf[i].iov_len), i++)  \
			memcpy(dptr, lu_cmd->iobuf[i].iov_base, lu_cmd->iobuf[i].iov_len);	\
		freedata = 1;					\
	}									\
}
#define getdata(data, lu_cmd) {			\
	if (lu_cmd->iobufindx == -1) {		\
		data = NULL;					\
		ISTGT_ERRLOG("data null\n");	\
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;\
		break;							\
	} else {							\
		/*?? malloc and copy over all data to data = lu_c*/\
	    uint64_t _nb = 0; uint8_t *dptr; int i;				\
	    for (i = 0; i <= lu_cmd->iobufindx; i++)  \
		        _nb += lu_cmd->iobuf[i].iov_len; \
		dptr = data = xmalloc(_nb);		\
	    for (i = 0; i <= lu_cmd->iobufindx; (dptr += lu_cmd->iobuf[i].iov_len), i++)  \
			memcpy(dptr, lu_cmd->iobuf[i].iov_base, lu_cmd->iobuf[i].iov_len);	\
		freedata = 1;					\
	}									\
}

static int
istgt_lu_disk_unmap(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint8_t *data, int pllen)
{
	int markedForFree = 0, diskIoPendingL = 0, lerr = 0;
	int len = 8, ret = 0; //actual block starts with offset 8
	off_t unmbd[2];
	int markedForReturn = 0;
	uint64_t lba = 0, lblen = 0;
	timediffw(lu_cmd, 'w');
	uint64_t maxlba = spec->blockcnt;
	while (len + 16 <= pllen) {
		unmbd[0] = DGET64(&data[len]);
		unmbd[1] = DGET32(&data[len + 8]);
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d unmap lba:%lu +%lu blocks (%d/%d)\n", conn->id, unmbd[0], unmbd[1], len, pllen);
		if (unmbd[1] != 0) {
			lba = unmbd[0];
			lblen = unmbd[1];
			lu_cmd->lba = lba; 
			lu_cmd->lblen = lblen;
			if (lba >= maxlba || lblen > maxlba || lba > (maxlba - lblen)) {
				ISTGT_ERRLOG("c#%d end of media in unmap\n", conn->id);
				return -1;
			}
			unmbd[0] *= spec->blocklen;
			unmbd[1] *= spec->blocklen;
			enterblockingcall(endofmacro1)
			if (markedForReturn == 1) {
				ret = -1;
				break;
			} else if (markedForReturn == 2) {
				errno = EBUSY;
				return -1;
			}

			if (lu_cmd->aborted == 1) {
				ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
				exitblockingcall(endofmacro3)
				return -1;
			}

#ifndef	REPLICATION
			//TODO
			#ifdef __FreeBSD__
			ret = ioctl(spec->fd, DIOCGDELETE, unmbd);
			#endif
#endif

			exitblockingcall(endofmacro2)
			if (markedForFree == 1 || markedForReturn == 1) {
				if (diskIoPendingL == 0 && markedForFree == 1)
					lu_cmd->connGone = 1;
				ret = -1;
				break;
			} else if (ret == -1) {
				lerr = errno;
				break;
			}
		}
		len += 16;
	}
	timediffw(lu_cmd, 'D');

	if (markedForFree == 1 || markedForReturn == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (unmap lba:%lu+%lu)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, lba, lblen);
	} else if (ret == -1) {
		ISTGT_ERRLOG("c#%d unmap lba:%lu +%lu blocks faild:%d (%d/%d)\n", conn->id, lba, lblen, lerr, len, pllen);
	}
	return ret;
}

static int
istgt_lu_disk_scsi_reserve(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_LU_CMD lu_cmd2;
	uint64_t LUI;
	uint64_t rkey;
	uint8_t cdb[10]= {0};
	uint8_t PRO_data[24] = {0};
	int parameter_len;
	int rc;
	int luni = (spec->lu != NULL) ? spec->lu->num : -1;
	int i = 0;
	ISTGT_LU_PR_KEY *prkey = NULL;

	memset(&lu_cmd2, 0, sizeof lu_cmd2);
	//memset(&cdb, 0, sizeof cdb);
	lu_cmd->sense_data_len = 0;
	lu_cmd2.sense_data = xmalloc(ISTGT_SNSBUFSIZE);
	lu_cmd2.sense_alloc_len = ISTGT_SNSBUFSIZE;
	lu_cmd2.sense_data_len = 0;
	parameter_len = sizeof PRO_data;

	LUI = istgt_get_lui(spec->lu->name, spec->lun & 0xffffU);
	rkey = istgt_get_rkey(conn->initiator_name, LUI);

	/* issue register action of PERSISTENT RESERVE OUT */
	cdb[0] = SPC_PERSISTENT_RESERVE_OUT;
	BDSET8W(&cdb[1], 0x06, 4, 5); /* REGISTER AND IGNORE EXISTING KEY */
	cdb[2] = 0;
	cdb[3] = 0;
	cdb[4] = 0;
	DSET32(&cdb[5], parameter_len);
	cdb[9] = 0;
	lu_cmd2.cdb = &cdb[0];

	//memset(&PRO_data, 0, sizeof PRO_data);
	DSET64(&PRO_data[0], 0);
	DSET64(&PRO_data[8], rkey); // SERVICE ACTION RESERVATION KEY

	rc = istgt_lu_disk_scsi_persistent_reserve_out(spec, conn, &lu_cmd2,
	    0x06, 0, 0,
	    PRO_data, parameter_len, 1);
	if (rc < 0) {
		ISTGT_LOG("c#%d LU%d:RSV-UPD  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
					conn->id, luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
					spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);
		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			if (prkey != NULL ) {
				ISTGT_LOG("c#%d LU%d:RSV-UPD.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
					conn->id, luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
					prkey->ninitiator_ports);
			}
		}
		lu_cmd->status = lu_cmd2.status;
		if (lu_cmd->status == ISTGT_SCSI_STATUS_RESERVATION_CONFLICT) {
			ISTGT_ERRLOG("c#%d Reservation Conflict while reserving initiator_port:%s\n", conn->id, conn->initiator_port);
			xfree(lu_cmd2.sense_data);
			return -1;
		}
		/* INTERNAL TARGET FAILURE */
		BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		xfree(lu_cmd2.sense_data);
		return -1;
	}

	/* issue reserve action of PERSISTENT RESERVE OUT */
	cdb[0] = SPC_PERSISTENT_RESERVE_OUT;
	BDSET8W(&cdb[1], 0x01, 4, 5); /* RESERVE */
	BDSET8W(&cdb[2], 0x00, 7, 4); /* LU_SCOPE */
	BDADD8W(&cdb[2], 0x03, 3, 4); /* ISTGT_LU_PR_TYPE_EXCLUSIVE_ACCESS */
	cdb[3] = 0;
	cdb[4] = 0;
	DSET32(&cdb[5], parameter_len);
	cdb[9] = 0;
	lu_cmd2.cdb = &cdb[0];

	memset(&PRO_data, 0, sizeof PRO_data);
	DSET64(&PRO_data[0], rkey); // RESERVATION KEY
	DSET64(&PRO_data[8], 0);

	rc = istgt_lu_disk_scsi_persistent_reserve_out(spec, conn, &lu_cmd2,
	    0x01, 0x00, 0x03,
	    PRO_data, parameter_len, 1);
	if (rc < 0) {
		ISTGT_LOG("c#%d LU%d:RSV-UPD  0x%16.16lx gen:%d spc2:%d scope:%d type:%d  port=[%s] npr:%d",
					conn->id, luni, spec->rsv_key, spec->pr_generation, spec->spc2_reserved,
					spec->rsv_scope, spec->rsv_type, spec->rsv_port, spec->npr_keys);
		for (i = 0; i < spec->npr_keys; i++) {
			prkey = &spec->pr_keys[i];
			if (prkey != NULL ) {
				ISTGT_LOG("c#%d LU%d:RSV-UPD.%2.2d  key:%lx at:%d [I:%s T:%s] idx:%d tag:%d NIN:%d ",
					conn->id, luni, i, prkey->key, prkey->all_tpg, prkey->registered_initiator_port,
					prkey->registered_target_port, prkey->pg_idx, prkey->pg_tag,
					prkey->ninitiator_ports);
			}
		}
		lu_cmd->status = lu_cmd2.status;
		if (lu_cmd->status == ISTGT_SCSI_STATUS_RESERVATION_CONFLICT) {
			ISTGT_ERRLOG("c#%d Reservation Conflict while reserving initiator_port:%s\n", conn->id, conn->initiator_port);
			xfree(lu_cmd2.sense_data);
			return -1;
		}
		/* INTERNAL TARGET FAILURE */
		BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		xfree(lu_cmd2.sense_data);
		return -1;
	}

	spec->spc2_reserved = 1;

	lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
	xfree(lu_cmd2.sense_data);
	return 0;
}

static int
istgt_lu_disk_lbread(ISTGT_LU_DISK *spec, CONN_Ptr conn __attribute__((__unused__)), ISTGT_LU_CMD_Ptr lu_cmd, uint64_t lba, uint32_t len)
{
	uint64_t maxlba;
	uint64_t llen;
	uint64_t blen;
	uint64_t offset;
	uint64_t nbytes;
	int64_t rc = 0;
#ifndef REPLICATION
	uint8_t *data;
#endif
	int diskIoPendingL = 0, markedForFree = 0;
	int markedForReturn = 0;

	if (len == 0) {
		lu_cmd->data = NULL;
		lu_cmd->data_len = 0;
		return 0;
	}

	maxlba = spec->blockcnt;
	llen = (uint64_t) len;
	blen = spec->blocklen;
	offset = lba * blen;
	nbytes = llen * blen;


	if (lba >= maxlba || llen > maxlba || lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media max:%lu (lba:%lu+%u)\n", conn->id, maxlba, lba, len);
		return -1;
	}
	if(spec->error_inject  & READ_INFLIGHT_ISCSI)
		if(spec->inject_cnt > 0)
		{
			spec->inject_cnt--;
			sleep(8);
		}

	enterblockingcall(endofmacro1)
	if (markedForReturn == 1) {
		ISTGT_ERRLOG("Error in locking");
		return -1;
	} else if (markedForReturn == 2) {
		errno = EBUSY;
		return -1;
	}

	if (lu_cmd->aborted == 1) {
		ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
		exitblockingcall(endofmacro3)
		return -1;
	}

	if(spec->error_inject  & READ_INFLIGHT_ONDISK)
		if(spec->inject_cnt > 0)
		{
			spec->inject_cnt--;
			sleep(8);
		}

	timediffw(lu_cmd, 'w');

#ifdef REPLICATION
	rc = replicate(spec, lu_cmd, offset, nbytes);
#else
	data = xmalloc(nbytes);
	rc = pread(spec->fd, data, nbytes, offset);
#endif
	timediffw(lu_cmd, 'D');

	exitblockingcall(endofmacro2)
	if (markedForFree == 1 || markedForReturn == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (read:%zd/%zd lba:%lu+%u)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nbytes, rc, lba, len);
		if (diskIoPendingL == 0 && markedForFree == 1)
			lu_cmd->connGone = 1;
#ifndef REPLICATION
		xfree(data);
#endif
		return -1;
	}

	if (rc < 0 || (uint64_t) rc != nbytes) {
#ifndef REPLICATION
		xfree(data);
#endif
		errlog(lu_cmd, "c#%d lu_disk_read() failed errnor:%d read:%ld (%lu+%lu, lba:%lu+%u)", conn->id, errno, rc, offset, nbytes, lba, len)
		return -1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d Read %"PRId64"/%"PRIu64" bytes (lba:%lu+%u)\n",
	    conn->id, rc, nbytes, lba, len);

#ifndef REPLICATION
	lu_cmd->data = data;
	lu_cmd->data_len = rc;
#endif
	return 0;
}
static int
istgt_lu_disk_lbwrite(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint64_t lba, uint32_t len)
{
	uint64_t maxlba;
	uint64_t llen;
	uint64_t blen;
	uint64_t offset;
	uint64_t l_offset = 0;
	uint64_t nbytes;
	unsigned long nbits = 0;
	unsigned long nthbitset = 0;
	int64_t rc = 0;
	int iovcnt;
	struct iovec iov[40];
	int diskIoPendingL = 0, markedForFree = 0;
	int markedForReturn = 0;
	const char *msg = "write";
	int i;
#ifndef	REPLICATION
	uint64_t actual;
#endif

	if (len == 0) {
		lu_cmd->data_len = 0;
		return 0;
	}

	maxlba = spec->blockcnt;
	llen = (uint64_t) len;
	blen = spec->blocklen;
	offset = lba * blen;
	nbytes = llen * blen;


	if (lba >= maxlba || llen > maxlba || lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media, max:%lu write lba:%lu+%u\n", conn->id, maxlba, lba, len);
		return -1;
	}

	rc = istgt_lu_disk_transfer_data(conn, lu_cmd, nbytes);
	if (rc < 0) {
		ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed (write lba:%lu+%u)\n", conn->id, lba, len);
		return -1;
	}

	iovcnt = lu_cmd->iobufindx+1;
	//memcpy(&iov[0], &lu_cmd->iobuf[0], sizeof(iov));
	for (i=0; i<iovcnt; ++i) {
		iov[i].iov_base = lu_cmd->iobuf[i].iov_base;
		iov[i].iov_len = lu_cmd->iobuf[i].iov_len;
	}

#ifdef	REPLICATION
	while (spec->quiesce) {
		ISTGT_ERRLOG("c#%d LU%d: quiescing write IOs\n", conn->id, spec->lu->num);
		sleep(1);
	}
#endif

	if (nbytes != lu_cmd->iobufsize) { //aj-the below call doesn't read anything
		ISTGT_ERRLOG("c#%d nbytes(%zu) != iobufsize(%zu) (write lba:%lu+%u)\n",
		    conn->id, (size_t) nbytes, lu_cmd->iobufsize, lba, len);
freeiovcnt:
		for (i=0; i<iovcnt; ++i) {
			if(iov[i].iov_base == NULL)
				xfree(iov[i].iov_base);
			iov[i].iov_base = NULL;
		}

		if (markedForReturn == 2)
			errno = EBUSY;
		return -1;
	}
	if (spec->lu->readonly) {
		ISTGT_ERRLOG("c#%d LU%d: readonly unit\n", conn->id, spec->lu->num);
		goto freeiovcnt;
	}
	if(spec->wzero) {
		nbits = nbytes << 3;
		nthbitset = find_first_bit(lu_cmd);
	}

	if(spec->error_inject  & WRITE_INFLIGHT_ISCSI)
		if(spec->inject_cnt > 0)
		{
			spec->inject_cnt--;
			sleep(8);
		}

	enterblockingcall(endofmacro1);
	if (markedForReturn == 1) {
		ISTGT_ERRLOG("c#%d Error in entering block call", conn->id);
		goto freeiovcnt;
	} else if (markedForReturn == 2)
		goto freeiovcnt;

	if(lu_cmd->aborted == 1)
	{
		ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
		exitblockingcall(endofmacro3);
		goto freeiovcnt;
	}

	if(spec->error_inject  & WRITE_INFLIGHT_ONDISK)
		if(spec->inject_cnt > 0)
		{
			spec->inject_cnt--;
			sleep(8);
		}

	timediffw(lu_cmd, 'w');
	if (spec->wzero && nthbitset == nbits) {
		msg = "wzero";
#ifndef	REPLICATION
		//TODO
		#ifdef __FreeBSD__
		off_t unmbd[2];
		unmbd[0] = offset;
		unmbd[1] = nbytes;
		rc = ioctl(spec->fd, DIOCGDELETE, unmbd);
		if (rc == 0)
			rc = nbytes;
		#endif
#endif
	} else {
#ifdef REPLICATION
		rc = replicate(spec, lu_cmd, offset, nbytes);
		lu_cmd->data = NULL;
#else
		actual = lu_cmd->iobufsize; l_offset = offset;
		while (actual > 0) {
			rc = pwritev(spec->fd, &lu_cmd->iobuf[0], iovcnt, l_offset);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d pwritev() failed errno:%d\n", conn->id, errno);
				break;
			}
			l_offset += rc;
			actual -= rc;
			if (actual == 0)
				break;
			/* adjust iovec length */
			for (i = 0; i < iovcnt; i++) {
				if (lu_cmd->iobuf[i].iov_len != 0 && lu_cmd->iobuf[i].iov_len > (size_t)rc) {
					lu_cmd->iobuf[i].iov_base = (void *) (((char *)lu_cmd->iobuf[i].iov_base) + rc);
					lu_cmd->iobuf[i].iov_len -= rc;
					break;
				} else {
					rc -= lu_cmd->iobuf[i].iov_len;
					lu_cmd->iobuf[i].iov_len = 0;
				}
			}
		}
#endif
	}

	lu_cmd->iobufsize = 0;
	lu_cmd->iobufindx = -1;
	for (i=0; i< iovcnt; ++i) {
		lu_cmd->iobuf[i].iov_base = NULL;
		lu_cmd->iobuf[i].iov_len = 0;
	}
	
	exitblockingcall(endofmacro2)
	timediffw(lu_cmd, 'D');
	if (markedForFree == 1 || markedForReturn == 1) {
		ISTGT_ERRLOG("c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (wrote:%zd/%zd lba:%lu+%u)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nbytes, rc, lba, len);
		if (diskIoPendingL == 0 && markedForFree == 1)
			lu_cmd->connGone = 1;
#ifndef REPLICATION
		for (i=0; i<iovcnt; ++i)
			xfree(iov[i].iov_base);
#endif
		return -1;
	}
	if (rc < 0)  {
		errlog(lu_cmd, "c#%d lu_disk_write() failed wrote:%lu of %lu+%lu (%lu) lba:%lu+%u", conn->id, rc, offset, nbytes, l_offset, lba, len)
#ifndef REPLICATION
		for (i=0; i<iovcnt; ++i)
			xfree(iov[i].iov_base);
#endif
		return -1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d Wrote %lu/%lu bytes (lba:%lu+%u) %s\n",
	    conn->id, rc, nbytes, lba, len, msg);

	lu_cmd->data_len = rc;
#ifndef REPLICATION
	for (i=0; i<iovcnt; ++i)
		xfree(iov[i].iov_base);
#endif
	return 0;
}
static int
istgt_lu_disk_lbwrite_same(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint64_t lba, uint32_t len)
{
	uint8_t *data = NULL;
	uint8_t *workbuf = NULL;
	uint64_t worksize = ISTGT_LU_WORK_BLOCK_SIZE;
	uint64_t maxlba;
	uint64_t llen;
	uint64_t blen;
	uint64_t offset;
	uint64_t offset_local;
	uint64_t nbytes;
	uint64_t wblocks;
	uint64_t nblocks;
	int64_t rc = 0;
	int diskIoPendingL = 0, markedForFree = 0;
	int markedForReturn = 0;
	unsigned long nbits = 0;
	unsigned long nthbitset = 0;
	int eno;
	int freedata = 0;
	maxlba = spec->blockcnt;
	llen = (uint64_t) len;
	const char *msg = "wsame";

	if (llen == 0) {
		if (lba >= maxlba) {
			ISTGT_ERRLOG("end of media\n");
			return -1;
		}
		llen = maxlba - lba;
	}

	blen = spec->blocklen;
	offset = lba * blen;
	nbytes = 1 * blen;

	ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
	    "c#%d Write Same: max=%"PRIu64", lba=%"PRIu64", len=%u\n",
	    conn->id, maxlba, lba, len);

	if (lba >= maxlba || llen > maxlba || lba > (maxlba - llen)) {
		ISTGT_ERRLOG("end of media\n");
		return -1;
	}


	if (spec->lu->readonly) {
		ISTGT_ERRLOG("c#%d LU%d: readonly unit\n", conn->id, spec->lu->num);
		return -1;
	}
	getdata2(data, lu_cmd)
	
	if (spec->wzero) {
		nbits = nbytes << 3;
		nthbitset = find_first_bit(lu_cmd);
	}
	if (spec->wzero && nthbitset == nbits) {
		msg = "wzero";
		enterblockingcall(endofmacro1);
		if (markedForReturn == 1 || markedForReturn == 2) {
			if(freedata == 1)
				xfree(data);
			if (markedForReturn == 1) {
				ISTGT_ERRLOG("c#%d Error in locking", conn->id);
			} else if (markedForReturn == 2) {
				errno = EBUSY;
			}
			return -1;
		}

		if (lu_cmd->aborted == 1) {
			ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
			exitblockingcall(endofmacro5)
			if(freedata == 1)
				xfree(data);
			return -1;
		}
#ifndef	REPLICATION
		//TODO
		#ifdef __FreeBSD__	
		off_t unmbd[2];
		unmbd[0] = offset;
		unmbd[1] = llen * blen;
		rc = ioctl(spec->fd, DIOCGDELETE, unmbd);
		#endif
#endif
		eno = errno;
		exitblockingcall(endofmacro2);
		if (markedForFree == 1 || markedForReturn == 1) {
			ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (wsameX lba:%lu+%u ret:%ld).",
					conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, lba, len, rc);
			if(freedata == 1)
				xfree(data);
			if (diskIoPendingL == 0 && markedForFree == 1)
				lu_cmd->connGone = 1;
			return -1;
		}
		if (rc == -1) {
			if(freedata == 1)
				xfree(data);
			ISTGT_ERRLOG("c#%d wsameX lba:%lu+%u failed %ld/%d\n", conn->id, lba, len, rc, eno);
			return -1;
		}
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d wzero_same done lba:%lu+%u\n", conn->id, lba, len);

		if(freedata == 1)
			xfree(data);
		lu_cmd->data_len = nbytes;
		return 0;
	}

	wblocks = (int64_t)worksize / nbytes;
	if (wblocks == 0) {
		ISTGT_ERRLOG("c#%d work buffer is too small\n", conn->id);
		if (freedata == 1) xfree(data);
		return -1;
	}

	nblocks = 0;
	workbuf = xmalloc(worksize);
	while (nblocks < wblocks) {
		memcpy(workbuf + (nblocks * nbytes), data, nbytes);
		nblocks++;
	}
	nblocks = 0;
	offset_local = offset;
	timediffw(lu_cmd, 'w');
	while (nblocks < llen) {
		uint64_t reqblocks = DMIN64(wblocks, (llen - nblocks));
		uint64_t reqbytes = reqblocks * nbytes;
		enterblockingcall(endofmacro3);
		if (markedForReturn == 1 || markedForReturn == 2) {
			if(freedata == 1)
				xfree(data);
			xfree(workbuf);
			if (markedForReturn == 1) {
				ISTGT_ERRLOG("c#%d Error in locking", conn->id);
			} else if (markedForReturn == 2) {
				errno = EBUSY;
			}
			return -1;
		}

		if (lu_cmd->aborted == 1) {
			ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
			exitblockingcall(endofmacro6)
			if(freedata == 1)
				xfree(data);
			xfree(workbuf);
			return -1;
		}
#ifndef	REPLICATION
		rc = pwrite(spec->fd, workbuf, reqbytes, offset_local);
#endif
		exitblockingcall(endofmacro4);
		if (markedForFree == 1 || markedForReturn == 1) {
			timediffw(lu_cmd, 'D');
			ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (wrote:%zd of %zd. ret:%ld).",
					conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nblocks, llen, rc);
			if (diskIoPendingL == 0 && markedForFree == 1)
				lu_cmd->connGone = 1;
			if (freedata == 1) xfree(data);
			xfree(workbuf);
			return -1;
		}
		if (rc < 0 || (uint64_t) rc != reqbytes) {
			timediffw(lu_cmd, 'D');
			errlog(lu_cmd, "c#%d lu_disk_write() failed wrote:%ld, %lu of %lu blksize, blksz:%zd", conn->id, rc, nblocks, llen, nbytes)
			if (freedata == 1) xfree(data);
			xfree(workbuf);
			return -1;
		}
		offset_local += reqbytes;
		nblocks += reqblocks;
	}

	timediffw(lu_cmd, 'D');
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d Wrote %"PRId64" at %"PRIu64" bytes %s\n",
	    conn->id, (llen * nbytes), offset, msg);

	lu_cmd->data_len = nbytes;

	if (freedata == 1) xfree(data);
	xfree(workbuf);
	return 0;
}

static int
istgt_lu_disk_lbwrite_ats(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint64_t lba, uint32_t len)
{
	uint8_t *data = NULL;
	uint8_t *watsbuf;
	uint64_t maxlba;
	uint64_t llen;
	uint64_t blen;
	uint64_t offset;
	uint64_t nbytes;
	int64_t rc = 0;
	int diskIoPendingL = 0, markedForFree = 0;
	int markedForReturn = 0;
	int freedata = 0;
	if (len == 0) {
		lu_cmd->data_len = 0;
		return 0;
	}

	maxlba = spec->blockcnt;
	llen = (uint64_t) len;
	blen = spec->blocklen;
	offset = lba * blen;
	nbytes = llen * blen;

	 ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
	 	"c#%d Write ATS: max=%"PRIu64", lba=%"PRIu64", len=%u\n",
		 conn->id, maxlba, lba, len);

	if (lba >= maxlba || llen > maxlba || lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media\n", conn->id);
		return -1;
	}

	if (spec->lu->readonly) {
		ISTGT_ERRLOG("c#%d LU%d: readonly unit\n", conn->id, spec->lu->num);
		return -1;
	}

	getdata2(data, lu_cmd)

	/* start atomic test and set */

	watsbuf = xmalloc(nbytes);
	enterblockingcall(endofmacro1)
	if (markedForReturn == 1 || markedForReturn == 2) {
		if (freedata == 1)
			xfree(data);
		xfree(watsbuf);
		if (markedForReturn == 1) {
			ISTGT_ERRLOG("c#%d Error in locking", conn->id);
		} else if (markedForReturn == 2) {
			errno = EBUSY;
		}
		return -1;
	}

	timediffw(lu_cmd, 'w');
	if (lu_cmd->aborted == 1) {
		ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
		exitblockingcall(endofmacro5)
		if(freedata == 1)
			xfree(data);
		xfree(watsbuf);
		return -1;
	}
#ifndef	REPLICATION
	rc = pread(spec->fd, watsbuf, nbytes, offset);
#endif
	exitblockingcall(endofmacro2)
	if (markedForFree == 1 || markedForReturn == 1) {
		timediffw(lu_cmd, 'D');
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (ats read:%zd/%zd @%zd)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nbytes, rc, offset);
		if (diskIoPendingL == 0 && markedForFree == 1)
			lu_cmd->connGone = 1;
		if (freedata == 1) xfree(data);
		xfree(watsbuf);
		return -1;
	}
	if (rc < 0 || (uint64_t) rc != nbytes) {
		timediffw(lu_cmd, 'D');
		errlog(lu_cmd, "c#%d lu_disk_read()ats failed %ld/%lu\n", conn->id, rc, nbytes)
		xfree(watsbuf);
		if (freedata == 1) xfree(data);
		return -1;
	}

#if 0
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "ATS VERIFY", data, nbytes);
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "ATS WRITE", data + nbytes, nbytes);
	ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "ATS DATA", spec->watsbuf, nbytes);
#endif
	if (memcmp(watsbuf, data, nbytes) != 0) {
		timediffw(lu_cmd, 'D');
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"c#%d ATS: Miscompare %s\n", conn->id, spec->file);
		/* MISCOMPARE DURING VERIFY OPERATION */
		BUILD_SENSE(MISCOMPARE, 0x1d, 0x00);
		xfree(watsbuf);
		if (freedata == 1) xfree(data);
		return -1;
	}

	enterblockingcall(endofmacro3)
	if (markedForReturn == 1 || markedForReturn == 2) {
		if(freedata == 1)
			xfree(data);
		xfree(watsbuf);
		if (markedForReturn == 1) {
			ISTGT_ERRLOG("c#%d Error in locking", conn->id);
		} else if (markedForReturn == 2) {
			errno = EBUSY;
		}
		return -1;
	}

	if (lu_cmd->aborted == 1) {
		ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
		exitblockingcall(endofmacro6)
		if(freedata == 1)
			xfree(data);
		xfree(watsbuf);
		return -1;
	}
#ifndef	REPLICATION
	rc = pwrite(spec->fd, data + nbytes, nbytes, offset);
#endif
	exitblockingcall(endofmacro4)
	timediffw(lu_cmd, 'D');
	if (markedForFree == 1 || markedForReturn == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (ats write:%zd/%zd @%zd)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nbytes, rc, offset);
		if (diskIoPendingL == 0 && markedForFree == 1)
			lu_cmd->connGone = 1;
		xfree(watsbuf);
		if (freedata == 1) xfree(data);
		return -1;
	}
	if (rc < 0 || (uint64_t) rc != nbytes) {
		errlog(lu_cmd, "c#%d lu_disk_write()ats failed %ld/%lu errno:%d\n", conn->id, rc, nbytes, errno)
		xfree(watsbuf);
		if (freedata == 1) xfree(data);
		return -1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d Wrote %"PRId64"/%"PRIu64" bytes\n",
	    conn->id, rc, nbytes);

	/* end atomic test and set */

	lu_cmd->data_len = nbytes * 2;
	xfree(watsbuf);
	if (freedata == 1) xfree(data);
	return 0;
}

static int
istgt_lu_disk_lbsync(ISTGT_LU_DISK *spec, CONN_Ptr conn __attribute__((__unused__)), ISTGT_LU_CMD_Ptr lu_cmd __attribute__((__unused__)), uint64_t lba, uint32_t len)
{
	uint64_t maxlba;
	uint64_t llen;
	uint64_t blen;
	uint64_t offset;
	uint64_t nbytes;
	int64_t rc;
	int diskIoPendingL = 0, markedForFree = 0;
	int markedForReturn = 0;

	if (len == 0) {
		return 0;
	}

	maxlba = spec->blockcnt;
	llen = (uint64_t) len;
	blen = spec->blocklen;
	offset = lba * blen;
	nbytes = llen * blen;

	ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
	    "c#%d Sync: max=%"PRIu64", lba=%"PRIu64", len=%u\n",
	    conn->id, maxlba, lba, len);

	if (lba >= maxlba || llen > maxlba || lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media\n", conn->id);
		return -1;
	}

#ifdef  REPLICATION
	while (spec->quiesce) {
		ISTGT_ERRLOG("c#%d LU%d: quiescing sync IOs\n", conn->id, spec->lu->num);
		sleep(1);
	}
#endif

	enterblockingcall(endofmacro1)
	if (markedForReturn == 1) {
		ISTGT_ERRLOG("c#%d Error in locking", conn->id);
		return -1;
	} else if (markedForReturn == 2) {
		errno = EBUSY;
		return -1;
	}

	timediffw(lu_cmd, 'w');
	if (lu_cmd->aborted == 1) {
		ISTGT_LOG("(0x%x) c#%d aborting the IO\n", lu_cmd->CmdSN, conn->id);
		exitblockingcall(endofmacro3)
		return -1;
	}
#ifdef REPLICATION
	rc = replicate(spec, lu_cmd, offset, nbytes);
#else
	rc = spec->sync(spec, offset, nbytes);
#endif
	exitblockingcall(endofmacro2)
	timediffw(lu_cmd, 'D');
	if (markedForFree == 1 || markedForReturn == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_NET, "c#%d connGone(%d)OrMarkedReturn(%d):%p:%d pendingIO:%d (sync:%zd/%zd @%zd)",
				conn->id, markedForFree, markedForReturn, conn, conn->cid, diskIoPendingL, nbytes, rc, offset);
		if (diskIoPendingL == 0 && markedForFree == 1)
			lu_cmd->connGone = 1;
		return -1;
	}
	if (rc < 0) {
		errlog(lu_cmd, "c#%d lu_disk_sync() failed %ld %lu-%lu\n", conn->id, rc, offset, nbytes)
		return -1;
	}

	return 0;
}

void
istgt_lu_scsi_build_sense_data(ISTGT_LU_CMD_Ptr lu_cmd, int sk, int asc, int ascq)
{
	uint8_t *cp;
	int resp_code;
	int hlen = 0, len = 0, plen;
	int total;
	uint8_t *data = lu_cmd->sense_data = xmalloc(200);

	resp_code = 0x70; /* Current + Fixed format */

	/* SenseLength */
	DSET16(&data[0], 0);
	hlen = 2;

	/* Sense Data */
	cp = &data[hlen + len];

	/* VALID(7) RESPONSE CODE(6-0) */
	BDSET8(&cp[0], 1, 7);
	BDADD8W(&cp[0], resp_code, 6, 7);
	/* Obsolete */
	cp[1] = 0;
	/* FILEMARK(7) EOM(6) ILI(5) SENSE KEY(3-0) */
	BDSET8W(&cp[2], sk, 3, 4);
	/* INFORMATION */
	cp[3] = 0; cp[4] = 0; cp[5] = 0; cp[6] = 0; //memset(&cp[3], 0, 4);
	/* ADDITIONAL SENSE LENGTH */
	cp[7] = 0;
	len = 8;

	/* COMMAND-SPECIFIC INFORMATION */
	cp[8] = 0; cp[9] = 0; cp[10] = 0; cp[11] = 0; //memset(&cp[8], 0, 4);
	/* ADDITIONAL SENSE CODE */
	cp[12] = asc;
	/* ADDITIONAL SENSE CODE QUALIFIER */
	cp[13] = ascq;
	/* FIELD REPLACEABLE UNIT CODE */
	cp[14] = 0;
	/* SKSV(7) SENSE KEY SPECIFIC(6-0,7-0,7-0) */
	cp[15] = 0;
	cp[16] = 0;
	cp[17] = 0;
	/* Additional sense bytes */
	//data[18] = 0;
	plen = 18 - len;

	/* ADDITIONAL SENSE LENGTH */
	cp[7] = plen;

	total = hlen + len + plen;

	/* SenseLength */
	DSET16(&data[0], total - 2);

	lu_cmd->sense_data_len = total;
	return;
}

void
istgt_lu_scsi_build_sense_data2(ISTGT_LU_CMD_Ptr lu_cmd, int sk, int asc, int ascq)
{
	uint8_t *cp;
	int resp_code;
	int hlen = 0, len = 0, plen;
	int total;
	uint8_t *data = lu_cmd->sense_data = xmalloc(200);

	resp_code = 0x71; /* Deferred + Fixed format */

	/* SenseLength */
	DSET16(&data[0], 0);
	hlen = 2;

	/* Sense Data */
	cp = &data[hlen + len];

	/* VALID(7) RESPONSE CODE(6-0) */
	BDSET8(&cp[0], 1, 7);
	BDADD8W(&cp[0], resp_code, 6, 7);
	/* Obsolete */
	cp[1] = 0;
	/* FILEMARK(7) EOM(6) ILI(5) SENSE KEY(3-0) */
	BDSET8W(&cp[2], sk, 3, 4);
	/* INFORMATION */
	cp[3] = 0; cp[4] = 0; cp[5] = 0; cp[6] = 0; //memset(&cp[3], 0, 4);
	/* ADDITIONAL SENSE LENGTH */
	cp[7] = 0;
	len = 8;

	/* COMMAND-SPECIFIC INFORMATION */
	cp[8] = 0; cp[9] = 0; cp[10] = 0; cp[11] = 0; //memset(&cp[8], 0, 4);
	/* ADDITIONAL SENSE CODE */
	cp[12] = asc;
	/* ADDITIONAL SENSE CODE QUALIFIER */
	cp[13] = ascq;
	/* FIELD REPLACEABLE UNIT CODE */
	cp[14] = 0;
	/* SKSV(7) SENSE KEY SPECIFIC(6-0,7-0,7-0) */
	cp[15] = 0;
	cp[16] = 0;
	cp[17] = 0;
	/* Additional sense bytes */
	//data[18] = 0;
	plen = 18 - len;

	/* ADDITIONAL SENSE LENGTH */
	cp[7] = plen;

	total = hlen + len + plen;

	/* SenseLength */
	DSET16(&data[0], total - 2);

	lu_cmd->sense_data_len = total;
	return;
}


IT_NEXUS *
istgt_lu_disk_get_nexus(ISTGT_LU_DISK *spec, const char *initiator_port)
{
	IT_NEXUS *nexus;
	TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
		if (nexus == NULL)
			break;
		if (strcasecmp(nexus->initiator_port, initiator_port) == 0)
			return (nexus);
	}
	return NULL;
}

int
istgt_lu_disk_build_ua(istgt_ua_type ua_pending, int *sk,  int *asc, int *ascq)
{
	istgt_ua_type ua_to_build = ISTGT_UA_NONE;
	uint64_t i;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_build_ua\n");

	if (ua_pending == ISTGT_UA_NONE)
		return(ua_pending);

	for (i =0 ; i < (sizeof(ua_pending)* 8); i++) {
		if (ua_pending & (1 << i)) {
			ua_to_build = 1 << i;
			break;
		}
	}

	*sk = ISTGT_SCSI_SENSE_UNIT_ATTENTION;

	switch (ua_to_build) {
		case ISTGT_UA_POWERON:
			/* 29h/01h  POWER ON OCCURRED */
			*asc = 0x29;
			*ascq = 0x01;
			break;
		case ISTGT_UA_BUS_RESET:
			/* 29h/02h  SCSI BUS RESET OCCURRED */
			*asc = 0x29;
			*ascq = 0x02;
			break;
		case ISTGT_UA_TARG_RESET:
			/* 29h/03h  BUS DEVICE RESET FUNCTION OCCURRED*/
			*asc = 0x29;
			*ascq = 0x03;
			break;
		case ISTGT_UA_LUN_RESET:
			/* 29h/00h  POWER ON, RESET, OR BUS DEVICE RESET OCCURRED */
			/*
			 * Since we don't have a specific ASC/ASCQ pair for a LUN
			 * reset, just return the generic reset code.
			 */
			*asc = 0x29;
			*ascq = 0x00;
			break;
		case ISTGT_UA_LUN_CHANGE:
			/* 3Fh/0Eh  REPORTED LUNS DATA HAS CHANGED */
			*asc = 0x3F;
			*ascq = 0x0E;
			break;
		case ISTGT_UA_MODE_CHANGE:
			/* 2Ah/01h  MODE PARAMETERS CHANGED */
			*asc = 0x2A;
			*ascq = 0x01;
			break;
		case ISTGT_UA_LOG_CHANGE:
			/* 2Ah/02h  LOG PARAMETERS CHANGED */
			*asc = 0x2A;
			*ascq = 0x02;
			break;
		case ISTGT_UA_LVD:
			/* 29h/06h  TRANSCEIVER MODE CHANGED TO LVD */
			*asc = 0x29;
			*ascq = 0x06;
			break;
		case ISTGT_UA_SE:
			/* 29h/05h  TRANSCEIVER MODE CHANGED TO SINGLE-ENDED */
			*asc = 0x29;
			*ascq = 0x05;
			break;
		case ISTGT_UA_RES_PREEMPT:
			/* 2Ah/03h  RESERVATIONS PREEMPTED */
			*asc = 0x2A;
			*ascq = 0x03;
			break;
		case ISTGT_UA_RES_RELEASE:
			/* 2Ah/04h  RESERVATIONS RELEASED */
			*asc = 0x2A;
			*ascq = 0x04;
			break;
		case ISTGT_UA_REG_PREEMPT:
			/* 2Ah/05h  REGISTRATIONS PREEMPTED */
			*asc = 0x2A;
			*ascq = 0x05;
			break;
		case ISTGT_UA_ASYM_ACC_CHANGE:
			/* 2Ah/06n  ASYMMETRIC ACCESS STATE CHANGED */
			*asc = 0x2A;
			*ascq = 0x06;
			break;
		case ISTGT_UA_CAPACITY_CHANGED:
			/* 2Ah/09n  CAPACITY DATA HAS CHANGED */
			*asc = 0x2A;
			*ascq = 0x09;
			break;
		default:
			ua_to_build = ISTGT_UA_NONE;
			return (ua_to_build);
			break; /* NOTREACHED */
	}

	return ua_to_build;
}

int
istgt_lu_disk_persist_reservation(ISTGT_LU_Ptr lu, int lun, char* arg)
{
        ISTGT_LU_DISK *spec;

        if (lu == NULL) {
                ISTGT_LOG("lu is NULL");
                return -1;
        }
        if (lun >= lu->maxlun) {
                ISTGT_LOG("lu %d > max %d", lun, lu->maxlun);
                return -1;
        }
        if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
                ISTGT_LOG("lu %d type %d", lun, lu->lun[lun].type);
                return -1;
        }
        if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
                ISTGT_LOG("lu1 %d type %d", lun, lu->lun[lun].type);
                return -1;
        }
        spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
        if(*arg == '1')
                spec->persist = 1;
        else
                spec->persist = 0;
        return 0;
}

int
istgt_lu_disk_sync_reservation(ISTGT_LU_Ptr lu, int lun)
{
        ISTGT_LU_DISK *spec;
        int rc;
        int ret;

        if (lu == NULL) {
                return -1;
        }
        if (lun >= lu->maxlun) {
                return -1;
        }
        if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
                return -1;
        }
        if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
                return -1;
        }
        spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;

        if(spec->persist==0){
                ISTGT_LOG("SYNCing even with PERSIST DISABLED");
        }

        MTX_LOCK(&spec->pr_rsv_mutex);
        /* write reservation to file */
        ISTGT_LOG("Syncing Reservation");
        rc = istgt_lu_disk_update_reservation(spec);
        if (rc < 0) {
                ISTGT_ERRLOG("istgt_lu_disk_update_reservation() failed\n");
        }

        ret = 0;
        MTX_UNLOCK(&spec->pr_rsv_mutex);

        return ret;

}





int
istgt_lu_disk_reset(ISTGT_LU_Ptr lu, int lun , istgt_ua_type ua_type)
{
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	int cleared = 0;

	if (lu == NULL) {
		return -1;
	}
	if (lun >= lu->maxlun) {
		return -1;
	}
	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;

	#if 0
		if (spec->lock) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "unlock by reset\n");
			spec->lock = 0;
		}
	#endif

	/* Abort alll the commands in the queue */
	if (lu->queue_depth != 0) {
		cleared = istgt_lu_disk_queue_clear_all(lu, lun);
		if (cleared < 0) {
			ISTGT_ERRLOG("lu_disk_queue_clear_all() failed\n");
			return -1;
		}
	}

#ifndef	REPLICATION
	/* re-open file */
	if (!spec->lu->readonly) {
		if (spec->sync(spec, 0, spec->size) < 0) {
			ISTGT_ERRLOG("LU%d: LUN%d: lu_disk_sync() failed\n",
			lu->num, lun);
			/* ignore error */
		}
	}

	/* SAM-3 (SCSI ARCHITECTURE MODEL):
	 * Before returning a FUNCTION COMPLETE response, the target port
	 * shall perform logical unit reset functions specified in spec.

	 * Relase all the Reservation Established using RESERVE/RELEASE
	 * Management methods (Persistant Reservation shall not be altered).

	 * A unit_attention (UA) condition for all initiators that have
	 * access shall be created on each of these logical units.
	 */

	istgt_lu_disk_seek_raw(spec, 0);
#endif

	if (spec->spc2_reserved) {
		/* release reservation by key */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"LU%d: Lun Reset cleared the SPC2 reservations \n",  lu->num);
		xfree(spec->rsv_port);
		spec->rsv_port = NULL;
		spec->rsv_key = 0;
		spec->spc2_reserved = 0;
		spec->rsv_scope = 0;
		spec->rsv_type = 0;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Set U/A to all the initiator that have access to LU%d \n", lu->num);
	TAILQ_FOREACH(nexus, &spec->nexus, nexus_next) {
		if (nexus == NULL)
			break;
		nexus->ua_pending |= ua_type;
	}
	return cleared;
}

int
istgt_lu_disk_clear_reservation(ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_DISK *spec, *spec_bkp;
	int i;
	int bkp_success = 1;
	int rc;

	if (lu == NULL) {
		return -1;
	}
	if (lun >= lu->maxlun) {
		return -1;
	}
	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	spec_bkp = xmalloc(sizeof(*spec_bkp));
        memset(spec_bkp, 0, sizeof(*spec_bkp));

	MTX_LOCK(&spec->pr_rsv_mutex);
	rc = istgt_lu_disk_copy_reservation(spec_bkp, spec);
	if (rc < 0) {
		/* Ignore error */
		bkp_success = -1;
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		xfree(spec_bkp);
		return rc;
	}
	/* release reservation */
	xfree(spec->rsv_port);
	spec->rsv_port = NULL;
	spec->rsv_key = 0;
	spec->rsv_scope = 0;
	spec->rsv_type = 0;

	/* remove all registrations */
	for (i = 0; i < spec->npr_keys; i++) {
		istgt_lu_disk_free_pr_key(&spec->pr_keys[i]);
	}
	spec->npr_keys = 0;
	for (i = 0; i < MAX_LU_RESERVE; i++) {
		memset(&spec->pr_keys[i], 0, sizeof(spec->pr_keys[i]));
	}
	if(spec->persist && spec->spc2_reserved == 0){
		rc = istgt_lu_disk_update_reservation(spec); 
		if (rc < 0) {
			ISTGT_ERRLOG("istgt_lu_disk_update_reservation() failed\n");
			/* Copy only if the backup was successful */
			if (bkp_success == 1) {
				rc = istgt_lu_disk_copy_reservation(spec, spec_bkp);
				if (rc < 0) {
					ISTGT_LOG("istgt_lu_disk_copy_reservation() failed\n");	
				}
			}
		}
	}
	spec->spc2_reserved = 0;
	MTX_UNLOCK(&spec->pr_rsv_mutex);
	xfree(spec_bkp);
	return rc;
}

int
istgt_lu_disk_start(ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_DISK *spec;
	int flags;
	int rc;

	if (lu == NULL) {
		return -1;
	}
	if (lun >= lu->maxlun) {
		return -1;
	}
	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;

	flags = lu->readonly ? O_RDONLY : O_RDWR;
	if(spec->fd > -1) {
		ISTGT_ERRLOG("LU%d: LUN%d:istgt_lu_disk_start() failed, Device is already open\n",
			lu->num, lun);
		return -1;
	}
	rc = spec->open(spec, flags, 0666);
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: LUN%d: lu_disk_open() failed\n",
		    lu->num, lun);
		return -1;
	}

	MTX_LOCK(&spec->state_mutex);
	spec->state = ISTGT_LUN_ONLINE;
	spec->ex_state = ISTGT_LUN_OPEN;
	MTX_UNLOCK(&spec->state_mutex);
	lu->limit_q_size=0;

	/* Online the logical unit */
	lu->online = 1;

	return 0;
}

int
istgt_lu_disk_stop(ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_DISK *spec;
	CONN_Ptr conn;
	int rc;
	int j;
	int workers_signaled = 0, loop = 0;
	struct timespec  _s1, _s2, _s3, _wrk, _wrkx;

	if (lu == NULL) {
		return -1;
	}
	if (lun >= lu->maxlun) {
		return -1;
	}
	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	clock_gettime(clockid, &_wrk);
	if (lu->queue_depth != 0) {
		rc = istgt_lu_disk_queue_clear_all(lu, lun);
		if (rc < 0) {
			ISTGT_ERRLOG("lu_disk_queue_clear_all() failed\n");
			return -1;
		}
	}

	for (j = 1; j < MAX_LU_TSIH; j++) {
		if (lu->tsih[j].initiator_port != NULL
			&& lu->tsih[j].tsih != 0) {
			/* Find conn */
			conn = istgt_find_conn(lu->tsih[j].initiator_port, lu->name, lu->tsih[j].tsih);
			if (conn == NULL)
				continue;
			/* Send asyn notification to the client */
			istgt_iscsi_send_async(conn);
		}
	}

	/* Offline the Logical Unit
         * Complete Logical Unit Need not be offlined if we have to stop a particular LUN.
         * CloudByte Supports/Configures one LUN per Logical Unit so making complete LU to be offline.
         */
	lu->online = 0;
	timesdiff(clockid, _wrk, _wrkx, _s1)
	do {
		MTX_LOCK(&spec->state_mutex);
		spec->state = ISTGT_LUN_BUSY;
		if (spec->ludsk_ref == 0)
			workers_signaled = 1;
		spec->ex_state = ISTGT_LUN_CLOSE_INPROGRESS;
		MTX_UNLOCK(&spec->state_mutex);
		if (workers_signaled == 0)
			sleep(1);
	} while ((workers_signaled == 0) && (++loop  < 10));
	timesdiff(clockid, _wrkx, _wrk, _s2)

#ifndef	REPLICATION
	/* re-open file */
	if (!spec->lu->readonly) {
		rc = spec->sync(spec, 0, spec->size);
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d: LUN%d: lu_disk_sync() failed\n",
			    lu->num, lun);
			lu->online = 1;
			return -1;
		}
	}
	rc = spec->close(spec);
	timesdiff(clockid, _wrk, _wrkx, _s3)
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: LUN%d: lu_disk_close() failed\n",
		    lu->num, lun);
		lu->online = 1;
		return -1;
	}
#endif

	timesdiff(clockid, _wrk, _wrkx, _s3)
	MTX_LOCK(&spec->state_mutex);
	spec->ex_state = ISTGT_LUN_CLOSE;
	MTX_UNLOCK(&spec->state_mutex);
	ISTGT_LOG("LU%d: LUN%d %s: storage_stop [%ld.%ld - %ld.%ld - %ld.%ld]\n",
		lu->num, lun, lu->name ? lu->name  : "-",
		_s1.tv_sec, _s1.tv_nsec, _s2.tv_sec, _s2.tv_nsec, _s3.tv_sec, _s3.tv_nsec);

	return 0;
}


int
istgt_lu_disk_modify(ISTGT *istgt, int dofake)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_DISK *spec;
	int i, j;
	int rc;
	int failsignal = 0;
	const istgt_action act = dofake ? ACTION_CLOSE : ACTION_OPEN;
	const char *mode = dofake ? "fake" : "real";
	struct timespec st, wrk, wrkx, dif1, dif2;
	const istgt_lun_ex_state expstate1 = dofake ? ISTGT_LUN_CLOSE_PENDING : ISTGT_LUN_OPEN_PENDING;
	const istgt_lun_ex_state expstate2 = dofake ? ISTGT_LUN_CLOSE_INPROGRESS : ISTGT_LUN_OPEN_INPROGRESS;
	int notyet = 0, signaled = 0, skipped = 0, loopCnt = 0;

	ISTGT_LOG("istgtcontrol modify %s starting\n", mode);
	clock_gettime(clockid, &st);

	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
		lu = istgt->logical_unit[i];
		if (lu == NULL)
			continue;
		for (j = 0; j < lu->maxlun; j++) {
			spec = (ISTGT_LU_DISK *) lu->lun[j].spec;
			if (spec == NULL)
				continue;
			spec->dofake = dofake;
			rc = istgt_lu_disk_signal_action(lu, j, &st, act); //rc = istgt_lu_disk_update_raw(lu, j, dofake);
			if (rc < 0)
				++failsignal;
			else if (rc == 1)
				++signaled;
			else
				++skipped;
		}
	}
	timesdiff(clockid, st,wrk,dif1)

	notyet = signaled;	

	while ((notyet > 0) && (++loopCnt < 180)) {
		sleep(1);
		for (notyet = 0, i = 0; i < MAX_LOGICAL_UNIT; i++) {
			lu = istgt->logical_unit[i];
			if (lu == NULL)
				continue;
			for (j = 0; j < lu->maxlun; j++) {
				spec = (ISTGT_LU_DISK *) lu->lun[j].spec;
				if (spec == NULL)
					continue;
				if (spec->ex_state == expstate1 || spec->ex_state == expstate2)
					++notyet;
			}
		}
	}
	timesdiff(clockid, wrk,wrkx,dif2)
	ISTGT_LOG("istgtcontrol modify %s done signaled:%d notdone:%d [%ld.%ld - %ld.%ld]\n",
			mode, signaled, notyet, dif1.tv_sec, dif1.tv_nsec, dif2.tv_sec, dif2.tv_nsec);
	return  (failsignal || notyet) ? -1 : 0;
}


int
istgt_lu_disk_status(ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_DISK *spec;
	int status;

	if (lu == NULL) {
		return -1;
	}
	if (lun >= lu->maxlun) {
		return -1;
	}
	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;

	MTX_LOCK(&spec->state_mutex);
#ifdef	REPLICATION
	MTX_LOCK(&spec->rq_mtx);
	if (spec->state == ISTGT_LUN_BUSY || spec->ready == false) {
#else
	if (spec->state == ISTGT_LUN_BUSY) {
#endif
		status = ISTGT_LUN_BUSY;
	} else {
		status = ISTGT_LUN_ONLINE;
	}
#ifdef	REPLICATION
	MTX_UNLOCK(&spec->rq_mtx);
#endif
	MTX_UNLOCK(&spec->state_mutex);

	return (status);
}

static int
istgt_lu_disk_queue_clear_internal(CONN_Ptr conn, ISTGT_LU_DISK *spec, const char *initiator_port, int all_cmds, uint32_t CmdSN)
{
	ISTGT_QUEUE_Ptr r_ptr;
	ISTGT_LU_TASK_Ptr lu_task, tptr;
	void *cookie;
	ISTGT_LU_CMD_Ptr lu_cmd;
	ISTGT_QUEUE saved_queue;
	ISTGT_QUEUE saved_bqueue;
	ISTGT_QUEUE saved_queue1;
	ISTGT_QUEUE saved_bqueue1;
	int need_signal = 0;
	struct timespec now;
	int rc = 0;
	int k;
	char buf[2048];
	char *bp = buf;
	int rem = 2040, ln, i, used;
	char c;
	int cleared = 0;
	int ilen = initiator_port ? strlen(initiator_port) : 0;
	struct timespec abstime;
        time_t now_wait;
	int already_waited = 0;
	memset(&abstime, 0, sizeof abstime);

	if (spec == NULL)
		return -1;

	if (all_cmds != 0) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "queue clear by port=%s\n",
		    initiator_port);
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "queue clear by port=%s, CmdSN=0x%x\n",
		    initiator_port, CmdSN);
	}

	istgt_queue_init(&saved_queue);
	istgt_queue_init(&saved_bqueue);
	istgt_queue_init(&saved_queue1);
	istgt_queue_init(&saved_bqueue1);

	clock_gettime(clockid, &now); //now = time(NULL);
	MTX_LOCK(&spec->complete_queue_mutex);
	while (1) {
		lu_task = istgt_queue_dequeue(&spec->cmd_queue);
		if (lu_task == NULL)
			break;
		if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
		    && (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
			//Return Release Aborted, but not actually aborting it. Just the response won't be sent
			if((abort_release == 1) && ((lu_task->lu_cmd.cdb[0] == SPC2_RELEASE_6) || (lu_task->lu_cmd.cdb[0] == SPC2_RELEASE_10))) {
				lu_task->lu_cmd.release_aborted = 1;
				cleared++;
				ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu Release cleared from cmdq\n",
			    		lu_task->lu_cmd.CmdSN,
			   		lu_task->lu_cmd.cdb[0],
			    		lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    		(now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
				goto saved_cmd_queue;
			}
			ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared from cmdq\n",
			    lu_task->lu_cmd.CmdSN,
			    lu_task->lu_cmd.cdb[0],
			    lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
			istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
			lu_task->complete_queue_ptr = NULL;
			rc = istgt_lu_destroy_task(lu_task);
			if (rc < 0) {
				if (need_signal)
					pthread_cond_signal(&spec->cmd_queue_cond);
				need_signal = 0;
				MTX_UNLOCK(&spec->complete_queue_mutex);
				ISTGT_ERRLOG("lu_destory_task() failed\n");
				goto error_return;
			}
			cleared++;
			need_signal = 1;
			continue;
		}
saved_cmd_queue:
		r_ptr = istgt_queue_enqueue(&saved_queue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}
	while (1) {
		lu_task = istgt_queue_dequeue(&saved_queue);
		if (lu_task == NULL)
			break;
		r_ptr = istgt_queue_enqueue(&spec->cmd_queue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}

	while (1) {
		lu_task = istgt_queue_dequeue(&spec->blocked_queue);
		if (lu_task == NULL)
			break;
		if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
		    && (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
			if((abort_release == 1) && ((lu_task->lu_cmd.cdb[0] == SPC2_RELEASE_6) || (lu_task->lu_cmd.cdb[0] == SPC2_RELEASE_10))) {
				lu_task->lu_cmd.release_aborted = 1;
				cleared++;
				ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu Release cleared from blockedq\n",
			    		lu_task->lu_cmd.CmdSN,
			   		lu_task->lu_cmd.cdb[0],
			    		lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    		(now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
				goto saved_blocked_queue;
			}
			ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared from blockedq\n",
			    lu_task->lu_cmd.CmdSN,
			    lu_task->lu_cmd.cdb[0],
			    lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
			istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
			lu_task->complete_queue_ptr = NULL;
			rc = istgt_lu_destroy_task(lu_task);
			if (rc < 0) {
				if (need_signal)
					pthread_cond_signal(&spec->cmd_queue_cond);
				need_signal = 0;
				MTX_UNLOCK(&spec->complete_queue_mutex);
				ISTGT_ERRLOG("lu_destory_task() failed\n");
				goto error_return;
			}
			cleared++;
			need_signal = 1;
			continue;
		}
saved_blocked_queue:
		r_ptr = istgt_queue_enqueue(&saved_bqueue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}
	while (1) {
		lu_task = istgt_queue_dequeue(&saved_bqueue);
		if (lu_task == NULL)
			break;
		r_ptr = istgt_queue_enqueue(&spec->blocked_queue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}

	if (need_signal)
		pthread_cond_signal(&spec->cmd_queue_cond);

	need_signal = 0;

	while (1) {
		lu_task = istgt_queue_dequeue(&spec->maint_cmd_queue);
		if (lu_task == NULL)
			break;
		if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
		    && (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
			ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared from maintcmdq\n",
			    lu_task->lu_cmd.CmdSN,
			    lu_task->lu_cmd.cdb[0],
			    lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
			istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
			lu_task->complete_queue_ptr = NULL;
			rc = istgt_lu_destroy_task(lu_task);
			if (rc < 0) {
				if (need_signal)
					pthread_cond_signal(&spec->cmd_queue_cond);
				need_signal = 0;
				MTX_UNLOCK(&spec->complete_queue_mutex);
				ISTGT_ERRLOG("lu_destory_task() failed\n");
				goto error_return;
			}
			cleared++;
			need_signal = 1;
			continue;
		}
		r_ptr = istgt_queue_enqueue(&saved_queue1, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}
	while (1) {
		lu_task = istgt_queue_dequeue(&saved_queue1);
		if (lu_task == NULL)
			break;
		r_ptr = istgt_queue_enqueue(&spec->maint_cmd_queue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}

	while (1) {
		lu_task = istgt_queue_dequeue(&spec->maint_blocked_queue);
		if (lu_task == NULL)
			break;
		if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
		    && (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
			ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared from maintblkdq\n",
			    lu_task->lu_cmd.CmdSN,
			    lu_task->lu_cmd.cdb[0],
			    lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
			    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
			istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
			lu_task->complete_queue_ptr = NULL;
			rc = istgt_lu_destroy_task(lu_task);
			if (rc < 0) {
				if (need_signal)
					pthread_cond_signal(&spec->cmd_queue_cond);
				need_signal = 0;
				MTX_UNLOCK(&spec->complete_queue_mutex);
				ISTGT_ERRLOG("lu_destory_task() failed\n");
				goto error_return;
			}
			cleared++;
			need_signal = 1;
			continue;
		}
		r_ptr = istgt_queue_enqueue(&saved_bqueue1, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}
	while (1) {
		lu_task = istgt_queue_dequeue(&saved_bqueue1);
		if (lu_task == NULL)
			break;
		r_ptr = istgt_queue_enqueue(&spec->maint_blocked_queue, lu_task);
		if (r_ptr == NULL) {
			if (need_signal)
				pthread_cond_signal(&spec->cmd_queue_cond);
			need_signal = 0;
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("queue_enqueue() failed\n");
			goto error_return;
		}
	}
	if (need_signal)
		pthread_cond_signal(&spec->maint_cmd_queue_cond);
	MTX_UNLOCK(&spec->complete_queue_mutex);

	/* check wait task */
	MTX_LOCK(&spec->wait_lu_task_mutex);
	for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
		lu_task = spec->wait_lu_task[k];
		if (lu_task != NULL) {
			if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
			    && (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
				/* conn had gone? */
				MTX_LOCK(&lu_task->trans_mutex);
				{
					ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu aborted\n",
					    lu_task->lu_cmd.CmdSN,
					    lu_task->lu_cmd.cdb[0],
						lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
					    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
					/* force error */
					lu_task->error = 1;
					lu_task->abort = 1;
					lu_task->lu_cmd.aborted = 1;
					cleared++;
					rc = pthread_cond_signal(&lu_task->trans_cond);
					if (rc != 0) {
						/* ignore error */
						rc = 0;
					}
				}
				MTX_UNLOCK(&lu_task->trans_mutex);
			}
		}
	}
	MTX_UNLOCK(&spec->wait_lu_task_mutex);


	for (i=0; i<spec->luworkers+1; i++) {
		MTX_LOCK(&spec->luworker_mutex[i]);
		if (spec->inflight_io[i] != NULL) {
			lu_task = spec->inflight_io[i];
			lu_cmd = &(lu_task->lu_cmd);
			ln = 0;
			if(rem > 30)
	                        ln = snprintf(bp, rem, "%dC0x%x CS%d TA%d.%d TS%d 0x%x(lba:%lx+%x)%lu.%lu[%c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld  %c:%ld.%9.9ld %c:%ld.%9.9ld %c:%ld.%9.9ld] ", i,
                                                        lu_task->lu_cmd.CmdSN, lu_task->lu_cmd.status, lu_task->execute, lu_task->use_cond, spec->luworker_waiting[i],
                                                        lu_task->lu_cmd.cdb[0], lu_task->lu_cmd.lba,
                                                        lu_task->lu_cmd.lblen,
                                                        (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec),
                                                        (now.tv_nsec - lu_task->lu_cmd.create_time.tv_nsec),
                                                        lu_cmd->caller[1] ? lu_cmd->caller[1] : '9', lu_cmd->tdiff[1].tv_sec, lu_cmd->tdiff[1].tv_nsec,
                                                        lu_cmd->caller[2] ? lu_cmd->caller[2] : '9', lu_cmd->tdiff[2].tv_sec, lu_cmd->tdiff[2].tv_nsec,
                                                        lu_cmd->caller[3] ? lu_cmd->caller[3] : '9', lu_cmd->tdiff[3].tv_sec, lu_cmd->tdiff[3].tv_nsec,
                                                        lu_cmd->caller[4] ? lu_cmd->caller[4] : '9', lu_cmd->tdiff[4].tv_sec, lu_cmd->tdiff[4].tv_nsec,
                                                        lu_cmd->caller[5] ? lu_cmd->caller[5] : '9', lu_cmd->tdiff[5].tv_sec, lu_cmd->tdiff[5].tv_nsec,
                                                        lu_cmd->caller[6] ? lu_cmd->caller[6] : '9', lu_cmd->tdiff[6].tv_sec, lu_cmd->tdiff[6].tv_nsec,
                                                        lu_cmd->caller[7] ? lu_cmd->caller[7] : '9', lu_cmd->tdiff[7].tv_sec, lu_cmd->tdiff[7].tv_nsec,
                                                        lu_cmd->caller[8] ? lu_cmd->caller[8] : '9', lu_cmd->tdiff[8].tv_sec, lu_cmd->tdiff[8].tv_nsec
                                                        );
			if(ln <0) 
				ln = 0;
			else if(ln > rem)
				ln = rem;
			rem -= ln;
			bp += ln;
			*bp = '\0';
			if (((all_cmds != 0) || (lu_task->lu_cmd.CmdSN == CmdSN))
		    		&& (lu_task->in_plen == ilen && (strcasecmp(lu_task->in_port, initiator_port) == 0))) {
				ISTGT_ERRLOG("cmd 0x%x to abort matches with inflight %d abort:%d:%d\n", lu_task->lu_cmd.CmdSN, i, lu_task->abort, lu_cmd->aborted);

				if(lu_cmd->aborted == 1)
					goto avoidif;

				if(wait_inflights == 1) {
					MTX_UNLOCK(&spec->luworker_mutex[i]);
					MTX_LOCK(&spec->lu_tmf_mutex[i]);
					spec->lu_tmf_wait[i] = 1;
					now_wait = time(NULL);
					abstime.tv_sec = now_wait + TMF_TIMEOUT;
					abstime.tv_nsec = 0;
					if(already_waited == 0){
						rc = pthread_cond_timedwait(&spec->lu_tmf_cond[i],&spec->lu_tmf_mutex[i], &abstime);
						if(rc == ETIMEDOUT) {
							ISTGT_LOG("TMF_WAIT ETIMEDOUT: Aborting cmd 0x%x\n", CmdSN);
							already_waited = 1;
						} else {
							ISTGT_LOG("Not Aborting cmd 0x%x, Successfully came out of inflight\n", CmdSN);
						}
					}
					if(spec->lu_tmf_wait[i] == 1 || already_waited == 1) {
						if((abort_release == 1) && ((lu_cmd->cdb[0] == SPC2_RELEASE_6) || (lu_cmd->cdb[0] == SPC2_RELEASE_10))) {
							lu_cmd->release_aborted = 1;
							ISTGT_LOG("CmdSN(0x%x), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu Release cleared from blockedq\n",
			    					lu_cmd->CmdSN,
			   					lu_cmd->cdb[0],
			    					lu_cmd->lba, lu_cmd->lblen,
			    					(now.tv_sec - lu_cmd->create_time.tv_sec));
						} else {
							lu_cmd->aborted = 1;
						}
						if(lu_task->abort != 1)
							cleared++;
					}
					MTX_UNLOCK(&spec->lu_tmf_mutex[i]);
					continue;
				}
				else
				{
					lu_cmd->aborted = 1;
					if(lu_task->abort != 1)
						cleared++;
				}
avoidif:
				;
			}
		}
		else if(spec->inflight_io[i] == NULL)
		{
			ln = 0;
			if(rem > 30)
				ln = snprintf(bp, rem, "%dTS%d ", i, spec->luworker_waiting[i]);
			if(ln < 0) 
				ln = 0;
			else if(ln > rem)
				ln = rem;
			rem -= ln;
			bp += ln;
			*bp = '\0';
		}
		MTX_UNLOCK(&spec->luworker_mutex[i]);
	}
	if(conn!=NULL && conn->state != CONN_STATE_EXITING && abort_result_queue == 1) {
		MTX_LOCK(&conn->result_queue_mutex);    
		while ((tptr= (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&conn->result_queue, &cookie)) != NULL) {
			if(tptr->lu_cmd.CmdSN == CmdSN){
				tptr->lu_cmd.aborted = 1;
				cleared++;
				break;
			}
		}
		MTX_UNLOCK(&conn->result_queue_mutex);
	}

	if (rem != 2040) {
		used = (bp - buf) + 2;
		if(used > 900)
		{
			c = buf[900];
			buf[900] = '\0';
		}
		ISTGT_ERRLOG("inflight-aborted dskRef:%d inflight:%d errCnt: %d freeMatrix0: %d freeMatrix1: %d schdlrWaiting: %d schdlrCmdWaiting: %d %s\n",
				 spec->ludsk_ref, spec->inflight, spec->error_count,
				 spec->lu_free_matrix[0], spec->lu_free_matrix[1], spec->schdler_waiting, spec->schdler_cmd_waiting, buf);
		if(used > 900)
		{
			buf[900] = c;
			ISTGT_ERRLOG("%s\n", buf+900);
		}
	}

error_return:
	if (istgt_queue_count(&saved_queue) == 0 ) {

		istgt_queue_destroy(&saved_queue);
	}
	else {
		ISTGT_ERRLOG("temporary queue is not empty\n");
		rc = -1;
	}

	if (istgt_queue_count(&saved_bqueue) == 0) {
		istgt_queue_destroy(&saved_bqueue);
	}
	else {
		ISTGT_ERRLOG("temporary bqueue is not empty\n");
		rc = -1;
	}
	if (istgt_queue_count(&saved_queue1) == 0 ) {

		istgt_queue_destroy(&saved_queue1);
	}
	else {
		ISTGT_ERRLOG("temporary queue is not empty\n");
		rc = -1;
	}

	if (istgt_queue_count(&saved_bqueue1) == 0) {
		istgt_queue_destroy(&saved_bqueue1);
	}
	else {
		ISTGT_ERRLOG("temporary bqueue is not empty\n");
		rc = -1;
	}
	return cleared;
}

static int
istgt_lu_disk_queue_abort_ITL(ISTGT_LU_DISK *spec, const char *initiator_port)
{
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "queue abort by port=%s\n",
	    initiator_port);

	rc = istgt_lu_disk_queue_clear_internal(NULL, spec, initiator_port,
	    1, 0U); /* ALL, CmdSN=0 */
	return rc;
}

int
istgt_lu_disk_queue_clear_IT(CONN_Ptr conn, ISTGT_LU_Ptr lu)
{
	ISTGT_LU_DISK *spec;
	int cleared = 0;
	int i;

	if (lu == NULL)
		return -1;

	for (i = 0; i < lu->maxlun; i++) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
#if 0
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d none\n",
						   lu->num, i);
#endif
			continue;
		}
		if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
			ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
			return -1;
		}
		spec = (ISTGT_LU_DISK *) lu->lun[i].spec;
		if (spec == NULL) {
			continue;
		}

		cleared = istgt_lu_disk_queue_clear_ITL(conn, lu, i);
		if (cleared < 0) {
			return -1;
		}
	}

	return cleared;
}

int
istgt_lu_disk_queue_clear_ITL(CONN_Ptr conn, ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_DISK *spec;
	int cleared = 0;

	if (lu == NULL)
		return -1;
	if (lun >= lu->maxlun)
		return -1;

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "queue clear by name=%s, port=%s\n",
	    conn->initiator_name, conn->initiator_port);

	cleared = istgt_lu_disk_queue_clear_internal(conn, spec, conn->initiator_port,
	    1, 0U); /* ALL, CmdSN=0 */
	return cleared;
}

int
istgt_lu_disk_queue_clear_ITLQ(CONN_Ptr conn, ISTGT_LU_Ptr lu, int lun, uint32_t CmdSN)
{
	ISTGT_LU_DISK *spec;
	int cleared = 0;

	if (lu == NULL)
		return -1;
	if (lun >= lu->maxlun)
		return -1;

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "queue clear by name=%s, port=%s\n",
	    conn->initiator_name, conn->initiator_port);

	cleared = istgt_lu_disk_queue_clear_internal(conn, spec, conn->initiator_port,
	    0, CmdSN);
	return cleared;
}

int
istgt_lu_disk_queue_clear_all(ISTGT_LU_Ptr lu, int lun)
{
	ISTGT_LU_TASK_Ptr lu_task;
	ISTGT_LU_DISK *spec;
	struct timespec now;
	int rc = 0, cleared = 0;
	int k;

	if (lu == NULL)
		return -1;
	if (lun >= lu->maxlun)
		return -1;

	if (lu->lun[lun].type == ISTGT_LU_LUN_TYPE_NONE) {
		return -1;
	}
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;

	clock_gettime(clockid, &now);
	MTX_LOCK(&spec->complete_queue_mutex);
	while (1) {
		lu_task = istgt_queue_dequeue(&spec->cmd_queue);
		if (lu_task == NULL)
			break;
		ISTGT_LOG("CmdSN(%u), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared\n",
		    lu_task->lu_cmd.CmdSN,
		    lu_task->lu_cmd.cdb[0],
			lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
		    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
		istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
		lu_task->complete_queue_ptr = NULL;
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("lu_destory_task() failed\n");
			return -1;
		}
		cleared++;
	}
	/* Clear all the commads that are blocked */
	while(1) {
		 lu_task = istgt_queue_dequeue(&spec->blocked_queue);
		if (lu_task == NULL)
			break;
		ISTGT_LOG("CmdSN(%u), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared\n",
		    lu_task->lu_cmd.CmdSN,
		    lu_task->lu_cmd.cdb[0],
			lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
		    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
		istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
		lu_task->complete_queue_ptr = NULL;
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("lu_destory_task() failed\n");
			return -1;
		}
		cleared++;
	}

	while (1) {
		lu_task = istgt_queue_dequeue(&spec->maint_cmd_queue);
		if (lu_task == NULL)
			break;
		ISTGT_LOG("CmdSN(%u), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared\n",
		    lu_task->lu_cmd.CmdSN,
		    lu_task->lu_cmd.cdb[0],
			lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
		    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
		istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
		lu_task->complete_queue_ptr = NULL;
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("lu_destory_task() failed\n");
			return -1;
		}
		cleared++;
	}
	/* Clear all the commads that are blocked */
	while(1) {
		 lu_task = istgt_queue_dequeue(&spec->maint_blocked_queue);
		if (lu_task == NULL)
			break;
		ISTGT_LOG("CmdSN(%u), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu cleared\n",
		    lu_task->lu_cmd.CmdSN,
		    lu_task->lu_cmd.cdb[0],
			lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
		    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
		istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
		lu_task->complete_queue_ptr = NULL;
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			MTX_UNLOCK(&spec->complete_queue_mutex);
			ISTGT_ERRLOG("lu_destory_task() failed\n");
			return -1;
		}
		cleared++;
	}
	MTX_UNLOCK(&spec->complete_queue_mutex);

	/* check wait task */
	MTX_LOCK(&spec->wait_lu_task_mutex);
	for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
		lu_task = spec->wait_lu_task[k];
		if (lu_task != NULL) {
			/* conn had gone? */
			rc = pthread_mutex_trylock(&lu_task->trans_mutex);
			if (rc == 0) {
				ISTGT_LOG("CmdSN(%u), OP=0x%x (lba %"PRIu64", %u blocks), ElapsedTime=%lu aborted\n",
				    lu_task->lu_cmd.CmdSN,
				    lu_task->lu_cmd.cdb[0],
				    lu_task->lu_cmd.lba, lu_task->lu_cmd.lblen,
				    (unsigned long) (now.tv_sec - lu_task->lu_cmd.create_time.tv_sec));
				/* force error */
				lu_task->error = 1;
				lu_task->abort = 1;
				cleared++;
				rc = pthread_cond_signal(&lu_task->trans_cond);
				if (rc != 0) {
					/* ignore error */
				}
				MTX_UNLOCK(&lu_task->trans_mutex);
			}
		}
	}
	MTX_UNLOCK(&spec->wait_lu_task_mutex);

	return rc == 0 ? cleared : rc;
}

/*
 * Incoming request encounter a conflict if there are already blocked entries
 * or if the currently executed items conflict with the incoming items
 * Note that we do this to avoid severe pipeline hazards in the order of execution.
 */

char qact[8][20] = {
	"BLOCK",
	"BLOCK_OVERLAP",
	"BLOCK_OVERLAP_TAG",
	"BLOCK_SUSPECT",
	"PASS",
	"SKIP",
	"ERROR",
	"UNK"
};

int
istgt_lu_print_q(ISTGT_LU_Ptr lu, int lun)
{
#define adjbuf() {    \
	if (wn < 0)    \
		wn = 0;    \
	else if (wn > brem) \
		wn = brem; \
	bptr += wn;    \
	brem -= wn;    \
}
#define tdiff(_s, _n, _r) {                     \
	if ((_n.tv_nsec - _s.tv_nsec) < 0) {        \
		_r.tv_sec  = _n.tv_sec - _s.tv_sec-1;   \
		_r.tv_nsec = 1000000000 + _n.tv_nsec - _s.tv_nsec; \
	} else {                                    \
		_r.tv_sec  = _n.tv_sec - _s.tv_sec;     \
		_r.tv_nsec = _n.tv_nsec - _s.tv_nsec;   \
	}                                           \
}
#define _BSZ_ 4086
	char buf[_BSZ_+10];
	int  brem = _BSZ_, wn = 0;
	int  toprint, chunk;
	char *bptr = buf;
	int i = 0;

	ISTGT_LU_DISK *spec;
	ISTGT_LU_TASK_Ptr tptr;
	void *cookie=NULL;
	struct timespec now;
	struct timespec r;

	if (lu == NULL)
		return -1;
	if (lun >= lu->maxlun)
		return -1;
	if (lu->lun[lun].type != ISTGT_LU_LUN_TYPE_STORAGE) {
		return -1;
	}

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
		return -1;

	uint64_t gb_size = spec->size / ISTGT_LU_1GB;
	uint64_t mb_size = (spec->size % ISTGT_LU_1GB) / ISTGT_LU_1MB;

	MTX_LOCK(&spec->complete_queue_mutex);
	int rpm = lu->lun[lun].rotationrate;
	int cq = istgt_queue_count(&spec->cmd_queue);
	int bq = istgt_queue_count(&spec->blocked_queue);
	int cq1 = istgt_queue_count(&spec->maint_cmd_queue);
	int bq1 = istgt_queue_count(&spec->maint_blocked_queue);
	int inf = spec->ludsk_ref;

	clock_gettime(clockid, &now);
	wn = snprintf(bptr, brem, " cmdQ:%d:%d blked:%d:%d inf:%d", cq, cq1, bq, bq1, inf);
	adjbuf()

	while ((tptr= (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&spec->cmd_queue, &cookie)) != NULL) {
		tdiff(tptr->lu_cmd.times[0], now, r)
		wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
				tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
				r.tv_sec, r.tv_nsec);
		adjbuf()
	}
	*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem-=3;
	cookie = NULL;
	while ((tptr= (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&spec->blocked_queue, &cookie)) != NULL) {
		tdiff(tptr->lu_cmd.times[0], now, r)
		wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
				tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
				r.tv_sec, r.tv_nsec);
		adjbuf()
	}
	*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem-=3;

	cookie = NULL;
	while ((tptr= (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&spec->maint_cmd_queue, &cookie)) != NULL) {
		tdiff(tptr->lu_cmd.times[0], now, r)
		wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
				tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
				r.tv_sec, r.tv_nsec);
		adjbuf()
	}
	*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem-=3;
	cookie = NULL;
	while ((tptr= (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&spec->maint_blocked_queue, &cookie)) != NULL) {
		tdiff(tptr->lu_cmd.times[0], now, r)
		wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
				i++, tptr->lu_cmd.CmdSN, tptr->lu_cmd.cdb0,
				tptr->lu_cmd.lba, tptr->lu_cmd.lblen,
				r.tv_sec, r.tv_nsec);
		adjbuf()
	}
	*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem-=3;
	MTX_UNLOCK(&spec->complete_queue_mutex);


	/* luworker waiting data from zvol */
	for (i=0; i < spec->luworkers+1; i++ ) {
		MTX_LOCK(&spec->luworker_mutex[i]);
		if(spec->inflight_io[i] != NULL) {
			tdiff(spec->inflight_io[i]->lu_cmd.times[0], now, r)
			wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
							i, spec->inflight_io[i]->lu_cmd.CmdSN,
							spec->inflight_io[i]->lu_cmd.cdb0,
							spec->inflight_io[i]->lu_cmd.lba,
							spec->inflight_io[i]->lu_cmd.lblen,
							r.tv_sec, r.tv_nsec);
			adjbuf()
		}
		MTX_UNLOCK(&spec->luworker_mutex[i]);
	}
	*bptr++ = ' '; *bptr++ = '-'; *bptr++ = ' '; brem-=3;
	/* luworker waiting data from network */
	MTX_LOCK(&spec->wait_lu_task_mutex);
	for (i=0; i < ISTGT_MAX_NUM_LUWORKERS; i++ ) {
		if (spec->wait_lu_task[i] != NULL) {
			tdiff(spec->wait_lu_task[i]->lu_cmd.times[0], now, r)
			wn = snprintf(bptr, brem, " %d:%x 0x%x.%lu+%uT%ld.%9.9ld",
					i, spec->wait_lu_task[i]->lu_cmd.CmdSN,
					spec->wait_lu_task[i]->lu_cmd.cdb0,
					spec->wait_lu_task[i]->lu_cmd.lba,
					spec->wait_lu_task[i]->lu_cmd.lblen,
					r.tv_sec, r.tv_nsec);
			adjbuf()
		}
	}
	MTX_UNLOCK(&spec->wait_lu_task_mutex);

	ISTGT_NOTICELOG("LU%d:QUE %s %s [%s, %luGB.%luMB, %lu blks of %lu bytes, phy:%u %s%s%s%s%s%s%s rpm:%d] q:%d thr:%d/%d inflight:%d %d/%d\n",
				lu->num, lu->name ? lu->name : "-", lu->readonly ? "readonly " : "",
				spec->file, gb_size, mb_size, spec->blockcnt, spec->blocklen, spec->rshift,
				spec->readcache ? "" : "RCD", spec->writecache ? " WCE" : "",
				spec->ats ? " ATS" : "",spec->xcopy ? " XCOPY" : "", spec->unmap ? " UNMAP" : "",
				spec->wsame ? " WSAME" : "", spec->dpofua ? " DPOFUA" : "",
				rpm, spec->queue_depth, spec->luworkers, spec->luworkersActive, spec->ludsk_ref,
				brem, _BSZ_ - brem);


	toprint = _BSZ_ - brem;
	bptr = buf;
	i = 0;
	while(toprint > 0)
	{
		if (toprint > 1023)
			chunk = 1024;
		else
			chunk = toprint;
		ISTGT_NOTICELOG("LU%d:QUE%d  %.*s", lu->num, i, chunk, bptr);
		toprint -= chunk;
		bptr += chunk;
		++i;
	}
	return 0;
}

static inline istgt_task_action
istgt_check_for_blockage(ISTGT_LU_TASK_Ptr pending_task, ISTGT_LU_TASK_Ptr ooa_task)
{
	struct istgt_cmd_entry *pending_entry, *ooa_entry;
	istgt_serialize_action *serialize_row;
	istgt_tag_type const ptype = pending_task->lu_cmd.Attr_bit;
	istgt_tag_type const otype = ooa_task->lu_cmd.Attr_bit;
	/*
	 * The initiator attempted multiple untagged commands at the same
	 * time.  Can't do that.
	 */
	if ((ptype == ISTGT_TAG_UNTAGGED)
	 && (otype == ISTGT_TAG_UNTAGGED)
	 && ((pending_task->in_plen == ooa_task->in_plen) &&
		 (strcmp(pending_task->in_port, ooa_task->in_port) == 0))
	  && ((ooa_task->flags & ISTGT_FLAG_ABORT) == 0))
		return (ISTGT_TASK_OVERLAP);

	/*
	 * The initiator attempted to send multiple tagged commands with
	 * the same ID.  (It's fine if different initiators have the same
	 * tag ID.)
	 *
	 * Even if all of those conditions are true, we don't kill the I/O
	 * if the command ahead of us has been aborted.  We won't end up
	 * sending it to the FETD, and it's perfectly legal to resend a
	 * command with the same tag number as long as the previous
	 * instance of this tag number has been aborted somehow.
	 */
	if ((ptype!= ISTGT_TAG_UNTAGGED)
	 && (otype != ISTGT_TAG_UNTAGGED)
	 && (ptype == otype)
	 && (pending_task->lu_cmd.task_tag == ooa_task->lu_cmd.task_tag)
	 && ((pending_task->in_plen == ooa_task->in_plen) &&
		 (strcmp(pending_task->in_port, ooa_task->in_port) == 0))
	 && ((ooa_task->flags & ISTGT_FLAG_ABORT) == 0))
		return (ISTGT_TASK_OVERLAP_TAG);

	/*
	 * If we get a head of queue tag, SAM-3 says that we should
	 * immediately execute it.
	 *
	 * What happens if this command would normally block for some other
	 * reason?  e.g. a request sense with a head of queue tag
	 * immediately after a write.  Normally that would block, but this
	 * will result in its getting executed immediately...
	 *
	 * We currently return "pass" instead of "skip", so we'll end up
	 * going through the rest of the queue to check for overlapped tags.
	 *
	 * XXX KDM check for other types of blockage first??
	 */
	if (ptype == ISTGT_TAG_HEAD_OF_QUEUE)
		return (ISTGT_TASK_PASS);

	/*
	 * Ordered tags have to block until all items ahead of them
	 * have completed.  If we get called with an ordered tag, we always
	 * block, if something else is ahead of us in the queue.
	 */
	if (ptype == ISTGT_TAG_ORDERED)
		return (ISTGT_TASK_BLOCK);

	/*
	 * Simple tags get blocked until all head of queue and ordered tags
	 * ahead of them have completed.  I'm lumping untagged commands in
	 * with simple tags here.  XXX KDM is that the right thing to do?
	 */
	if (((ptype == ISTGT_TAG_UNTAGGED)
	  || (ptype== ISTGT_TAG_SIMPLE))
	 && ((otype == ISTGT_TAG_HEAD_OF_QUEUE)
	  || (otype == ISTGT_TAG_ORDERED)))
		return (ISTGT_TASK_BLOCK);

	pending_entry = &istgt_cmd_table[pending_task->lu_cmd.cdb[0]];
	ooa_entry = &istgt_cmd_table[ooa_task->lu_cmd.cdb[0]];

	serialize_row = istgt_serialize_table[ooa_entry->seridx];

	switch (serialize_row[pending_entry->seridx]) {
		case ISTGT_SER_BLOCK:
			return (ISTGT_TASK_BLOCK);
			break; /* NOTREACHED */
		case ISTGT_SER_EXTENT:
			if (pending_task->cdb0 == 0xFF || ooa_task->cdb0  == 0xFF)
				return (ISTGT_TASK_BLOCK_SUSPECT); //this is a suspect case
			else if ( (pending_task->lbE < ooa_task->lba)  ||
					(ooa_task->lbE < pending_task->lba) )
				return (ISTGT_TASK_PASS);
			else
				return (ISTGT_TASK_BLOCK);
			break; /* NOTREACHED */
		case ISTGT_SER_PASS:
			return (ISTGT_TASK_PASS);
			break; /* NOTREACHED */
		case ISTGT_SER_SKIP:
			return (ISTGT_TASK_SKIP);
			break;
		default:
			ISTGT_ERRLOG("invalid serialization value %d",
					serialize_row[pending_entry->seridx]);
			break;
	}
	ISTGT_ERRLOG("Could not find the serialization value %d",
		serialize_row[pending_entry->seridx]);	
	return (ISTGT_TASK_ERROR);
}

/* cmd_queue_mutex is required */
istgt_task_action istgt_check_for_istgt_queue(ISTGT_QUEUE_Ptr queue, ISTGT_LU_TASK_Ptr unblocked_lu_task)
{
	ISTGT_LU_TASK_Ptr cmd_lu_task;
	void *cookie;
	istgt_task_action action;
	action = ISTGT_TASK_PASS;
	cookie = NULL;
	while ((cmd_lu_task = (ISTGT_LU_TASK_Ptr)istgt_queue_walk(queue, &cookie))) {
		action = istgt_check_for_blockage(unblocked_lu_task,cmd_lu_task);
		if (action != ISTGT_TASK_SKIP && action != ISTGT_TASK_PASS) {
			return action;
		}
	}
	return action;
}
	
istgt_task_action istgt_check_for_all_luworkers(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task)
{
	int i;
	istgt_task_action action = ISTGT_TASK_PASS;
	ISTGT_LU_TASK_Ptr inflight_task;
	for (i=0; i < spec->luworkers; i++ ) {
		MTX_LOCK(&spec->luworker_mutex[i]);
		if(spec->inflight_io[i] == NULL){
			MTX_UNLOCK(&spec->luworker_mutex[i]);
			continue;
		}
		inflight_task = spec->inflight_io[i];
		action = istgt_check_for_blockage(unblocked_lu_task, inflight_task);
		MTX_UNLOCK(&spec->luworker_mutex[i]);
		if (action != ISTGT_TASK_SKIP && action != ISTGT_TASK_PASS) {
			return action;
		}
	}
	return action;
}

istgt_task_action istgt_check_for_all_maint_luworkers(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task)
{
	int i;
	istgt_task_action action = ISTGT_TASK_PASS;
	ISTGT_LU_TASK_Ptr inflight_task;
	for (i=spec->luworkers; i < (spec->luworkers + 1); i++ ) {
		MTX_LOCK(&spec->luworker_mutex[i]);
		if(spec->inflight_io[i] == NULL){
			MTX_UNLOCK(&spec->luworker_mutex[i]);
			continue;
		}
		inflight_task = spec->inflight_io[i];
		action = istgt_check_for_blockage(unblocked_lu_task, inflight_task);
		MTX_UNLOCK(&spec->luworker_mutex[i]);
		if (action != ISTGT_TASK_SKIP && action != ISTGT_TASK_PASS) {
			return action;
		}
	}
	return action;
}
/*Check if the IO is blocked by any IO which came before it
Reverse walk beacuse we want to put the maintenance IOs just after the last IO which is blocking it
cookie = NULL;//When the IO has just arrived
cookie = istgt_get_prev_qptr(unblocked_lu_task->complete_queue_ptr);//While Scheduling blocked IOs
*/
istgt_task_action istgt_check_for_parallel_ios(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task)
{
	ISTGT_LU_TASK_Ptr lu_task;
	ISTGT_QUEUE_Ptr queue = &spec->complete_queue;
	void *cookie;
	istgt_task_action action;
	action = ISTGT_TASK_PASS;
	if(unblocked_lu_task->lu_cmd.flags == 0)
		cookie = NULL;
	else
		cookie = istgt_get_prev_qptr(unblocked_lu_task->complete_queue_ptr);
	while ((lu_task = (ISTGT_LU_TASK_Ptr)istgt_queue_reverse_walk(queue, &cookie))) {
		action = istgt_check_for_blockage(unblocked_lu_task,lu_task);
		if (action != ISTGT_TASK_SKIP && action != ISTGT_TASK_PASS) {
			unblocked_lu_task->blocked_by = istgt_get_next_qptr(cookie);
			return action;
		}
	}
	unblocked_lu_task->blocked_by = NULL;
	return action;
}

int is_maintenance_io(ISTGT_LU_TASK_Ptr task)
{
	switch(task->lu_cmd.cdb[0])
	{
		case 0x00: /* 00 TEST UNIT READY */
		case 0x12: /* 12 INQUIRY */
		case 0xA0: /* A0 REPORT LUNS */
		case 0x03: /* 03 REQUEST SENSE */
		case 0x4D: /* 4D LOG SENSE */
		case 0x15: /* 15 MODE SELECT 6 */
		case 0x55: /* 55 MODE SELECT 10 */
		case 0x1a: /* 1a MODE SENSE 6 */
		case 0x5a: /* 5a MODE SENSE 10 */
		case 0xa3: /* a3 MAINTENANCE IN */
			return 1;
		default:
			return 0;
	}
	return 0;
}
/*If it is a maintenance IO and is blocked we will enqueue it just after the IO 
which is blocking it in complete queue, else normal enqueue at the end
*/
static istgt_task_action
istgt_is_exec_pipeline_conflict(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr pending_task)
{
	ISTGT_QUEUE_Ptr r_ptr;
	istgt_task_action action;

	if(!is_maintenance_io(pending_task))
	{
		if((istgt_queue_count(&spec->blocked_queue) != 0) ||
			(istgt_queue_count(&spec->maint_blocked_queue) != 0)){
			if(spec->do_avg == 1)
			{
				spec->avgs[18].count++;
			}
			r_ptr = istgt_queue_enqueue(&spec->complete_queue, pending_task);
			pending_task->complete_queue_ptr = r_ptr;
			return ISTGT_TASK_BLOCK;
		}
	}
	action = istgt_check_for_parallel_ios(spec, pending_task);
	switch (action) {
		case ISTGT_TASK_BLOCK:
		case ISTGT_TASK_OVERLAP:
		case ISTGT_TASK_OVERLAP_TAG:
		case ISTGT_TASK_ERROR:
		case ISTGT_TASK_BLOCK_SUSPECT:
			if(is_maintenance_io(pending_task)) { 
				r_ptr = istgt_queue_enqueue_after(&spec->complete_queue, pending_task->blocked_by, pending_task);
				pending_task->blocked_by = NULL;
				pending_task->complete_queue_ptr = r_ptr;
			} else {
				r_ptr = istgt_queue_enqueue(&spec->complete_queue, pending_task);
				pending_task->complete_queue_ptr = r_ptr;
			}
			if(spec->do_avg == 1)
			{
				spec->avgs[18].tot_sec++;
			}
			return (action);
		case ISTGT_TASK_SKIP:
		case ISTGT_TASK_PASS:
			r_ptr = istgt_queue_enqueue(&spec->complete_queue, pending_task);
			pending_task->complete_queue_ptr = r_ptr;
			break;
		default:
			ISTGT_ERRLOG("Invalid action in exec_pipeline for cmdsn:%x\n", pending_task->lu_cmd.CmdSN);
			break;
	}
	return (ISTGT_TASK_PASS);
}
/*Scheduling maintenance commands--If the first cmd in the complete queue is maint, 
we will find that command in the maint_blocked_queue and move it to maint_cmd_queue.
 
Scheduling normal commands --We examine utmost 2 blocked entries so as not to have a n*2 unblocking. Eventually
every 1 request ejected out of the cmd queue would inturn schedule 2 requests and 
hence we have a exponential reschedules for blocked items. This will drain the blocked
queue in reasonable time 
*/

void
istgt_schedule_blocked_requests(ISTGT_LU_DISK *spec, ISTGT_QUEUE_Ptr cmd_queue, ISTGT_QUEUE_Ptr blocked_queue, int maint)
{
	ISTGT_LU_TASK_Ptr unblocked_lu_task, lu_task;
	void *cookie1 = NULL;
	void *cookie2 = NULL;
	istgt_task_action action;
	int todo_count=3;
	int  unblocked = 0;

	if(spec == NULL || cmd_queue == NULL || blocked_queue == NULL)
		return;
	/*Since we have only one maintenance thread*/ 
	if( maint == 1) {
		if((lu_task = (ISTGT_LU_TASK_Ptr)istgt_queue_walk(&spec->complete_queue, &cookie1))) {
			if(is_maintenance_io(lu_task)) {
				while ((unblocked_lu_task = (ISTGT_LU_TASK_Ptr)istgt_queue_walk(blocked_queue, &cookie2))) {
					if(unblocked_lu_task == lu_task ) {
						istgt_queue_dequeue_middle(blocked_queue, istgt_get_prev_qptr(cookie2));
						++unblocked;
						unblocked_lu_task->lu_cmd.flags |= ISTGT_UNBLOCKED;
						timediffw(&unblocked_lu_task->lu_cmd, 'U');
						istgt_queue_enqueue(cmd_queue, unblocked_lu_task); //dequeue from maint blocked queue
						break;
					}
				}
				if(unblocked == 0)
					ISTGT_ERRLOG("task not found in blocked_q\n");
			}
		}
	}else {
		if (istgt_queue_count(blocked_queue)!=0) {
			while (todo_count--) {
				unblocked_lu_task = istgt_queue_dequeue(blocked_queue);
				if(unblocked_lu_task == NULL)
					break;

				action = istgt_check_for_parallel_ios(spec, unblocked_lu_task);
				if(action != ISTGT_TASK_SKIP && action != ISTGT_TASK_PASS)
				{
					istgt_queue_enqueue_first(blocked_queue,unblocked_lu_task);
					goto unlock_return;
				}
				/*All inflight request to disk should be checked for conflict */
				++unblocked;
				unblocked_lu_task->lu_cmd.flags |= ISTGT_UNBLOCKED;
				timediffw(&unblocked_lu_task->lu_cmd, 'U');
				istgt_queue_enqueue(cmd_queue, unblocked_lu_task);
			}
		}
	}
unlock_return:
	return;
}

int
istgt_lu_disk_signal_worker(ISTGT_LU_Ptr lu)
{
	int rc = 0;
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Signal the LU%d worker", lu->num);
	MTX_LOCK(&lu->queue_mutex);
	lu->queue_check = 1;
	rc = pthread_cond_signal(&lu->queue_cond);
	MTX_UNLOCK(&lu->queue_mutex);
	if (rc != 0) {
		ISTGT_ERRLOG("LU%d: cond_broadcast() failed\n", lu->num);
		return -1;
	}
	return 0;
}

int
istgt_lu_disk_queue(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_QUEUE_Ptr r_ptr;
	ISTGT_LU_TASK_Ptr lu_task;
	ISTGT_LU_Ptr lu;
	ISTGT_LU_DISK *spec;
	uint8_t *data = NULL;
	uint8_t *cdb;
	uint32_t allocation_len;
	int data_len;
	int data_alloc_len = 1024 * 1024 * 1;
	int lun_i;
	int maxq;
	int qcnt, ccnt, bcnt, icnt;
	int rc;
	const char *msg = "";
	istgt_task_action action;
	int sindx = 7;
	ISTGT_QUEUE_Ptr q;
	struct timespec sch1, sch2, r;
	int id;
	unsigned long secs, nsecs;
#define ttdiff(_s, _n, _r) {                     \
	if(unlikely(spec->do_avg == 1))	\
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

	if (lu_cmd == NULL)
		return -1;
	lu = lu_cmd->lu;
	if (lu == NULL) {
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}
	spec = NULL;
	cdb = lu_cmd->cdb;
	//data = lu_cmd->data;
	//data_alloc_len = lu_cmd->alloc_len;

	lun_i = istgt_lu_islun2lun(lu_cmd->lun);
	if (lun_i >= lu->maxlun) {
/*
		ISTGT_ERRLOG("LU%d: LUN%4.4d invalid\n", lu->num, lun_i);
*/
		if (cdb[0] == SPC_INQUIRY) {
			allocation_len = DGET16(&cdb[3]);
			if (allocation_len > (size_t) data_alloc_len) {
				ISTGT_ERRLOG("alloc_len %d too big\n", allocation_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data_len = 96;
			data = lu_cmd->data = xmalloc(data_len + 200);
			memset(data, 0, data_len+200);
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], 0x03, 7, 3);
			BDADD8W(&data[0], 0x1f, 4, 5);
			memset(&data[1], 0, data_len - 1);
			/* ADDITIONAL LENGTH */
			data[4] = data_len - 5;
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			return ISTGT_LU_TASK_RESULT_IMMEDIATE;
		} else {
			/* LOGICAL UNIT NOT SUPPORTED */
			ISTGT_ERRLOG("istgt_lu_disk_queue: Illegal_Request, lun_i %d maxlun %d \n", lun_i , lu->maxlun);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x25, 0x00);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return ISTGT_LU_TASK_RESULT_IMMEDIATE;
		}
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun_i].spec;
	if (spec == NULL) {
		/* LOGICAL UNIT NOT SUPPORTED */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x25, 0x00);
		lu_cmd->data_len = 0;
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return ISTGT_LU_TASK_RESULT_IMMEDIATE;
	}
	/* ready to enqueue, spec is valid for LUN access */

	/* allocate task and copy LU_CMD(PDU) */
	lu_task = istgt_lu_create_task(conn, lu_cmd, lun_i, spec);
	if (lu_task == NULL) {
		ISTGT_ERRLOG("lu_create_task() failed\n");
		return -1;
	}

	/* enqueue SCSI command */
        timediff(&lu_task->lu_cmd, 'e', __LINE__);
	MTX_LOCK(&spec->complete_queue_mutex);
	ccnt = istgt_queue_count(&spec->cmd_queue);
	bcnt = istgt_queue_count(&spec->blocked_queue);
	ccnt += istgt_queue_count(&spec->maint_cmd_queue);
	bcnt += istgt_queue_count(&spec->maint_blocked_queue);
	qcnt = ccnt + bcnt;
	icnt = spec->ludsk_ref;
	maxq = spec->queue_depth * lu->istgt->MaxSessions;
	if (qcnt > maxq) {
		MTX_UNLOCK(&spec->complete_queue_mutex);
		lu_cmd->data_len = 0;
		lu_cmd->status = ISTGT_SCSI_STATUS_TASK_SET_FULL;
		ISTGT_ERRLOG("ISTGT_LU_TASK_RESULT_QUEUE_FULL: cq#%d bq#%d max#%d\n",
				ccnt, bcnt, maxq);
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			ISTGT_ERRLOG("lu_destroy_task() failed\n");
			return -1;
		}
		if(spec->schdler_cmd_waiting) { 
			rc = pthread_cond_signal(&spec->cmd_queue_cond);
		}
		if (rc < 0) {
			ISTGT_ERRLOG("LU%d:Failed to signal worker\n", lu->num);
		}
		return ISTGT_LU_TASK_RESULT_QUEUE_FULL;
	}
	lu_task->lu_cmd.flags = 0;
	action = istgt_is_exec_pipeline_conflict(spec, lu_task);
	sindx = action;
	if(is_maintenance_io(lu_task))
		q = &spec->maint_cmd_queue;
	else
		q = &spec->cmd_queue;
	switch( action ){
		case ISTGT_TASK_PASS:
		case ISTGT_TASK_SKIP:
			if(is_maintenance_io(lu_task))
				lu_task->lu_cmd.flags |= ISTGT_ADD_TO_MAINT;
			else
				lu_task->lu_cmd.flags |= ISTGT_ADD_TO_CMD;
			break;
		case ISTGT_TASK_BLOCK_SUSPECT:
		case ISTGT_TASK_BLOCK:
		case ISTGT_TASK_OVERLAP:
			/* BUILD SENSE illegal_request asc = 0x4E, ascq = 0x00 */
		case ISTGT_TASK_OVERLAP_TAG:
			/* BUILD SENSE illegal_request asc = 0x4D, ascq = task_tag */
			lu_task->flags |= ISTGT_FLAG_BLOCKED;
			if(is_maintenance_io(lu_task))
			{
				lu_task->lu_cmd.flags |= (ISTGT_ADD_TO_BLOCKED_MAINT);
				q = &spec->maint_blocked_queue;
			}
			else
			{
				lu_task->lu_cmd.flags |= (ISTGT_ADD_TO_BLOCKED_CMD);
				q = &spec->blocked_queue;
			}
			if(spec->do_avg == 1)
				spec->avgs[10].count++;
			break;
		case ISTGT_TASK_ERROR:
			/* Internal Failure */
		default:
			//assert(!"Internal failure");
			BUILD_SENSE(HARDWARE_ERROR, 0x44, 0x00);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ISTGT_ERRLOG("LU%d: Queue%s: Task_error (q:%d/%d %d), CmdSN=%u, OP=0x%x (lba %"PRIu64", blks:%u), LUN=0x%16.16"PRIx64"\n",
				lu->num, qact[sindx < 0 || sindx > 6 ? 7 : sindx], ccnt, bcnt, icnt, lu_cmd->CmdSN, lu_cmd->cdb[0], lu_cmd->lba, lu_cmd->lblen, lu_cmd->lun);
			if(action == ISTGT_TASK_ERROR)
				goto error_return;
			else
				goto error_return_no_dequeue;
	}

	/* enqueue task to LUN */
	switch (lu_cmd->Attr_bit) {
		case 0x03: /* Head of Queue */
			msg = "HeadofQueue";
			if(is_maintenance_io(lu_task))
				r_ptr = istgt_queue_enqueue_first(&spec->maint_cmd_queue, lu_task);
			else
				r_ptr = istgt_queue_enqueue_first(&spec->cmd_queue, lu_task);
			break;
		case 0x00: /* Untagged */
			msg = "Untagged";
			r_ptr = istgt_queue_enqueue(q, lu_task);
			break;
		case 0x01: /* Simple */
			msg = "Simple";
			r_ptr = istgt_queue_enqueue(q, lu_task);
			break;
		case 0x02: /* Ordered */
			msg = "Ordered";
			r_ptr = istgt_queue_enqueue(q, lu_task);
			break;
		case 0x04: /* ACA */
			msg = "ACA";
			r_ptr = istgt_queue_enqueue(q, lu_task);
			break;
		default: /* Reserved */
			msg = "Reserved";
			r_ptr = istgt_queue_enqueue(q, lu_task);
			break;
	}
	if (r_ptr == NULL) {
		ISTGT_ERRLOG("LU%d: Queue%s enqerror(%s)(q:%d/%d %d) CmdSN=%u,0x%x.%lu+%u\n",
				lu->num, qact[sindx < 0 || sindx > 6 ? 7 : sindx], msg, ccnt, bcnt, icnt, lu_cmd->CmdSN, lu_cmd->cdb[0], lu_cmd->lba, lu_cmd->lblen);
error_return:
		istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);
		lu_task->complete_queue_ptr = NULL;
		MTX_UNLOCK(&spec->complete_queue_mutex);
error_return_no_dequeue:
		rc = istgt_lu_destroy_task(lu_task);
		if (rc < 0) {
			ISTGT_ERRLOG("lu_destroy_task() failed\n");
			return -1;
		}
/* should we send error back to client? */
		return -1;
	}
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI, "LU%d: Queue%s (%s)(q:%d/%d %d) CmdSN=%u,0x%x.%lu+%u\n",
		lu->num, qact[sindx < 0 || sindx > 6 ? 7 : sindx], msg, ccnt, bcnt, icnt, lu_cmd->CmdSN, lu_cmd->cdb[0], lu_cmd->lba, lu_cmd->lblen);

	/* notify LUN thread */
	if(!is_maintenance_io(lu_task))
	{
		if(spec->schdler_cmd_waiting) {
			clock_gettime(clockid, &sch1);
			rc = pthread_cond_signal(&spec->cmd_queue_cond);
			if (rc != 0) {
/* We are not removing from the cmd/blocked qeuue */
				ISTGT_ERRLOG("LU%d: cond_broadcast() failed rc = %d\n errno = %d", lu->num, rc, errno);
				goto error_return;
			}
			clock_gettime(clockid, &sch2);
			id = 20;
			ttdiff(sch1, sch2, r);
		}
	}
	else
	{
		rc = pthread_cond_signal(&spec->maint_cmd_queue_cond);
		if (rc != 0) {
/* We are not removing from the maint cmd/blocked qeuue */
			ISTGT_ERRLOG("LU%d: cond_broadcast() failed rc = %d\n errno = %d", lu->num, rc, errno);
			goto error_return;
		}
	}
	MTX_UNLOCK(&spec->complete_queue_mutex);

	return ISTGT_LU_TASK_RESULT_QUEUE_OK;
}
/*
int
istgt_lu_disk_queue_count(ISTGT_LU_Ptr lu, int *lun)
{
	ISTGT_LU_DISK *spec;
	int qcnt;
	int bqcnt;
	int luns;
	int i;

	if (lun == NULL)
		return -1;

	i = *lun;
	if (i >= lu->maxlun) {
		*lun = 0;
		i = 0;
	}

	qcnt = 0;
	bqcnt = 0;
	for (luns = lu->maxlun; luns >= 0 ; luns--) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
			goto next_lun;
		}
		if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
			ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
			goto next_lun;
		}
		spec = (ISTGT_LU_DISK *) lu->lun[i].spec;
		if (spec == NULL) {
			goto next_lun;
		}

		MTX_LOCK(&spec->cmd_queue_mutex);
		qcnt = istgt_queue_count(&spec->cmd_queue);
		bqcnt = istgt_queue_count(&spec->blocked_queue);
		MTX_UNLOCK(&spec->cmd_queue_mutex);

		if (qcnt+bqcnt > 0) {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "LU%d: LUN%d queue(%d) bqueue(%d)\n",
			    lu->num, i, qcnt,bqcnt);
			*lun = spec->lun;
			break;
		}

	next_lun:
		i++;
		if (i >= lu->maxlun) {
			i = 0;
		}
	}
	return qcnt+bqcnt;
}
*/
/*
int
istgt_lu_disk_cmd_queue_count(ISTGT_LU_Ptr lu, int *lun)
{
	ISTGT_LU_DISK *spec = NULL;
	int qcnt;
	int luns;
	int i;

	if (lun == NULL)
		return -1;

	i = *lun;
	if (i >= lu->maxlun) {
		*lun = 0;
		i = 0;
	}

	qcnt = 0;
	for (luns = lu->maxlun; luns >= 0 ; luns--) {
		if (lu->lun[i].type == ISTGT_LU_LUN_TYPE_NONE) {
			goto next_lun;
		}
		if (lu->lun[i].type != ISTGT_LU_LUN_TYPE_STORAGE) {
			ISTGT_ERRLOG("LU%d: unsupported type\n", lu->num);
			goto next_lun;
		}
		spec = (ISTGT_LU_DISK *) lu->lun[i].spec;
		if (spec == NULL) {
			goto next_lun;
		}

		MTX_LOCK(&spec->cmd_queue_mutex);
		qcnt = istgt_queue_count(&spec->cmd_queue);
		MTX_UNLOCK(&spec->cmd_queue_mutex);

		if (qcnt > 0) {
			*lun = spec->lun;
			break;
		}

	next_lun:
		i++;
		if (i >= lu->maxlun) {
			i = 0;
		}
	}
	return qcnt;
}
*/
#define INFLIGHT_IO_CLEANUP	\
		{\
			MTX_LOCK(&spec->luworker_mutex[worker_id]);\
			MTX_LOCK(&spec->lu_tmf_mutex[worker_id]);\
			if(spec->lu_tmf_wait[worker_id] == 1)\
			{\
				spec->lu_tmf_wait[worker_id] = 0;\
				rc = pthread_cond_signal(&spec->lu_tmf_cond[worker_id]);\
				if(rc != 0) {\
					ISTGT_ERRLOG("signalling failed\n");\
				}\
			}\
			MTX_UNLOCK(&spec->lu_tmf_mutex[worker_id]);\
			if(likely(lu_task != NULL)) {\
				if((lu_task->lu_cmd).aborted == 1) {\
					ISTGT_LOG("CmdSN %d aborted in inflight", (lu_task->lu_cmd).CmdSN);\
					aborted = 1;\
				}\
				if(likely(spec->inflight_io[worker_id] == lu_task))\
				{\
	                        	lu_task->lu_cmd.flags |= ISTGT_COMPLETED_EXEC;\
					spec->inflight_io[worker_id] = NULL;\
					if(likely(lu_task != NULL))\
						decrement_conn_inflight = 1;\
				}\
				else {\
					spec->error_count++;\
					ISTGT_ERRLOG("LU%d: Error thread %d: Inflight IO overwrite!!! \n", lu->num, worker_id);\
				}\
			}\
			if(unlikely(worker_id >= spec->luworkers))\
			{\
				MTX_UNLOCK(&spec->luworker_mutex[worker_id]);\
			}\
			else\
			{\
				MTX_LOCK(&spec->schdler_mutex);\
				if(in_lu_worker_exit == 0)\
					BSET32(spec->lu_free_matrix[(worker_id >> 5)], (worker_id & 31));\
				spec->inflight--;\
				if(spec->schdler_waiting) {\
					MTX_UNLOCK(&spec->schdler_mutex);\
					MTX_UNLOCK(&spec->luworker_mutex[worker_id]);\
					pthread_cond_signal(&spec->schdler_cond);\
				} else {\
					MTX_UNLOCK(&spec->schdler_mutex);\
					MTX_UNLOCK(&spec->luworker_mutex[worker_id]);\
				}\
			}\
/* No need to wake up maint_thread as there is only thread and it is looping */\
			if(likely(lu_task != NULL)) {\
				MTX_LOCK(&spec->complete_queue_mutex);\
				istgt_queue_dequeue_middle(&spec->complete_queue, lu_task->complete_queue_ptr);\
				lu_task->complete_queue_ptr = NULL;\
				if(likely(decrement_conn_inflight == 1))\
				{\
					lu_task->conn->inflight--;\
					decrement_conn_inflight = 0;\
				}\
			}\
			if(spec->schdler_cmd_waiting == 1)\
			{\
				MTX_UNLOCK(&spec->complete_queue_mutex);\
				pthread_cond_signal(&spec->cmd_queue_cond);\
			}\
			else\
			{\
				MTX_UNLOCK(&spec->complete_queue_mutex);\
			}\
		}

void luworker_exit(void *void_arg);

void luworker_exit(void *void_arg)
{
	struct luworker_exit_args *arg = (struct luworker_exit_args *)void_arg;
	ISTGT_LU_DISK *spec = arg->spec;
	int worker_id = arg->workerid;
	ISTGT_LU_TASK_Ptr lu_task = arg->lu_task;
	ISTGT_LU_Ptr lu = spec->lu;
	int decrement_conn_inflight = 0;
	int rc;
	int aborted = 0;
	int in_lu_worker_exit = 1;

	INFLIGHT_IO_CLEANUP;
	ISTGT_WARNLOG("luworker:%p %d thread_exit ios_aborted:%d\n", spec, worker_id, aborted);

	return;
}

int
istgt_lu_disk_queue_start(ISTGT_LU_Ptr lu, int lun, int worker_id)
{
	ISTGT_QUEUE_Ptr r_ptr = NULL;
	ISTGT_LU_DISK *spec = NULL;
	ISTGT_LU_TASK_Ptr lu_task;
	CONN_Ptr conn;
	ISTGT_LU_CMD_Ptr lu_cmd;
	struct timespec abstime;
	time_t start, now;
	int rc, i, timed_wait_rc = 0;
	uint32_t CmdSN;
	int opcode, printmsg = 1, aborted = 0;
	const char *msg = "";
	int decrement_conn_inflight = 0;
	int retval = -1;
	int in_lu_worker_exit = 0;

	if (lun < 0 || lun >= lu->maxlun) {
		return -1;
	}

	spec = (ISTGT_LU_DISK *) lu->lun[lun].spec;
	if (spec == NULL)
	{
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "LU%d: LUN%d queue start: returning, storage unavailable\n",
			lu->num, lun);
		return -1;
	}

	MTX_LOCK(&spec->luworker_mutex[worker_id]);
	lu_task = spec->inflight_io[worker_id];
	MTX_UNLOCK(&spec->luworker_mutex[worker_id]);
	if(lu_task == NULL)
	{
		INFLIGHT_IO_CLEANUP;
		return 0;
	}

	spec->luworker_exit_arg[worker_id].spec = spec;
	spec->luworker_exit_arg[worker_id].workerid = worker_id;
	spec->luworker_exit_arg[worker_id].lu_task = lu_task;
	
	pthread_cleanup_push(luworker_exit, &(spec->luworker_exit_arg[worker_id]));
	lu_task->thread = pthread_self();
	conn = lu_task->conn;
	lu_cmd = &lu_task->lu_cmd;
	timediffw(lu_cmd, 'q');

	CmdSN = lu_cmd->CmdSN;
	opcode = lu_cmd->cdb[0];

	if(spec->exit_lu_worker)
	{
		MTX_LOCK(&conn->result_queue_mutex);
		if(spec->exit_lu_worker)
		{
			spec->exit_lu_worker = 0;
			MTX_UNLOCK(&conn->result_queue_mutex);
			pthread_exit(NULL);
		}
		MTX_UNLOCK(&conn->result_queue_mutex);
	}

	retval = -1;
	if (lu_cmd->W_bit) {
		if (lu_cmd->pdu->data_segment_len >= lu_cmd->transfer_len) {
			i = ++lu_cmd->iobufindx;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d LU%d: CSN:0x%x LUN%d Task Write Immediate Start, op:0x%x [%d,%lu->%lu]\n",
			    conn->id, lu->num, CmdSN, lun, opcode,
				i, lu_cmd->iobuf[i].iov_len, lu_cmd->pdu->data_segment_len);
			lu_cmd->iobuf[i].iov_base = lu_cmd->pdu->data;
			lu_cmd->iobuf[i].iov_len = lu_cmd->pdu->data_segment_len;
			lu_cmd->iobufsize = lu_cmd->pdu->data_segment_len;
			lu_cmd->pdu->data = NULL; lu_cmd->pdu->data_segment_len = 0;
			lu_task->dup_iobuf = 1;

			//MTX_LOCK(&lu_cmd->lu->mutex);
			rc = istgt_lu_disk_execute(conn, lu_cmd);
			//MTX_UNLOCK(&lu_cmd->lu->mutex);

			if ((rc < 0) || (lu_cmd->connGone == 1)) {
				if(lu_cmd->connGone == 1)
					msg = "lu_disk_execute2 failed Error:connGone";
				else
					msg = "lu_disk_execute1 failed";
				goto error_return;
			}
			lu_task->execute = 1;

			/* response */
			MTX_LOCK(&conn->result_queue_mutex);
			INFLIGHT_IO_CLEANUP;
			if (aborted == 1) {
				MTX_UNLOCK(&conn->result_queue_mutex);
				goto error_return_no_cleanup;
			}
			r_ptr = istgt_queue_enqueue(&conn->result_queue, lu_task);
			if (r_ptr == NULL) {
				MTX_UNLOCK(&conn->result_queue_mutex);
				msg = "rsltq1 failed";
				goto error_return_no_cleanup;
			}
			lu_task->lu_cmd.flags |= ISTGT_RESULT_Q_ENQUEUED;
			rc = 0;
			if(conn->sender_waiting == 1)
				rc = pthread_cond_signal(&conn->result_queue_cond);
			MTX_UNLOCK(&conn->result_queue_mutex);
			if (rc != 0) {
				msg = "rsltq1 bcast failed";
				goto error_return_no_destroy;
			}

		} else {
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
			    "c#%d LU%d: CSN:0x%x LUN%d Task Write Start, op:0x%x\n",
			    conn->id, lu->num, CmdSN, lun, opcode);

#if 0
			MTX_LOCK(&spec->wait_lu_task_mutex);
			spec->wait_lu_task = NULL;
			MTX_UNLOCK(&spec->wait_lu_task_mutex);
#endif
			rc = pthread_mutex_init(&lu_task->trans_mutex, NULL);
			if (rc != 0) {
				msg = "mtx2 failed";
				goto error_return;
			}
			rc = pthread_cond_init(&lu_task->trans_cond, NULL);
			if (rc != 0) {
				msg = "cond2 failed";
				goto error_return;
			}
			rc = pthread_cond_init(&lu_task->exec_cond, NULL);
			if (rc != 0) {
				msg = "cond2a failed";
				goto error_return;
			}
			lu_task->use_cond = 1;

			//lu_cmd->iobufsize = lu_task->lu_cmd.iobufsize;
			//lu_cmd->iobuf = lu_task->iobuf;

			lu_task->req_transfer_out = 1;
			abstime.tv_sec = 0;
			abstime.tv_nsec = 0;

			MTX_LOCK(&conn->task_queue_mutex);
			r_ptr = istgt_queue_enqueue(&conn->task_queue, lu_task);
			MTX_UNLOCK(&conn->task_queue_mutex);
			if (r_ptr == NULL) {
				msg = "tskq enqueue failed";
				goto error_return;
			}
			rc = write(conn->task_pipe[1], "Q", 1); //something other than E
			if(rc < 0 || rc != 1) {
				msg = "tskpipe write failed";
				INFLIGHT_IO_CLEANUP;
				//Should we dequeue enqueued task?
				goto error_return_no_destroy;
			}
			if(spec->error_inject & R2T_TASK_BEFORE_WAIT)
				if(spec->inject_cnt > 0)
				{
					spec->inject_cnt--;
					sleep(8);
				}

			start = now = time(NULL);
			abstime.tv_sec = now + lu_task->condwait; // / 1000);
			abstime.tv_nsec = 0; //(lu_task->condwait % 1000) * 1000000;
			
			MTX_LOCK(&spec->wait_lu_task_mutex);
			spec->wait_lu_task[worker_id] = lu_task;
			MTX_UNLOCK(&spec->wait_lu_task_mutex);
			MTX_LOCK(&lu_task->trans_mutex);
			rc = 0;
			while (lu_task->req_transfer_out == 1) {
				rc = pthread_cond_timedwait(&lu_task->trans_cond,
				    &lu_task->trans_mutex,
				    &abstime);
				if(spec->error_inject & R2T_TASK_AFTER_WAIT)
					if(spec->inject_cnt > 0)
					{
						spec->inject_cnt--;
						sleep(8);
					}
				if (rc == ETIMEDOUT) {
					if (lu_task->req_transfer_out == 1) {
						INFLIGHT_IO_CLEANUP;
						lu_task->error = 1;
						lu_task->lu_cmd.aborted = 1;
						MTX_LOCK(&spec->wait_lu_task_mutex);
						spec->wait_lu_task[worker_id] = NULL;
						MTX_UNLOCK(&spec->wait_lu_task_mutex);
						MTX_UNLOCK(&lu_task->trans_mutex);

						now = time(NULL);
						ISTGT_ERRLOG("c#%d timeout trans_cond CmdSN=0x%x "
						    "(time=%f)\n",
						    conn->id, lu_task->lu_cmd.CmdSN,
						    difftime(now, start));
						/* timeout */
						goto return_retval;
					}
					/* OK cond */
					if (lu_task->error == 0)
						rc = 0;
					else {
						lu_task->lu_cmd.aborted = 1;
						rc = -1;
					}
					break;
				}
				if (lu_task->error != 0) {
					rc = -1;
					lu_task->lu_cmd.aborted = 1;
					break;
				}
				if (rc != 0) {
					break;
				}
			}
			MTX_UNLOCK(&lu_task->trans_mutex);

			MTX_LOCK(&spec->wait_lu_task_mutex);
			spec->wait_lu_task[worker_id] = NULL;
			MTX_UNLOCK(&spec->wait_lu_task_mutex);

			if (rc != 0) {
				lu_task->lu_cmd.aborted = 1;
				timed_wait_rc = rc;
				INFLIGHT_IO_CLEANUP; //This modifies rc
				rc = timed_wait_rc;
				if (rc < 0) {
					lu_task->error = 1;
					if (lu_task->abort) {
						ISTGT_WARNLOG("c#%d transfer abort CmdSN=0x%x\n",
						    conn->id, lu_task->lu_cmd.CmdSN);
						//destroy lu_task?
						retval = -2;
						goto return_retval;
					} else {
						ISTGT_ERRLOG("c#%d transfer error CmdSN=0x%x\n",
						    conn->id, lu_task->lu_cmd.CmdSN);
						//destroy lu_task?
						goto return_retval;
					}
				}
				if (rc == ETIMEDOUT) {
					lu_task->error = 1;
					now = time(NULL);
					//timedout tasks should not be deleted
					ISTGT_ERRLOG("c#%d timeout trans_cond CmdSN=0x%x (time=%f)\n",
					    conn->id, lu_task->lu_cmd.CmdSN, difftime(now, start));
					goto return_retval;
				}
				lu_task->error = 1;
				//destroy lu_task?
				ISTGT_ERRLOG("c#%d cond_timedwait rc=%d\n",conn->id, rc);
				goto return_retval;
			}

			if (lu_task->req_execute == 0) {
				msg = "wrong request!";
				goto error_return;
			}
			//MTX_LOCK(&lu_cmd->lu->mutex);
			rc = istgt_lu_disk_execute(conn, lu_cmd);
			//MTX_UNLOCK(&lu_cmd->lu->mutex);

			if ((rc < 0) || (lu_cmd->connGone == 1)) {
				lu_task->error = 1;
				if(lu_cmd->connGone == 1)
					msg = "lu_disk_execute2 failed Error:connGone";
				else
					msg = "lu_disk_execute2 failed";
				goto error_return;
			}
			lu_task->execute = 1;

			/* response */
			MTX_LOCK(&conn->result_queue_mutex);
			INFLIGHT_IO_CLEANUP;
			if (aborted == 1) {
				MTX_UNLOCK(&conn->result_queue_mutex);
				goto error_return_no_cleanup;
			}
			r_ptr = istgt_queue_enqueue(&conn->result_queue, lu_task);
			if (r_ptr == NULL) {
				MTX_UNLOCK(&conn->result_queue_mutex);
				msg = "rsltq2 failed";
				goto error_return_no_cleanup;
			}
			lu_task->lu_cmd.flags |= ISTGT_RESULT_Q_ENQUEUED;
			rc = 0;
			if(conn->sender_waiting == 1)
				rc = pthread_cond_signal(&conn->result_queue_cond);
			MTX_UNLOCK(&conn->result_queue_mutex);
			if (rc != 0) {
				msg = "rsltq2 bcast failed";
				goto error_return_no_destroy;
			}
		}
	} else {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
		    "c#%d LU%d: CSN:%x LUN%d Task Read Start, op:0x%x\n",
		    conn->id, lu->num, CmdSN, lun, opcode);

		//lu_cmd->iobufsize = lu_task->lu_cmd.iobufsize;
		//lu_cmd->iobuf = lu_task->iobuf;

		//MTX_LOCK(&lu_cmd->lu->mutex);
		rc = istgt_lu_disk_execute(conn, lu_cmd);
		//MTX_UNLOCK(&lu_cmd->lu->mutex);

		if ((rc < 0) || (lu_cmd->connGone == 1)) {
			if(lu_cmd->connGone == 1)
				msg = "lu_disk_execute2 failed Error:connGone";
			else
				msg = "lu_disk_execute3 failed";
			goto error_return;
		}
		lu_task->execute = 1;

		/* response */
		MTX_LOCK(&conn->result_queue_mutex);
		INFLIGHT_IO_CLEANUP;
		if (aborted == 1) {
			MTX_UNLOCK(&conn->result_queue_mutex);
			goto error_return_no_cleanup;
		}
		r_ptr = istgt_queue_enqueue(&conn->result_queue, lu_task);
		if (r_ptr == NULL) {
			MTX_UNLOCK(&conn->result_queue_mutex);
			msg = "rsltq3 failed";
			goto error_return_no_cleanup;
		}
		lu_task->lu_cmd.flags |= ISTGT_RESULT_Q_ENQUEUED;
		rc = 0;
		if(conn->sender_waiting == 1)
			rc = pthread_cond_signal(&conn->result_queue_cond);
		MTX_UNLOCK(&conn->result_queue_mutex);
		if (rc != 0) {
			msg = "rsltq3 bcast failed";
			goto error_return_no_destroy;
		}
	}
	retval = 0;
	goto return_retval;

error_return:
	INFLIGHT_IO_CLEANUP;	
error_return_no_cleanup:
	if (lu_cmd && conn && lu_cmd->connGone == 1) {
		ISTGT_ERRLOG("c#%d LU%d: LUN%d connGone set, error:%s, op:0x%x cmdSN:0x%x (ret:%d)\n",
		    conn->id, lu->num, lun, msg, opcode, CmdSN, rc);
		printmsg = 0;
	}
	istgt_lu_destroy_task(lu_task);

error_return_no_destroy:
	if (printmsg == 1) {
		ISTGT_ERRLOG("c#%d LU%d: LUN%d error:%s, op:0x%x cmdSN:0x%x (ret:%d)\n",
		    conn->id, lu->num, lun, msg, opcode, CmdSN, rc);
	}
return_retval:
	;
	pthread_cleanup_pop(0);
	return retval;
}

int
istgt_lu_disk_busy_excused(int opcode)
{
	int ret  = 0;
	switch(opcode) {
		case  SPC_INQUIRY:
		case  SPC_TEST_UNIT_READY:
		case SPC_MODE_SENSE_10:
		case SPC_REQUEST_SENSE:
		//case  SPC2_RESERVE_6:
		case  SPC_REPORT_LUNS :
		case SBC_READ_CAPACITY_10:
		case SCC_MAINTENANCE_IN:
			ret =1;
			break;
		default:
			break;
	}
	return ret;
	
}
#define checklength \
	if(transfer_len*spec->blocklen > (uint64_t)lu->MaxBurstLength) { \
		ISTGT_WARNLOG("c#%d checklength error: transferlen %lu should be < maxburstlen %lu\n", \
			conn->id, transfer_len*spec->blocklen, (uint64_t)(lu->MaxBurstLength)); \
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION; \
		BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);\
		break;\
	}

int
istgt_lu_disk_execute(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_LU_Ptr lu;
	ISTGT_LU_DISK *spec;
	IT_NEXUS *nexus;
	istgt_ua_type ua_pending;
	uint8_t *data = NULL;
	int freedata = 0;
	uint8_t *cdb;
	uint32_t allocation_len;
	int data_len;
	int data_alloc_len = 1024*1024*1;
	int max_alloc_size = 1024*1024*1;
	uint64_t lba;
	uint32_t len;
	uint32_t transfer_len;
	uint32_t parameter_len;
	int lun_i;
	int rc = 0;
	int lunum, dolog = 0;
	const char *msg = "";
	uint64_t *tptr;
	int sa= 0;
	if (lu_cmd == NULL)
		return -1;
	lu = lu_cmd->lu;
	if (lu == NULL) {
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}
	lunum = lu->num;
	spec = NULL;
	cdb = lu_cmd->cdb;
	errno = 0;

	lu_cmd->flags |= ISTGT_DISK_EXEC;

	lun_i = istgt_lu_islun2lun(lu_cmd->lun);
	if (lun_i >= lu->maxlun) {
		if (cdb[0] == SPC_INQUIRY) {
			allocation_len = DGET16(&cdb[3]);
			data_len = 96;
			data = lu_cmd->data = xmalloc(data_len + 200);
			memset(data, 0, data_len+200);
			/* PERIPHERAL QUALIFIER(7-5) PERIPHERAL DEVICE TYPE(4-0) */
			BDSET8W(&data[0], 0x03, 7, 3);
			BDADD8W(&data[0], 0x1f, 4, 5);
			memset(&data[1], 0, data_len - 1);
			/* ADDITIONAL LENGTH */
			data[4] = data_len - 5;
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
/*
			ISTGT_ERRLOG("LU%d: LUN%4.4d invalid\n", lu->num, lun_i);
*/
			return 0;
		} else {
			/* LOGICAL UNIT NOT SUPPORTED */
			ISTGT_ERRLOG("c#%d Illegal_Request, lun_i %d maxlun %d\n",conn->id, lun_i, lu->maxlun);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x25, 0x00);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return 0;
		}
	}
	spec = (ISTGT_LU_DISK *) lu->lun[lun_i].spec;
	if (spec == NULL) {
		/* LOGICAL UNIT NOT SUPPORTED */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x25, 0x00);
		lu_cmd->data_len = 0;
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return 0;
	}

	MTX_LOCK(&spec->state_mutex);
	if ((spec->state == ISTGT_LUN_BUSY && (!istgt_lu_disk_busy_excused(cdb[0])))
#ifdef	REPLICATION
	    || !spec->ready
#endif
	    ) {
		lu_cmd->data_len  = 0;
		lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
		MTX_UNLOCK(&spec->state_mutex);
		MTX_LOCK(&spec->sleep_mutex);
		sleep(1);
		MTX_UNLOCK(&spec->sleep_mutex);
		return 0;
	}
	MTX_UNLOCK(&spec->state_mutex);

	{
	int sk =0 , asc = 0, ascq = 0;
	if (spec->sense != 0) {
		if (cdb[0] != SPC_INQUIRY
		    && cdb[0] != SPC_REPORT_LUNS) {
			sk = (spec->sense >> 16) & 0xffU;
			asc = (spec->sense >> 8) & 0xffU;
			ascq = (spec->sense >> 0) & 0xffU;
			spec->sense = 0;
			rc = 1;
			goto return_sense;
		}
	}

	nexus = istgt_lu_disk_get_nexus(spec, conn->initiator_port);
	if (nexus != NULL) {
		MTX_LOCK(&nexus->nexus_mutex);
		ua_pending = nexus->ua_pending;
		if (ua_pending != ISTGT_UA_NONE) {
			/* Send priority UA */
			ua_pending =  istgt_lu_disk_build_ua(ua_pending, &sk, &asc, &ascq);
			if (ua_pending != ISTGT_UA_NONE) {
				rc = 1;
				nexus->ua_pending &= ~ua_pending;
				MTX_UNLOCK(&nexus->nexus_mutex);
				goto return_sense;
			}
		}
		MTX_UNLOCK(&nexus->nexus_mutex);
	}

	return_sense:
		if (rc){
			ISTGT_LOG( "c#%d Generate sk=0x%x, asc=0x%x, ascq=0x%x\n", conn->id, sk, asc, ascq);
			istgt_lu_scsi_build_sense_data(lu_cmd, sk, asc, ascq);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return 0;
		}
	}


	//ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
	//    "SCSI OP=0x%x, LUN=0x%16.16"PRIx64"\n",
	//    cdb[0], lu_cmd->lun);
#if 0 //ISTGT_TRACE_DISK
	if (cdb[0] != SPC_TEST_UNIT_READY) {
		istgt_scsi_dump_cdb(cdb);
	}
#endif /* ISTGT_TRACE_DISK */

	switch (cdb[0]) {
	case SPC_INQUIRY:
		if (lu_cmd->R_bit == 0) {
			ISTGT_ERRLOG("c#%d INQUIRY R_bit == 0\n", conn->id);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return -1;
		}
		allocation_len = DGET16(&cdb[3]);
		data_alloc_len = allocation_len + 2048;
		if (allocation_len > (size_t) max_alloc_size) {
			ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
					conn->id, data_alloc_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return -1;
		}
		data = lu_cmd->data = xmalloc(data_alloc_len);
		memset(data, 0, allocation_len);
		data_len = istgt_lu_disk_scsi_inquiry(spec, conn, cdb,
		    data, data_alloc_len);
		if (data_len < 0) {
			if(errno == EBUSY)
				lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
			else
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d INQUIRY error:%d\n", conn->id, data_len);
			break;
		}
		if (data_len > data_alloc_len) {
			ISTGT_ERRLOG("c#%d INQUIRY mem-error:%d-%d\n", conn->id, data_len, data_alloc_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
		}
		ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "INQUIRY", data, data_len);
		lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;

	case SPC_REPORT_LUNS:
		{
			int sel;

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d REPORT_LUNS R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			sel = cdb[2];
			allocation_len = DGET32(&cdb[6]);
			data_alloc_len = allocation_len + 2048;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			if (allocation_len < 16) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, allocation_len);
			data_len = istgt_lu_disk_scsi_report_luns(lu, conn, cdb, sel,
			    data, data_alloc_len);
			if (data_len < 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else {
					BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				}
				break;
			}
			ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG, "REPORT LUNS", data, data_len);
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;

			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d REPORT_LUNS sel:%x len:%lu (%d/%u/%u)\n",
					conn->id, sel, lu_cmd->data_len, data_len, lu_cmd->transfer_len, allocation_len);
		}
		break;

	case SPC_TEST_UNIT_READY:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d TEST_UNIT_READY\n", conn->id);
		if (spec->rsv_key) {
			rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
			if (rc != 0) {
				ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				break;
			}
		}
		lu_cmd->data_len = 0;
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;

	case SBC_START_STOP_UNIT:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d START_STOP_UNIT\n", conn->id);
		{
			int pc, start;

			pc = BGET8W(&cdb[4], 7, 4);
			start = BGET8(&cdb[4], 0);

			if (start != 0 || pc != 0) {
				if (spec->rsv_key) {
					rc = istgt_lu_disk_check_pr(spec, conn,
					    PR_ALLOW(0,0,1,0,0));
					if (rc != 0) {
						ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
						lu_cmd->status
							= ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
						break;
					}
				}
			}

			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		}
		break;

	case SBC_READ_CAPACITY_10:
		if (lu_cmd->R_bit == 0) {
			ISTGT_ERRLOG("c#%d READ_CAPACITY_10 R_bit == 0\n", conn->id);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			return -1;
		}
		data = lu_cmd->data = xmalloc(200);
		if (spec->blockcnt - 1 > 0xffffffffULL) {
			DSET32(&data[0], 0xffffffffUL);
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ_CAPACITY_10 blklen:%lu\n", conn->id, spec->blocklen);
		} else {
			DSET32(&data[0], (uint32_t) (spec->blockcnt - 1));
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ_CAPACITY_10 blkcnt:%lu blklen:%lu\n", conn->id, spec->blockcnt, spec->blocklen);
		}
		DSET32(&data[4], (uint32_t) spec->blocklen);
		tptr = (uint64_t *)&(data[8]);
		*tptr = 0; *(tptr+1) = 0;
		data_len = 8;
		lu_cmd->data_len = data_len;
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
		    "SBC_READ_CAPACITY_10", data, data_len);
		break;

	case SPC_SERVICE_ACTION_IN_16:
		sa = BGET8W(&cdb[1], 4, 5); /* SERVICE ACTION */
		switch (sa) {
		case SBC_SAI_READ_CAPACITY_16:
			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d READ_CAPACITY_16 R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			allocation_len = DGET32(&cdb[10]);
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d READ_CAPACITY_16 data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
 			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d READ_CAPACITY_16 blkcnt:%lu  blklen:%lu rshift:%u/%u %s\n",
 					conn->id, spec->blockcnt, spec->blocklen, spec->rshift, spec->rshiftreal, spec->unmap ? "thin" : "-");

			data_len = 32;
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			data = lu_cmd->data = xmalloc(data_len + 20);
			memset(data, 0, data_len+20);
			DSET64(&data[0], spec->blockcnt - 1);
			DSET32(&data[8], (uint32_t) spec->blocklen);
			data[12] = 0;                   /* RTO_EN(1) PROT_EN(0) */
			data[13] = 0;
 			data[13] = spec->rshift & 0x0f;
 			//if (spec->rshift > 0)
 			//	BDSET8W(&data[13], spec->rshift, 3, 4);
			if (spec->unmap)
				data[14] = 0x80;
			else 
				data[14] = 0;
			data[15] = 0;
			tptr = (uint64_t *)(&(data[16]));
			*tptr = 0; *(tptr+1) = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		case SBC_SAI_READ_LONG_16:
		default:
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d SA_IN_16:op:0x%2.2x unhandled\n", conn->id, sa);
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
		}
		break;

	case SPC_MODE_SELECT_6:
#if 0
		istgt_scsi_dump_cdb(cdb);
#endif
		{
			int pf, sp, pllen;
			int bdlen;

			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			pf = BGET8(&cdb[1], 4);
			sp = BGET8(&cdb[1], 0);
			pllen = cdb[4];             /* Parameter List Length */

			if (pllen == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			/* Data-Out */
			rc = istgt_lu_disk_transfer_data(conn, lu_cmd, pllen);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (pllen < 4) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
#if 0
			istgt_dump("MODE SELECT(6)", lu_cmd->iobuf, pllen);
#endif
			getdata(data, lu_cmd)
			bdlen = data[3];            /* Block Descriptor Length */

			/* Short LBA mode parameter block descriptor */
			/* data[4]-data[7] Number of Blocks */
			/* data[8]-data[11] Block Length */

			/* page data */
			data_len = istgt_lu_disk_scsi_mode_select_page(spec, conn, cdb, pf, sp, &data[4 + bdlen], pllen - (4 + bdlen));
			if (data_len != 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = pllen;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SPC_MODE_SELECT_10:
#if 0
		istgt_scsi_dump_cdb(cdb);
#endif
		{
			int pf, sp, pllen;
			int  bdlen;
			int llba;

			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			pf = BGET8(&cdb[1], 4);
			sp = BGET8(&cdb[1], 0);
			pllen = DGET16(&cdb[7]);    /* Parameter List Length */

			if (pllen == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			/* Data-Out */
			rc = istgt_lu_disk_transfer_data(conn, lu_cmd, pllen);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (pllen < 4) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
#if 0
			istgt_dump("MODE SELECT(10)", lu_cmd->iobuf, pllen);
#endif
			getdata(data, lu_cmd) //data = lu_cmd->iobuf;
			llba = BGET8(&data[4], 0);  /* Long LBA */
			bdlen = DGET16(&data[6]);   /* Block Descriptor Length */

			if (llba) {
				/* Long LBA mode parameter block descriptor */
				/* data[8]-data[15] Number of Blocks */
				/* data[16]-data[19] Reserved */
				/* data[20]-data[23] Block Length */
			} else {
				/* Short LBA mode parameter block descriptor */
				/* data[8]-data[11] Number of Blocks */
				/* data[12]-data[15] Block Length */
			}

			/* page data */
			data_len = istgt_lu_disk_scsi_mode_select_page(spec, conn, cdb, pf, sp, &data[8 + bdlen], pllen - (8 + bdlen));
			if (data_len != 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = pllen;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SPC_MODE_SENSE_6:
#if 0
		istgt_scsi_dump_cdb(cdb);
#endif
		{
			int dbd, pc, page, subpage;

			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("R_bit == 0\n");
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			dbd = BGET8(&cdb[1], 3);
			pc = BGET8W(&cdb[2], 7, 2);
			page = BGET8W(&cdb[2], 5, 6);
			subpage = cdb[3];

			allocation_len = cdb[4];
			data_alloc_len = allocation_len + 2048;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, data_alloc_len);
			data_len = istgt_lu_disk_scsi_mode_sense6(spec, conn, cdb, dbd, pc, page, subpage, data, data_alloc_len);
			if (data_len < 0) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d MODE SENSE6 dbd:%x pc:%x page:%x subpage:%x alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, dbd,  pc, page, subpage,
					allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
#if 0
			istgt_dump("MODE SENSE(6)", data, data_len);
#endif
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MODE SENSE6 dbd:%x pc:%x page:%x subpage:%x alen:%d dlen:%d tlen:%d",
					conn->id, dbd,  pc, page, subpage,
					allocation_len, data_len, lu_cmd->transfer_len);

			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SPC_MODE_SENSE_10:
#if 0
		istgt_scsi_dump_cdb(cdb);
#endif
		{
			int dbd, pc, page, subpage;
			int llbaa;

			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			llbaa = BGET8(&cdb[1], 4);
			dbd = BGET8(&cdb[1], 3);
			pc = BGET8W(&cdb[2], 7, 2);
			page = BGET8W(&cdb[2], 5, 6);
			subpage = cdb[3];

			allocation_len = DGET16(&cdb[7]);
			data_alloc_len = allocation_len + 2048;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, data_alloc_len);

			data_len = istgt_lu_disk_scsi_mode_sense10(spec, conn, cdb, llbaa, dbd, pc, page, subpage, data, data_alloc_len);
			if (data_len < 0) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d MODE SENSE10 dbd:%x pc:%x page:%x subpage:%x alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, dbd,  pc, page, subpage,
					allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

#if 0
			istgt_dump("MODE SENSE(10)", data, data_len);
#endif
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			ISTGT_NOTICELOG("c#%d MODE SENSE10 dbd:%x pc:%x page:%x subpage:%x alen:%d dlen:%d tlen:%d",
					conn->id, dbd,  pc, page, subpage,
					allocation_len, data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SPC_LOG_SELECT:
		{
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}
		}
	case SPC_LOG_SENSE:
		/* INVALID COMMAND OPERATION CODE */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		break;

	case SPC_REQUEST_SENSE:
		{
			int desc;
			int sk, asc, ascq;

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			desc = BGET8(&cdb[1], 0);
			if (desc != 0) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

			allocation_len = cdb[4];
			data_alloc_len = allocation_len + 2048;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);

			if (!spec->sense) {
				/* NO ADDITIONAL SENSE INFORMATION */
				sk = ISTGT_SCSI_SENSE_NO_SENSE;
				asc = 0x00;
				ascq = 0x00;
			} else {
				sk = (spec->sense >> 16) & 0xffU;
				asc = (spec->sense >> 8) & 0xffU;
				ascq = spec->sense & 0xffU;
			}
			istgt_lu_scsi_build_sense_data(lu_cmd, sk, asc, ascq);
			data_len = lu_cmd->sense_data_len;
			if (data_len < 0 || data_len < 2) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			/* omit SenseLength */
			data_len -= 2;
			memcpy(data, lu_cmd->sense_data + 2, data_len);
#if 0
			istgt_dump("REQUEST SENSE", data, data_len);
#endif
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			if (lu_cmd->data_len > (size_t)data_len)
				memset(data+data_len, 0, lu_cmd->data_len - data_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SPC_RECEIVE_COPY_RESULTS:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}
			allocation_len = DGET32(&cdb[10]);
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n", conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			if (allocation_len == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			data_len = istgt_lu_disk_receive_copy_results(conn, lu_cmd);
			if (data_len < 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				ISTGT_ERRLOG("c#%d lu_disk_receive_copy_results() failed\n", conn->id);
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_6:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("R_bit == 0\n");
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) (DGET24(&cdb[1]) & 0x001fffffU);
			transfer_len = (uint32_t) DGET8(&cdb[4]);
			if (transfer_len == 0) {
				transfer_len = 256;
			}
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d READ_6(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbread(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbread() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_10:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d READ_10(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbread(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbread() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_12:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("R_bit == 0\n");
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[6]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d READ_12(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbread(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbread() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_16:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d READ_16(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbread(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbread() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_6:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) (DGET24(&cdb[1]) & 0x001fffffU);
			transfer_len = (uint32_t) DGET8(&cdb[4]);
			if (transfer_len == 0) {
				transfer_len = 256;
			}
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d WRITE_6(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbwrite() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_10:
	case SBC_WRITE_AND_VERIFY_10:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d WRITE_10(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbwrite() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_12:
	case SBC_WRITE_AND_VERIFY_12:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[6]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d WRITE_12(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbwrite() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_16:
	case SBC_WRITE_AND_VERIFY_16:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			checklength;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d WRITE_16(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbwrite() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_VERIFY_10:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET16(&cdb[7]);
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d VERIFY_10(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, len);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_VERIFY_12:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[6]);
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d VERIFY_12(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, len);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_VERIFY_16:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(1,0,1,1,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			lba = (uint64_t) DGET64(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[10]);
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d VERIFY_16(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, len);
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_SAME_10:
		{
			int pbdata, lbdata;

			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET32(&cdb[2]);
			transfer_len = (uint32_t) DGET16(&cdb[7]);
			checklength;
			//group_no = BGET8W(&cdb[6], 4, 5)
			/* only PBDATA=0 and LBDATA=0 support */
			if (pbdata || lbdata) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d WRITE_SAME_10(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite_same(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbwrite_same() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_WRITE_SAME_16:
		{
			int anchor, unmap, pbdata, lbdata;

#if 0
			istgt_scsi_dump_cdb(cdb);
#endif
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			anchor = BGET8(&cdb[1], 4);
			unmap = BGET8(&cdb[1], 3);
			pbdata = BGET8(&cdb[1], 2);
			lbdata = BGET8(&cdb[1], 1);
			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET32(&cdb[10]);
			checklength;
			//group_no = BGET8W(&cdb[14], 4, 5);
			/* only PBDATA=0 and LBDATA=0 support */
			if (pbdata || lbdata) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (anchor) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			     "c#%d WRITE_SAME_16(lba %"PRIu64", len %u blocks) %s\n",
				 conn->id, lba, transfer_len, unmap ? "UNMAP bit" : " ");
			rc = istgt_lu_disk_lbwrite_same(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d WRITE_SAME_16(lba %"PRIu64", len %u blocks) failed. %s\n",
						conn->id, lba, transfer_len, unmap ? "UNMAP bit" : " ");
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_COMPARE_AND_WRITE:
		{
			int64_t maxlen;
#if 0
			istgt_scsi_dump_cdb(cdb);
#endif
			if (spec->lu->istgt->swmode == ISTGT_SWMODE_TRADITIONAL) {
				/* INVALID COMMAND OPERATION CODE */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("W_bit == 0\n");
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			lba = (uint64_t) DGET64(&cdb[2]);
			transfer_len = (uint32_t) DGET8(&cdb[13]);

			maxlen = ISTGT_LU_WORK_ATS_BLOCK_SIZE / spec->blocklen;
			if (maxlen > 0xff) {
				maxlen = 0xff;
			}
			if (transfer_len > maxlen) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d COMPARE_AND_WRITE(lba %"PRIu64", len %u blocks)\n",
			    conn->id, lba, transfer_len);
			rc = istgt_lu_disk_lbwrite_ats(spec, conn, lu_cmd, lba, transfer_len);
			if (rc < 0) {
				//ISTGT_ERRLOG("lu_disk_lbwrite_ats() failed\n");
				/* sense data build by function */
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_SYNCHRONIZE_CACHE_10:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n");
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			lba = (uint64_t) DGET32(&cdb[2]);
			len = (uint32_t) DGET16(&cdb[7]);
			if (len == 0) {
				len = spec->blockcnt;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d SYNCHRONIZE_CACHE_10(lba %"PRIu64
			    ", len %u blocks)\n",
			    conn->id, lba, len);
			rc = istgt_lu_disk_lbsync(spec, conn, lu_cmd, lba, len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbsync() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_SYNCHRONIZE_CACHE_16:
		{
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}

			lba = (uint64_t) DGET64(&cdb[2]);
			len = (uint32_t) DGET32(&cdb[10]);
			if (len == 0) {
				len = spec->blockcnt;
			}
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI,
			    "c#%d SYNCHRONIZE_CACHE_10(lba %"PRIu64
			    ", len %u blocks)\n",
			    conn->id, lba, len);
			rc = istgt_lu_disk_lbsync(spec, conn, lu_cmd, lba, len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_lbsync() failed\n", conn->id);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = 0;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_DEFECT_DATA_10:
		{
			int req_plist, req_glist, list_format;
			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			req_plist = BGET8(&cdb[2], 4);
			req_glist = BGET8(&cdb[2], 3);
			list_format = BGET8W(&cdb[2], 2, 3);

			allocation_len = (uint32_t) DGET16(&cdb[7]);
			data_alloc_len = allocation_len + 8192;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, allocation_len);

			data_len = istgt_lu_disk_scsi_read_defect10(spec, conn, cdb,
			    req_plist, req_glist, list_format, data, data_alloc_len);
			if (data_len < 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d READ_DEFECT_DATA10  alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SBC_READ_DEFECT_DATA_12:
		{
			int req_plist, req_glist, list_format;

			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d R_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			req_plist = BGET8(&cdb[2], 4);
			req_glist = BGET8(&cdb[2], 3);
			list_format = BGET8W(&cdb[2], 2, 3);

			allocation_len = DGET32(&cdb[6]);
			data_alloc_len = allocation_len + 8192;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, allocation_len);

			data_len = istgt_lu_disk_scsi_read_defect12(spec, conn, cdb,
			    req_plist, req_glist, list_format, data, data_alloc_len);
			if (data_len < 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d READ_DEFECT_DATA12  alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	case SCC_MAINTENANCE_IN:
		sa = BGET8W(&cdb[1], 4, 5); /* SERVICE ACTION */
		switch (sa) { /* SERVICE ACTION */
		case SPC_MI_REPORT_TARGET_PORT_GROUPS:
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MAINTENANCE_IN: sa:0x%2.2x:REPORT_TARGET_PORT_GROUPS\n", conn->id, sa);
			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("R_bit == 0\n");
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			allocation_len = DGET32(&cdb[6]);
			data_alloc_len = allocation_len + 8192;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, allocation_len);
			data_len = istgt_lu_disk_scsi_report_target_port_groups(spec, conn, cdb, data, data_alloc_len);
			if (data_len < 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d REPORT_TPGs alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

			ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
			    "REPORT_TARGET_PORT_GROUPS", data, data_len);
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		default:
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MAINTENANCE_IN: sa:0x%2.2x:unhandled\n", conn->id, sa);
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
		}
		break;

	case SCC_MAINTENANCE_OUT:
		sa = BGET8W(&cdb[1], 4, 5);  /* SERVICE ACTION */
		switch (sa) { /* SERVICE ACTION */
		case SPC_MO_SET_TARGET_PORT_GROUPS:
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MAINTENANCE_OUT: 0x%2.2x:SET_TARGET_PORT_GROUPS\n", conn->id, sa);
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}
			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d W_bit == 0\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			parameter_len = DGET32(&cdb[6]);
			if (parameter_len == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			/* Data-Out */
			rc = istgt_lu_disk_transfer_data(conn, lu_cmd, parameter_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (parameter_len < 4) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			/*ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
			    "SET_TARGET_PORT_GROUPS",
			    lu_cmd->iobuf, parameter_len);*/
			getdata(data, lu_cmd)
			/* data[0]-data[3] Reserved */
			/* Set target port group descriptor(s) */
			data_len = istgt_lu_disk_scsi_set_target_port_groups(spec, conn, cdb, &data[4], parameter_len - 4);
			if (data_len < 0) {
				/* INVALID FIELD IN PARAMETER LIST */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->data_len = parameter_len;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		default:
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d MAINTENANCE_OUT: sa:0x%2.2x:unhandled\n", conn->id,  sa);
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
		}
		break;

	case SPC_PERSISTENT_RESERVE_IN:
		{
#ifdef	REPLICATION
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RESERVE_IN not handled\n", conn->id);
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
#else
			sa = BGET8W(&cdb[1], 4, 5);
			if (lu_cmd->R_bit == 0) {
				ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_IN: sa:0x%2.2x R_bit == 0\n", conn->id, sa);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			allocation_len = DGET16(&cdb[7]);
			data_alloc_len = allocation_len + 8192;
			if (allocation_len > (size_t) max_alloc_size) {
				ISTGT_ERRLOG("c#%d data_alloc_len(%d) too small\n",
				    conn->id, data_alloc_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}
			data = lu_cmd->data = xmalloc(data_alloc_len);
			memset(data, 0, allocation_len);
			MTX_LOCK(&spec->pr_rsv_mutex);
			data_len = istgt_lu_disk_scsi_persistent_reserve_in(spec, conn, lu_cmd, sa, data, allocation_len);
			MTX_UNLOCK(&spec->pr_rsv_mutex);
			if (data_len < 0) {
				/* status build by function */
				break;
			}
			if (data_len > data_alloc_len) {
				ISTGT_ERRLOG("c#%d PRIN alen:%d dlen:%d/%d tlen:%d mem-issue",
					conn->id, allocation_len, data_len, data_alloc_len, lu_cmd->transfer_len);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
			    "PERSISTENT_RESERVE_IN", data, data_len);
			lu_cmd->data_len = DMIN32((size_t)data_len, lu_cmd->transfer_len);
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d PERSISTENT_RESERVE_IN sa:0x%2.2x data:%u/%u/%u\n", conn->id, sa, data_len, lu_cmd->transfer_len, allocation_len);
#endif
		}
		break;

	case SPC_PERSISTENT_RESERVE_OUT:
		{
#ifdef	REPLICATION
			ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RESERVE_OUT not handled\n", conn->id);
			/* INVALID COMMAND OPERATION CODE */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
#else
			int scope, type;
			sa = BGET8W(&cdb[1], 4, 5);

			if (lu_cmd->W_bit == 0) {
				ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_OUT sa:0x%2.2x W_bit == 0\n", conn->id, sa);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				return -1;
			}

			scope = BGET8W(&cdb[2], 7, 4);
			type = BGET8W(&cdb[2], 3, 4);
			parameter_len = DGET32(&cdb[5]);

			/* Data-Out */
			rc = istgt_lu_disk_transfer_data(conn, lu_cmd, parameter_len);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_OUT sa:0x%2.2x scope:%x type:%x plen:%d - disk_transfer_data failed:%d\n",
						conn->id, sa, scope, type, parameter_len, rc);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (parameter_len < 24) {
				ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_OUT sa:0x%2.2x scope:%x type:%x plen:%d less than 24\n",
						conn->id, sa, scope, type, parameter_len);
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}

			/*ISTGT_TRACEDUMP(ISTGT_TRACE_DEBUG,
			    "PERSISTENT_RESERVE_OUT",
			    lu_cmd->iobuf, parameter_len);*/
			getdata(data, lu_cmd)
			MTX_LOCK(&spec->pr_rsv_mutex);
			data_len = istgt_lu_disk_scsi_persistent_reserve_out(spec, conn, lu_cmd, sa, scope, type, &data[0], parameter_len, 0);
			MTX_UNLOCK(&spec->pr_rsv_mutex);
			if (data_len < 0) {
				ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_OUT sa:0x%2.2x scope:%x type:%x plen:%d, function failed:%d\n",
						conn->id, sa, scope, type, parameter_len, data_len);
				/* status build by function */
				break;
			}
			lu_cmd->data_len = parameter_len;
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			ISTGT_ERRLOG("c#%d PERSISTENT_RESERVE_OUT sa:0x%2.2x scope:%x type:%x plen:%d success (%d)",
						conn->id, sa, scope, type, parameter_len, data_len);
#endif
		}
		break;

	/* XXX TODO: fix */
	case 0x85: /* ATA PASS-THROUGH(16) */
	case 0xA1: /* ATA PASS-THROUGH(12) */
		/* INVALID COMMAND OPERATION CODE */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		break;
	case SPC_EXTENDED_COPY:
	{
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d EXTENDED_COPY\n", conn->id);
		sa = BGET8W(&cdb[1], 4, 5); /* LID1  - 00h / LID4 - 01h  */
		if (sa != 0x00) {
			/* INVALID COMMAND SERVICE ACTION */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
			break;
		}
		if (spec->rsv_key) {
			rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
			if (rc != 0) {
				ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
				break;
			}
		}
		rc = istgt_lu_disk_xcopy(spec, conn, lu_cmd);
		if (rc < 0) {
			/* build by function */
			break;
		}
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;
	}
	case SPC2_RELEASE_6:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RELEASE_6\n", conn->id);
		if(spec->delay_release)
			sleep(spec->delay_release);
		MTX_LOCK(&spec->pr_rsv_mutex);
		rc = istgt_lu_disk_scsi_release(spec, conn, lu_cmd);
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		if (rc < 0) {
			/* build by function */
			break;
		}
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;
	case SPC2_RELEASE_10:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RELEASE_10\n", conn->id);
		if(spec->delay_release)
			sleep(spec->delay_release);
		MTX_LOCK(&spec->pr_rsv_mutex);
		rc = istgt_lu_disk_scsi_release(spec, conn, lu_cmd);
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		if (rc < 0) {
			/* build by function */
			break;
		}
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;
	case SPC2_RESERVE_6:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RESERVE_6\n", conn->id);
		if(spec->delay_reserve)
			sleep(spec->delay_reserve);
		MTX_LOCK(&spec->pr_rsv_mutex);
		rc = istgt_lu_disk_scsi_reserve(spec, conn, lu_cmd);
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		if (rc < 0) {
			/* build by function */
			break;
		}
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;
	case SPC2_RESERVE_10:
		ISTGT_TRACELOG(ISTGT_TRACE_SCSI, "c#%d RESERVE_10\n", conn->id);
		if(spec->delay_reserve)
			sleep(spec->delay_reserve);
		MTX_LOCK(&spec->pr_rsv_mutex);
		rc = istgt_lu_disk_scsi_reserve(spec, conn, lu_cmd);
		MTX_UNLOCK(&spec->pr_rsv_mutex);
		if (rc < 0) {
			/* build by function */
			break;
		}
		lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
		break;

	case SBC_UNMAP:
		if (spec->unmap) {
			int pllen;
			uint16_t unmap_datalen, block_des_datalen;
			if(lu->limit_q_size != 0)
			{
				lu_cmd->data_len  = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				break;
			}
			if (spec->rsv_key) {
				rc = istgt_lu_disk_check_pr(spec, conn, PR_ALLOW(0,0,1,0,0));
				if (rc != 0) {
					ISTGT_ERRLOG("c#%d ISTGT_SCSI_STATUS_RESERVATION_CONFLICT\n", conn->id);
					lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
					break;
				}
			}
			pllen = DGET16(&cdb[7]);    /* Parameter List Length */
			if (pllen == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			/* Data-Out */
			rc = istgt_lu_disk_transfer_data(conn, lu_cmd, pllen);
			if (rc < 0) {
				ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (pllen < 4) {
				/* INVALID FIELD IN CDB */
				BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			getdata(data, lu_cmd) //data = lu_cmd->iobuf;

			unmap_datalen = DGET16(data);
			block_des_datalen = DGET16(&data[2]);
			if (block_des_datalen == 0) {
				lu_cmd->data_len = 0;
				lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
				break;
			}
			if (pllen != unmap_datalen + 2) {
				ISTGT_ERRLOG("c#%d unmap_datalen %d pllen %d not matching\n", conn->id, unmap_datalen, pllen);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			if (pllen != block_des_datalen + 8) {
				ISTGT_ERRLOG("c#%d block_des_datalen %d pllen %d not matching\n", conn->id, block_des_datalen, pllen);
				lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			rc = istgt_lu_disk_unmap(spec, conn, lu_cmd, data, pllen);
			if (rc != 0) {
				if(errno == EBUSY)
					lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
				else
					lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
				break;
			}
			lu_cmd->status = ISTGT_SCSI_STATUS_GOOD;
			break;
		}

	default:
		ISTGT_ERRLOG("c#%d LUN=0x%16.16"PRIx64" unsupported SCSI OP=0x%x\n", conn->id, lu_cmd->lun, cdb[0]);
		/* INVALID COMMAND OPERATION CODE */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x20, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		break;
	}

	if (lu_cmd->status == ISTGT_SCSI_STATUS_CHECK_CONDITION
			|| lu_cmd->status == ISTGT_SCSI_STATUS_GOOD) {
		/* Do we need this? */
		MTX_LOCK(&spec->state_mutex);
		if (lu_cmd->status == ISTGT_SCSI_STATUS_CHECK_CONDITION && IS_SPEC_BUSY(spec)) {
			msg = "(CheckCond_on_fake changed to Busy)"; dolog = 1;
			lu_cmd->status = ISTGT_SCSI_STATUS_BUSY;
		}
	
#if 0 
		else if(lu_cmd->status == ISTGT_SCSI_STATUS_GOOD && spec->state != ISTGT_LUN_BUSY) {
			/* Read/Write the reservation from/to ZAP if previously failed */
			if(spec->persist){
				if (spec->rsv_pending & ISTGT_RSV_READ) //lock and check?
				rereadrsv = 1;
			}
			else if (spec->rsv_pending & ISTGT_RSV_READ)
				ISTGT_LOG("persist is off, but, read reservation is pending");
		}
#endif
		MTX_UNLOCK(&spec->state_mutex);
	}
#if 0
	if (rereadrsv == 1) {
		/* Read/Write the reservation from/to ZAP if previously failed */
		MTX_LOCK(&spec->pr_rsv_mutex);
		if (spec->rsv_pending & ISTGT_RSV_READ) {
			rc = istgt_lu_disk_get_reservation(spec);
			if (rc < 0) {
				msg = "(istgt_lu_disk_get_reservation failed, will retry)"; dolog = 1;
			}
		}
		MTX_UNLOCK(&spec->pr_rsv_mutex);
	}
#endif
	if (dolog == 1) {
	ISTGT_LOG(
		"c#%d LU%d.%lx: CSN:%x OP=0x%x/%x (%lu+%u) complete [%c:%ld.%9.9ld %c:%ld.%9.9ld]%s\n",
	    	conn->id, lunum, lu_cmd->lun, lu_cmd->CmdSN, cdb[0], lu_cmd->status, lu_cmd->lba, lu_cmd->lblen,
		lu_cmd->caller[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0],
		lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_sec,
		lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_nsec,
		lu_cmd->caller[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0],
		lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_sec,
		lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_nsec,
		msg);
	} else {
	ISTGT_TRACELOG(ISTGT_TRACE_ISCSI,
		"c#%d LU%d.%lx: CSN:%x OP=0x%x/%x (%lu+%u) complete [%c:%ld.%9.9ld %c:%ld.%9.9ld]%s\n",
	    	conn->id, lunum, lu_cmd->lun, lu_cmd->CmdSN, cdb[0], lu_cmd->status, lu_cmd->lba, lu_cmd->lblen,
		lu_cmd->caller[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0],
		lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_sec,
		lu_cmd->tdiff[lu_cmd->_andx > 1 ? lu_cmd->_andx - 2 : 0].tv_nsec,
		lu_cmd->caller[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0],
		lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_sec,
		lu_cmd->tdiff[lu_cmd->_andx > 0 ? lu_cmd->_andx - 1 : 0].tv_nsec,
		msg);
	}
	if ((data != NULL)  && (freedata == 1))
		xfree(data);
	return 0;
}

