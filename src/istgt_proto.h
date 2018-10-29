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

#ifndef ISTGT_PROTO_H
#define ISTGT_PROTO_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef USE_ATOMIC
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_MACHINE_ATOMIC_H
#include <machine/atomic.h>
#endif
#ifdef HAVE_SYS_ATOMIC_H
#include <sys/atomic.h>
#endif
#endif /* USE_ATOMIC */


#include <stdint.h>
#include <signal.h>
#include <sys/socket.h>
#ifdef HAVE_UUID_H
#include <uuid.h>
#endif
#include "istgt.h"
#include "istgt_iscsi.h"
#include "istgt_lu.h"

#if !defined(__GNUC__)
#undef __attribute__
#define __attribute__(x)
#endif
#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#define ISTGT_GNUC_PREREQ(ma,mi) \
	(__GNUC__ > (ma) || (__GNUC__ == (ma) && __GNUC_MINOR__ >= (mi)))
#else
#define ISTGT_GNUC_PREREQ(ma,mi) 0
#endif

/* istgt_iscsi.c */
int istgt_chap_get_authinfo(ISTGT_CHAP_AUTH *auth, const char *authfile, const char *authuser, int ag_tag);
int istgt_iscsi_transfer_out(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, size_t transfer_len);
int istgt_create_sess(ISTGT_Ptr istgt, CONN_Ptr conn, ISTGT_LU_Ptr lu);
int istgt_create_conn(ISTGT_Ptr istgt, PORTAL_Ptr portal, int sock, struct sockaddr *sa, socklen_t salen);
void istgt_lock_gconns(void);
void istgt_unlock_gconns(void);
int istgt_get_gnconns(void);
CONN_Ptr istgt_get_gconn(int idx);
int istgt_get_active_conns(void);
int istgt_stop_conns(void);
CONN_Ptr istgt_find_conn(const char *initiator_port, const char *target_name, uint16_t tsih);
int istgt_iscsi_init(ISTGT_Ptr istgt);
int istgt_iscsi_shutdown(ISTGT_Ptr istgt);

/* istgt_lu.c */
int istgt_lu_allow_netmask(const char *netmask, const char *addr);
void istgt_connection_status(CONN_Ptr conn, const char *status);
int istgt_lu_access(CONN_Ptr conn, ISTGT_LU_Ptr lu, const char *iqn, const char *addr);
int istgt_lu_visible(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu, const char *iqn, int pg_tag);
int istgt_lu_sendtargets(CONN_Ptr conn, const char *iiqn, const char *iaddr, const char *tiqn, uint8_t *data, int alloc_len, int data_len);
ISTGT_LU_Ptr istgt_lu_find_target(ISTGT_Ptr istgt, const char *target_name);
ISTGT_LU_Ptr istgt_lu_find_target_by_volname(ISTGT_Ptr istgt, const char *target_name);
#ifdef	REPLICATION
int istgt_lu_create_snapshot(spec_t *spec, char *snapname, int, int);
int istgt_lu_destroy_snapshot(spec_t *spec, char *snapname);
void istgt_lu_mempool_stats(char **resp);
void istgt_lu_replica_stats(char *volname, char **resp);
#endif
int istgt_lu_add_nexus(ISTGT_LU_Ptr lu, char *initiator_port);
int istgt_lu_remove_nexus(ISTGT_LU_Ptr lu, char *initiator_port);
uint16_t istgt_lu_allocate_tsih(ISTGT_LU_Ptr lu, const char *initiator_port, int tag);
void istgt_lu_free_tsih(ISTGT_LU_Ptr lu, uint16_t tsih, char *initiator_port);
char *istgt_lu_get_media_flags_string(int flags, char *buf, size_t len);
uint64_t istgt_lu_get_devsize(const char *file);
uint64_t istgt_lu_get_filesize(const char *file);
uint64_t istgt_lu_parse_size(const char *size);
int istgt_lu_parse_media_flags(const char *flags);
uint64_t istgt_lu_parse_media_size(const char *file, const char *size, int *flags);
PORTAL_GROUP *istgt_lu_find_portalgroup(ISTGT_Ptr istgt, int tag);
INITIATOR_GROUP *istgt_lu_find_initiatorgroup(ISTGT_Ptr istgt, int tag);
int istgt_lu_init(ISTGT_Ptr istgt);
int istgt_lu_reload_delete(ISTGT_Ptr istgt);
int istgt_lu_reload_update(ISTGT_Ptr istgt);
int istgt_lu_update_ig(ISTGT_Ptr istgt, INITIATOR_GROUP *igp_new);
int istgt_lu_set_all_state(ISTGT_Ptr istgt, ISTGT_STATE state);
int istgt_lu_create_threads(ISTGT_Ptr istgt);
int istgt_lu_shutdown(ISTGT_Ptr istgt);
int istgt_lu_islun2lun(uint64_t islun);
uint64_t istgt_lu_lun2islun(int lun, int maxlun);
int istgt_lu_reset(ISTGT_LU_Ptr lu, uint64_t lun, istgt_ua_type ua_type);
int istgt_lu_reset_all(ISTGT_Ptr istgt, istgt_ua_type ua_type);
ISTGT_LU_TASK_Ptr istgt_lu_create_task(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, int lun, ISTGT_LU_DISK * spec);
int istgt_lu_destroy_task(ISTGT_LU_TASK_Ptr lu_task);
int istgt_lu_clear_task_IT(CONN_Ptr conn, ISTGT_LU_Ptr lu);
int istgt_lu_clear_task_ITL(CONN_Ptr conn, ISTGT_LU_Ptr lu, uint64_t lun);
int istgt_lu_clear_task_ITLQ(CONN_Ptr conn, ISTGT_LU_Ptr lu, uint64_t lun, uint32_t CmdSN);
int istgt_lu_clear_all_task(ISTGT_LU_Ptr lu, uint64_t lun);

/* istgt_lu_ctl.c */
int istgt_create_uctl(ISTGT_Ptr istgt, PORTAL_Ptr portal, int sock, struct sockaddr *sa, socklen_t salen);
int istgt_uctl_init(ISTGT_Ptr istgt);
int istgt_uctl_shutdown(ISTGT_Ptr istgt);

/* istgt_lu_disk.c */
struct istgt_lu_disk_t;
void istgt_scsi_dump_cdb(uint8_t *cdb);
void istgt_strcpy_pad(uint8_t *dst, size_t size, const char *src, int pad);
#ifdef HAVE_UUID_H
uint64_t istgt_uuid2uint64(uuid_t *uuid);
#endif /* HAVE_UUID_H */
uint64_t istgt_get_lui(const char *name, int lun);
uint64_t istgt_get_rkey(const char *initiator_name, uint64_t lui);
int istgt_lu_set_lid(uint8_t *buf, uint64_t vid);
int istgt_lu_set_id(uint8_t *buf, uint64_t vid);
int istgt_lu_set_extid(uint8_t *buf, uint64_t vid, uint64_t vide);
void istgt_lu_scsi_build_sense_data(ISTGT_LU_CMD_Ptr lu_cmd, int sk, int asc, int ascq);
void istgt_lu_scsi_build_sense_data2(ISTGT_LU_CMD_Ptr lu_cmd, int sk, int asc, int ascq);
int istgt_lu_disk_init(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_disk_shutdown(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_disk_modify(ISTGT *istgt, int lun);
int istgt_lu_disk_persist_reservation(ISTGT_LU_Ptr lu, int lun, char *arg);
int istgt_lu_disk_sync_reservation(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_reset(ISTGT_LU_Ptr lu, int lun, istgt_ua_type ua_type);
int istgt_lu_disk_clear_reservation(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_start(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_stop(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_status(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_execute(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);
int istgt_lu_disk_queue_clear_IT(CONN_Ptr conn, ISTGT_LU_Ptr lu);
int istgt_lu_disk_queue_clear_ITL(CONN_Ptr conn, ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_queue_clear_ITLQ(CONN_Ptr conn, ISTGT_LU_Ptr lu, int lun, uint32_t CmdSN);
int istgt_lu_disk_queue_clear_all(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_queue(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);
int istgt_lu_disk_queue_count(ISTGT_LU_Ptr lu, int *lun);
int istgt_lu_disk_cmd_queue_count(ISTGT_LU_Ptr lu, int *lun);
int istgt_lu_disk_blockedq_count(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_cmdq_count(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_disk_queue_start(ISTGT_LU_Ptr lu, int lun, int tind);
istgt_task_action istgt_check_for_istgt_queue(ISTGT_QUEUE_Ptr queue, ISTGT_LU_TASK_Ptr unblocked_lu_task);
istgt_task_action istgt_check_for_all_luworkers(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task);
istgt_task_action istgt_check_for_all_maint_luworkers(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task);
istgt_task_action istgt_check_for_parallel_ios(ISTGT_LU_DISK *spec, ISTGT_LU_TASK_Ptr unblocked_lu_task);
int is_maintenance_io(ISTGT_LU_TASK_Ptr task);
void istgt_schedule_blocked_requests(ISTGT_LU_DISK *spec, ISTGT_QUEUE_Ptr cmd, ISTGT_QUEUE_Ptr blocked, int maint);
int istgt_lu_disk_signal_worker(ISTGT_LU_Ptr lu);
void istgt_lu_disk_aio_done(siginfo_t *info);
int istgt_lu_disk_build_sense_data(ISTGT_LU_DISK *spec, uint8_t *data, int sk, int asc, int ascq);

int istgt_lu_disk_add_nexus(ISTGT_LU_Ptr lu, int lun, const char * initiator_port);
int istgt_lu_disk_remove_nexus(ISTGT_LU_Ptr lu, int lun, const char * initiator_port);
int istgt_lu_disk_build_ua(istgt_ua_type ua_pending, int *sk, int *asc, int *ascq);
int istgt_lu_disk_busy_excused(int opcode);
IT_NEXUS * istgt_lu_disk_get_nexus(ISTGT_LU_DISK *spec, const char * initiator_port);


/* istgt_lu_disk_vbox.c */
int istgt_lu_disk_vbox_lun_init(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_disk_vbox_lun_shutdown(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);

int istgt_lu_disk_transfer_data(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, size_t len);
int istgt_lu_disk_check_pr(ISTGT_LU_DISK *spec, CONN_Ptr conn, int pr_allow);

int64_t  istgt_lu_disk_read_raw(ISTGT_LU_DISK *spec, void *buf, uint64_t nbytes);


/* istgt_lu_dvd.c */
struct istgt_lu_dvd_t;
int istgt_lu_dvd_media_present(struct istgt_lu_dvd_t *spec);
int istgt_lu_dvd_media_lock(struct istgt_lu_dvd_t *spec);
int istgt_lu_dvd_load_media(struct istgt_lu_dvd_t *spec);
int istgt_lu_dvd_unload_media(struct istgt_lu_dvd_t *spec);
int istgt_lu_dvd_change_media(struct istgt_lu_dvd_t *spec, char *type, char *flags, char *file, char *size);
int istgt_lu_dvd_init(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_dvd_shutdown(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_dvd_reset(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_dvd_execute(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);

/* istgt_lu_tape.c */
struct istgt_lu_tape_t;
int istgt_lu_tape_media_present(struct istgt_lu_tape_t *spec);
int istgt_lu_tape_media_lock(struct istgt_lu_tape_t *spec);
int istgt_lu_tape_load_media(struct istgt_lu_tape_t *spec);
int istgt_lu_tape_unload_media(struct istgt_lu_tape_t *spec);
int istgt_lu_tape_change_media(struct istgt_lu_tape_t *spec, char *type, char *flags, char *file, char *size);
int istgt_lu_tape_init(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_tape_shutdown(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_tape_reset(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_tape_execute(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);

/* istgt_lu_pass.c */
int istgt_lu_pass_init(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_pass_shutdown(ISTGT_Ptr istgt, ISTGT_LU_Ptr lu);
int istgt_lu_pass_reset(ISTGT_LU_Ptr lu, int lun);
int istgt_lu_pass_execute(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd);

#ifdef USE_ATOMIC
static inline __attribute__((__always_inline__)) int
istgt_lu_get_state(ISTGT_LU_Ptr lu)
{
	ISTGT_STATE state;
#if defined HAVE_ATOMIC_LOAD_ACQ_INT
	state = atomic_load_acq_int((unsigned int *)&lu->state);
#elif defined HAVE_ATOMIC_OR_UINT_NV
	state = (int)atomic_or_uint_nv((unsigned int *)&lu->state, 0);
#else
#error "no atomic operation"
#endif
	return state;
}
static inline __attribute__((__always_inline__)) void
istgt_lu_set_state(ISTGT_LU_Ptr lu, ISTGT_STATE state)
{
#if defined HAVE_ATOMIC_STORE_REL_INT
	atomic_store_rel_int((unsigned int *)&lu->state, state);
#elif defined HAVE_ATOMIC_SWAP_UINT
	(void)atomic_swap_uint((unsigned int *)&lu->state, state);
#if defined HAVE_MEMBAR_PRODUCER
	membar_producer();
#endif
#else
#error "no atomic operation"
#endif
}
#elif defined (USE_GCC_ATOMIC)
/* gcc >= 4.1 builtin functions */
static inline __attribute__((__always_inline__)) int
istgt_lu_get_state(ISTGT_LU_Ptr lu)
{
	ISTGT_STATE state;
	state = __sync_fetch_and_add((unsigned int *)&lu->state, 0);
	return state;
}
static inline __attribute__((__always_inline__)) void
istgt_lu_set_state(ISTGT_LU_Ptr lu, ISTGT_STATE state)
{
	ISTGT_STATE state_old;
	do {
		state_old = __sync_fetch_and_add((unsigned int *)&lu->state, 0);
	} while (__sync_val_compare_and_swap((unsigned int *)&lu->state,
		state_old, state) != state_old);
#if defined (HAVE_GCC_ATOMIC_SYNCHRONIZE)
	__sync_synchronize();
#endif
}
#else /* !USE_ATOMIC && !USE_GCC_ATOMIC */
static inline __attribute__((__always_inline__)) int
istgt_lu_get_state(ISTGT_LU_Ptr lu)
{
	ISTGT_STATE state;
	MTX_LOCK(&lu->state_mutex);
	state = lu->state;
	MTX_UNLOCK(&lu->state_mutex);
	return state;
}

static inline __attribute__((__always_inline__)) void
istgt_lu_set_state(ISTGT_LU_Ptr lu, ISTGT_STATE state)
{
	MTX_LOCK(&lu->state_mutex);
	lu->state = state;
	MTX_UNLOCK(&lu->state_mutex);
}

#ifdef REPLICATION
//int initialize_volume(spec_t *, int, int);
#endif

#endif /* USE_ATOMIC */

#endif /* ISTGT_PROTO_H */

