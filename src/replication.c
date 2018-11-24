#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <istgt_proto.h>
#include <sys/prctl.h>
#include <sys/eventfd.h>
#include <json-c/json_object.h>
#include "zrepl_prot.h"
#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"
#include "istgt_crc32c.h"
#include "istgt_misc.h"
#include "ring_mempool.h"
#include "istgt_scsi.h"
#include "assert.h"

int extraWait = 5;
extern int replica_timeout;
cstor_conn_ops_t cstor_ops = {
	.conn_listen = replication_listen,
	.conn_connect = replication_connect,
};

int replication_initialized = 0;
size_t rcmd_mempool_count = RCMD_MEMPOOL_ENTRIES;
struct timespec istgt_start_time;

static int start_rebuild(void *buf, replica_t *replica, uint64_t data_len);
static void handle_mgmt_conn_error(replica_t *r, int sfd, struct epoll_event *events,
    int ev_count);
static int read_io_resp(spec_t *spec, replica_t *replica, io_event_t *revent, mgmt_cmd_t *mgmt_cmd);
static void respond_with_error_for_all_outstanding_mgmt_ios(replica_t *r);
static void inform_data_conn(replica_t *r);
static void free_replica(replica_t *r);
static int handle_mgmt_event_fd(replica_t *replica);

#define build_rcomm_cmd(rcomm_cmd, cmd, offset, nbytes) 						\
	do {								\
		uint64_t blockcnt = 0;                                  \
		rcomm_cmd = malloc(sizeof (*rcomm_cmd));		\
		memset(rcomm_cmd, 0, sizeof (*rcomm_cmd));		\
		rcomm_cmd->copies_sent = 0;				\
		rcomm_cmd->total_len = 0;				\
		rcomm_cmd->offset = offset;				\
		rcomm_cmd->data_len = nbytes;				\
		rcomm_cmd->state = CMD_CREATED;				\
		rcomm_cmd->luworker_id = cmd->luworkerindx;		\
		rcomm_cmd->mutex = 					\
		    &spec->luworker_rmutex[cmd->luworkerindx];		\
		rcomm_cmd->cond_var = 					\
		    &spec->luworker_rcond[cmd->luworkerindx];		\
		rcomm_cmd->healthy_count = spec->healthy_rcount;	\
		rcomm_cmd->io_seq = ++spec->io_seq;			\
		rcomm_cmd->replication_factor = 			\
		    spec->replication_factor;				\
		rcomm_cmd->consistency_factor =				\
		    spec->consistency_factor;				\
		rcomm_cmd->state = CMD_ENQUEUED_TO_WAITQ;		\
		switch (cmd->cdb0) {					\
			case SBC_WRITE_6:				\
			case SBC_WRITE_10:				\
			case SBC_WRITE_12:				\
			case SBC_WRITE_16:				\
				cmd_write = true;			\
				rcomm_cmd->opcode = ZVOL_OPCODE_WRITE;	\
				rcomm_cmd->iovcnt = cmd->iobufindx + 1;	\
				__sync_add_and_fetch(&spec->writes, 1); \
				__sync_add_and_fetch(&spec->writebytes, \
							nbytes);        \
				blockcnt = (nbytes/spec->blocklen);     \
				__sync_add_and_fetch(&spec->totalwriteblockcount,\
							blockcnt);      \
				break;					\
									\
			case SBC_READ_6:				\
			case SBC_READ_10:				\
			case SBC_READ_12:				\
			case SBC_READ_16:				\
				rcomm_cmd->opcode = ZVOL_OPCODE_READ;	\
				rcomm_cmd->iovcnt = 0;			\
				__sync_add_and_fetch(&spec->reads, 1);	\
				__sync_add_and_fetch(&spec->readbytes,  \
							nbytes);	\
				blockcnt = (nbytes/spec->blocklen);     \
				__sync_add_and_fetch(&spec->totalreadblockcount,\
							blockcnt);      \
				break;					\
									\
			case SBC_SYNCHRONIZE_CACHE_10:			\
			case SBC_SYNCHRONIZE_CACHE_16:			\
				rcomm_cmd->opcode = ZVOL_OPCODE_SYNC;	\
				rcomm_cmd->iovcnt = 0;			\
				rcomm_cmd->data_len = 0;		\
				break;					\
			default:					\
				break;					\
		}							\
		if (cmd_write) {					\
			for (i=1; i < iovcnt + 1; i++) {		\
				rcomm_cmd->iov[i].iov_base =		\
				    cmd->iobuf[i-1].iov_base;		\
				rcomm_cmd->iov[i].iov_len =		\
				    cmd->iobuf[i-1].iov_len;		\
			}						\
			rcomm_cmd->total_len += cmd->iobufsize;		\
		}							\
	} while (0)							\

#define	CHECK_IO_TYPE(_cmd, _is_read, _is_write, _is_sync)		\
	do {								\
		switch (_cmd->cdb0) {					\
			case SBC_WRITE_6:				\
			case SBC_WRITE_10:				\
			case SBC_WRITE_12:				\
			case SBC_WRITE_16:				\
				_is_write = true;			\
				break;					\
									\
			case SBC_READ_6:				\
			case SBC_READ_10:				\
			case SBC_READ_12:				\
			case SBC_READ_16:				\
				_is_read = true;			\
				break;					\
									\
			case SBC_SYNCHRONIZE_CACHE_10:			\
			case SBC_SYNCHRONIZE_CACHE_16:			\
				_is_sync = true;			\
				break;					\
									\
			default:					\
				break;					\
		}							\
	} while (0)

#define UPDATE_INFLIGHT_SPEC_IO_CNT(_spec, _lun_cmd, _value)		\
do {									\
	switch (_lun_cmd->cdb0) {					\
			case SBC_WRITE_6:                               \
			case SBC_WRITE_10:                              \
			case SBC_WRITE_12:                              \
			case SBC_WRITE_16:                              \
				_spec->inflight_write_io_cnt +=		\
				    (_value);				\
				break;					\
									\
			case SBC_READ_6:				\
			case SBC_READ_10:				\
			case SBC_READ_12:				\
			case SBC_READ_16:				\
				_spec->inflight_read_io_cnt +=		\
				    (_value);				\
				break;					\
									\
			case SBC_SYNCHRONIZE_CACHE_10:			\
			case SBC_SYNCHRONIZE_CACHE_16:			\
				_spec->inflight_sync_io_cnt +=		\
				    (_value);				\
				break;					\
									\
			default:					\
				break;					\
		}							\
	} while (0)

#define BUILD_REPLICA_MGMT_HDR(_mgmtio_hdr, _mgmt_opcode, _data_len)	\
	do {								\
		_mgmtio_hdr = malloc(sizeof(zvol_io_hdr_t));		\
		memset(_mgmtio_hdr, 0, sizeof(zvol_io_hdr_t));		\
		_mgmtio_hdr->opcode = _mgmt_opcode;			\
		_mgmtio_hdr->version = REPLICA_VERSION;			\
		_mgmtio_hdr->len    = _data_len;			\
	} while (0)

#define	clear_mgmt_cmd(_replica, _mgmt_cmd)				\
	do {								\
		TAILQ_REMOVE(&_replica->mgmt_cmd_queue, _mgmt_cmd,	\
		    mgmt_cmd_next);					\
		if (_mgmt_cmd->io_hdr)					\
			free(_mgmt_cmd->io_hdr);			\
		if (_mgmt_cmd->data)					\
			free(_mgmt_cmd->data);				\
		free(_mgmt_cmd);					\
	} while(0)

/*
 * breaks if fd is closed or drained recv buf of fd
 * updates amount of data read to continue next time
 */
#define CHECK_AND_ADD_BREAK_IF_PARTIAL(_io_read, _count, _reqlen,	\
	    _donecount)							\
{									\
	if (_count == -1) {						\
		_donecount = -1;					\
		break;							\
	}								\
	if (_donecount == 1 && _count) {				\
		/*							\
		 * Target or replica can only send one command/response.\
		 * If _donecount is 1 then target/replica has already	\
		 * processed one mgmt command.				\
		 */							\
		REPLICA_ERRLOG("protocol error occurred for "		\
		    "replica(%lu)\n", replica->zvol_guid);		\
		_donecount = -1;					\
		break;							\
	}								\
	if ((uint64_t) _count != _reqlen) {				\
		(_io_read) += _count;					\
		break;							\
	}								\
}

static rcommon_mgmt_cmd_t *
allocate_rcommon_mgmt_cmd(uint64_t buf_size)
{

	rcommon_mgmt_cmd_t *rcomm_mgmt;
	rcomm_mgmt = (struct rcommon_mgmt_cmd *)malloc(
	    sizeof (struct rcommon_mgmt_cmd));
	pthread_mutex_init(&rcomm_mgmt->mtx, NULL);
	rcomm_mgmt->cmds_sent = 0;
	rcomm_mgmt->cmds_succeeded = 0;
	rcomm_mgmt->cmds_failed = 0;
	rcomm_mgmt->caller_gone = 0;
	rcomm_mgmt->buf_size = buf_size;
	rcomm_mgmt->buf = NULL;
	if (buf_size != 0) {
		rcomm_mgmt->buf = malloc(buf_size);
		memset(rcomm_mgmt->buf, 0, buf_size);
	}
	return (rcomm_mgmt);
}

static void 
free_rcommon_mgmt_cmd(rcommon_mgmt_cmd_t *rcomm_mgmt)
{
	if (rcomm_mgmt->buf != NULL)
		free(rcomm_mgmt->buf);
	free(rcomm_mgmt);
	return;
}

static int
count_of_replicas_helping_rebuild(spec_t *spec, replica_t *healthy_replica)
{

	if (healthy_replica != NULL)
		return (1);
	return ((MAX_OF(spec->replication_factor - spec->consistency_factor + 1,
	    spec->consistency_factor)) - 1);
}

static int
check_header_sanity(zvol_io_hdr_t *resp_hdr)
{

	switch (resp_hdr->opcode) {
		case ZVOL_OPCODE_HANDSHAKE:
		case ZVOL_OPCODE_PREPARE_FOR_REBUILD:
			if (resp_hdr->len != sizeof (mgmt_ack_t)) {
				REPLICA_ERRLOG("hdr->len length(%lu) is not"
				    " matching with size of mgmt_ack_t..\n",
				    resp_hdr->len);
				return -1;
			}
			break;

		case ZVOL_OPCODE_REPLICA_STATUS:
			if(resp_hdr->len != sizeof (zrepl_status_ack_t)) {
				REPLICA_ERRLOG("hdr->len length(%lu) is not "
				    "matching with zrepl_status_ack_t..\n",
				    resp_hdr->len);
				return -1;
			}
			break;
		
		case ZVOL_OPCODE_STATS:
			if((resp_hdr->len % sizeof (zvol_op_stat_t)) != 0) {
				REPLICA_ERRLOG("hdr->len length(%lu) is non "
				    "matching with zvol_op_stat..\n",
				    resp_hdr->len);
				return -1;
			}
			break;

		case ZVOL_OPCODE_SNAP_CREATE:
		case ZVOL_OPCODE_START_REBUILD:
		case ZVOL_OPCODE_SNAP_DESTROY:
			if(resp_hdr->len != 0) {
				REPLICA_ERRLOG("hdr->len length(%lu) is non "
				    "zero, should be zero..\n",
				    resp_hdr->len);
				return -1;
			}
			break;
		default:
			assert(!"Please handle this opcode\n");
			break;
	}
	return 0;
}

static int
enqueue_prepare_for_rebuild(spec_t *spec, replica_t *replica,
    struct rcommon_mgmt_cmd *rcomm_mgmt,
    zvol_op_code_t opcode)
{
	int ret = 0;
	uint64_t num = 1;
	zvol_io_hdr_t *rmgmtio = NULL;
    	mgmt_cmd_t *mgmt_cmd;
	uint64_t data_len = strlen(spec->volname) + 1;

	mgmt_cmd = malloc(sizeof (mgmt_cmd_t));
	memset(mgmt_cmd, 0, sizeof (mgmt_cmd_t));
	BUILD_REPLICA_MGMT_HDR(rmgmtio, opcode, data_len);

	mgmt_cmd->io_hdr = rmgmtio;
	mgmt_cmd->data = (char *)malloc(data_len);
	snprintf((char *)mgmt_cmd->data, data_len, "%s", spec->volname);
	mgmt_cmd->mgmt_cmd_state = WRITE_IO_SEND_HDR;
	mgmt_cmd->rcomm_mgmt = rcomm_mgmt;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_INSERT_TAIL(&replica->mgmt_cmd_queue, mgmt_cmd, mgmt_cmd_next);
	MTX_UNLOCK(&replica->r_mtx);

	rcomm_mgmt->cmds_sent++;

	if (write(replica->mgmt_eventfd1, &num, sizeof (num)) != sizeof (num)) {
		REPLICA_NOTICELOG("Failed to inform to mgmt_eventfd for "
		    "replica(%lu) (%s:%d) mgmt_fd:%d\n", replica->zvol_guid,
		    replica->ip, replica->port, replica->mgmt_fd);
		ret = -1;
		rcomm_mgmt->cmds_failed++;
		/*
		 * Since insertion and processing/deletion happens in same
		 * thread(mgmt_thread), it is safe to remove cmd from queue
		 * in error case. Be cautious when you replicate this code.  
		 */
		MTX_LOCK(&replica->r_mtx);
		clear_mgmt_cmd(replica, mgmt_cmd);
		MTX_UNLOCK(&replica->r_mtx);
	}
	return ret; 
}

/*
 * Case 1: Rebuild from healthy replica
 * Case 2: Mesh rebuild i.e all replicas are downgraded
 */
static int 
send_prepare_for_rebuild_or_trigger_rebuild(spec_t *spec,
    replica_t *dw_replica,
    replica_t *healthy_replica)
{
	int ret = 0;
	int replica_cnt;
	uint64_t size;
	uint64_t data_len;
	mgmt_ack_t *mgmt_data;
	replica_t *replica;
	struct rcommon_mgmt_cmd *rcomm_mgmt;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));

	spec->rebuild_info.dw_replica = dw_replica;
	spec->rebuild_info.healthy_replica = healthy_replica;
	spec->rebuild_info.rebuild_in_progress = true;
	ASSERT(spec->rebuild_info.dw_replica);

	replica_cnt = count_of_replicas_helping_rebuild(spec, healthy_replica);
	assert(replica_cnt || (spec->replication_factor ==  1));

	/*
	 * If replication_factor is 1 i.e. single replica,
	 * trigger rebuild directly from here to change 
	 * state at replica side to make it healthy.
	 */
	if (replica_cnt == 0) {
		assert(spec->ready == true);

		data_len = strlen(spec->volname) + 1;
		mgmt_data = (mgmt_ack_t *)malloc(sizeof (*mgmt_data));
		memset(mgmt_data, 0, sizeof (*mgmt_data));
		snprintf(mgmt_data->dw_volname, data_len, "%s",
		    spec->volname);
		ret = start_rebuild(mgmt_data,
		    spec->rebuild_info.dw_replica, sizeof (*mgmt_data));
		if (ret == -1) {
			spec->rebuild_info.dw_replica = NULL;
			spec->rebuild_info.healthy_replica = NULL;
			spec->rebuild_info.rebuild_in_progress = false;
		}
		return ret;
	}


	size = replica_cnt * sizeof (mgmt_ack_t); 
	rcomm_mgmt = allocate_rcommon_mgmt_cmd(size);

	// Case 1
	if (healthy_replica != NULL) {
		replica = healthy_replica;
		ret = enqueue_prepare_for_rebuild(spec, replica, rcomm_mgmt,
		    ZVOL_OPCODE_PREPARE_FOR_REBUILD);
		if (ret == -1) {
			goto exit;
		}
		replica_cnt--;
		goto exit;
	}

	// Case 2
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if (replica == dw_replica)
			continue;
		
		if (replica_cnt == 0)
			break;
    
		ret = enqueue_prepare_for_rebuild(spec, replica, rcomm_mgmt,
		    ZVOL_OPCODE_PREPARE_FOR_REBUILD);
		if (ret == -1) {
			goto exit;
		}
		replica_cnt--;
	}

exit:
	if (ret == -1) {
		if (rcomm_mgmt->cmds_failed == rcomm_mgmt->cmds_sent) {
			free_rcommon_mgmt_cmd(rcomm_mgmt);
			spec->rebuild_info.dw_replica = NULL;
			spec->rebuild_info.healthy_replica = NULL;
			spec->rebuild_info.rebuild_in_progress = false;
		}
	}
	assert(((ret == -1) && replica_cnt) || (ret == replica_cnt));
	
	return ret;
}

static int
start_rebuild(void *buf, replica_t *replica, uint64_t data_len)
{
	
	int ret = 0;
	uint64_t num = 1;
	zvol_io_hdr_t *rmgmtio = NULL;
	zvol_op_code_t mgmt_opcode = ZVOL_OPCODE_START_REBUILD;
	mgmt_cmd_t *mgmt_cmd;

	ASSERT(MTX_LOCKED(&replica->spec->rq_mtx));

	mgmt_cmd = malloc(sizeof (mgmt_cmd_t));
	memset(mgmt_cmd, 0, sizeof (mgmt_cmd_t));
	BUILD_REPLICA_MGMT_HDR(rmgmtio, mgmt_opcode, data_len);

	mgmt_cmd->io_hdr = rmgmtio;
	mgmt_cmd->data = buf;
	mgmt_cmd->mgmt_cmd_state = WRITE_IO_SEND_HDR;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_INSERT_TAIL(&replica->mgmt_cmd_queue, mgmt_cmd, mgmt_cmd_next);
	MTX_UNLOCK(&replica->r_mtx);

	if (write(replica->mgmt_eventfd1, &num, sizeof (num)) !=
	    sizeof (num)) {
		REPLICA_NOTICELOG("Failed to inform to mgmt_eventfd for "
		    "replica(%lu) (%s:%d) mgmt_fd:%d\n", replica->zvol_guid,
		    replica->ip, replica->port, replica->mgmt_fd);
		MTX_LOCK(&replica->r_mtx);
		clear_mgmt_cmd(replica, mgmt_cmd);
		MTX_UNLOCK(&replica->r_mtx);
		return (ret = -1);
	}
	REPLICA_LOG("start_rebuild opcode sent for Replica(%lu)"
	    "state:%d\n", replica->zvol_guid, replica->state);
	return ret;
}

/* Rebuild can be of two type:-
 * 1. Rebuilding a replica from healthy replica
 * 2. Rebuilding a replica from all other replica(Mesh rebuild)
 * API does the following tasks:-
 * 1. find if there is healthy replica available for rebuilding
 * 2. find downgraded replica for rebuild
 * 3. send prepare_for_rebuild on healthy replica/ other replicas
 * to get rebuild_IP & rebuild_port for rebuilding.
 */
static void
trigger_rebuild(spec_t *spec)
{
	int ret = 0;
	uint64_t max = 0;
	struct timespec now, diff;
	replica_t *replica = NULL;
	replica_t *dw_replica = NULL;
	replica_t *healthy_replica = NULL;
	int non_zero_inflight_replica_found = 0;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));

	if (spec->ready != true) {
		REPLICA_NOTICELOG("Volume(%s) is not ready to accept IOs\n",
		    spec->volname);
		return;
	}

	if (spec->rebuild_info.rebuild_in_progress == true) {
		assert(spec->ready == true);
		REPLICA_NOTICELOG("Rebuild is already in progress "
		    "on volume(%s)\n", spec->volname);
		return;
	}

	if (!spec->degraded_rcount) {
		REPLICA_NOTICELOG("No downgraded replica on volume(%s) "
		", rebuild will not be attempted\n", spec->volname);
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &now);

	/*
	 * istgt starts rebuild on a replica after 2*timeout of its start time.
	 * This is done to make sure that if any pending IOs on helping replicas
	 * are already available at degraded replica before rebuild is started.

	 * optimization is that rebuild will be started on a replica if all the
	 * connected replicas are not having any pending IOs with them.
	 */
	non_zero_inflight_replica_found = 0;
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if (replica->replica_inflight_write_io_cnt != 0 ||
		    replica->replica_inflight_sync_io_cnt != 0) {
			non_zero_inflight_replica_found = 1;
			break;
		}
	}

#ifdef	DEBUG
	const char* non_zero_inflight_replica_str =
	    getenv("non_zero_inflight_replica_cnt");
	unsigned int non_zero_inflight_replica_cnt = 0;

	if (non_zero_inflight_replica_str != NULL)
		non_zero_inflight_replica_cnt =
		    (unsigned int)strtol(non_zero_inflight_replica_str, NULL, 10);
	if (non_zero_inflight_replica_cnt == 1)
		non_zero_inflight_replica_found = 1;
#endif
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		/* Find healthy replica */
		if (replica->state == ZVOL_STATUS_HEALTHY) {
			if (healthy_replica == NULL)
				healthy_replica = replica;
			continue;
		}

		timesdiff(CLOCK_MONOTONIC, replica->create_time, now, diff);

		if ((spec->replication_factor != 1) &&
		    (diff.tv_sec <= (2 * replica_timeout))) {
			if (non_zero_inflight_replica_found == 1) {
				REPLICA_LOG("Replica:%p added very recently, "
				    "skipping rebuild.\n", replica);
				continue;
			}
		}

		if (max <= replica->initial_checkpointed_io_seq) {
			max = replica->initial_checkpointed_io_seq;
			dw_replica = replica;
		}
	}

	if (dw_replica == NULL)
		return;

	REPLICA_LOG("Healthy count(%d) degraded count(%d) consistency factor(%d)"
	    " replication factor(%d)\n", spec->healthy_rcount,
	    spec->degraded_rcount, spec->consistency_factor,
	    spec->replication_factor);

	ret = send_prepare_for_rebuild_or_trigger_rebuild(spec,
	    dw_replica, healthy_replica);
	if (ret == 0) {
		REPLICA_LOG("%s rebuild will be attempted on replica(%lu) "
		    "state:%d\n", (healthy_replica ? "Normal" : "Mesh"),
		    dw_replica->zvol_guid, dw_replica->state);
	} else {
		REPLICA_ERRLOG("Failed to trigger rebuild on replica(%lu)\n",
		    dw_replica->zvol_guid);
	}
}

/*
 * is_replica_newly_connected returns whether the connection is newly created
 * from a familiar replica or newly connected from an unknown replica that
 * target is waiting for.
 * return value :
 *	true  : if the connection is from a familiar replica (which does not
 *		have an active session with the target) or from an unknown
 *		replica for which target is waiting
 *	false : if the connection is from a familiar replica which is already
 *		connected to target or target is not waiting for any new
 *		replica.
 */
static bool
is_replica_newly_connected(spec_t *spec, replica_t *new_replica)
{
	known_replica_t *kr = NULL;
	int familiar_replicas = 0;
	bool newly_connected = false, found = false;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));

	TAILQ_FOREACH(kr, &spec->identified_replica, next) {
		familiar_replicas++;
		if (kr->zvol_guid == new_replica->zvol_guid) {
			found = true;
			if (!kr->is_connected) {
				newly_connected = true;
				kr->is_connected = true;
			}
			break;
		}
	}

	if (!found && familiar_replicas < spec->replication_factor) {
		kr = malloc(sizeof (known_replica_t));
		kr->zvol_guid = new_replica->zvol_guid;
		kr->is_connected = true;
		newly_connected = true;
		TAILQ_INSERT_TAIL(&spec->identified_replica, kr, next);
	}

	/*
	 * If the target has already an active session with this replica
	 * then the target will not allow new session
	 */
	if (found && !newly_connected) {
		REPLICA_ERRLOG("replica(%lu) has already an active session\n",
		    kr->zvol_guid);
		/*
		 * handle_mgmt_conn_error will update identified_replica status
		 * according to replica's zvol_guid. To avoid conflict, unset
		 * new replica's zvol_guid
		 */
		new_replica->zvol_guid = 0;
	}

	return newly_connected;
}

static bool
is_rf_replicas_connected(spec_t *spec, replica_t *replica)
{
	int replica_count;
	bool ret = false;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));

	replica_count = spec->healthy_rcount + spec->degraded_rcount;
	if (replica_count >= spec->replication_factor) {
		REPLICA_ERRLOG("removing replica(%s:%d) since max replica "
		    "connection limit(%d) reached\n", replica->ip,
		    replica->port, spec->replication_factor);
		ret = true;
	}

	return ret;
}

void
update_volstate(spec_t *spec)
{
	uint64_t max = 0;
	replica_t *replica;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));

	if (((spec->healthy_rcount + spec->degraded_rcount >=
	    spec->consistency_factor) && (spec->healthy_rcount >= 1)) ||
	    (spec->healthy_rcount  + spec->degraded_rcount >=
	    MAX_OF(spec->replication_factor - spec->consistency_factor + 1,
	    spec->consistency_factor))) { 
		TAILQ_FOREACH(replica, &spec->rq, r_next) {
			if (max < replica->initial_checkpointed_io_seq) {
				max = replica->initial_checkpointed_io_seq;
			}
		}

		/*
		 * If io_seq is 0, it means either iSCSI controller
		 * is online very first time or boot-back after crash.
		 */
		if (spec->io_seq == 0) {
			max = (max == 0) ? 10 : max + (1<<20);
			spec->io_seq = max;
		}
		spec->ready = true;
		REPLICA_NOTICELOG("volume(%s) is ready for IOs now.. io_seq(%lu) "
		    "healthy_replica(%d) degraded_replica(%d)\n",
		    spec->volname, spec->io_seq, spec->healthy_rcount,
		    spec->degraded_rcount);
	} else {
		spec->ready = false;
		REPLICA_NOTICELOG("Marking volume(%s) not ready for IOs\n", spec->volname);
	}
}

/*
 * perform read/write on fd for 'len' according to state
 * sets 'errorno' if read/write operation returns < 0
 * closes fd if operation returns < 0 && errno != EAGAIN|EWOULDBLOCK|EINTR,
 * and sets fd_closed also closes fd if read return 0, i.e., EOF
 * returns number of bytes read/written
 */
ssize_t
perform_read_write_on_fd(int fd, uint8_t *data, uint64_t len, int state)
{
	int64_t rc = -1;
	ssize_t nbytes = 0;
	int read_cmd = 0;

	ASSERT(len);

	while(1) {
		switch (state) {
			case READ_IO_RESP_HDR:
			case READ_IO_RESP_DATA:
				rc = read(fd, data + nbytes, len - nbytes);
				read_cmd = 1;
				break;

			case WRITE_IO_SEND_HDR:
			case WRITE_IO_SEND_DATA:
				rc = write(fd, data + nbytes, len - nbytes);
				break;

			default:
				REPLICA_ERRLOG("received invalid state(%d)\n", state);
				errno = EINVAL;
				break;
		}

		if(rc < 0) {
			if (errno == EINTR) {
				continue;
			} else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				return nbytes;
			} else {
				REPLICA_ERRLOG("received err(%d) on fd(%d), closing it..\n", errno, fd);
				return -1;
			}
		} else if (rc == 0 && read_cmd) {
			REPLICA_ERRLOG("received EOF on fd(%d), closing it..\n", fd);
			return -1;
		}

		nbytes += rc;
		if((size_t)nbytes == len) {
			break;
		}
	}

	return nbytes;
}

/*
 * get_all_read_resp_data_chunk will create/update io_data_chunk_list
 * with response received from replica according to io_numer.
 */
static void
get_all_read_resp_data_chunk(replica_rcomm_resp_t *resp, size_t block_len,
    struct io_data_chunk_list_t *io_chunk_list)
{
	zvol_io_hdr_t *hdr = &resp->io_resp_hdr;
	struct zvol_io_rw_hdr *io_hdr;
	uint8_t *dataptr = resp->data_ptr;
	uint64_t parsed = 0, data_len;
	io_data_chunk_t  *io_chunk_entry;

	if (!TAILQ_EMPTY(io_chunk_list)) {
		io_chunk_entry = TAILQ_FIRST(io_chunk_list);

		while (parsed < hdr->len) {
			io_hdr = (struct zvol_io_rw_hdr *)dataptr;
			data_len = 0;

			while (data_len < io_hdr->len) {
				if (io_chunk_entry->io_num < io_hdr->io_num) {
					io_chunk_entry->data = dataptr + sizeof(struct zvol_io_rw_hdr) + data_len;
					io_chunk_entry->io_num = io_hdr->io_num;
				}
				data_len += block_len;
				io_chunk_entry = TAILQ_NEXT(io_chunk_entry, io_data_chunk_next);
			}

			dataptr += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
			parsed += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
		}
	} else {
		while (parsed < hdr->len) {
			io_hdr = (struct zvol_io_rw_hdr *)dataptr;
			data_len = 0;

			while (data_len < io_hdr->len) {
				io_chunk_entry = malloc(sizeof(io_data_chunk_t));
				io_chunk_entry->io_num = io_hdr->io_num;
				io_chunk_entry->data = dataptr + sizeof(struct zvol_io_rw_hdr) + data_len;
				TAILQ_INSERT_TAIL(io_chunk_list, io_chunk_entry, io_data_chunk_next);
				data_len += block_len;
			}

			dataptr += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
			parsed += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
		}
	}
}

/*
 * process_chunk_read_resp forms a response data from io_data_chunk_list.
 */
static uint8_t *
process_chunk_read_resp(struct io_data_chunk_list_t  *io_chunk_list,
    uint64_t len, uint64_t block_len)
{
	uint64_t parsed = 0;
	uint8_t *read_data;
	io_data_chunk_t *io_chunk_entry, *io_chunk_next;

	read_data = xmalloc(len);

	io_chunk_entry = TAILQ_FIRST(io_chunk_list);
	while (io_chunk_entry) {
		memcpy(read_data + parsed, io_chunk_entry->data, block_len);
		parsed += block_len;

		io_chunk_next = TAILQ_NEXT(io_chunk_entry, io_data_chunk_next);
		TAILQ_REMOVE(io_chunk_list, io_chunk_entry, io_data_chunk_next);
		free(io_chunk_entry);
		io_chunk_entry = io_chunk_next;
	}
	return read_data;
}

/*
 * get_read_resp_data will for a data-response for read command
 * from a single replica's response
 */
static uint8_t *
get_read_resp_data(replica_rcomm_resp_t *resp, size_t len)
{
	zvol_io_hdr_t *hdr = &resp->io_resp_hdr;
	struct zvol_io_rw_hdr *io_hdr;
	uint8_t *dataptr = resp->data_ptr;
	uint8_t *resp_data, *resp_ptr;
	uint64_t parsed = 0;

	resp_data = xmalloc(len);
	resp_ptr = resp_data;

	while (parsed < hdr->len) {
		io_hdr = (struct zvol_io_rw_hdr *)dataptr;

		memcpy(resp_ptr, dataptr + sizeof(struct zvol_io_rw_hdr), io_hdr->len);
		dataptr += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
		parsed += (sizeof(struct zvol_io_rw_hdr) + io_hdr->len);
		resp_ptr += io_hdr->len;
	}
	return resp_data;
}

/* creates replica entry and adds to spec's rwaitq list after creating mgmt connection */
replica_t *
create_replica_entry(spec_t *spec, int epfd, int mgmt_fd)
{
	replica_t *replica = NULL;
	int rc;

	ASSERT(epfd > 0);
	ASSERT(mgmt_fd > 0);

	replica = (replica_t *)malloc(sizeof(replica_t));
	if (!replica)
		return NULL;

	memset(replica, 0, sizeof(replica_t));
	replica->epfd = epfd;
	replica->mgmt_fd = mgmt_fd;

	TAILQ_INIT(&(replica->mgmt_cmd_queue));

	replica->initial_checkpointed_io_seq = 0;
	replica->mgmt_io_resp_hdr = malloc(sizeof(zvol_io_hdr_t));
	memset(replica->mgmt_io_resp_hdr, 0, sizeof (zvol_io_hdr_t));
	replica->mgmt_io_resp_data = NULL;

	MTX_LOCK(&spec->rq_mtx);
	TAILQ_INSERT_TAIL(&spec->rwaitq, replica, r_waitnext);
	MTX_UNLOCK(&spec->rq_mtx);

	replica->mgmt_eventfd2 = -1;
	replica->iofd = -1;
	replica->spec = spec;

	rc = pthread_mutex_init(&replica->r_mtx, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_mutex_init() failed err(%d) for "
		    "replica(%s:%d)\n", rc, replica->ip, replica->port);
		goto error;
	}

	rc = pthread_cond_init(&replica->r_cond, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_cond_init() failed err(%d) for "
		    "replica(%s:%d)\n", rc, replica->ip, replica->port);
		(void) pthread_mutex_destroy(&replica->r_mtx);
error:
		MTX_LOCK(&spec->rq_mtx);
		TAILQ_REMOVE(&spec->rwaitq, replica, r_waitnext);
		MTX_UNLOCK(&spec->rq_mtx);

		free(replica->mgmt_io_resp_hdr);
		free(replica);
		return NULL;
	}

	return replica;
}

/*
 * update_replica_entry updates replica entry with IP/port,
 * perform handshake on data connection
 * starts replica thread to send/receive IOs to/from replica
 * removes replica from spec's rwaitq and adds it to spec's rq
 * Note: Locks in update_replica_entry are avoided since update_replica_entry is being
 *	 executed once only during handshake with replica.
 */
int
update_replica_entry(spec_t *spec, replica_t *replica, int iofd)
{
	int rc;
	zvol_io_hdr_t *rio_hdr = NULL;
	pthread_t r_thread;
	zvol_io_hdr_t *ack_hdr;
	mgmt_ack_t *ack_data;
	zvol_op_open_data_t *rio_payload = NULL;
	int i;

	ack_hdr = replica->mgmt_io_resp_hdr;	
	ack_data = (mgmt_ack_t *)replica->mgmt_io_resp_data;

	TAILQ_INIT(&replica->waitq);
	TAILQ_INIT(&replica->blockedq);
	TAILQ_INIT(&replica->readyq);

	replica->ongoing_io = NULL;
	replica->ongoing_io_len = 0;
	replica->ongoing_io_buf = NULL;
	replica->iofd = iofd;
	replica->ip = malloc(strlen(ack_data->ip)+1);
	strcpy(replica->ip, ack_data->ip);
	replica->port = ack_data->port;
	replica->state = ZVOL_STATUS_DEGRADED;
	replica->initial_checkpointed_io_seq =
	    MAX_OF(ack_data->checkpointed_io_seq,
	    ack_data->checkpointed_degraded_io_seq);

	replica->pool_guid = ack_data->pool_guid;
	replica->zvol_guid = ack_data->zvol_guid;

	MTX_LOCK(&spec->rq_mtx);
	if (!is_replica_newly_connected(spec, replica)) {
		MTX_UNLOCK(&spec->rq_mtx);
		REPLICA_ERRLOG("replica(ip:%s port:%d "
		    "guid:%lu) is not permitted to connect\n", replica->ip, replica->port,
		    replica->zvol_guid);
		goto replica_error;
	}
	MTX_UNLOCK(&spec->rq_mtx);

	replica->spec = spec;
	replica->io_resp_hdr = (zvol_io_hdr_t *) malloc(sizeof (zvol_io_hdr_t));
	memset(replica->io_resp_hdr, 0, sizeof (zvol_io_hdr_t));
	replica->io_state = READ_IO_RESP_HDR;
	replica->io_read = 0;

	rio_hdr = (zvol_io_hdr_t *) malloc(sizeof (zvol_io_hdr_t));
	memset(rio_hdr, 0, sizeof (zvol_io_hdr_t));
	rio_hdr->opcode = ZVOL_OPCODE_OPEN;
	rio_hdr->io_seq = 0;
	rio_hdr->offset = 0;
	rio_hdr->len = sizeof (zvol_op_open_data_t);
	rio_hdr->version = REPLICA_VERSION;

	rio_payload = (zvol_op_open_data_t *) malloc(
	    sizeof (zvol_op_open_data_t));
	rio_payload->timeout = (3 * replica_timeout);
	rio_payload->tgt_block_size = spec->blocklen;
	strncpy(rio_payload->volname, spec->volname,
	    sizeof (rio_payload->volname));

	REPLICA_LOG("replica(%lu) connected successfully from %s:%d\n",
	    replica->zvol_guid, replica->ip, replica->port);

	if (write(replica->iofd, rio_hdr, sizeof (*rio_hdr)) !=
	    sizeof (*rio_hdr)) {
		REPLICA_ERRLOG("failed to send io hdr to replica(%lu)\n",
		    replica->zvol_guid);
		goto replica_error;
	}

	if (write(replica->iofd, rio_payload, sizeof (zvol_op_open_data_t)) !=
	    sizeof (zvol_op_open_data_t)) {
		REPLICA_ERRLOG("failed to send data-open payload to "
		    "replica(%lu)\n", replica->zvol_guid);
		goto replica_error;
	}

	if (read(replica->iofd, rio_hdr, sizeof (*rio_hdr)) !=
	    sizeof (*rio_hdr)) {
		REPLICA_ERRLOG("failed to read data-open response from "
		    "replica(%lu)\n", replica->zvol_guid);
		goto replica_error;
	}

	if (rio_hdr->status != ZVOL_OP_STATUS_OK) {
		REPLICA_ERRLOG("data-open response is not OK for "
		    "replica(%lu)\n", replica->zvol_guid);
		goto replica_error;
	}

	if (init_mempool(&replica->cmdq, rcmd_mempool_count, 0, 0,
	    "replica_cmd_mempool", NULL, NULL, NULL, false)) {
		REPLICA_ERRLOG("Failed to initialize replica(%lu) cmdq\n",
		    replica->zvol_guid);
		goto replica_error;
	}

	rc = make_socket_non_blocking(iofd);
	if (rc == -1) {
		REPLICA_ERRLOG("make_socket_non_blocking() failed for"
		    " replica(%lu)\n", replica->zvol_guid);
		goto replica_error;
	}

	MTX_LOCK(&spec->rq_mtx);
	if (is_rf_replicas_connected(spec, replica)) {
		MTX_UNLOCK(&spec->rq_mtx);
		REPLICA_ERRLOG("Already %d replicas are connected..."
		    " disconnecting new replica(ip:%s port:%d "
		    "guid:%lu)\n", spec->replication_factor, replica->ip, replica->port,
		    replica->zvol_guid);
		goto replica_error;
	}

	MTX_UNLOCK(&spec->rq_mtx);

	rc = pthread_create(&r_thread, NULL, &replica_thread,
			(void *)replica);
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_create(r_thread) failed for "
		    "replica(%lu)\n", replica->zvol_guid);
replica_error:
		destroy_mempool(&replica->cmdq);
		replica->iofd = -1;
		close(iofd);
		if (rio_hdr)
			free(rio_hdr);
		if (rio_payload)
			free(rio_payload);
		return -1;
	}

	free(rio_hdr);
	free(rio_payload);

	MTX_LOCK(&spec->rq_mtx);
	MTX_LOCK(&replica->r_mtx);
	for (i = 0; (i < 10) && (replica->mgmt_eventfd2 == -1); i++) {
		MTX_UNLOCK(&replica->r_mtx);
		sleep(1);
		MTX_LOCK(&replica->r_mtx);
	}

	if (replica->mgmt_eventfd2 == -1) {
		REPLICA_ERRLOG("unable to set mgmteventfd2 for more than 10 "
		    "seconfs for replica(%lu)\n", replica->zvol_guid);
		replica->dont_free = 1;
		replica->iofd = -1;
		MTX_UNLOCK(&replica->r_mtx);
		MTX_UNLOCK(&spec->rq_mtx);
		shutdown(iofd, SHUT_RDWR);
		close(iofd);
		return -1;
	}
	MTX_UNLOCK(&replica->r_mtx);

	spec->degraded_rcount++;
	TAILQ_REMOVE(&spec->rwaitq, replica, r_waitnext);

	TAILQ_INSERT_TAIL(&spec->rq, replica, r_next);

	/* Update the volume ready state */
	update_volstate(spec);
	MTX_UNLOCK(&spec->rq_mtx);
	clock_gettime(CLOCK_MONOTONIC, &replica->create_time);
	return 0;
}

/*
 * This function sends a handshake query to replica
 */
static int
send_replica_handshake_query(replica_t *replica, spec_t *spec)
{
	zvol_io_hdr_t *rmgmtio = NULL;
	size_t data_len = 0;
	uint8_t *data;
	uint64_t num = 1;
	zvol_op_code_t mgmt_opcode = ZVOL_OPCODE_HANDSHAKE;
	mgmt_cmd_t *mgmt_cmd;
	int ret = 0;

	mgmt_cmd = malloc(sizeof(mgmt_cmd_t));
	memset(mgmt_cmd, 0, sizeof (mgmt_cmd_t));

	data_len = strlen(spec->volname) + 1;

	BUILD_REPLICA_MGMT_HDR(rmgmtio, mgmt_opcode, data_len);

	data = (uint8_t *)malloc(data_len);
	snprintf((char *)data, data_len, "%s", spec->volname);

	mgmt_cmd->io_hdr = rmgmtio;
	mgmt_cmd->data = data;
	mgmt_cmd->mgmt_cmd_state = WRITE_IO_SEND_HDR;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_INSERT_TAIL(&replica->mgmt_cmd_queue, mgmt_cmd, mgmt_cmd_next);
	MTX_UNLOCK(&replica->r_mtx);
	if (write(replica->mgmt_eventfd1, &num, sizeof (num)) != sizeof (num)) {
		REPLICA_NOTICELOG("Failed to inform to mgmt_eventfd for "
		    "replica(%lu) (%s:%d) mgmt_fd:%d\n", replica->zvol_guid,
		    replica->ip, replica->port, replica->mgmt_fd);
		ret = -1;
		MTX_LOCK(&replica->r_mtx);
		clear_mgmt_cmd(replica, mgmt_cmd);
		MTX_UNLOCK(&replica->r_mtx);
	}
	return ret;
}

/*
 * This function handles the response for SNAP_CREATE opcode.
 * In case of timeout, when the thread that triggered snapshot goes away,
 * this function handles deletion of rcommon_mgmt_cmd_t once all the responses
 * are received
 */
static void
handle_snap_create_resp(replica_t *replica, mgmt_cmd_t *mgmt_cmd)
{
	zvol_io_hdr_t *hdr = replica->mgmt_io_resp_hdr;
	replica_t *hr = NULL, *dr = NULL;
	rcommon_mgmt_cmd_t *rcomm_mgmt = mgmt_cmd->rcomm_mgmt;
	bool delete = false;
	if (hdr->status != ZVOL_OP_STATUS_OK) {
		/*
		 * If replica that is healthy and in rebuild process
		 * responds failure for SNAP_CREATE, dw replica that is
		 * involved in rebuild process need to be disconnected.
		 * After reconnection, dwreplica will get this snap
		 * if snap create was successful.
		 * Healthy replica will disconnect by itself, and, gets
		 * the snap after reconnecting as part of its rebuild.
		 */
		MTX_LOCK(&replica->spec->rq_mtx);
		hr = replica->spec->rebuild_info.healthy_replica;
		dr = replica->spec->rebuild_info.dw_replica;
		if (replica == hr)
			inform_mgmt_conn(dr);
		MTX_UNLOCK(&replica->spec->rq_mtx);
	}
	MTX_LOCK(&rcomm_mgmt->mtx);
	if (hdr->status != ZVOL_OP_STATUS_OK)
		rcomm_mgmt->cmds_failed++;
	else
		rcomm_mgmt->cmds_succeeded++;
	if ((rcomm_mgmt->caller_gone == 1) &&
	    (rcomm_mgmt->cmds_sent == (rcomm_mgmt->cmds_failed + rcomm_mgmt->cmds_succeeded)))
		delete = true;
	MTX_UNLOCK(&rcomm_mgmt->mtx);
	if (delete == true)
		free(rcomm_mgmt);
}

static int
send_replica_snapshot(spec_t *spec, replica_t *replica, uint64_t io_seq,
    char *snapname, zvol_op_code_t opcode, rcommon_mgmt_cmd_t *rcomm_mgmt)
{
	zvol_io_hdr_t *rmgmtio = NULL;
	size_t data_len;
	char *data;
	zvol_op_code_t mgmt_opcode = opcode;
	mgmt_cmd_t *mgmt_cmd;
	uint64_t num = 1;
	int ret = 0;

	mgmt_cmd = malloc(sizeof(mgmt_cmd_t));
	memset(mgmt_cmd, 0, sizeof (mgmt_cmd_t));
	mgmt_cmd->rcomm_mgmt = rcomm_mgmt;
	data_len = strlen(spec->volname) + strlen(snapname) + 2;

	BUILD_REPLICA_MGMT_HDR(rmgmtio, mgmt_opcode, data_len);

	data = (char *)malloc(data_len);
	snprintf(data, data_len, "%s@%s", spec->volname, snapname);

	rmgmtio->io_seq = io_seq;

	mgmt_cmd->io_hdr = rmgmtio;
	mgmt_cmd->data = data;
	mgmt_cmd->mgmt_cmd_state = WRITE_IO_SEND_HDR;

	MTX_LOCK(&replica->r_mtx);
	//TODO: Add it as the second IO, rather than tailing
	TAILQ_INSERT_TAIL(&replica->mgmt_cmd_queue, mgmt_cmd, mgmt_cmd_next);
	MTX_UNLOCK(&replica->r_mtx);

	if (rcomm_mgmt != NULL)
		rcomm_mgmt->cmds_sent++;

	if (write(replica->mgmt_eventfd1, &num, sizeof (num)) != sizeof (num)) {
		REPLICA_ERRLOG("Failed to inform to mgmt_eventfd for "
		    "replica(%lu)\n", replica->zvol_guid);
		ret = -1;
	}

	return ret;
}

static void
disconnect_nonresponding_replica(replica_t *replica, uint64_t io_seq,
    zvol_op_code_t opcode)
{
	mgmt_cmd_t *mgmt_cmd;
	replica_t *hr = NULL, *dr = NULL;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_FOREACH(mgmt_cmd, &replica->mgmt_cmd_queue, mgmt_cmd_next) {
		if (mgmt_cmd->io_hdr->io_seq == io_seq &&
		    mgmt_cmd->io_hdr->opcode == opcode) {

			/*
			 * If replica that is healthy and in rebuild process
			 * doesn't respond for SNAP_CREATE, dw replica that is
			 * involved in rebuild process need to be disconnected.
			 * After reconnection, both of them will get this snap
			 * if it is successful
			 */
			hr = replica->spec->rebuild_info.healthy_replica;
			dr = replica->spec->rebuild_info.dw_replica;
			if (replica == hr)
				inform_mgmt_conn(dr);
			inform_mgmt_conn(replica);
			break;
		}
	}

	MTX_UNLOCK(&replica->r_mtx);
}

/*
static bool
any_ongoing_snapshot_command(spec_t *spec)
{
	replica_t *replica;
	mgmt_cmd_t *cmd;
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		MTX_LOCK(&replica->r_mtx);
		TAILQ_FOREACH(cmd, &replica->mgmt_cmd_queue, mgmt_cmd_next) {
			if (cmd->io_hdr->opcode == ZVOL_OPCODE_SNAP_CREATE) {
				MTX_UNLOCK(&replica->r_mtx);
				MTX_UNLOCK(&spec->rq_mtx);
				return true;
			}
		}
		MTX_UNLOCK(&replica->r_mtx);
	}
	MTX_UNLOCK(&spec->rq_mtx);
	return false;
}
*/

static int
can_take_snapshot(spec_t *spec)
{
	if (spec->healthy_rcount < spec->consistency_factor)
		return false;
	return true;
}

/*
 * This function quiesces write IOs, waits for ongoing write IOs
 * If volume is not healthy or timeout happens while waiting for ongoing IOs,
 * write IOs will be allowed, and false will be returned.
 * If there are no pending write IOs and volume is healthy, true will be returned.
 * rq_mtx is required to be held by caller.
 * Returns true when IOs are paused and no ongoing IOs within a given time
 * else returns false.
 */
static bool
pause_and_timed_wait_for_ongoing_ios(spec_t *spec, int sec)
{
	struct timespec last, now, diff;
	bool ret = false;
	bool io_found = false;	/* Write or Sync IOs */
	replica_t *replica;

	ASSERT(MTX_LOCKED(&spec->rq_mtx));
	spec->quiesce = 1;

	clock_gettime(CLOCK_MONOTONIC, &last);
	timesdiff(CLOCK_MONOTONIC, last, now, diff);

	while ((diff.tv_sec < sec) && (can_take_snapshot(spec) == true)) {
		io_found = false;
		if (spec->inflight_write_io_cnt != 0 ||
		    spec->inflight_sync_io_cnt != 0)
			io_found = true;
		else {
			TAILQ_FOREACH(replica, &spec->rq, r_next) {
				if (replica->replica_inflight_write_io_cnt != 0 ||
				    replica->replica_inflight_sync_io_cnt != 0) {
					io_found = true;
					break;
				}
			}
		}
		if (io_found == false) {
			ret = true;
			break;
		}
		MTX_UNLOCK(&spec->rq_mtx);
		/*
		 * inflight write/sync IOs in spec, or in replica,
		 * so, wait for some time
		 */
		sleep (1);
		MTX_LOCK(&spec->rq_mtx);
		timesdiff(CLOCK_MONOTONIC, last, now, diff);
	}

	if (ret == false)
		spec->quiesce = 0;

	return ret;
}

int istgt_lu_destroy_snapshot(spec_t *spec, char *snapname)
{
	replica_t *replica;
	TAILQ_FOREACH(replica, &spec->rq, r_next)
		send_replica_snapshot(spec, replica, 0, snapname, ZVOL_OPCODE_SNAP_DESTROY, NULL);
	return true;
}

/*
 * This API will create snapshot with given name on the spec.
 * It will wait for io_wait_time seconds to complete ongoing IOs.
 * Overall, this API will wait for wait_time seconds to get response
 * for snapshot command (this includes io_wait_time).
 * In case of any failures, snapshot destroy command will be sent to all replicas.
 */
int istgt_lu_create_snapshot(spec_t *spec, char *snapname, int io_wait_time, int wait_time)
{
	bool r;
	replica_t *replica;
	int free_rcomm_mgmt = 0;
	struct timespec last, now, diff;
	rcommon_mgmt_cmd_t *rcomm_mgmt;
	uint64_t io_seq;

	clock_gettime(CLOCK_MONOTONIC, &last);
	MTX_LOCK(&spec->rq_mtx);

	/* Wait for any ongoing snapshot commands */
	while (spec->quiesce == 1) {
		MTX_UNLOCK(&spec->rq_mtx);
		sleep(1);
		MTX_LOCK(&spec->rq_mtx);
	}

	if (can_take_snapshot(spec) == false) {
		MTX_UNLOCK(&spec->rq_mtx);
		REPLICA_ERRLOG("volume is not healthy..\n");
		return false;
	}

	r = pause_and_timed_wait_for_ongoing_ios(spec, io_wait_time);
	if (r == false) {
		MTX_UNLOCK(&spec->rq_mtx);
		REPLICA_ERRLOG("pausing failed..\n");
		return false;
	}

	rcomm_mgmt = allocate_rcommon_mgmt_cmd(0);
	free_rcomm_mgmt = 0;

	r = false;
	io_seq = ++spec->io_seq;
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		(void) send_replica_snapshot(spec, replica, io_seq, snapname, ZVOL_OPCODE_SNAP_CREATE, rcomm_mgmt);
	}

	uint8_t cf = spec->consistency_factor;

	timesdiff(CLOCK_MONOTONIC, last, now, diff);
	MTX_LOCK(&rcomm_mgmt->mtx);

	while (diff.tv_sec < wait_time) {
		if (rcomm_mgmt->cmds_sent == (rcomm_mgmt->cmds_succeeded + rcomm_mgmt->cmds_failed))
			break;
		MTX_UNLOCK(&rcomm_mgmt->mtx);
		MTX_UNLOCK(&spec->rq_mtx);
		sleep(1);
		MTX_LOCK(&spec->rq_mtx);
		MTX_LOCK(&rcomm_mgmt->mtx);
		timesdiff(CLOCK_MONOTONIC, last, now, diff);
	}
	rcomm_mgmt->caller_gone = 1;
	if (rcomm_mgmt->cmds_sent == (rcomm_mgmt->cmds_succeeded + rcomm_mgmt->cmds_failed)) {
		free_rcomm_mgmt = 1;
	}
	if (rcomm_mgmt->cmds_succeeded >= cf) {
		r = true;
	}
	MTX_UNLOCK(&rcomm_mgmt->mtx);

	if (r == false) {
		TAILQ_FOREACH(replica, &spec->rq, r_next)
			send_replica_snapshot(spec, replica, io_seq, snapname, ZVOL_OPCODE_SNAP_DESTROY, NULL);
	} else {
		/*
		 * disconnect the replica from which we have
		 * not received the response yet. As a part of the
		 * reconnecting, it will start the rebuild process
		 * and resync the snapshot.
		 * Snapshot failure will be handled by zrepl.
		 */
		TAILQ_FOREACH(replica, &spec->rq, r_next)
			disconnect_nonresponding_replica(replica, io_seq,
			    ZVOL_OPCODE_SNAP_CREATE);
	}
	spec->quiesce = 0;
	MTX_UNLOCK(&spec->rq_mtx);
	if (r == false)
		REPLICA_ERRLOG("snap create ioseq: %lu resp: %d\n", io_seq, r);
	if (free_rcomm_mgmt == 1)
		free(rcomm_mgmt);
	return r;
}

static void
get_replica_stats_json(replica_t *replica, struct json_object **jobj)
{
	struct json_object *j_stats;
	struct timespec now;

	j_stats = json_object_new_object();
	json_object_object_add(j_stats, "replicaId",
	    json_object_new_uint64(replica->zvol_guid));

	json_object_object_add(j_stats, "status",
	    json_object_new_string((replica->state == ZVOL_STATUS_HEALTHY) ?
	    REPLICA_STATUS_HEALTHY : REPLICA_STATUS_DEGRADED));

	json_object_object_add(j_stats, "checkpointedIOSeq",
	    json_object_new_uint64(replica->initial_checkpointed_io_seq));

	json_object_object_add(j_stats, "inflightRead",
	    json_object_new_uint64(replica->replica_inflight_read_io_cnt));

	json_object_object_add(j_stats, "inflightWrite",
	    json_object_new_uint64(replica->replica_inflight_write_io_cnt));

	json_object_object_add(j_stats, "inflightSync",
	    json_object_new_uint64(replica->replica_inflight_sync_io_cnt));

	clock_gettime(CLOCK_MONOTONIC, &now);
	json_object_object_add(j_stats, "upTime",
	    json_object_new_int64(now.tv_sec - replica->create_time.tv_sec));

	*jobj = j_stats;
}

const char *
get_cv_status(spec_t *spec, int replica_cnt, int healthy_replica_cnt)
{
	if (spec->ready == false)
		return VOL_STATUS_OFFLINE;
	if (healthy_replica_cnt >= spec->consistency_factor)
		return VOL_STATUS_HEALTHY;
	return VOL_STATUS_DEGRADED;
}

void
istgt_lu_replica_stats(char *volname, char **resp)
{
	replica_t *replica;
	spec_t *spec = NULL;
	struct json_object *j_all_spec, *j_replica, *j_spec, *j_obj;
	const char *json_string = NULL;
	uint64_t resp_len = 0;
	int replica_cnt = 0, healthy_replica_cnt = 0;
	const char *status;

	j_all_spec = json_object_new_array();

	MTX_LOCK(&specq_mtx);

	TAILQ_FOREACH(spec, &spec_q, spec_next) {
		MTX_LOCK(&spec->rq_mtx);
		if (volname) {
			if(!strncmp(spec->volname, volname, strlen(volname))) {
				j_spec = json_object_new_object();
				j_replica = json_object_new_array();

				replica_cnt = 0;
				healthy_replica_cnt = 0;
				TAILQ_FOREACH(replica, &spec->rq, r_next) {
					MTX_LOCK(&replica->r_mtx);
					get_replica_stats_json(replica, &j_obj);
					MTX_UNLOCK(&replica->r_mtx);
					replica_cnt++;
	    				if (replica->state == ZVOL_STATUS_HEALTHY)
						healthy_replica_cnt++;
					json_object_array_add(j_replica, j_obj);
				}
				json_object_object_add(j_spec,
				    "name", json_object_new_string(spec->volname));
				status = get_cv_status(spec, replica_cnt, healthy_replica_cnt);
				MTX_UNLOCK(&spec->rq_mtx);
				json_object_object_add(j_spec, "status",
				    json_object_new_string(status));
				json_object_object_add(j_spec,
				    "replicaStatus", j_replica);
				json_object_array_add(j_all_spec, j_spec);
				break;
			}
			MTX_UNLOCK(&spec->rq_mtx);
		} else {
			j_spec = json_object_new_object();
			j_replica = json_object_new_array();

			replica_cnt = 0;
			healthy_replica_cnt = 0;
			TAILQ_FOREACH(replica, &spec->rq, r_next) {
				MTX_LOCK(&replica->r_mtx);
				get_replica_stats_json(replica, &j_obj);
				MTX_UNLOCK(&replica->r_mtx);
				replica_cnt++;
	    			if (replica->state == ZVOL_STATUS_HEALTHY)
					healthy_replica_cnt++;
				json_object_array_add(j_replica, j_obj);
			}

			json_object_object_add(j_spec,
			    "name", json_object_new_string(spec->volname));
			status = get_cv_status(spec, replica_cnt, healthy_replica_cnt);
			MTX_UNLOCK(&spec->rq_mtx);
			json_object_object_add(j_spec, "status",
			    json_object_new_string(status));
			json_object_object_add(j_spec,
			    "replicaStatus", j_replica);
			json_object_array_add(j_all_spec, j_spec);
		}
	}

	MTX_UNLOCK(&specq_mtx);

	j_obj = json_object_new_object();
	json_object_object_add(j_obj, "volumeStatus", j_all_spec);
	json_string = json_object_to_json_string_ext(j_obj,
	    JSON_C_TO_STRING_PLAIN);
	resp_len = strlen(json_string) + 1;
	*resp = malloc(resp_len);
	memset(*resp, 0, resp_len);
	strncpy(*resp, json_string, resp_len);
	json_object_put(j_obj);
}

/*
 * This function sends status query for a volume to replica
 */
static int
send_replica_query(replica_t *replica, spec_t *spec, zvol_op_code_t opcode)
{
	zvol_io_hdr_t *rmgmtio = NULL;
	size_t data_len;
	char *data;
	zvol_op_code_t mgmt_opcode = opcode;
	mgmt_cmd_t *mgmt_cmd;

	mgmt_cmd = malloc(sizeof(mgmt_cmd_t));
	memset(mgmt_cmd, 0, sizeof (mgmt_cmd_t));
	data_len = strlen(spec->volname) + 1;
	BUILD_REPLICA_MGMT_HDR(rmgmtio, mgmt_opcode, data_len);

	data = (char *)malloc(data_len);
	snprintf(data, data_len, "%s", spec->volname);

	mgmt_cmd->io_hdr = rmgmtio;
	mgmt_cmd->data = data;
	mgmt_cmd->mgmt_cmd_state = WRITE_IO_SEND_HDR;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_INSERT_TAIL(&replica->mgmt_cmd_queue, mgmt_cmd, mgmt_cmd_next);
	MTX_UNLOCK(&replica->r_mtx);

	return handle_write_data_event(replica);
}

/*
 * get_replica_stats will send stats query to any healthy replica
 */
static void
get_replica_stats(spec_t *spec)
{
	int ret;
	replica_t *replica;

again:
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if (replica->state != ZVOL_STATUS_HEALTHY)
			continue;

		ret = send_replica_query(replica, spec, ZVOL_OPCODE_STATS);
		if (ret == -1) {
			REPLICA_ERRLOG("Failed to send stats "
			    "on replica(%lu) ..\n", replica->zvol_guid);
			MTX_UNLOCK(&spec->rq_mtx);
			handle_mgmt_conn_error(replica, 0, NULL, 0);
			goto again;
		} else
			break;
	}
	MTX_UNLOCK(&spec->rq_mtx);
}

/*
 * ask_replica_status will send replica_status query to all degraded replica
 */
static void
ask_replica_status_all(spec_t *spec)
{
	int ret;
	replica_t *replica;

again:
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if (replica->state == ZVOL_STATUS_HEALTHY)
			continue;

		ret = send_replica_query(replica, spec, ZVOL_OPCODE_REPLICA_STATUS);
		if (ret == -1) {
			REPLICA_ERRLOG("Failed to send mgmtIO for querying "
			    "status on replica(%lu) ..\n", replica->zvol_guid);
			MTX_UNLOCK(&spec->rq_mtx);
			handle_mgmt_conn_error(replica, 0, NULL, 0);
			goto again;
		}
	}
	MTX_UNLOCK(&spec->rq_mtx);
}

static void
handle_start_rebuild_resp(spec_t *spec, zvol_io_hdr_t *hdr)
{

	if (hdr->status == ZVOL_OP_STATUS_OK)
		return;

	MTX_LOCK(&spec->rq_mtx);
	spec->rebuild_info.dw_replica = NULL;
	spec->rebuild_info.healthy_replica = NULL;
	spec->rebuild_info.rebuild_in_progress = false;
	MTX_UNLOCK(&spec->rq_mtx);
}

static void
handle_prepare_for_rebuild_resp(spec_t *spec, zvol_io_hdr_t *hdr,
    mgmt_ack_t *ack_data, mgmt_cmd_t *mgmt_cmd)
{

	int ret = 0;
	size_t data_len;
	rcommon_mgmt_cmd_t *rcomm_mgmt = mgmt_cmd->rcomm_mgmt;
	mgmt_ack_t *buf = (mgmt_ack_t *)rcomm_mgmt->buf;
	
	if (hdr->status != ZVOL_OP_STATUS_OK) {
		rcomm_mgmt->cmds_failed++;
	} else {
		data_len = strlen(spec->volname) + 1;
		snprintf((char *)ack_data->dw_volname, data_len, "%s",
		    spec->volname);
		memcpy(&buf[rcomm_mgmt->cmds_succeeded++], ack_data,
		    sizeof (mgmt_ack_t));
	}

	if (rcomm_mgmt->cmds_sent == rcomm_mgmt->cmds_succeeded) {
		MTX_LOCK(&spec->rq_mtx);
		replica_t *dw_replica = spec->rebuild_info.dw_replica;
		if (dw_replica) {
			ret = start_rebuild(buf, dw_replica,
			    rcomm_mgmt->buf_size);
			rcomm_mgmt->buf = NULL;
			if (ret == 0) {
				REPLICA_LOG("Rebuild triggered on Replica(%lu) "
				    "state:%d\n", dw_replica->zvol_guid,
				    dw_replica->state);
			} else {
				REPLICA_LOG("Unable to start rebuild on Replica(%lu)"
				    " state:%d\n", dw_replica->zvol_guid,
				    dw_replica->state);
				spec->rebuild_info.dw_replica = NULL;
				spec->rebuild_info.healthy_replica = NULL;
				spec->rebuild_info.rebuild_in_progress = false;
			}
		} else {
			ASSERT(spec->rebuild_info.rebuild_in_progress == false);
			ASSERT(spec->rebuild_info.healthy_replica == NULL);
		}
		MTX_UNLOCK(&spec->rq_mtx);
		free_rcommon_mgmt_cmd(rcomm_mgmt);
		mgmt_cmd->rcomm_mgmt = NULL;
	} else if (rcomm_mgmt->cmds_sent ==
	    (rcomm_mgmt->cmds_failed + rcomm_mgmt->cmds_succeeded)) {
		free_rcommon_mgmt_cmd(rcomm_mgmt);
		mgmt_cmd->rcomm_mgmt = NULL;
		MTX_LOCK(&spec->rq_mtx);
		if (spec->rebuild_info.dw_replica)
			REPLICA_LOG("Unable to prepare rebuild for Replica(%lu) "
			    "state:%d\n",
			    spec->rebuild_info.dw_replica->zvol_guid,
			    spec->rebuild_info.dw_replica->state);
		spec->rebuild_info.dw_replica = NULL;
		spec->rebuild_info.healthy_replica = NULL;
		spec->rebuild_info.rebuild_in_progress = false;
		MTX_UNLOCK(&spec->rq_mtx);
	}
}

/*
 * Handler for stats opcode response from replica
 */
static void
handle_update_spec_stats(spec_t *spec, zvol_io_hdr_t *hdr, void *resp)
{
	zvol_op_stat_t *stats = (zvol_op_stat_t *)resp;
	if (hdr->status != ZVOL_OP_STATUS_OK)
		return;
	if (strcmp(stats->label, "used") == 0)
		spec->stats.used = stats->value;
	clock_gettime(CLOCK_MONOTONIC, &spec->stats.updated_stats_time);
	return;
}

static int
update_replica_status(spec_t *spec, replica_t *replica)
{
	zrepl_status_ack_t *repl_status;
	replica_state_t last_state;

	repl_status = (zrepl_status_ack_t *)replica->mgmt_io_resp_data;

	REPLICA_ERRLOG("Replica(%lu) state:%d rebuild status:%d\n",
	    replica->zvol_guid, repl_status->state,
	    repl_status->rebuild_status);

	MTX_LOCK(&spec->rq_mtx);
	MTX_LOCK(&replica->r_mtx);

	last_state = replica->state;
	replica->state = (replica_state_t) repl_status->state;
	MTX_UNLOCK(&replica->r_mtx);

	if(last_state != repl_status->state) {
		REPLICA_NOTICELOG("Replica(%lu) (%s:%d) mgmt_fd:%d state "
		    "changed from %s to %s\n", replica->zvol_guid, replica->ip,
		    replica->port, replica->mgmt_fd,
		    (last_state == ZVOL_STATUS_HEALTHY) ? "healthy" :
		    "degraded",
		    (repl_status->state == ZVOL_STATUS_HEALTHY) ? "healthy" :
		    "degraded");
		if (repl_status->state == ZVOL_STATUS_DEGRADED) {
			spec->degraded_rcount++;
			spec->healthy_rcount--;
		} else if (repl_status->state == ZVOL_STATUS_HEALTHY) {
			/* master_replica became healthy*/
			spec->degraded_rcount--;
			spec->healthy_rcount++;
			assert(spec->rebuild_info.dw_replica == replica);
			spec->rebuild_info.dw_replica = NULL;
			spec->rebuild_info.healthy_replica = NULL;
			spec->rebuild_info.rebuild_in_progress = false;
			REPLICA_ERRLOG("Replica(%lu) marked healthy,"
		    	    " seting master_replica to NULL\n",
			    replica->zvol_guid);
		}
	} else if ((repl_status->state == ZVOL_STATUS_DEGRADED) &&
	    (repl_status->rebuild_status == ZVOL_REBUILDING_FAILED) &&
	    (replica == spec->rebuild_info.dw_replica)) {
		/*
		 * If last rebuild failed on master_replica
		 * then trigger another one
		 */
		spec->rebuild_info.rebuild_in_progress = false;
		spec->rebuild_info.dw_replica = NULL;
		spec->rebuild_info.healthy_replica = NULL;
	}

	/*Trigger rebuild if possible */
	trigger_rebuild(spec);
	MTX_UNLOCK(&spec->rq_mtx);
	return 0;
}

/*
 * forms data connection to replica, updates replica entry
 */
int
zvol_handshake(spec_t *spec, replica_t *replica)
{
	int rc, iofd;
	zvol_io_hdr_t *ack_hdr;
	mgmt_ack_t *ack_data;

	ack_hdr = replica->mgmt_io_resp_hdr;	
	ack_data = (mgmt_ack_t *)replica->mgmt_io_resp_data;

	if (ack_hdr->status != ZVOL_OP_STATUS_OK) {
		REPLICA_ERRLOG("mgmt_ack status is not ok.. for "
		    "replica(%s:%d)\n", replica->ip, replica->port);
		return -1;
	}

	if(strcmp(ack_data->volname, spec->volname) != 0) {
		REPLICA_ERRLOG("volname(%s) not matching with spec(%s) volname"
		    " for replica(%s:%d)\n", ack_data->volname,
		    spec->volname, replica->ip, replica->port);
		return -1;
	}

	if((iofd = cstor_ops.conn_connect(ack_data->ip, ack_data->port)) < 0) {
		REPLICA_ERRLOG("Failed to open data connection for replica"
		    "(%s:%d)\n", replica->ip, replica->port);
		return -1;
	}

	rc = update_replica_entry(spec, replica, iofd);

	return rc;
}

/*
 * accepts (mgmt) connections on which handshake and other management IOs are sent
 * sends handshake IO to start handshake on accepted (mgmt) connection
 */
void
accept_mgmt_conns(int epfd, int sfd)
{
	struct epoll_event event;
	int rc, rcount=0;
	spec_t *spec = NULL;
	int mgmt_fd;
	mgmt_event_t *mevent1, *mevent2;

	while (1) {
		struct sockaddr saddr;
		socklen_t slen;
		char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
		replica_t *replica = NULL;

		slen = sizeof(saddr);
		mgmt_fd = accept(sfd, &saddr, &slen);
		if (mgmt_fd == -1) {
			if((errno != EAGAIN) && (errno != EWOULDBLOCK))
				REPLICA_ERRLOG("Failed to accept connection on"
				    " fd(%d) err(%d)\n",
				    sfd, errno);
			break;
		}

		rc = getnameinfo(&saddr, slen,
				hbuf, sizeof(hbuf),
				sbuf, sizeof(sbuf),
				NI_NUMERICHOST | NI_NUMERICSERV);
		if (rc == 0) {
			rcount++;
			REPLICA_LOG("Accepted connection on descriptor(%d) "
			    "(host=%s port=%s)\n", mgmt_fd, hbuf, sbuf);
		}
		rc = make_socket_non_blocking(mgmt_fd);
		if (rc == -1) {
			REPLICA_ERRLOG("make_socket_non_blocking() failed on "
			    "fd(%d), closing it..", mgmt_fd);
			close(mgmt_fd);
			continue;
		}

		rc = set_socket_keepalive(mgmt_fd);
		if (rc) {
			REPLICA_ERRLOG("Failed to set keepalive for fd(%d)\n", mgmt_fd);
			close(mgmt_fd);
			continue;
		}

                MTX_LOCK(&specq_mtx);
                TAILQ_FOREACH(spec, &spec_q, spec_next) {
			// Since we are supporting single spec per controller
			// we will continue using first spec only	
                        break;
                }
                MTX_UNLOCK(&specq_mtx);

		if (!spec) {
			REPLICA_ERRLOG("Spec is not configured\n");
			shutdown(mgmt_fd, SHUT_RDWR);
			close(mgmt_fd);
			continue;
		}

		/*
		 * As of now, we are supporting single spec_t per target
		 * So, we can assign spec to replica here.
		 * TODO: In case of multiple spec, asignment of spec to replica
		 * 	 should be handled in update_replica_entry func according to
		 * 	 volume name provided by replica.
		 */
		replica = create_replica_entry(spec, epfd, mgmt_fd);
		if (!replica) {
			REPLICA_ERRLOG("Failed to create replica for fd(%d) "
			    "closing it..", mgmt_fd);
			shutdown(mgmt_fd, SHUT_RDWR);
			close(mgmt_fd);
			continue;
		}

		mevent1 = (mgmt_event_t *)malloc(sizeof(mgmt_event_t));
		mevent2 = (mgmt_event_t *)malloc(sizeof(mgmt_event_t));

		replica->mgmt_eventfd1 = eventfd(0, EFD_NONBLOCK);
		if (replica->mgmt_eventfd1 < 0) {
			REPLICA_ERRLOG("error for replica(%s:%d) "
			    "mgmt_eventfd(%d) err(%d)\n", replica->ip,
			    replica->port, replica->mgmt_eventfd1, errno);
			goto cleanup;
		}

		mevent1->fd = replica->mgmt_eventfd1;
		mevent1->r_ptr = replica;
		event.data.ptr = mevent1;
		event.events = EPOLLIN;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, replica->mgmt_eventfd1, &event);
		if(rc == -1) {
			REPLICA_ERRLOG("epoll_ctl() failed on fd(%d), "
			    "err(%d).. closing it.. for replica(%s:%d)\n",
			    mgmt_fd, errno, replica->ip, replica->port);
			goto cleanup;
		}

		mevent2->fd = mgmt_fd;
		mevent2->r_ptr = replica;
		event.data.ptr = mevent2;
		event.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLET | EPOLLOUT | EPOLLRDHUP;

#ifdef	DEBUG
		replica->replica_mgmt_dport = atoi(sbuf);
#endif

		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmt_fd, &event);
		if(rc == -1) {
			REPLICA_ERRLOG("epoll_ctl() failed on fd(%d), "
			    "err(%d).. closing it.. for replica(%s:%d)\n",
			    mgmt_fd, errno, replica->ip, replica->port);
cleanup:
			if (replica->mgmt_eventfd1 != -1) {
				(void) epoll_ctl(epfd, EPOLL_CTL_DEL, replica->mgmt_eventfd1, NULL);
				close(replica->mgmt_eventfd1);
			}

			if (mevent1) {
				free(mevent1);
				mevent1 = NULL;
			}

			if (mevent2) {
				free(mevent2);
				mevent2 = NULL;
			}

			if (replica) {
				pthread_mutex_destroy(&replica->r_mtx);
				pthread_cond_destroy(&replica->r_cond);
				MTX_LOCK(&spec->rq_mtx);
				TAILQ_REMOVE(&spec->rwaitq, replica, r_waitnext);
				MTX_UNLOCK(&spec->rq_mtx);
				free(replica);
			}
			shutdown(mgmt_fd, SHUT_RDWR);
			close(mgmt_fd);
			continue;
		}
		replica->m_event1 = mevent1;
		replica->m_event2 = mevent2;

		send_replica_handshake_query(replica, spec);

		mevent1 = NULL;
		mevent2 = NULL;
	}
}

/*
 * write_io_data will write IOs to mgmt connection only..
 * initial state is write_io_send_hdr, which will write header.
 * it transitions to write_io_send_data based on length in hdr.
 * once data is written, it will change io_state to READ_IO_RESP_HDR
 * to read data from replica.
 */
static int
write_io_data(replica_t *replica, io_event_t *wevent)
{
	int fd = wevent->fd;
	int *state = wevent->state;
	zvol_io_hdr_t *write_hdr = wevent->io_hdr;
	void **data = wevent->io_data;
	int *write_count = wevent->byte_count;
	uint64_t reqlen;
	ssize_t count;
	int donecount = 0;
	(void)replica;

	switch(*state) {
		case WRITE_IO_SEND_HDR:
			reqlen = sizeof (zvol_io_hdr_t) - (*write_count);
			ASSERT(reqlen != 0 && reqlen <= sizeof(zvol_io_hdr_t));
			count = perform_read_write_on_fd(fd,
			    ((uint8_t *)write_hdr) + (*write_count), reqlen, *state);
			CHECK_AND_ADD_BREAK_IF_PARTIAL((*write_count), count, reqlen, donecount);

			*write_count = 0;
			*state = WRITE_IO_SEND_DATA;

		case WRITE_IO_SEND_DATA:
			reqlen = write_hdr->len - (*write_count);
			if (reqlen != 0) {
				count = perform_read_write_on_fd(fd,
				    ((uint8_t *)(*data)) + (*write_count), reqlen, *state);
				CHECK_AND_ADD_BREAK_IF_PARTIAL((*write_count), count, reqlen, donecount);
			}
			free(*data);
			*data = NULL;
			*write_count = 0;
			donecount++;
			*state = READ_IO_RESP_HDR;
			break;
		default:
			REPLICA_ERRLOG("got invalid write state(%d) for "
			    "replica(%lu).. aborting..\n", *state,
			    replica->zvol_guid);
                        abort();
			break;
	}
	return donecount;
}


/*
 * initial state is read io_resp_hdr, which reads IO response.
 * it transitions to read io_resp_data based on length in hdr.
 * once data is handled, it goes to read hdr which can be new response.
 * this goes on until EAGAIN or connection gets closed.
 */
static int
read_io_resp(spec_t *spec, replica_t *replica, io_event_t *revent, mgmt_cmd_t *mgmt_cmd)
{
	int fd = revent->fd;
	int *state = revent->state;
	zvol_io_hdr_t *resp_hdr = revent->io_hdr;
	void **resp_data = revent->io_data;
	int *read_count = revent->byte_count;
	uint64_t reqlen;
	ssize_t count;
	int rc = 0;
	int donecount = 0;

	switch(*state) {
		case READ_IO_RESP_HDR:
read_io_resp_hdr:
			reqlen = sizeof (zvol_io_hdr_t) - (*read_count);
			ASSERT(reqlen != 0 && reqlen <= sizeof (zvol_io_hdr_t));
			count = perform_read_write_on_fd(fd,
			    ((uint8_t *)resp_hdr) + (*read_count), reqlen, *state);
			CHECK_AND_ADD_BREAK_IF_PARTIAL((*read_count), count, reqlen, donecount);

			*read_count = 0;
			if (check_header_sanity(resp_hdr) != 0) {
				return -1;
			}
			if (resp_hdr->opcode == ZVOL_OPCODE_WRITE)
				resp_hdr->len = 0;
			if (resp_hdr->len != 0) {
				(*resp_data) = malloc(resp_hdr->len);
				memset(*resp_data, 0, resp_hdr->len);
			}
			*state = READ_IO_RESP_DATA;

		case READ_IO_RESP_DATA:
			reqlen = resp_hdr->len - (*read_count);
			if (reqlen != 0) {
				count = perform_read_write_on_fd(fd,
				    ((uint8_t *)(*resp_data)) + (*read_count), reqlen, *state);
				CHECK_AND_ADD_BREAK_IF_PARTIAL((*read_count), count, reqlen, donecount);
			}

			*read_count = 0;

			switch (resp_hdr->opcode) {
				case ZVOL_OPCODE_HANDSHAKE:
					VERIFY3U(resp_hdr->len, ==, sizeof (mgmt_ack_t));
					/* dont process handshake on data connection */
					ASSERT(fd != replica->iofd);

					rc = zvol_handshake(spec, replica);

					memset(resp_hdr, 0, sizeof(zvol_io_hdr_t));
					free(*resp_data);
					mgmt_cmd->cmd_completed = 1;

					if (rc == -1)
						return (rc);
					break;

				case ZVOL_OPCODE_REPLICA_STATUS:
					VERIFY3U(resp_hdr->len, ==, sizeof (zrepl_status_ack_t));
					/* replica status must come from mgmt connection */
					ASSERT(fd != replica->iofd);

					update_replica_status(spec, replica);
					free(*resp_data);
					break;

				case ZVOL_OPCODE_STATS:
					handle_update_spec_stats(spec, resp_hdr, *resp_data);
					free(*resp_data);
					break;

				case ZVOL_OPCODE_PREPARE_FOR_REBUILD:
				
					/* replica status must come from mgmt connection */
					assert(fd != replica->iofd);
					handle_prepare_for_rebuild_resp(spec, resp_hdr, *resp_data, mgmt_cmd);
					free(*resp_data);
					break;

				case ZVOL_OPCODE_SNAP_CREATE:
					/*
					 * snap create response must come from
					 * mgmt connection
					 */
					assert(fd != replica->iofd);
					handle_snap_create_resp(replica, mgmt_cmd);
					break;

				case ZVOL_OPCODE_START_REBUILD:
					assert(fd != replica->iofd);
					handle_start_rebuild_resp(spec, resp_hdr);
					break;

				case ZVOL_OPCODE_SNAP_DESTROY:
					break;

				default:
					REPLICA_ERRLOG("unsupported opcode"
					    "(%d) received for replica(%lu)\n",
					    resp_hdr->opcode,
					    replica->zvol_guid);
					break;
			}
			*resp_data = NULL;
			*read_count = 0;
			donecount++;
			mgmt_cmd->cmd_completed = 1;
			*state = READ_IO_RESP_HDR;
			/*
			 * Try to read data from replica again if replica is
			 * sending more or multiple reponses for the same command.
			 */
			goto read_io_resp_hdr;
			break;
		default:
			REPLICA_ERRLOG("got invalid read state(%d) for "
			    "replica(%lu).. aborting..\n", *state,
			    replica->zvol_guid);
			abort();
			break;
	}

	return donecount;
}

/*
 * This function send a command to replica from replica's
 * management command queue.
 */
int
handle_write_data_event(replica_t *replica)
{
	io_event_t wevent;
	int rc = 0;
	mgmt_cmd_t *mgmt_cmd = NULL;

	MTX_LOCK(&replica->r_mtx);
	mgmt_cmd = TAILQ_FIRST(&replica->mgmt_cmd_queue);
	if (!mgmt_cmd) {
		rc = 0;
		MTX_UNLOCK(&replica->r_mtx);
		return rc;
	}

	if (mgmt_cmd->mgmt_cmd_state != WRITE_IO_SEND_HDR &&
		mgmt_cmd->mgmt_cmd_state != WRITE_IO_SEND_DATA) {
		MTX_UNLOCK(&replica->r_mtx);
		REPLICA_DEBUGLOG("write IO is in wait state on mgmt "
		    "connection.. for replica(%lu)\n", replica->zvol_guid);
		return rc;
	}

	MTX_UNLOCK(&replica->r_mtx);

	wevent.fd = replica->mgmt_fd;
	wevent.state = &(mgmt_cmd->mgmt_cmd_state);
	wevent.io_hdr = mgmt_cmd->io_hdr;
	wevent.io_data = (void **)(&(mgmt_cmd->data));
	wevent.byte_count = &(mgmt_cmd->io_bytes);

	rc = write_io_data(replica, &wevent);
	return rc;
}

/*
 * This function will inform replica_thread(data_connection)
 * regarding error in replica's management connection.
 */
static void
inform_data_conn(replica_t *r)
{
	uint64_t num = 1;
	r->disconnect_conn = 1;
	if (write(r->mgmt_eventfd2, &num, sizeof (num)) != sizeof (num))
		REPLICA_NOTICELOG("Failed to inform err to data_conn for "
		    "replica(%lu) (%s:%d) mgmt_fd:%d\n", r->zvol_guid, r->ip,
		    r->port, r->mgmt_fd);
}

/*
 * This function will cleanup replica structure
 */
static void
free_replica(replica_t *r)
{
	pthread_mutex_destroy(&r->r_mtx);
	pthread_cond_destroy(&r->r_cond);

#ifdef	DEBUG
	if (r->cmdq.ring)
		ASSERT0(get_num_entries_from_mempool(&r->cmdq));
#endif
	destroy_mempool(&r->cmdq);

	free(r->mgmt_io_resp_hdr);
	free(r->m_event1);
	free(r->m_event2);

	if (r->ip)
		free(r->ip);
	free(r);
}

/*
 * This function will remove fd from epoll and close it.
 * epollfd - epoll fd
 * fd - fd needs to be closed
 */
void
close_fd(int epollfd, int fd)
{
	int rc;
	(void)rc;
	rc = epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
	ASSERT0(rc);

	shutdown(fd, SHUT_RDWR);
	close(fd);
}

/*
 * This function empties mgmt queue, and
 * calls the callback with mgmt cmd status as failed
 */
static void
empty_mgmt_q_of_replica(replica_t *r)
{
	mgmt_cmd_t *mgmt_cmd;
	while ((mgmt_cmd = TAILQ_FIRST(&r->mgmt_cmd_queue))) {
		mgmt_cmd->io_hdr->status = ZVOL_OP_STATUS_FAILED;
		if (!mgmt_cmd->cmd_completed) {
			switch (mgmt_cmd->io_hdr->opcode) {
				case ZVOL_OPCODE_SNAP_CREATE:
					handle_snap_create_resp(r, mgmt_cmd);
					break;
				case ZVOL_OPCODE_PREPARE_FOR_REBUILD:
					handle_prepare_for_rebuild_resp(r->spec,
					    mgmt_cmd->io_hdr, NULL, mgmt_cmd);
					break;
				case ZVOL_OPCODE_STATS:
					handle_update_spec_stats(r->spec, mgmt_cmd->io_hdr,
					    NULL);
					break;
				default:
					break;
			}
			REPLICA_NOTICELOG("mgmt command(%d) failed for "
			    "replica(%lu) (%s:%d) mgmt_fd:%d\n",
			    mgmt_cmd->io_hdr->opcode, r->zvol_guid, r->ip,
			    r->port, r->mgmt_fd);
		}
		clear_mgmt_cmd(r, mgmt_cmd);
	}
}

/*
 * This function will cleanup replica's management command queue
 */
static void
respond_with_error_for_all_outstanding_mgmt_ios(replica_t *r)
{
	ASSERT(r->conn_closed == 2);
	empty_mgmt_q_of_replica(r);
}

#define build_rcmd() 							\
	do {								\
		uint8_t *ldata = malloc(sizeof(zvol_io_hdr_t) + 	\
		    sizeof(struct zvol_io_rw_hdr));			\
		zvol_io_hdr_t *rio = (zvol_io_hdr_t *)ldata;		\
		struct zvol_io_rw_hdr *rio_rw_hdr =			\
		    (struct zvol_io_rw_hdr *)(ldata +			\
		    sizeof(zvol_io_hdr_t));				\
		memset(ldata, 0, sizeof(zvol_io_hdr_t) +		\
		    sizeof(struct zvol_io_rw_hdr));			\
		rcmd = malloc(sizeof(*rcmd));				\
		memset(rcmd, 0, sizeof (*rcmd));			\
		rcmd->opcode = rcomm_cmd->opcode;			\
		rcmd->offset = rcomm_cmd->offset;			\
		rcmd->data_len = rcomm_cmd->data_len;			\
		rcmd->io_seq = rcomm_cmd->io_seq;			\
		rcmd->idx = rcomm_cmd->copies_sent - 1;			\
		rcomm_cmd->resp_list[rcmd->idx].status |= 		\
		    (replica->state == ZVOL_STATUS_HEALTHY) ? 		\
		    SENT_TO_HEALTHY : SENT_TO_DEGRADED;			\
		rcmd->healthy_count = spec->healthy_rcount;		\
		rcmd->rcommq_ptr = rcomm_cmd;				\
		rcmd->iovcnt = rcomm_cmd->iovcnt;			\
		for (i=1; i < rcomm_cmd->iovcnt + 1; i++) {		\
			rcmd->iov[i].iov_base = 			\
			     rcomm_cmd->iov[i].iov_base;		\
			rcmd->iov[i].iov_len = 				\
			    rcomm_cmd->iov[i].iov_len;			\
		}							\
		rio->opcode = rcmd->opcode;				\
		rio->version = REPLICA_VERSION;				\
		rio->io_seq = rcmd->io_seq;				\
		rio->offset = rcmd->offset;				\
		if (rcmd->opcode == ZVOL_OPCODE_WRITE) {		\
			rio->len = rcmd->data_len +			\
			    sizeof(struct zvol_io_rw_hdr);		\
		} else {						\
			if (!spec->healthy_rcount)			\
				rio->flags |=				\
				    ZVOL_OP_FLAG_READ_METADATA;		\
			rio->len = rcmd->data_len;			\
		}							\
		rcmd->iov_data = ldata;					\
		rio_rw_hdr->io_num = rcmd->io_seq;			\
		rio_rw_hdr->len = rcmd->data_len;			\
		rcmd->iov[0].iov_base = rio;				\
		if (rcomm_cmd->opcode == ZVOL_OPCODE_WRITE)		\
			rcmd->iov[0].iov_len = sizeof(zvol_io_hdr_t) +	\
			    sizeof(struct zvol_io_rw_hdr);		\
		else							\
			rcmd->iov[0].iov_len = sizeof(zvol_io_hdr_t);	\
		rcmd->iovcnt++;						\
	} while (0);							\

/*
 * This function will check response received for read command
 * from all replica and process it according to io number
 */
static uint8_t *
handle_read_consistency(rcommon_cmd_t *rcomm_cmd, ssize_t block_len,
    bool check_all)
{
	int i;
	uint8_t *dataptr = NULL;
	struct io_data_chunk_list_t io_data_chunk_list;

	TAILQ_INIT(&(io_data_chunk_list));

	for (i = 0; i < rcomm_cmd->copies_sent; i++) {
		if (rcomm_cmd->resp_list[i].status & RECEIVED_OK) {
			if (check_all) {
				get_all_read_resp_data_chunk(
				    &rcomm_cmd->resp_list[i], block_len,
				    &io_data_chunk_list);
			} else {
				dataptr = get_read_resp_data(
				    &rcomm_cmd->resp_list[i],
				    rcomm_cmd->data_len);
				return dataptr;
			}
		}
	}

	dataptr = process_chunk_read_resp(&io_data_chunk_list,
	    rcomm_cmd->data_len, block_len);

	return dataptr;
}

static int
check_for_command_completion(spec_t *spec, rcommon_cmd_t *rcomm_cmd, ISTGT_LU_CMD_Ptr cmd)
{
	int i, rc = 0;
	uint8_t *data = NULL;
	uint8_t success = 0, failure = 0, healthy_response = 0, response_received;
	int min_response;
	int healthy_replica = 0;

	for (i = 0; i < rcomm_cmd->copies_sent; i++) {
		if (rcomm_cmd->resp_list[i].status & RECEIVED_OK) {
			success++;
			if (rcomm_cmd->resp_list[i].status & SENT_TO_HEALTHY)
				healthy_response++;
		} else if (rcomm_cmd->resp_list[i].status & RECEIVED_ERR) {
			failure++;
		}

		if (rcomm_cmd->resp_list[i].status & SENT_TO_HEALTHY)
			healthy_replica++;
	}

	response_received = success + failure;
	min_response = MAX_OF(rcomm_cmd->replication_factor -
	    rcomm_cmd->consistency_factor + 1, rcomm_cmd->consistency_factor);

	rc = 0;
	if (rcomm_cmd->opcode == ZVOL_OPCODE_READ) {
		/*
		 * If the command is sent to single replica only then
		 * min_response should be set to 1.
		 */
		min_response = (rcomm_cmd->copies_sent == 1) ? 1 : min_response;
		if (success >= min_response) {
			/*
			 * we got the successful response from the required
			 * number of the replica. So, we can reply back to the
			 * client with success
			 */
			data = handle_read_consistency(rcomm_cmd,
			    spec->blocklen, (rcomm_cmd->copies_sent == 1) ?
			    false : true);
			cmd->data = data;
			rc = 1;
		} else if (response_received == rcomm_cmd->copies_sent) {
			/*
			 * we have received the response from all the replicas
			 * to whom we have sent a command, but we didn't get
			 * enough number of successful response. So, we will
			 * reply to the client with an error.
			 */
			rc = -1;
			REPLICA_ERRLOG("didn't receive success from replica.."
			    " cmd:read io(%lu) cs(%d)\n", rcomm_cmd->io_seq,
			    rcomm_cmd->copies_sent);
		} else if ((rcomm_cmd->copies_sent - failure) < min_response) {
			/*
			 * In this case, we will avoid waiting for replicas
			 * which haven't sent response yet.
			 */
			REPLICA_ERRLOG("got error from %d replicas.. io(%lu)"
			    "cs(%d)\n", failure, rcomm_cmd->io_seq,
			    rcomm_cmd->copies_sent);
			rc = -1;
		}
	} else if ((rcomm_cmd->opcode == ZVOL_OPCODE_WRITE) ||
		   (rcomm_cmd->opcode == ZVOL_OPCODE_SYNC)) {
		if (healthy_response >= rcomm_cmd->consistency_factor) {
			/*
			 * We got the successful response from required healthy
			 * replicas.
			 */
			ASSERT(healthy_replica >= rcomm_cmd->consistency_factor);
			rc = 1;
		} else if (success >= min_response) {
			/*
			 * If we didn't get the required number of response
			 * from healthy replica's but success response matches
			 * with min_response.
			 */
			rc = 1;
		} else if (response_received == rcomm_cmd->copies_sent) {
			rc = -1;
			REPLICA_ERRLOG("didn't receive success from replica.."
			    " cmd:write io(%lu) cs(%d)\n", rcomm_cmd->io_seq,
			    rcomm_cmd->copies_sent);
		}
	}

	return rc;
}

int64_t
replicate(ISTGT_LU_DISK *spec, ISTGT_LU_CMD_Ptr cmd, uint64_t offset, uint64_t nbytes)
{
	int rc = -1, i;
	bool cmd_write = false, cmd_read = false, cmd_sync = false;
	replica_t *replica, *last_replica = NULL;
	rcommon_cmd_t *rcomm_cmd;
	rcmd_t *rcmd = NULL;
	int iovcnt = cmd->iobufindx + 1;
	bool replica_choosen = false;
	struct timespec abstime, now, extra_wait;
	int nsec, err_num = 0;
	int skip_count = 0;
	uint64_t num_read_ios = 0;
	uint64_t inflight_read_ios = 0;
	int count =0 ;
	bool success_resp = false;

	(void) cmd_read;
	CHECK_IO_TYPE(cmd, cmd_read, cmd_write, cmd_sync);

again:
	MTX_LOCK(&spec->rq_mtx);
	if(spec->ready == false) {
		REPLICA_LOG("SPEC(%s) is not ready\n", spec->volname);
		MTX_UNLOCK(&spec->rq_mtx);
		return -1;
	}

	/* Quiesce write/sync IOs based on flag */
	if ((cmd_write || cmd_sync) && spec->quiesce == 1) { 
		MTX_UNLOCK(&spec->rq_mtx);
		sleep(1);
		goto again;
	}

	UPDATE_INFLIGHT_SPEC_IO_CNT(spec, cmd, 1);

	ASSERT(spec->io_seq);
	build_rcomm_cmd(rcomm_cmd, cmd, offset, nbytes);
retry_read:
	replica_choosen = false;
	skip_count = 0;
	last_replica = NULL;
	num_read_ios = 0;

	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		/*
		 * If there are some healthy replica then send read command
		 * to all healthy replica else send read command to all
		 * degraded replica.
		 */

		if (spec->healthy_rcount &&
		    rcomm_cmd->opcode == ZVOL_OPCODE_READ) {
			/*
			 * If there are some healthy replica then don't send
			 * a read command to degraded replica
			 */
			if (replica->state == ZVOL_STATUS_DEGRADED)
				continue;
			else {
				inflight_read_ios = replica->replica_inflight_read_io_cnt;

				if (inflight_read_ios == 0) {
					replica_choosen = true;
				} else if (num_read_ios < inflight_read_ios) {
					if (!last_replica) {
						num_read_ios = inflight_read_ios;
						last_replica = replica;
					}
					skip_count++;
				} else if (num_read_ios > inflight_read_ios) {
					last_replica = replica;
					num_read_ios = inflight_read_ios;
					skip_count++;
				} else
					skip_count++;

				if (skip_count == spec->healthy_rcount) {
					replica = last_replica;
					replica_choosen = true;
				}

				if (!replica_choosen)
					continue;
			}
		}

		rcomm_cmd->copies_sent++;

		build_rcmd();

		INCREMENT_INFLIGHT_REPLICA_IO_CNT(replica, rcmd->opcode);

		put_to_mempool(&replica->cmdq, rcmd);

		eventfd_write(replica->data_eventfd, 1);

		if (replica_choosen)
			break;
	}

	TAILQ_INSERT_TAIL(&spec->rcommon_waitq, rcomm_cmd, wait_cmd_next);

	MTX_UNLOCK(&spec->rq_mtx);

	extra_wait.tv_sec = extra_wait.tv_nsec = 0;
	// now wait for command to complete
	while (1) {
		// check for status of rcomm_cmd
		rc = check_for_command_completion(spec, rcomm_cmd, cmd);
		if (rc) {
			success_resp = false;
			if (rc == 1) {
				rc = cmd->data_len = rcomm_cmd->data_len;
				success_resp = true;
			} else if (rcomm_cmd->opcode == ZVOL_OPCODE_READ &&
			    rcomm_cmd->copies_sent == 1) {
				rcomm_cmd->copies_sent = 0;
				memset(rcomm_cmd->resp_list, 0,
				    sizeof (rcomm_cmd->resp_list));
				MTX_LOCK(&spec->rq_mtx);
				TAILQ_REMOVE(&spec->rcommon_waitq, rcomm_cmd,
				    wait_cmd_next);
				goto retry_read;
			}

			count = 0;
			if (success_resp == true) {
				if (extra_wait.tv_sec == 0)
					clock_gettime(CLOCK_REALTIME,
					    &extra_wait);
				clock_gettime(CLOCK_REALTIME, &now);
				for (i = 0; i < rcomm_cmd->copies_sent; i++) {
					if (rcomm_cmd->resp_list[i].status &
					    (RECEIVED_OK|RECEIVED_ERR))
						count++;
				}
				if ((now.tv_sec - extra_wait.tv_sec) < extraWait) {
					if (count != rcomm_cmd->copies_sent)
						goto wait_for_other_responses;
				}
			}
			rcomm_cmd->state = CMD_EXECUTION_DONE;
#ifdef	DEBUG
			/*
			 * NOTE: This is for debugging purpose only
			 */
			if (err_num == ETIMEDOUT)
				fprintf(stderr, "last errno(%d) "
				    "opcode(%d)\n", errno, rcomm_cmd->opcode);
#endif

			MTX_LOCK(&spec->rq_mtx);
			TAILQ_REMOVE(&spec->rcommon_waitq, rcomm_cmd, wait_cmd_next);
			UPDATE_INFLIGHT_SPEC_IO_CNT(spec, cmd, -1);
			MTX_UNLOCK(&spec->rq_mtx);

			put_to_mempool(&spec->rcommon_deadlist, rcomm_cmd);
			break;
		}

wait_for_other_responses:
		/* wait for 500 ms(500000000 ns) */
		clock_gettime(CLOCK_REALTIME, &now);
		nsec = 1000000000 - now.tv_nsec;
		if (nsec > 500000000) {
			abstime.tv_sec = now.tv_sec;
			abstime.tv_nsec = now.tv_nsec + 500000000;
		} else {
			abstime.tv_sec = now.tv_sec + 1;
			abstime.tv_nsec = 500000000 - nsec;
		}

		MTX_LOCK(rcomm_cmd->mutex);
		rc = pthread_cond_timedwait(rcomm_cmd->cond_var,
		    rcomm_cmd->mutex, &abstime);
		err_num = errno;
		MTX_UNLOCK(rcomm_cmd->mutex);
	}

	return rc;
}

/*
 * This function handles error in replica's management interface
 * and inform replica_thread(data connection) regarding error
 */
static void
handle_mgmt_conn_error(replica_t *r, int sfd, struct epoll_event *events, int ev_count)
{
	int epollfd = r->epfd;
	int mgmtfd, mgmt_eventfd1;
	int i;
	mgmt_event_t *mevent;
	replica_t *r_ev;
	known_replica_t *kr = NULL;

	MTX_LOCK(&r->spec->rq_mtx);
	MTX_LOCK(&r->r_mtx);

	r->conn_closed++;
	if (r->conn_closed != 2) {
		ASSERT(r->conn_closed == 1);
		/*
		 * case where error happened while sending HANDSHAKE or
		 * sending is successful but error from zvol_handshake or
		 * data connection is closed before this
		 */
		if ((r->iofd == -1) || (r->mgmt_eventfd2 == -1)) {
			TAILQ_FOREACH(r_ev, &(r->spec->rwaitq), r_waitnext) {
				if (r_ev == r) {
					TAILQ_REMOVE(&r->spec->rwaitq, r, r_waitnext);
					r->conn_closed++;
					if (r->io_resp_hdr) {
						free(r->io_resp_hdr);
						r->io_resp_hdr = NULL;
					}
				}
			}
		}
		if (r->mgmt_eventfd2 != -1)
			inform_data_conn(r);
	} else {
		pthread_cond_signal(&r->r_cond);
	}

	mgmtfd = r->mgmt_fd;
	r->mgmt_fd = -1;
	MTX_UNLOCK(&r->r_mtx);
	MTX_UNLOCK(&r->spec->rq_mtx);

	close_fd(epollfd, mgmtfd);

	MTX_LOCK(&r->r_mtx);
	if (r->conn_closed != 2) {
		ASSERT(r->conn_closed == 1);
		pthread_cond_wait(&r->r_cond, &r->r_mtx);
	}

	/* this need to be called after replica is removed from spec list
	 * data_conn thread should have removed from spec list as conn_closed is 2 */
	respond_with_error_for_all_outstanding_mgmt_ios(r);
	MTX_UNLOCK(&r->r_mtx);

	mgmt_eventfd1 = r->mgmt_eventfd1;
	r->mgmt_eventfd1 = -1;
	close_fd(epollfd, mgmt_eventfd1);

	REPLICA_NOTICELOG("Replica(%lu) got disconnected from %s:%d "
	    "mgmt_fd:%d\n", r->zvol_guid, r->ip, r->port, r->mgmt_fd);

	for (i = 0; i < ev_count; i++) {
		if (events[i].data.fd == sfd) {
			continue;
		} else {
			if (events[i].data.ptr == NULL)
				continue;

			mevent = events[i].data.ptr;
			r_ev = mevent->r_ptr;

			if (r_ev != r)
				continue;

			if (mevent->fd == mgmt_eventfd1 ||
				mevent->fd == mgmtfd) {
				events[i].data.ptr = NULL;
			} else
				REPLICA_ERRLOG("unexpected fd(%d) for "
				    "replica(%lu)\n", mevent->fd, r->zvol_guid);
		}
	}

	MTX_LOCK(&r->spec->rq_mtx);
	if (r->spec->rebuild_info.dw_replica == r) {
		REPLICA_ERRLOG("Replica(%lu) was under rebuild,"
		    " seting master_replica to NULL\n",
		    r->zvol_guid);
		r->spec->rebuild_info.dw_replica = NULL;
		r->spec->rebuild_info.healthy_replica = NULL;
		r->spec->rebuild_info.rebuild_in_progress = false;
	}

	TAILQ_FOREACH(kr, &r->spec->identified_replica, next) {
		if (kr->zvol_guid == r->zvol_guid) {
			kr->is_connected = false;
			break;
		}
	}
	MTX_UNLOCK(&r->spec->rq_mtx);

	if (r->dont_free != 1)
		free_replica(r);
}

/*
 * This function will handle event received from
 * replica_thread
 */
static int
handle_mgmt_event_fd(replica_t *replica)
{
	int rc = -1;

	do_drainfd(replica->mgmt_eventfd1);

	MTX_LOCK(&replica->r_mtx);
	if (replica->disconnect_conn == 1) {
		MTX_UNLOCK(&replica->r_mtx);
		return rc;
	}
	MTX_UNLOCK(&replica->r_mtx);

	rc = handle_write_data_event(replica);
	return rc;
}

/*
 * reads data on management fd
 */
int
handle_read_data_event(replica_t *replica)
{
	io_event_t revent;
	mgmt_cmd_t *mgmt_cmd;
	int rc = 0;

	MTX_LOCK(&replica->r_mtx);
	mgmt_cmd = TAILQ_FIRST(&replica->mgmt_cmd_queue);
	if (!(mgmt_cmd != NULL &&
		(mgmt_cmd->mgmt_cmd_state == READ_IO_RESP_HDR ||
		mgmt_cmd->mgmt_cmd_state == READ_IO_RESP_DATA))) {
		MTX_UNLOCK(&replica->r_mtx);
		/*
		 * Though we didn't send any IO query on management connection,
		 * We have a read event on management connection. Thats an
		 * error as management connection is not working in stateful
		 * manner. So we will print error message and does cleanup
		 */
		REPLICA_ERRLOG("unexpected read IO on mgmt connection.. for "
		    "replica(%lu)\n", replica->zvol_guid);
		return (-1);
	}

	MTX_UNLOCK(&replica->r_mtx);

	revent.fd = replica->mgmt_fd;
	revent.state = &(mgmt_cmd->mgmt_cmd_state);
	revent.io_hdr = replica->mgmt_io_resp_hdr;
	revent.io_data = (void **)(&(replica->mgmt_io_resp_data));
	revent.byte_count = &(mgmt_cmd->io_bytes);

	rc = read_io_resp(replica->spec, replica, &revent, mgmt_cmd);
	if (rc > 0) {
		VERIFY3S(rc, ==, 1);
		MTX_LOCK(&replica->r_mtx);
		clear_mgmt_cmd(replica, mgmt_cmd);
		MTX_UNLOCK(&replica->r_mtx);
		rc = handle_write_data_event(replica);
	}
	return (rc);
}

int replica_poll_time = 30;

/*
 * initializes replication
 * - by starting listener to accept mgmt connections
 * - reads data on accepted mgmt connection
 */
void *
init_replication(void *arg __attribute__((__unused__)))
{
	struct epoll_event event, *events;
	int rc, sfd, event_count, i;
	int epfd;
	replica_t *r;
	int timeout;
	struct timespec last, now, diff;
	mgmt_event_t *mevent;
	pthread_t self = pthread_self();

	snprintf(tinfo, sizeof tinfo, "rm#%d.%d", (int)(((uint64_t *)self)[0]), getpid());

	//Create a listener for management connections from replica
	const char* externalIP = getenv("externalIP");
	ASSERT(externalIP);

	if((sfd = cstor_ops.conn_listen(externalIP, 6060, 32, 1)) < 0) {
		REPLICA_LOG("conn_listen() failed, sfd(%d)", sfd);
		exit(EXIT_FAILURE);
	}

	epfd = epoll_create1(0);
	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET | EPOLLERR | EPOLLHUP;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, err(%d)", errno);
		exit(EXIT_FAILURE);
	}

	events = calloc(MAXEVENTS, sizeof(event));
	timeout = replica_poll_time * 1000;
	clock_gettime(CLOCK_MONOTONIC, &last);

#ifdef	DEBUG
	/*
	 * This is added to test istgtcontrol execution path if
	 * replication module is not initialized
	 */
	const char* replication_delay = getenv("ReplicationDelay");
	unsigned int seconds = 0;
	if (replication_delay) {
		seconds = (unsigned int)strtol(replication_delay, NULL, 10);
		fprintf(stderr, "sleep in replication module for %d seconds\n",
		    seconds);
		sleep(seconds);
	}
#endif

	/*
	 * we have successfully initialized replication module.
	 * we can change value of replication_initialized to 1.
	 */
	replication_initialized = 1;

	while (1) {
		//Wait for management connections(on sfd) and management commands(on mgmt_rfds[]) from replicas
		event_count = epoll_wait(epfd, events, MAXEVENTS, timeout);
		if (event_count < 0) {
			if (errno == EINTR)
				continue;
			REPLICA_ERRLOG("epoll_wait ret(%d) err(%d).. better "
			    "to restart listener\n", event_count, errno);
			continue;
		}

		for(i=0; i< event_count; i++) {
			if (events[i].events & EPOLLHUP || events[i].events & EPOLLERR ||
				events[i].events & EPOLLRDHUP) {
				if (events[i].data.fd == sfd) {
					REPLICA_ERRLOG("epoll event(%d) on "
					    "fd(%d)... better to restart "
					    "listener\n",
					    events[i].events,
					    events[i].data.fd);
					/*
					 * Here, we can exit without performing
					 * cleanup for all replica
					 */
					exit(EXIT_FAILURE);
				} else {
					if (events[i].data.ptr == NULL)
						continue;
					mevent = events[i].data.ptr;
					ASSERT(mevent->r_ptr);
					r = mevent->r_ptr;
					REPLICA_ERRLOG("epoll event(%d) on "
					    "replica(%s:%d)\n",
					    events[i].events, r->ip, r->port);
					handle_mgmt_conn_error(r, sfd, events, event_count);
				}
			} else {
				if (events[i].data.fd == sfd) {
					//Accept management connections from replicas and add the replicas to replica queue
					accept_mgmt_conns(epfd, sfd);
				} else {
					if (events[i].data.ptr == NULL)
						continue;

					mevent = events[i].data.ptr;
					ASSERT(mevent->r_ptr);
					r = mevent->r_ptr;

					rc = 0;
					if (events[i].events & EPOLLIN) {
						if (mevent->fd == r->mgmt_fd)
							rc = handle_read_data_event(r);
						else
							rc = handle_mgmt_event_fd(r);
					}

					if ((rc != -1) &&
					    (events[i].events & EPOLLOUT)) {
						ASSERT(mevent->fd == r->mgmt_fd);
						rc = handle_write_data_event(r);
					}
					if (rc == -1)
						handle_mgmt_conn_error(r, sfd, events, event_count);
				}
			}
		}

		// send replica_status query to degraded replicas at max interval of '2*replica_poll_time' seconds
		timesdiff(CLOCK_MONOTONIC, last, now, diff);
		if (diff.tv_sec >= replica_poll_time) {
			spec_t *spec = NULL;
			MTX_LOCK(&specq_mtx);
			TAILQ_FOREACH(spec, &spec_q, spec_next) {
				ask_replica_status_all(spec);
				get_replica_stats(spec);
			}
			MTX_UNLOCK(&specq_mtx);
			clock_gettime(CLOCK_MONOTONIC, &last);
		}
	}

	free (events);
	close (sfd);
	close (epfd);
	return 0;
}

/*
 * initialize spec queue and mutex
 */
int
initialize_replication()
{
	//Global initializers for replication library
	int rc;
	TAILQ_INIT(&spec_q);
	rc = pthread_mutex_init(&specq_mtx, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("Failed to init specq_mtx err(%d)\n", rc);
		return -1;
	}
	clock_gettime(CLOCK_MONOTONIC_RAW, &istgt_start_time);
	return 0;
}

void
destroy_volume(spec_t *spec)
{
	ASSERT0(get_num_entries_from_mempool(&spec->rcommon_deadlist));
	destroy_mempool(&spec->rcommon_deadlist);

	ASSERT(TAILQ_EMPTY(&spec->rcommon_waitq));
	ASSERT(TAILQ_EMPTY(&spec->rq));
	ASSERT(TAILQ_EMPTY(&spec->rwaitq));

	pthread_mutex_destroy(&spec->rcommonq_mtx);
	pthread_mutex_destroy(&spec->rq_mtx);

	MTX_LOCK(&specq_mtx);
	TAILQ_REMOVE(&spec_q, spec, spec_next);
	MTX_UNLOCK(&specq_mtx);

	return;
}

int
initialize_volume(spec_t *spec, int replication_factor, int consistency_factor)
{
	int rc;
	pthread_t deadlist_cleanup_thread;

	spec->io_seq = 0;
	TAILQ_INIT(&spec->rcommon_waitq);
	TAILQ_INIT(&spec->rq);
	TAILQ_INIT(&spec->rwaitq);
	TAILQ_INIT(&spec->identified_replica);

	VERIFY(replication_factor > 0);
	VERIFY(consistency_factor > 0);

	init_mempool(&spec->rcommon_deadlist, rcmd_mempool_count, 0, 0,
	    "rcmd_mempool", NULL, NULL, NULL, false);

	spec->replication_factor = replication_factor;
	spec->consistency_factor = consistency_factor;
	spec->healthy_rcount = 0;
	spec->degraded_rcount = 0;
	spec->rebuild_info.rebuild_in_progress = false;
	spec->ready = false;
	spec->rebuild_info.dw_replica = NULL;
	spec->rebuild_info.healthy_replica = NULL;

	rc = pthread_mutex_init(&spec->rcommonq_mtx, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("Failed to ini rcommonq mtx err(%d)\n", rc);
		return -1;
	}

	rc = pthread_mutex_init(&spec->rq_mtx, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("Failed to init rq_mtx err(%d)\n", rc);
		return -1;
	}

	rc = pthread_create(&deadlist_cleanup_thread, NULL, &cleanup_deadlist,
			(void *)spec);
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_create(replicator_thread) failed "
		    "err(%d)\n", rc);
		return -1;
	}

	MTX_LOCK(&specq_mtx);
	TAILQ_INSERT_TAIL(&spec_q, spec, spec_next);
	MTX_UNLOCK(&specq_mtx);

	return 0;
}

void
istgt_lu_mempool_stats(char **resp)
{
	struct json_object *j_resp, *j_obj, *j_array;
	struct json_object *j_replica;
	spec_t *spec;
	replica_t *r;
	uint64_t resp_len;
	const char *json_string = NULL;

	j_resp = json_object_new_array();

	j_obj = json_object_new_object();
	j_array = json_object_new_array();

	MTX_LOCK(&specq_mtx);
	TAILQ_FOREACH(spec, &spec_q, spec_next) {
		TAILQ_FOREACH(r, &spec->rq, r_next) {
			j_replica = json_object_new_object();
			json_object_object_add(j_replica, "replicaId",
			    json_object_new_uint64(r->zvol_guid));
			json_object_object_add(j_replica, "in-flight read",
			    json_object_new_uint64(
			    r->replica_inflight_read_io_cnt));
			json_object_object_add(j_replica, "in-flight write",
			    json_object_new_uint64(
			    r->replica_inflight_write_io_cnt));
			json_object_object_add(j_replica, "in-flight sync",
			    json_object_new_uint64(
			    r->replica_inflight_sync_io_cnt));
			json_object_object_add(j_replica, "in-flight command",
			    json_object_new_int64(
			    get_num_entries_from_mempool(&r->cmdq)));
			json_object_array_add(j_array, j_replica);
		}
	}
	MTX_UNLOCK(&specq_mtx);
	json_object_object_add(j_obj, "replica usage", j_array);
	json_object_array_add(j_resp, j_obj);

	/* rcmd_mempool end */

	j_obj = json_object_new_object();
	json_object_object_add(j_obj, "mempool list", j_resp);

	json_string = json_object_to_json_string_ext(j_obj,
	    JSON_C_TO_STRING_PLAIN);
	resp_len = strlen(json_string) + 1;
	*resp = malloc(resp_len);
	memset(*resp, 0, resp_len);
	strncpy(*resp, json_string, resp_len);
	json_object_put(j_obj);
}

/*
 * destroy response received from replica for a rcommon_cmd
 */
static void
destroy_resp_list(rcommon_cmd_t *rcomm_cmd)
{
	int i;

	for (i = 0; i < rcomm_cmd->copies_sent; i++) {
		if (rcomm_cmd->resp_list[i].data_ptr) {
			free(rcomm_cmd->resp_list[i].data_ptr);
		}
	}
}

/*
 * perform cleanup for completed rcommon_cmd (whose response is
 * sent back to the client)
 */
void *
cleanup_deadlist(void *arg)
{
	spec_t *spec = (spec_t *)arg;
	rcommon_cmd_t *rcomm_cmd;
	int i, count = 0, entry_count = 0;

	while (1) {
		entry_count = get_num_entries_from_mempool(&spec->rcommon_deadlist);
		while (entry_count) {
			count = 0;
			rcomm_cmd = get_from_mempool(&spec->rcommon_deadlist);

			ASSERT(rcomm_cmd->state == CMD_EXECUTION_DONE);

			for (i = 0; i < rcomm_cmd->copies_sent; i++) {
				if (rcomm_cmd->resp_list[i].status &
				    (RECEIVED_OK|RECEIVED_ERR))
					count++;
			}

			if (count == rcomm_cmd->copies_sent) {
				destroy_resp_list(rcomm_cmd);

				for (i=1; i<rcomm_cmd->iovcnt + 1; i++)
					xfree(rcomm_cmd->iov[i].iov_base);

				free(rcomm_cmd);
			} else {
				put_to_mempool(&spec->rcommon_deadlist, rcomm_cmd);
			}
			entry_count--;
		}
		sleep(1);	//add predefined time here
	}
	return (NULL);
}

/*
int
remove_volume(spec_t *spec) {

	rcommon_cmd_t *cmd, *next_cmd = NULL;
	rcmd_t *rcmd, *next_rcmd = NULL;

	//Remove all cmds from rwaitq and rblockedq of all replicas
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if(replica == NULL) {
			perror("Replica not present");
			exit(EXIT_FAILURE);
		}
		MTX_LOCK(&replica->q_mtx);
		rcmd = TAILQ_FIRST(&replica->rwaitq);
		while (rcmd) {
			rcmd->status = -1;
			next_rcmd = TAILQ_NEXT(rcmd, rwait_cmd_next);
			TAILQ_REMOVE(&rwaitq, rcmd, rwait_cmd_next);
			rcmd = next_rcmd;
		}

		rcmd = TAILQ_FIRST(&replica->rblockedq);
		while (rcmd) {
			rcmd->status = -1;
			next_rcmd = TAILQ_NEXT(rcmd, rblocked_cmd_next);
			TAILQ_REMOVE(&rwaitq, rcmd, rblocked_cmd_next);
			rcmd = next_rcmd;
		}
		MTX_UNLOCK(replica->q_mtx);
	}

	//Remove all cmds from rcommon_sendq, rcommon_waitq, and rcommon_pendingq
	MTX_LOCK(&spec->rcommonq_mtx);
	cmd = TAILQ_FIRST(&spec->rcommon_sendq);
	while (cmd) {
		cmd->status = -1;
		cmd->completed = 1; \
		next_cmd = TAILQ_NEXT(cmd, send_cmd_next);
		TAILQ_REMOVE(&rcommon_sendq, cmd, send_cmd_next);
		cmd = next_cmd;
	}
	cmd = TAILQ_FIRST(&spec->rcommon_waitq);
	while (cmd) {
		cmd->status = -1;
		cmd->completed = 1; \
		next_cmd = TAILQ_NEXT(cmd, wait_cmd_next);
		TAILQ_REMOVE(&rcommon_waitq, cmd, wait_cmd_next);
		cmd = next_cmd;
	}
	cmd = TAILQ_FIRST(&spec->rcommon_pendingq);
	while (cmd) {
		cmd->status = -1;
		cmd->completed = 1; \
		next_cmd = TAILQ_NEXT(cmd, pending_cmd_next);
		TAILQ_REMOVE(&rcommon_pendingq, cmd, pending_cmd_next);
		cmd = next_cmd;
	}
	MTX_UNLOCK(spec->rcommonq_mtx);

	for(i=0; i<spec->luworkers; i++) {
		pthread_cond_signal(&spec->luworker_cond[i]);
	}

	pthread_mutex_destroy(&spec->rq_mtx);
	pthread_cond_destroy(&spec->rq_cond);
	MTX_LOCK(&specq_mtx);
	TAILQ_REMOVE(&spec_q, spec, spec_next);
	MTX_UNLOCK(&specq_mtx);
}
*/
