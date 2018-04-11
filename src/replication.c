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
#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"
#include "istgt_crc32c.h"
#include "istgt_misc.h"
#include "ring_mempool.h"
cstor_conn_ops_t cstor_ops = {
	.conn_listen = replication_listen,
	.conn_connect = replication_connect,
};

#define	RCMD_MEMPOOL_ENTRIES	100000
rte_smempool_t rcmd_mempool;
size_t rcmd_mempool_count = RCMD_MEMPOOL_ENTRIES;

extern rte_smempool_t rcommon_cmd_mempool;
extern size_t rcommon_cmd_mempool_count;

#define build_replica_io_hdr() \
{\
	rio = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));\
	rio->opcode = cmd->opcode;\
	rio->io_seq = cmd->io_seq;\
	rio->offset = cmd->offset;\
	rio->len    = cmd->data_len;\
}

#define build_replica_mgmt_hdr() \
{\
	rmgmtio = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));\
	rmgmtio->opcode = mgmt_opcode;\
	rmgmtio->len    = data_len;\
}

/*
#define build_mgmt_cmd() \
{\
	mgmt_cmd = malloc(sizeof(mgmt_cmd_t));\
	mgmt_cmd->opcode = opcode;\
	mgmt_cmd->data_len = malloc \
}
*/

#define build_rcmd() \
{\
	rcmd = get_from_mempool(&rcmd_mempool);\
	rcmd->opcode = cmd->opcode;\
	rcmd->offset = cmd->offset;\
	rcmd->data_len = cmd->data_len;\
	rcmd->io_seq = cmd->io_seq;\
	rcmd->ack_recvd = false;\
	rcmd->rcommq_ptr = cmd;\
	rcmd->status = 0;\
	rcmd->iovcnt = cmd->iovcnt;\
	for (i=1; i < rcmd->iovcnt + 1; i++) {\
		rcmd->iov[i].iov_base = cmd->iov[i].iov_base;\
		rcmd->iov[i].iov_len = cmd->iov[i].iov_len;\
	}\
}

#define check_for_blockage() {\
	uint64_t current_lba = cmd->offset; \
	uint64_t current_lbE = current_lba + cmd->data_len; \
	uint64_t pending_lba = pending_cmd->offset; \
	uint64_t pending_lbE = pending_lba + pending_cmd->data_len; \
	if ((current_lbE < pending_lba)  || \
			(pending_lbE < current_lba)) { \
		cmd_blocked = false; \
	} else {\
		cmd_blocked = true; \
	}\
}

#define signal_luworker() { \
	MTX_LOCK(&spec->luworker_rmutex[luworker_id]); \
	pthread_cond_signal(&spec->luworker_rcond[luworker_id]); \
	MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]); \
}

int allocate_replica_id(spec_t *spec) {
	int replica_id = ffs(~spec->replica_ids)-1;
	if(replica_id == -1) {
		REPLICA_ERRLOG("Replica limit reached\n");
		return -1;
	}
	spec->replica_ids |= (1 << replica_id);
	return replica_id;
}

void release_replica_id(spec_t *spec, int replica_id) {
	spec->replica_ids &= ~(1 << replica_id);
}


int
send_mgmtio(int fd, zvol_op_code_t mgmt_opcode, void *buf, size_t data_len) {
	zvol_io_hdr_t *rmgmtio = NULL;
	int iovcnt, i, nbytes = 0;
	int rc = 0;
	struct iovec iovec[2];
	build_replica_mgmt_hdr();
	iovec[0].iov_base = rmgmtio;
	nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
	iovec[1].iov_base = buf;
	iovec[1].iov_len = rmgmtio->len;
	nbytes += rmgmtio->len;
	iovcnt = 2;
	while (nbytes) {
		rc = writev(fd, iovec, iovcnt);
		if (rc < 0) {
			REPLICA_ERRLOG("writev failed on fd %d, errno:%d.. closing it..", fd, errno);
			free(rmgmtio);
			return -1;
		}
		nbytes -= rc;
		if (nbytes == 0)
			break;
		/* adjust iovec length */
		for (i = 0; i < iovcnt; i++) {
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
	free(rmgmtio);
	return 0;
}

replica_t *get_next_replica(spec_t *spec, replica_t *replica) {
	MTX_LOCK(&spec->rq_mtx);
	if(replica == NULL) {
		replica = TAILQ_FIRST(&spec->rq);
	} else {
		replica->refcount--;
		replica = TAILQ_NEXT(replica, r_next);
	}
	if(replica != NULL)
		replica->refcount++;
	MTX_UNLOCK(&spec->rq_mtx);
	return replica;
}

//Picks cmds from send queue and sends them to replica
void *
replicator(void *arg) {
	spec_t *spec = (spec_t *)arg;
	replica_t *replica;
	bool read_cmd_sent;
	uint64_t bitset;
	int i = 0;
	rcommon_cmd_t *cmd = NULL;
	rcmd_t *rcmd = NULL;
	bool cmd_blocked = false;
	uint64_t io_seq = 0;
	rcommon_cmd_t *pending_cmd;

	while(1) {
		MTX_LOCK(&spec->rcommonq_mtx);
dequeue_common_sendq:
		cmd = TAILQ_FIRST(&spec->rcommon_sendq);
		if(!cmd) {
			pthread_cond_wait(&spec->rcommonq_cond, &spec->rcommonq_mtx);
			goto dequeue_common_sendq;
		}
		cmd->io_seq = ++io_seq;
		cmd->acks_recvd = 0;
		cmd->bitset = 0;
		TAILQ_REMOVE(&spec->rcommon_sendq, cmd, send_cmd_next);
		cmd->state = CMD_ENQUEUED_TO_WAITQ; \
			     TAILQ_INSERT_TAIL(&spec->rcommon_waitq, cmd, wait_cmd_next);
		//Check for blockage in rcommon_pendingq and set corresponding blocked replica bits
		bitset = 0;
		TAILQ_FOREACH(pending_cmd, &spec->rcommon_pendingq, pending_cmd_next) {
			check_for_blockage();
			if(cmd_blocked == true) {
				bitset |= pending_cmd->bitset;
			}
		}
		MTX_UNLOCK(&spec->rcommonq_mtx);

		//Enqueue to individual replica cmd queues and send on the respective fds
		read_cmd_sent = false;
		replica = NULL;
		replica = get_next_replica(spec, replica);
		while(replica != NULL) {
			//Create an entry for replica queue
			build_rcmd();
			MTX_LOCK(&replica->r_mtx);
			if(replica->removed) {
				MTX_UNLOCK(&replica->r_mtx);
				continue;
			}
			cmd->bitset |= (1 << replica->id);
			cmd->copies_sent++;
			if(bitset & (1 << replica->id)) {
				TAILQ_INSERT_TAIL(&replica->blockedq, rcmd, rblocked_cmd_next);
			} else {
				TAILQ_INSERT_TAIL(&replica->sendq, rcmd, rsend_cmd_next);
				if(rcmd->opcode == ZVOL_OPCODE_READ) {
					read_cmd_sent = true;
				}
				pthread_cond_signal(&replica->r_cond);
			}
			MTX_UNLOCK(&replica->r_mtx);
			if((rcmd->opcode == ZVOL_OPCODE_READ) && read_cmd_sent) {
				break;
			}
			replica = get_next_replica(spec, replica);
		}
	}
}

int
wait_for_fd(int epfd) {
	int event_count = 0;
	struct epoll_event event;
	event_count = epoll_wait(epfd, &event, 1, -1);
	if ((event.events & EPOLLERR) ||
			(event.events & EPOLLHUP) ||
			(!(event.events & EPOLLOUT))) {
		REPLICA_LOG("epoll error: %d\n", errno);
		return -1;
	} else {
		return 0;
	}
}

int
sendio(int epfd, int fd, rcommon_cmd_t *cmd, rcmd_t *rcmd) {
	zvol_io_hdr_t *rio = NULL;
	int i = 0;
	int64_t rc = 0;
	int64_t nbytes = 0;
	nbytes = cmd->total_len;
	build_replica_io_hdr();
	rcmd->iov[0].iov_base = rio;
	rcmd->iov[0].iov_len = sizeof(zvol_io_hdr_t);
	nbytes += sizeof(zvol_io_hdr_t);
	while (nbytes) {
write_to_socket:
		rc = writev(fd, rcmd->iov, rcmd->iovcnt+1);
		if (rc < 0) {
			if (errno == EAGAIN) {
				wait_for_fd(epfd);
				goto write_to_socket;
			} else {
				//REPLICA_LOG("write error %d on replica %s\n", rc, rcmd->r->ip);
				free(rio);
				rio = NULL;
				return -1;
			}
		}
		nbytes -= rc;
		if (nbytes <= 0)
			break;
		for (i=0; i < rcmd->iovcnt + 1; i++) {
			if (rcmd->iov[i].iov_len != 0 && rcmd->iov[i].iov_len > (size_t)rc) {
				rcmd->iov[i].iov_base
					= (void *) (((uint8_t *)rcmd->iov[i].iov_base) + rc);
				rcmd->iov[i].iov_len -= rc;
				break;
			} else {
				rc -= rcmd->iov[i].iov_len;
				rcmd->iov[i].iov_len = 0;
			}
		}
	}
	free(rio);
	rio = NULL;
	return 0;
}

void *
replica_sender(void *arg) {
	replica_t *replica = (replica_t *)arg;
	MTX_LOCK(&replica->spec->rq_mtx);
	replica->refcount++;
	MTX_UNLOCK(&replica->spec->rq_mtx);
	int rc;
	rcommon_cmd_t *cmd = NULL;
	rcmd_t *rcmd = NULL;
	rcommon_cmd_t *rcommq_ptr;//REMOVE
	while(1) {
		MTX_LOCK(&replica->r_mtx);
dequeue_rsendq:
		rcmd = TAILQ_FIRST(&replica->sendq);
		if(!rcmd) {
			pthread_cond_wait(&replica->r_cond, &replica->r_mtx);
			if(replica->state == REPLICA_ERRORED) {
				MTX_UNLOCK(&replica->r_mtx);
				break;
			}
			goto dequeue_rsendq;
		}
		TAILQ_REMOVE(&replica->sendq, rcmd, rsend_cmd_next);
		if(rcmd->opcode == ZVOL_OPCODE_READ) {
			rcmd->rrio_seq = ++replica->rrio_seq;
			TAILQ_INSERT_TAIL(&replica->read_waitq, rcmd, rread_cmd_next);
		} else {
			rcmd->wrio_seq = ++replica->wrio_seq;
			rcommq_ptr = rcmd->rcommq_ptr;
			TAILQ_INSERT_TAIL(&replica->waitq, rcmd, rwait_cmd_next);
		}
		MTX_UNLOCK(&replica->r_mtx);
		cmd = rcmd->rcommq_ptr;
		rc = sendio(replica->sender_epfd, replica->iofd, cmd, rcmd);
		if(rc < 0) {
			if(replica->state == REPLICA_ERRORED) {
				break;
			}
			MTX_LOCK(&replica->r_mtx);
			remove_replica_from_list(replica->spec, replica->iofd);
			replica->state = REPLICA_ERRORED;
			MTX_UNLOCK(&replica->r_mtx);
			break;
		}
	}
	MTX_LOCK(&replica->spec->rq_mtx);
	replica->refcount--;
	MTX_UNLOCK(&replica->spec->rq_mtx);
	return(NULL);
}

void update_volstate(spec_t *spec) {
	if(((spec->healthy_rcount + spec->degraded_rcount >= spec->consistency_factor) &&
		(spec->healthy_rcount >= 1))||
		(spec->healthy_rcount  + spec->degraded_rcount 
			>= MAX(spec->replication_factor - spec->consistency_factor + 1, spec->consistency_factor))) {
		spec->ready = true;
		pthread_cond_broadcast(&spec->rq_cond);
	} else {
		spec->ready = false;
	}
}
//TODO Use locks for accessing rcommq_ptr, common waitq and common pendingq
void clear_replica_cmd(spec_t *spec, replica_t *replica, rcmd_t *rep_cmd) {
	int i;
	rcommon_cmd_t *rcommq_ptr = rep_cmd->rcommq_ptr;
	int luworker_id = rcommq_ptr->luworker_id;
	rcommq_ptr->bitset &= ~(1 << replica->id);
	//TODO Add check for acks_received in read also, same as write
	if(rep_cmd->opcode == ZVOL_OPCODE_READ) {
		rcommq_ptr->state = CMD_EXECUTION_DONE;
		rcommq_ptr->status = -1;
		rcommq_ptr->completed = true;
		signal_luworker();
	} else if(rep_cmd->opcode == ZVOL_OPCODE_WRITE) {
		rcommq_ptr->ios_aborted++;
		if(rcommq_ptr->acks_recvd + rcommq_ptr->ios_aborted
				== rcommq_ptr->copies_sent) {
			for (i=1; i < rcommq_ptr->iovcnt + 1; i++) {
				xfree(rcommq_ptr->iov[i].iov_base);
			}
			rcommq_ptr->ios_aborted++;
			if(rcommq_ptr->state == CMD_ENQUEUED_TO_PENDINGQ) {
				TAILQ_REMOVE(&spec->rcommon_pendingq, rcommq_ptr, pending_cmd_next);
			} else if(rcommq_ptr->state == CMD_ENQUEUED_TO_WAITQ) {
				TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next);
			}
			rcommq_ptr->state = CMD_EXECUTION_DONE;
			if(rcommq_ptr->completed) {
				put_to_mempool(&rcommon_cmd_mempool, rcommq_ptr);
			} else {
				rcommq_ptr->completed = true;
				if (rcommq_ptr->acks_recvd < spec->consistency_factor) {
					rcommq_ptr->status = -1;
					signal_luworker();
				}
			}
		}
	}
}


int
remove_replica_from_list(spec_t *spec, int iofd) {
	replica_t *replica;
	int ios_aborted = 0;
	rcmd_t *rep_cmd = NULL;
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if(iofd == replica->iofd) {
			REPLICA_LOG("REMOVE REPLICA FROM LIST\n");
			MTX_LOCK(&replica->r_mtx);
			replica->state = REPLICA_ERRORED;
			//Empty waitq of replica
			while((rep_cmd = TAILQ_FIRST(&replica->waitq))) {
				clear_replica_cmd(spec, replica, rep_cmd);
				TAILQ_REMOVE(&replica->waitq, rep_cmd, rwait_cmd_next);
				ios_aborted++;
			}
			//Empty blockedq of replica
			while((rep_cmd = TAILQ_FIRST(&replica->blockedq))) {
				clear_replica_cmd(spec, replica, rep_cmd);
				TAILQ_REMOVE(&replica->blockedq, rep_cmd, rblocked_cmd_next);
				ios_aborted++;
			}
			replica->removed = true;
			pthread_cond_signal(&replica->r_cond);
			//TODO Update spec->degraded_rcount || spec->healthy_rcount over here
			//based on state of  replica when it gets disconnected
			TAILQ_REMOVE(&spec->rq, replica, r_next);
			release_replica_id(spec, replica->id);
			cleanup_replica(replica);
			MTX_UNLOCK(&replica->r_mtx);
			update_volstate(spec);
			break;
		}
	}
	MTX_UNLOCK(&spec->rq_mtx);
	REPLICA_LOG("%d IOs aborted for replica %d", ios_aborted, replica->id);
	return 0;
}

/*
 * reads on fd for 'len' and copies to 'data'
 * sets 'errorno' if read returns < 0
 * closes fd if read returns < 0 && errno != EAGAIN, and sets fd_closed
 * also closes fd if read return 0, i.e., EOF
 * returns number of bytes read
 */
int64_t
read_data(int fd, uint8_t *data, uint64_t len, int *errorno, int *fd_closed) {
	int64_t rc;
	uint64_t nbytes = 0;
	if (fd_closed != NULL)
		*fd_closed = 0;
	if (errorno != NULL)
		*errorno = 0;

	while(1) {
		rc = read(fd, data + nbytes, len - nbytes);
		if(rc < 0) {
			if (errorno != NULL)
				*errorno = errno;
			if(errno == EAGAIN || errno == EWOULDBLOCK)
				return nbytes;
			else {
				REPLICA_ERRLOG("received err %d on fd %d, closing it..\n", errno, fd);
				/*
				 * TODO: cleanup of replica need to be happen
				 */
				close(fd);
				if (fd_closed != NULL)
					*fd_closed = 1;
				return -1;
			}
		}
		else if (rc == 0) {
			REPLICA_ERRLOG("received EOF on fd %d, closing it..\n", fd);
			close(fd);
			if (fd_closed != NULL)
				*fd_closed = 1;
			break;
		}
		nbytes += rc;
		if(nbytes == len) {
			break;
		}
	}
	return nbytes;
}

void unblock_blocked_cmds(replica_t *replica)
{
	rcmd_t *cmd, *pending_cmd;
	bool cmd_blocked, blocked = true;
	TAILQ_FOREACH(pending_cmd, &replica->blockedq, rblocked_cmd_next) {
		blocked = false;
		TAILQ_FOREACH(cmd, &replica->waitq, rwait_cmd_next) {
			check_for_blockage();
			if(cmd_blocked == true) {
/*
				if(!cmd->ack_recvd) {
					blocked = true;
					break;
				}
*/
				blocked = true;
				break;
			}
		}
		if(blocked) {
			break;
		} else {
			/* we need to have similar check_for_blockage code for read_waitq */
			TAILQ_REMOVE(&replica->blockedq, pending_cmd, rblocked_cmd_next);
			TAILQ_INSERT_TAIL(&replica->sendq, pending_cmd, rsend_cmd_next);
			pthread_cond_signal(&replica->r_cond);
		}
	}
}

int
handle_read_resp(spec_t *spec, replica_t *replica)
{
	int luworker_id;
	bool io_found = false;
	rcmd_t *rep_cmd = NULL;
	rcommon_cmd_t *rcommq_ptr = NULL;
	zvol_io_hdr_t *io_rsp = replica->io_resp_hdr;
	void *data = replica->io_resp_data;

	MTX_LOCK(&replica->r_mtx);
	//Find IO in read queue, signal luworker, and dequeue
	TAILQ_FOREACH(rep_cmd, &replica->read_waitq, rread_cmd_next) {
		if(io_rsp->io_seq == rep_cmd->io_seq) {
			io_found = true;
			rep_cmd->ack_recvd = true;
			TAILQ_REMOVE(&replica->read_waitq, rep_cmd, rread_cmd_next);
			MTX_UNLOCK(&replica->r_mtx);
			rcommq_ptr = rep_cmd->rcommq_ptr;
			rcommq_ptr->status = 1;
			rcommq_ptr->data = data;
			rcommq_ptr->data_len = io_rsp->len;
			luworker_id = rcommq_ptr->luworker_id;
			put_to_mempool(&rcmd_mempool, rep_cmd);

			signal_luworker();

			MTX_LOCK(&spec->rcommonq_mtx);
			TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next);
			rcommq_ptr->state = CMD_EXECUTION_DONE;
			if(rcommq_ptr->completed) {
				MTX_UNLOCK(&spec->rcommonq_mtx);
				put_to_mempool(&rcommon_cmd_mempool, rcommq_ptr);
			} else {
				rcommq_ptr->completed = true;
				MTX_UNLOCK(&spec->rcommonq_mtx);
			}
			return 0;
		}
	}
	MTX_UNLOCK(&replica->r_mtx);
	if(!io_found) {
		/* a print or a stats */
		return -1;
	}
	return 0;
}

#define handle_consistency_met() {\
	rcommq_ptr->status = 1; \
	MTX_LOCK(&spec->rcommonq_mtx); \
	TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next); \
	if (rcommq_ptr->acks_recvd != rcommq_ptr->copies_sent) { \
		rcommq_ptr->state = CMD_ENQUEUED_TO_PENDINGQ; \
		TAILQ_INSERT_TAIL(&spec->rcommon_pendingq, rcommq_ptr, pending_cmd_next); \
	} else { \
		for (i=1; i < rcommq_ptr->iovcnt + 1; i++) { \
			xfree(rcommq_ptr->iov[i].iov_base); \
		} \
		rcommq_ptr->state = CMD_EXECUTION_DONE; \
		rcommq_ptr->completed = true; \
	} \
	MTX_UNLOCK(&spec->rcommonq_mtx); \
	signal_luworker(); \
}

#define handle_all_resp_recvd() { \
	MTX_LOCK(&spec->rcommonq_mtx); \
	for (i=1; i < rcommq_ptr->iovcnt + 1; i++) { \
		xfree(rcommq_ptr->iov[i].iov_base); \
	} \
	if(rcommq_ptr->state == CMD_ENQUEUED_TO_PENDINGQ) { \
		TAILQ_REMOVE(&spec->rcommon_pendingq, rcommq_ptr, pending_cmd_next); \
	} else if(rcommq_ptr->state == CMD_ENQUEUED_TO_WAITQ) { \
		TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next); \
	} \
	rcommq_ptr->state = CMD_EXECUTION_DONE; \
	if(rcommq_ptr->completed) { \
		put_to_mempool(&rcommon_cmd_mempool, rcommq_ptr); \
		rcommq_ptr = NULL; \
	} else { \
		rcommq_ptr->completed = true; \
		if (rcommq_ptr->acks_recvd < spec->consistency_factor) { \
			rcommq_ptr->status = -1; \
			signal_luworker(); \
		} \
	} \
	MTX_UNLOCK(&spec->rcommonq_mtx); \
}

int
handle_write_resp(spec_t *spec, replica_t *replica)
{
	int luworker_id, i;
	rcmd_t *rep_cmd = NULL;
	rcommon_cmd_t *rcommq_ptr;
	zvol_io_hdr_t *io_rsp = replica->io_resp_hdr;

	MTX_LOCK(&replica->r_mtx);
	TAILQ_FOREACH(rep_cmd, &replica->waitq, rwait_cmd_next) {
		if(rep_cmd->io_seq == io_rsp->io_seq) {
			break;
		}
	}
	if(rep_cmd == NULL) {
		REPLICA_ERRLOG("rep_cmd not found io_seq:%lu\n", io_rsp->io_seq);
		MTX_UNLOCK(&replica->r_mtx);
		return -1;
	}
	rep_cmd->ack_recvd = true;

	/* why do we need below check? */
	if(rep_cmd->wrio_seq == replica->least_recvd + 1) {
		while((rep_cmd = TAILQ_FIRST(&replica->waitq))) {
			rcommq_ptr = rep_cmd->rcommq_ptr;
			if(rep_cmd->ack_recvd == true) {
				replica->least_recvd++;
				MTX_UNLOCK(&replica->r_mtx);
				rcommq_ptr->acks_recvd++;
				luworker_id = rcommq_ptr->luworker_id;

				rcommq_ptr->bitset &= ~(1 << replica->id);
				if (rcommq_ptr->acks_recvd == spec->consistency_factor) {
					handle_consistency_met();
				} else if (rcommq_ptr->acks_recvd + rcommq_ptr->ios_aborted
						== rcommq_ptr->copies_sent) {
					handle_all_resp_recvd();
				}
				MTX_LOCK(&replica->r_mtx);
				TAILQ_REMOVE(&replica->waitq, rep_cmd, rwait_cmd_next);
				put_to_mempool(&rcmd_mempool, rep_cmd);
				rep_cmd = NULL;
			} else {
				break;
			}
		}
	}
	MTX_UNLOCK(&replica->r_mtx);
	return 0;
}

typedef struct read_event {
	int fd;
	int *state;
	zvol_io_hdr_t *resp_hdr;
	void **resp_data;
	int *read_count;
} read_event_t;

int read_io_resp(spec_t *spec, replica_t *replica, read_event_t *revent);

//Receive IO responses in this thread
void *
replica_receiver(void *arg) {
	spec_t *spec = (spec_t *)arg;
	replica_t *replica;
	int i, event_count, ret;
	struct epoll_event event, *events;
	read_event_t revent;

	//Create a new epoll epfd
	int epfd = spec->receiver_epfd;
	events = calloc(MAXEVENTS, sizeof(event));
	while(1) {
		if(spec->degraded_rcount + spec->healthy_rcount == 0) {
			MTX_LOCK(&spec->rq_mtx);
			if(spec->degraded_rcount + spec->healthy_rcount == 0) {
				//Wait until at least one IO connection has been made to a registered replica
				pthread_cond_wait(&spec->rq_cond, &spec->rq_mtx);
			}
			MTX_UNLOCK(&spec->rq_mtx);
		}
		//Wait for events on all iofds
		event_count = epoll_wait(epfd, events, MAXEVENTS, -1);
		for(i=0; i< event_count; i++) {
			//Remove the replica from queue
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN))) {
				remove_replica_from_list(spec, events[i].data.fd);
				REPLICA_LOG("epoll error\n");
				continue;
			} else {
				MTX_LOCK(&spec->rq_mtx);
				TAILQ_FOREACH(replica, &spec->rq, r_next) {
					if(events[i].data.fd == replica->iofd) {
						replica->refcount++;
						break;
					}
				}
				MTX_UNLOCK(&spec->rq_mtx);

				revent.fd = events[i].data.fd;
				revent.state = &(replica->io_state);
				revent.resp_hdr = replica->io_resp_hdr;
				revent.resp_data = &(replica->io_resp_data);
				revent.read_count = &(replica->io_read);

				ret = read_io_resp(spec, replica, &revent);
				if (ret == -1) {
					cleanup_replica(replica);
					continue;
				}
				if (ret != 0) {
					MTX_LOCK(&replica->r_mtx);
					unblock_blocked_cmds(replica);
					MTX_UNLOCK(&replica->r_mtx);
				}
				MTX_LOCK(&spec->rq_mtx);
				replica->refcount--;
				MTX_UNLOCK(&spec->rq_mtx);
			}
		}
	}
	close(epfd);
	return NULL;
}

void cleanup_replica(replica_t *replica) {
	if(replica->mgmt_fd != -1)  {
		close(replica->mgmt_fd);
		replica->mgmt_fd = -1;
	}
	if(replica->iofd != -1)  {
		close(replica->iofd);
		replica->iofd = -1;
	}
	if(replica->sender_epfd != -1) {
		close(replica->sender_epfd);
		replica->sender_epfd = -1;
	}
	if(--replica->refcount != 0) {
		return;
	}
	if(replica->io_resp_hdr) {
		free(replica->io_resp_hdr);
	}
	if(replica->ip) {
		free(replica->ip);
	}
	free(replica->mgmt_ack);
	free(replica->mgmt_ack_data);
	free(replica);
}

/* creates replica entry and adds to spec's rwaitq list after creating mgmt connection */
replica_t *
create_replica_entry(spec_t *spec, int mgmt_fd)
{
	replica_t *replica = (replica_t *)malloc(sizeof(replica_t));
	replica->mgmt_fd = mgmt_fd;
	replica->refcount = 0;
	replica->iofd = -1;
	replica->io_resp_hdr = NULL;
	replica->ip = NULL;
	replica->mgmt_io_state = READ_IO_RESP_HDR;
	replica->mgmt_io_read = 0;
	replica->mgmt_ack = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));
	replica->mgmt_ack_data = (mgmt_ack_data_t *)malloc(sizeof(mgmt_ack_data_t));
	MTX_LOCK(&spec->rq_mtx);
	replica->id = allocate_replica_id(spec);
	if(replica->id == -1) {
		cleanup_replica(replica);
		MTX_UNLOCK(&spec->rq_mtx);
		return NULL;
	}
	TAILQ_INSERT_TAIL(&spec->rwaitq, replica, r_waitnext);
	MTX_UNLOCK(&spec->rq_mtx);
	return replica;
}

/*
 * updates replica entry with IP/port
 * removes from spec's rwaitq, adds to rq after data connection
 * starts sender thread to send IOs to replica
 */
replica_t *
update_replica_entry(spec_t *spec, replica_t *replica, int iofd, char *replicaip, int replica_port) {

	int rc;
	pthread_t replica_sender_thread;
	rc = pthread_mutex_init(&replica->r_mtx, NULL); //check
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_mutex_init() failed errno:%d\n", errno);
		goto cleanup;
	}
	rc = pthread_cond_init(&replica->r_cond, NULL); //check
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_cond_init() failed errno:%d\n", errno);
		rc = pthread_mutex_destroy(&replica->r_mtx);
		if(rc != 0) {
			ISTGT_ERRLOG("mutex_destroy() failed\n");
		}
		goto cleanup;
	}
	TAILQ_INIT(&replica->sendq);
	TAILQ_INIT(&replica->waitq);
	TAILQ_INIT(&replica->read_waitq);
	TAILQ_INIT(&replica->blockedq);
	replica->iofd = iofd;
	replica->ip = malloc(strlen(replicaip)+1);
	strcpy(replica->ip, replicaip);
	replica->port = replica_port;
	replica->state = REPLICA_DEGRADED;
	replica->least_recvd = 0;
	replica->wrio_seq = 0;
	replica->rrio_seq = 0;
	replica->spec = spec;
	replica->io_resp_hdr = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));
	replica->io_resp_data = NULL;
	replica->io_state = READ_IO_RESP_HDR;
	replica->io_read = 0;
	replica->removed = false;
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_REMOVE(&spec->rwaitq, replica, r_waitnext);
	spec->degraded_rcount++;
	TAILQ_INSERT_TAIL(&spec->rq, replica, r_next);
	if(spec->degraded_rcount + spec->healthy_rcount == 1)
		pthread_cond_signal(&spec->rq_cond);
	update_volstate(spec);
	MTX_UNLOCK(&spec->rq_mtx);
	zvol_io_hdr_t *rio;
	rio = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));
	rio->opcode = ZVOL_OPCODE_HANDSHAKE;
	rio->io_seq = 0;
	rio->offset = 0;
	rio->len    = strlen(spec->lu->volname);
	write(replica->iofd, rio, sizeof(zvol_io_hdr_t));
	write(replica->iofd, spec->lu->volname, strlen(spec->lu->volname));
	free(rio);
	rio = NULL;
	struct epoll_event event;
	event.data.fd = replica->iofd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(spec->receiver_epfd, EPOLL_CTL_ADD, iofd, &event);
	if (rc == -1) {
		return NULL;
	}
	replica->sender_epfd = epoll_create1(0);
	event.data.fd = replica->iofd;
	event.events = EPOLLOUT;
	rc = epoll_ctl(replica->sender_epfd, EPOLL_CTL_ADD, replica->iofd, &event);
	if (rc == -1) {
		REPLICA_LOG("epoll_ctl_add failed errno:%d\n", errno);
		return NULL;
	}
	rc = pthread_create(&replica_sender_thread, NULL, &replica_sender,
			(void *)replica);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create(replicator_thread) failed\n");
		return NULL;
	}
	return replica;
cleanup:
	cleanup_replica(replica);
	return NULL;
}

/*
 * forms data connection to replica, updates replica entry
 */
int
zvol_handshake(spec_t *spec, replica_t *replica)
{
	int rc, iofd;
	mgmt_ack_data_t *mgmt_ack_data = replica->mgmt_ack_data;

	if(strcmp(mgmt_ack_data->volname, spec->lu->volname) != 0) {
		REPLICA_ERRLOG("volname %s not matching with spec %s volname\n",
		    mgmt_ack_data->volname, spec->lu->volname);
		exit(EXIT_FAILURE);
	}

	if((iofd = cstor_ops.conn_connect(mgmt_ack_data->ip, mgmt_ack_data->port)) < 0) {
		REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
		exit(EXIT_FAILURE);
	}

	rc = make_socket_non_blocking(iofd);
	if (rc == -1) {
		REPLICA_ERRLOG("make_socket_non_blocking() failed errno:%d\n", errno);
		exit(EXIT_FAILURE);
	}
	update_replica_entry(spec, replica, iofd, mgmt_ack_data->ip, mgmt_ack_data->port);

	return 0;
}

/*
 * accepts (mgmt) connections on which handshake and other management IOs are sent
 * sends handshake IO to start handshake on accepted (mgmt) connection
 */
void
accept_mgmt_conns(int epfd, int sfd) {
	struct epoll_event event;
	int rc, rcount=0;
	spec_t *spec;
	int mgmtfd[MAXREPLICA];
	char *buf = malloc(BUFSIZE);
	zvol_op_code_t mgmt_opcode;
	int mgmt_fd, data_len = 0;
	replica_t *replica;

	while (1) {
		struct sockaddr saddr;
		socklen_t slen;
		char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
		slen = sizeof(saddr);
		mgmt_fd = accept(sfd, &saddr, &slen);
		if (mgmt_fd == -1) {
			if((errno != EAGAIN) && (errno != EWOULDBLOCK))
				REPLICA_ERRLOG("accept() failed on fd %d, errno:%d.. better to restart listener..", sfd, errno);
			break;
		}

		rc = getnameinfo(&saddr, slen,
				hbuf, sizeof(hbuf),
				sbuf, sizeof(sbuf),
				NI_NUMERICHOST | NI_NUMERICSERV);
		if (rc == 0) {
			mgmtfd[rcount] = mgmt_fd;
			rcount++;
			REPLICA_LOG("Accepted connection on descriptor %d "
					"(host=%s, port=%s)\n", mgmt_fd, hbuf, sbuf);
		}
		rc = make_socket_non_blocking(mgmt_fd);
		if (rc == -1) {
			REPLICA_ERRLOG("make_socket_non_blocking() failed on fd %d, errno:%d.. closing it..", mgmt_fd, errno);
			close(mgmt_fd);
			continue;
		}

		MTX_LOCK(&specq_mtx);
		TAILQ_FOREACH(spec, &spec_q, spec_next) {
			data_len = snprintf(buf, BUFSIZE, "%s", spec->lu->volname) + 1;
			break;//For now only one spec is supported per controller
		}
		MTX_UNLOCK(&specq_mtx);
		mgmt_opcode = ZVOL_OPCODE_HANDSHAKE;
		rc = send_mgmtio(mgmt_fd, mgmt_opcode, buf, data_len);
		if (rc == -1) {
			REPLICA_ERRLOG("send mgmtIO failed on fd %d, closing it..", mgmt_fd);
			close(mgmt_fd);
			continue;
		}

		event.data.fd = mgmt_fd;
		event.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLET;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmt_fd, &event);
		if(rc == -1) {
			REPLICA_ERRLOG("epoll_ctl() failed on fd %d, errno:%d.. closing it..", mgmt_fd, errno);
			close(mgmt_fd);
			continue;
		}
		if((replica = create_replica_entry(spec, mgmt_fd)) == NULL) {
			continue;
		}
	}
	free(buf);
}

/*
 * breaks if fd is closed or drained recv buf of fd
 * updates amount of data read to continue next time
 */
#define CHECK_AND_ADD_BREAK_IF_PARTIAL(fd_closed, io_read, count, reqlen) \
{ \
	if (fd_closed == 1) \
		break; \
	if (count != reqlen) { \
		(io_read) += count; \
		break; \
	} \
}

replica_t *
get_replica(int mgmt_fd, spec_t **s)
{
	replica_t *replica = NULL;
	spec_t *spec = NULL;
	*s = NULL;
	MTX_LOCK(&specq_mtx);
	TAILQ_FOREACH(spec, &spec_q, spec_next) {
		MTX_LOCK(&spec->rq_mtx);
		TAILQ_FOREACH(replica, &spec->rq, r_next) {
			if(replica->mgmt_fd == mgmt_fd) {
				*s = spec;
				break;
			}
		}
		if (replica != NULL) {
			MTX_UNLOCK(&spec->rq_mtx);
			break;
		}
		TAILQ_FOREACH(replica, &spec->rwaitq, r_waitnext) {
			if(replica->mgmt_fd == mgmt_fd) {
				*s = spec;
				break;
			}
		}
		MTX_UNLOCK(&spec->rq_mtx);
		if (replica != NULL)
			break;
	}
	MTX_UNLOCK(&specq_mtx);
	return replica;
}

/*
 * initial state is read io_resp_hdr, which reads IO response.
 * it transitions to read io_resp_data based on length in hdr.
 * once data is handled, it goes to read hdr which can be new response.
 * this goes on until EAGAIN or connection gets closed.
 */
int
read_io_resp(spec_t *spec, replica_t *replica, read_event_t *revent)
{
	int fd = revent->fd;
	int *state = revent->state;
	zvol_io_hdr_t *resp_hdr = revent->resp_hdr;
	void **resp_data = revent->resp_data;
	int *read_count = revent->read_count;
	uint64_t reqlen, count;
	int errorno = 0, fd_closed = 0;
	int donecount = 0;

	switch(*state) {
		case READ_IO_RESP_HDR:
read_io_resp_hdr:
			reqlen = sizeof (zvol_io_hdr_t) - (*read_count);
			count = read_data(fd, ((uint8_t *)resp_hdr) + (*read_count), reqlen, &errorno, &fd_closed);
			CHECK_AND_ADD_BREAK_IF_PARTIAL(fd_closed, (*read_count), count, reqlen);

			*read_count = 0;
			if (resp_hdr->len != 0) {
			/* this will not be NULL for mgmt_ack IO */
				if ((*resp_data) == NULL)
					(*resp_data) = malloc(resp_hdr->len);
			}
			*state = READ_IO_RESP_DATA;
			if (errorno == EAGAIN || errorno == EWOULDBLOCK)
				break;
			goto read_io_resp_data;
		case READ_IO_RESP_DATA:
read_io_resp_data:
			reqlen = resp_hdr->len - (*read_count);
			if (reqlen != 0) {
				count = read_data(fd, ((uint8_t *)(*resp_data)) + (*read_count), reqlen, &errorno, &fd_closed);
				CHECK_AND_ADD_BREAK_IF_PARTIAL(fd_closed, (*read_count), count, reqlen);
			}

			*read_count = 0;
			switch (resp_hdr->opcode) {
				case ZVOL_OPCODE_READ:
					handle_read_resp(spec, replica);
					replica->io_resp_data = NULL;
					break;
				case ZVOL_OPCODE_WRITE:
					handle_write_resp(spec, replica);
					break;
				case ZVOL_OPCODE_HANDSHAKE:
					if(resp_hdr->len != sizeof (mgmt_ack_data_t))
						REPLICA_ERRLOG("mgmt_ack_len %lu not matching with size of mgmt_ack_data..\n",
						    resp_hdr->len);

					/* dont process handshake on data connection */
					if (fd != replica->iofd)
						zvol_handshake(spec, replica);
					break;
			}
			donecount++;
			*state = READ_IO_RESP_HDR;
			if (errorno == EAGAIN || errorno == EWOULDBLOCK)
				break;
			goto read_io_resp_hdr;
	}
	if(fd_closed == 1) {
		return -1;
	} else {
		return donecount;
	}
}

/*
 * reads data on management fd until EAGAIN
 */
void
handle_read_data_event(int fd)
{
	spec_t *spec = NULL;
	read_event_t revent;

	replica_t *replica = get_replica(fd, &spec);
	if (replica == NULL || spec == NULL) {
		REPLICA_ERRLOG("error in getting replica %p/spec %p for fd %d..\n", replica, spec, fd);
		return;
	}

	revent.fd = fd;
	revent.state = &(replica->mgmt_io_state);
	revent.resp_hdr = replica->mgmt_ack;
	revent.resp_data = (void **)(&(replica->mgmt_ack_data));
	revent.read_count = &(replica->mgmt_io_read);

	read_io_resp(spec, replica, &revent);
}

/*
 * initializes replication
 * - by starting listener to accept mgmt connections
 * - reads data on accepted mgmt connection
 */
void *
init_replication(void *arg __attribute__((__unused__))) {
	struct epoll_event event, *events;
	int rc, sfd, event_count, i;
	int64_t epfd;

	//Create a listener for management connections from replica
	const char* externalIP = getenv("externalIP");
	if((sfd = cstor_ops.conn_listen(externalIP, 6060, 32)) < 0) {
		REPLICA_LOG("conn_listen() failed, errorno:%d sfd:%d", errno, sfd);
		exit(EXIT_FAILURE);
	}
	epfd = epoll_create1(0);
	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET | EPOLLERR | EPOLLHUP;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, errrno:%d", errno);
		exit(EXIT_FAILURE);
	}
	events = calloc(MAXEVENTS, sizeof(event));

	while (1) {
		//Wait for management connections(on sfd) and management commands(on mgmt_rfds[]) from replicas
		event_count = epoll_wait(epfd, events, MAXEVENTS, -1);
		if (event_count < 0) {
			if (errno == EINTR)
				continue;
			REPLICA_ERRLOG("epoll_wait ret %d err %d.. better to restart listener\n", event_count, errno);
			continue;
		}
		for(i=0; i< event_count; i++) {
			if (!(events[i].events & EPOLLIN)) {
				REPLICA_LOG("epoll err event %d on fd %d.. closing it..\n", events[i].events,
				    events[i].data.fd);
				close(events[i].data.fd);
			} else if (events[i].data.fd == sfd) {
				//Accept management connections from replicas and add the replicas to replica queue
				accept_mgmt_conns(epfd, sfd);
			} else {
				handle_read_data_event(events[i].data.fd);
			}
		}
	}
	free (events);
	close (sfd);
	return EXIT_SUCCESS;
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

int
initialize_replication() {
	//Global initializers for replication library
	int rc;
	TAILQ_INIT(&spec_q);
	rc = pthread_mutex_init(&specq_mtx, NULL);
	if (rc != 0) {
		perror("specq_init failed");
		return -1;
	}
	return 0;
}

int
initialize_volume(spec_t *spec) {
	int rc;
	pthread_t replicator_thread, replica_receiver_thread;
	TAILQ_INIT(&spec->rcommon_sendq);
	TAILQ_INIT(&spec->rcommon_wait_readq);
	TAILQ_INIT(&spec->rcommon_waitq);
	TAILQ_INIT(&spec->rcommon_pendingq);
	TAILQ_INIT(&spec->rq);
	TAILQ_INIT(&spec->rwaitq);
	spec->replica_ids = 0;
	spec->replication_factor = spec->lu->replication_factor;
	spec->consistency_factor = spec->lu->consistency_factor;
	spec->healthy_rcount = 0;
	spec->degraded_rcount = 0;
	spec->ready = false;
	spec->receiver_epfd = epoll_create1(0);
	if(spec->receiver_epfd < 0)
		return -1;
	rc = pthread_mutex_init(&spec->rcommonq_mtx, NULL); //check
	if (rc != 0) {
		perror("rq_mtx_init failed");
		return -1;
	}
	rc = pthread_cond_init(&spec->rcommonq_cond, NULL); //check
	if (rc != 0) {
		perror("rq_mtx_init failed");
		return -1;
	}
	rc = pthread_mutex_init(&spec->rq_mtx, NULL); //check
	if (rc != 0) {
		perror("rq_mtx_init failed");
		return -1;
	}
	pthread_cond_init(&spec->rq_cond, NULL); //check
	if (rc != 0) {
		perror("rq_cond_init failed");
		return -1;
	}
	rc = pthread_create(&replicator_thread, NULL, &replicator,
			(void *)spec);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create(replicator_thread) failed\n");
		return -1;
	}
	rc = pthread_create(&replica_receiver_thread, NULL, &replica_receiver,
			(void *)spec);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create(replicator_thread) failed\n");
		return -1;
	}

	MTX_LOCK(&specq_mtx);
	TAILQ_INSERT_TAIL(&spec_q, spec, spec_next);
	MTX_UNLOCK(&specq_mtx);
	return 0;
}
//When all the replicas are up, make replicas as RW and change state of istgt to REAL;


int
initialize_replication_mempool(bool should_fail) {
	int rc = 0;

	rc = init_mempool(&rcmd_mempool, rcmd_mempool_count, sizeof (rcmd_t), 0,
	    "rcmd_mempool", NULL, NULL, NULL);
	if (rc == -1) {
		ISTGT_ERRLOG("Failed to create mempool for command\n");
		goto error;
	} else if (rc) {
		ISTGT_NOTICELOG("rcmd mempool initialized with %u entries\n",
		    rcmd_mempool.length);
		if (should_fail) {
			goto error;
		}
		rc = 0;
	}

	rc = init_mempool(&rcommon_cmd_mempool, rcommon_cmd_mempool_count,
	    sizeof (rcommon_cmd_t), 0, "rcommon_mempool", NULL, NULL, NULL);
	if (rc == -1) {
		ISTGT_ERRLOG("Failed to create mempool for command\n");
		goto error;
	} else if (rc) {
		ISTGT_NOTICELOG("rcmd mempool initialized with %u entries\n",
		    rcommon_cmd_mempool.length);
		if (should_fail) {
			goto error;
		}
		rc = 0;
	}

	goto exit;

error:
	if (rcmd_mempool.ring)
		destroy_mempool(&rcmd_mempool);
	if (rcommon_cmd_mempool.ring)
		destroy_mempool(&rcommon_cmd_mempool);

exit:
	return rc;
}

int
destroy_relication_mempool(void) {
	int rc = 0;

	rc = destroy_mempool(&rcmd_mempool);
	if (rc) {
		ISTGT_ERRLOG("Failed to destroy mempool for rcmd.. err(%d)\n",
		    rc);
		goto exit;
	}

	rc = destroy_mempool(&rcommon_cmd_mempool);
	if (rc) {
		ISTGT_ERRLOG("Failed to destroy mempool for rcommon_cmd.."
		    " err(%d)\n", rc);
		goto exit;
	}

exit:
	return rc;
}
