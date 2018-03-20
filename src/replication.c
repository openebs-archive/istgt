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
cstor_conn_ops_t cstor_ops = {
	.conn_listen = replication_listen,
	.conn_connect = replication_connect,
};

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
	rcmd = malloc(sizeof(rcmd_t));\
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

#define check_read_error() {\
	if (count < 0)\
	{\
		if (errno != EAGAIN)\
		{\
			REPLICA_ERRLOG("read error");\
			close (events[i].data.fd);\
			break;\
		} else {\
			continue;\
		}\
	} else if(count == 0) {\
		close (events[i].data.fd);\
		break;\
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
	}\
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
		rc = writev(fd, iovec, 2);//Review iovec in this line
		if (rc < 0) {
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
		TAILQ_INSERT_TAIL(&spec->rcommon_waitq, cmd, wait_cmd_next);
		//Check for blockage in rcommon_pendingq and set corresponding blocked replica bits
		bitset = 0;
		TAILQ_FOREACH(pending_cmd, &spec->rcommon_pendingq, pending_cmd_next) {
			cmd_blocked = false;
			check_for_blockage();
			if(cmd_blocked == true) {
				bitset |= pending_cmd->bitset;
			}
		}

		//Enqueue to individual replica cmd queues and send on the respective fds
		read_cmd_sent = false;
		TAILQ_FOREACH(replica, &spec->rq, r_next) {
			if(replica == NULL) {
				REPLICA_LOG("Replica not present");
				exit(EXIT_FAILURE);
			}
			//Create an entry for replica queue
			build_rcmd();
			MTX_LOCK(&replica->r_mtx);
			if(replica->removed) {
				MTX_UNLOCK(&replica->r_mtx);
				continue;
			}
			cmd->bitset |= (1 << replica->id);
			if(bitset & (1 << replica->id)) {
				TAILQ_INSERT_TAIL(&replica->blockedq, rcmd, rblocked_cmd_next);
			} else {
				TAILQ_INSERT_TAIL(&replica->sendq, rcmd, rsend_cmd_next);
				if(rcmd->opcode == ZVOL_OPCODE_READ) {
					read_cmd_sent = true;
				}
			}
			pthread_cond_signal(&replica->r_cond);
			MTX_UNLOCK(&replica->r_mtx);
			if((rcmd->opcode == ZVOL_OPCODE_READ) && read_cmd_sent) {
				break;
			}
		}
		MTX_UNLOCK(&spec->rcommonq_mtx);
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
	int epfd, rc;
	rcommon_cmd_t *cmd = NULL;
	rcmd_t *rcmd = NULL;
	struct epoll_event event;
	rcommon_cmd_t *rcommq_ptr;//REMOVE	
	epfd = epoll_create1(0);

	event.data.fd = replica->iofd;
	event.events = EPOLLOUT;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, replica->iofd, &event);
	if (rc == -1) {
		REPLICA_LOG("ERROR\n");
		exit(1);
	}	
	while(1) {
		MTX_LOCK(&replica->r_mtx);
dequeue_rsendq:
		rcmd = TAILQ_FIRST(&replica->sendq);
		if(!rcmd) {
			pthread_cond_wait(&replica->r_cond, &replica->r_mtx);
			if(replica->removed == true) {
				MTX_UNLOCK(&replica->r_mtx);
				close(epfd);
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
		rc = sendio(epfd, replica->iofd, cmd, rcmd);
		if(rc < 0) {
			remove_replica_from_list(replica->spec, replica->iofd);
			close(epfd);
			break;
		}
	}
	return(NULL);
}

int
update_replica_list(int epfd, spec_t *spec, int replica_count) {
	replica_t *replica;
	struct epoll_event event;
	int rc;
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if(replica->state == ADDED_TO_EPOLL) {
			continue;
		} else if(replica->state == ADDED_TO_SPEC){
			event.data.fd = replica->iofd;
			event.events = EPOLLIN | EPOLLET;
			rc = epoll_ctl(epfd, EPOLL_CTL_ADD, replica->iofd, &event);
			if (rc == -1) {
				MTX_UNLOCK(&spec->rq_mtx);
				return -1;
			}
			replica->state = ADDED_TO_EPOLL;
			replica_count++;
		} else if(replica->state == NEED_REMOVAL_FROM_EPOLL) {
			rc = epoll_ctl(epfd, EPOLL_CTL_DEL, replica->iofd, &event);
			if (rc == -1) {
				MTX_UNLOCK(&spec->rq_mtx);
				return -1;
			}
			replica->state = REMOVED_FROM_EPOLL;
		}
	}
	MTX_UNLOCK(&spec->rq_mtx);
	return replica_count;
}

int
remove_replica_from_list(spec_t *spec, int iofd) {
	replica_t *replica;
	int luworker_id;
	rcmd_t *rep_cmd = NULL;
	rcommon_cmd_t *rcommq_ptr;
	MTX_LOCK(&spec->rq_mtx);
	TAILQ_FOREACH(replica, &spec->rq, r_next) {
		if(iofd == replica->iofd) {
			REPLICA_LOG("REMOVE REPLICA FROM LIST\n");
			MTX_LOCK(&replica->r_mtx);
			//Empty waitq of replica
			while((rep_cmd = TAILQ_FIRST(&replica->waitq))) {
				rcommq_ptr = rep_cmd->rcommq_ptr;
				luworker_id = rcommq_ptr->luworker_id;
				if(rep_cmd->opcode == ZVOL_OPCODE_READ) {
					rcommq_ptr->status = -1;
					MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
					pthread_cond_signal(&spec->luworker_rcond[luworker_id]); 
					MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);
				}else if(rcommq_ptr->acks_recvd == spec->replica_count - 1) {
					rcommq_ptr->status = -1;
					MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
					pthread_cond_signal(&spec->luworker_rcond[luworker_id]); 
					MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);
				} else {
					rcommq_ptr->acks_recvd++;
				}
				TAILQ_REMOVE(&replica->waitq, rep_cmd, rwait_cmd_next);
			}
			//Empty blockedq of replica
			while((rep_cmd = TAILQ_FIRST(&replica->blockedq))) {
				rcommq_ptr = rep_cmd->rcommq_ptr;
				luworker_id = rcommq_ptr->luworker_id;
				if(rep_cmd->opcode == ZVOL_OPCODE_READ) {
					rcommq_ptr->status = -1;
					MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
					pthread_cond_signal(&spec->luworker_rcond[luworker_id]); 
					MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);
				}else if(rcommq_ptr->acks_recvd == spec->replica_count - 1) {
					rcommq_ptr->status = -1;
					MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
					pthread_cond_signal(&spec->luworker_rcond[luworker_id]); 
					MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);
				} else {
					rcommq_ptr->acks_recvd++;
				}
				TAILQ_REMOVE(&replica->blockedq, rep_cmd, rblocked_cmd_next);
			}
			replica->removed = true;
			pthread_cond_signal(&replica->r_cond);
			MTX_UNLOCK(&replica->r_mtx);
			spec->replica_count--;
			if(spec->replica_count == 0){
				spec->ready = false;
			}
			TAILQ_REMOVE(&spec->rq, replica, r_next);
			close(replica->iofd);
		}
	}
	MTX_UNLOCK(&spec->rq_mtx);
	return 0;
}

int64_t
read_data(int fd, uint8_t *data, uint64_t len) {
	int64_t rc;
	uint64_t nbytes = 0;
	while((rc = read(fd, data + nbytes, len - nbytes))) {
		if(rc < 0) {
			if(nbytes > 0 && errno == EAGAIN) {
				return nbytes;
			} else {
				return -1;
			}
		}
		nbytes += rc;
		if(nbytes == len) {
			break;
		}
	}
	return nbytes;
}

#define read_io_resp() {\
	if(replica->read_rem_data) {\
		count = read_data(events[i].data.fd, (uint8_t *)replica->io_rsp_data + replica->recv_len, replica->total_len - replica->recv_len);\
		if(count < 0 && errno == EAGAIN) {\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else if(((uint64_t)count < (int64_t)replica->total_len - replica->recv_len) && errno == EAGAIN) {\
			replica->recv_len += count;\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else {\
			replica->recv_len = 0;\
			replica->total_len = 0;\
			replica->read_rem_data = false;\
			goto execute_io;\
		}\
	} else if(replica->read_rem_hdr) {\
		count = read_data(events[i].data.fd, (uint8_t *)replica->io_rsp + replica->recv_len, replica->total_len - replica->recv_len);\
		if(count < 0 && errno == EAGAIN) {\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else if(((uint64_t)count < replica->total_len - replica->recv_len) && errno == EAGAIN) {\
			replica->recv_len += count;\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else {\
			replica->read_rem_hdr = false;\
			replica->recv_len = 0;\
			replica->total_len = 0;\
		}\
	} else {\
		count = read_data(events[i].data.fd, (uint8_t *)replica->io_rsp, rsp_len);\
		if((count < 0) && (errno == EAGAIN)) {\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else if(((uint64_t)count < rsp_len) && (errno == EAGAIN)) {\
			replica->read_rem_hdr = true;\
			replica->recv_len = count;\
			replica->total_len = rsp_len;\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		}\
		replica->read_rem_hdr = false;\
	}\
	if(replica->io_rsp->opcode == ZVOL_OPCODE_HANDSHAKE) {\
		MTX_UNLOCK(&replica->r_mtx);\
		continue;\
	}\
	if(replica->io_rsp->opcode == ZVOL_OPCODE_READ) {\
		read_io = true;\
		data = malloc(replica->io_rsp->len);\
		replica->io_rsp_data = data;\
		count = read_data(events[i].data.fd, data, replica->io_rsp->len);\
		if (count == -1 && errno == EAGAIN) {\
			replica->read_rem_data = true;\
			replica->recv_len = 0;\
			replica->total_len = replica->io_rsp->len;\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		} else if((uint64_t)count < replica->io_rsp->len && errno == EAGAIN) {\
			replica->read_rem_data = true;\
			replica->recv_len = count;\
			replica->total_len = replica->io_rsp->len;\
			MTX_UNLOCK(&replica->r_mtx);\
			break;\
		}\
		replica->read_rem_data = false;\
	} else {\
		read_io = false;\
	}\
}

void unblock_blocked_cmds(replica_t *replica) {
	rcmd_t *cmd, *pending_cmd;
	bool cmd_blocked, blocked = true;
	TAILQ_FOREACH(pending_cmd, &replica->blockedq, rblocked_cmd_next) {
		TAILQ_FOREACH(cmd, &replica->waitq, rwait_cmd_next) {
			cmd_blocked = false;
			check_for_blockage();
			if(cmd_blocked == true) {
				if(!cmd->ack_recvd) {
					blocked = true;
					break;
				}
			} else {
				blocked = true;
			}
		}
		if(blocked) {
			break;
		} else {
			TAILQ_REMOVE(&replica->blockedq, pending_cmd, rblocked_cmd_next);
			TAILQ_INSERT_TAIL(&replica->sendq, pending_cmd, rsend_cmd_next);
			pthread_cond_signal(&replica->r_cond);
		}
	}
}
int
handle_read_resp(spec_t *spec, replica_t *replica, zvol_io_hdr_t *io_rsp, void *data) { 
	int luworker_id;
	bool io_found = false;
	rcmd_t *rep_cmd = NULL;
	rcommon_cmd_t *rcommq_ptr = NULL;
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
			free(rep_cmd);

			MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
			pthread_cond_signal(&spec->luworker_rcond[luworker_id]);
			MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);

			MTX_LOCK(&spec->rcommonq_mtx);
			TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next);
			if(rcommq_ptr->completed == 1) {
				free(rcommq_ptr);
			} else {
				rcommq_ptr->completed = 1;
			}
			MTX_UNLOCK(&spec->rcommonq_mtx);
			MTX_LOCK(&replica->r_mtx);
			break;
		}
	}
	MTX_UNLOCK(&replica->r_mtx);
	if(!io_found) {
		return -1;
	}
	return 0;
}

int
handle_write_resp(spec_t *spec, replica_t *replica, zvol_io_hdr_t *io_rsp) {
	int luworker_id, i;
	rcmd_t *rep_cmd = NULL;
	rcommon_cmd_t *rcommq_ptr;
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
	if(rep_cmd->wrio_seq == replica->least_recvd + 1) {
		while((rep_cmd = TAILQ_FIRST(&replica->waitq))) {
			rcommq_ptr = rep_cmd->rcommq_ptr;
			if(rep_cmd->ack_recvd == true) {
				replica->least_recvd++;
				MTX_UNLOCK(&replica->r_mtx);
				rcommq_ptr->acks_recvd++;
				luworker_id = rcommq_ptr->luworker_id;

				rcommq_ptr->bitset &= ~(1 << replica->id);
				if (rcommq_ptr->acks_recvd == spec->consistency_count) {
					rcommq_ptr->status = 1;
					//Transfer cmd from waitq to pendingq and wakeup luworker
					MTX_LOCK(&spec->rcommonq_mtx);
					TAILQ_REMOVE(&spec->rcommon_waitq, rcommq_ptr, wait_cmd_next);
					if (rcommq_ptr->acks_recvd != spec->replica_count) {
						TAILQ_INSERT_TAIL(&spec->rcommon_pendingq, rcommq_ptr, pending_cmd_next);
					} else {
						for (i=1; i < rcommq_ptr->iovcnt + 1; i++) {
							xfree(rcommq_ptr->iov[i].iov_base);
						}
						rcommq_ptr->completed = 1;
					}
					MTX_UNLOCK(&spec->rcommonq_mtx);

					MTX_LOCK(&spec->luworker_rmutex[luworker_id]);
					pthread_cond_signal(&spec->luworker_rcond[luworker_id]);
					MTX_UNLOCK(&spec->luworker_rmutex[luworker_id]);

				} else if (rcommq_ptr->acks_recvd == spec->replica_count) {
					MTX_LOCK(&spec->rcommonq_mtx);
					TAILQ_REMOVE(&spec->rcommon_pendingq, rcommq_ptr, pending_cmd_next);
					for (i=1; i < rcommq_ptr->iovcnt + 1; i++) {
						xfree(rcommq_ptr->iov[i].iov_base);
					}
					if(rcommq_ptr->completed == 1) {
						free(rcommq_ptr);
						rcommq_ptr = NULL;
					}
					MTX_UNLOCK(&spec->rcommonq_mtx);
				}
				MTX_LOCK(&replica->r_mtx);
				TAILQ_REMOVE(&replica->waitq, rep_cmd, rwait_cmd_next);
				free(rep_cmd);
				rep_cmd = NULL;
			} else {
				break;
			}
		}
	}
	MTX_UNLOCK(&replica->r_mtx);
	return 0;
}
//Receive IO responses in this thread
void *
replica_receiver(void *arg) {
	spec_t *spec = (spec_t *)arg;
	replica_t *replica;
	int i, epfd, event_count, replica_count = 0;
	int64_t count = 0;
	void *data;
	bool read_io = false;
	struct epoll_event event, *events;
	size_t rsp_len = sizeof(zvol_io_hdr_t);
	zvol_io_hdr_t *io_rsp = NULL;
	//Create a new epoll epfd
	epfd = epoll_create1(0);
	events = calloc(MAXEVENTS, sizeof(event));
	while(1) {
		if(replica_count == 0) {
			MTX_LOCK(&spec->rq_mtx);
			if(spec->replica_count == 0) {
				//Wait until at least one IO connection has been made to a registered replica
				pthread_cond_wait(&spec->rq_cond, &spec->rq_mtx);
			}
			MTX_UNLOCK(&spec->rq_mtx);
		}
		//Check if number of available replicas have changed
		if(replica_count != spec->replica_count) {
			replica_count = update_replica_list(epfd, spec, replica_count);
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
				while(1) {
					MTX_LOCK(&spec->rq_mtx);
					TAILQ_FOREACH(replica, &spec->rq, r_next) {
						if(events[i].data.fd == replica->iofd) {
							break;
						}
					}
					MTX_UNLOCK(&spec->rq_mtx);
					read_io_resp();
execute_io:
					io_rsp = replica->io_rsp;
					//TODO A local replica queue needs to be maintained to avoid locing spec again and again
					if(read_io) {
						handle_read_resp(spec, replica, io_rsp, replica->io_rsp_data);
					} else {
						handle_write_resp(spec, replica, io_rsp);
					}
					MTX_LOCK(&replica->r_mtx);
					unblock_blocked_cmds(replica);
					MTX_UNLOCK(&replica->r_mtx);
				}
			}
		}
	}
	close(epfd);
	return NULL;
}

replica_t *
create_replica_entry(spec_t *spec, int iofd, char *replicaip, int replica_port) {

	int rc;
	pthread_t replica_sender_thread;
	replica_t *replica = (replica_t *)malloc(sizeof(replica_t));
	rc = pthread_mutex_init(&replica->r_mtx, NULL); //check
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_mutex_init() failed errno:%d\n", errno);
		exit(EXIT_FAILURE);
	}
	rc = pthread_cond_init(&replica->r_cond, NULL); //check
	if (rc != 0) {
		REPLICA_ERRLOG("pthread_cond_init() failed errno:%d\n", errno);
		exit(EXIT_FAILURE);
	}
	TAILQ_INIT(&replica->sendq);
	TAILQ_INIT(&replica->waitq);
	TAILQ_INIT(&replica->read_waitq);
	TAILQ_INIT(&replica->blockedq);
	replica->iofd = iofd;
	replica->ip = malloc(strlen(replicaip)+1);
	strcpy(replica->ip, replicaip);
	replica->port = replica_port;
	replica->state = ADDED_TO_SPEC;
	replica->least_recvd = 0;
	replica->wrio_seq = 0;
	replica->rrio_seq = 0;
	replica->spec = spec;
	replica->io_rsp = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));
	replica->io_rsp_data = NULL;
	replica->recv_len = 0;
	replica->read_rem_data = false;
	replica->read_rem_hdr = false;
	replica->total_len = 0;
	replica->removed = false;
	MTX_LOCK(&spec->rq_mtx);
	replica->id = spec->replica_count;
	spec->replica_count++;
	spec->consistency_count = spec->replica_count/2 +1;
	TAILQ_INSERT_TAIL(&spec->rq, replica, r_next);
	if(spec->replica_count == 1)
		pthread_cond_signal(&spec->rq_cond);
	MTX_UNLOCK(&spec->rq_mtx);
	rc = pthread_create(&replica_sender_thread, NULL, &replica_sender,
			(void *)replica);
	if (rc != 0) {
		ISTGT_ERRLOG("pthread_create(replicator_thread) failed\n");
		return NULL;
	}	
	zvol_io_hdr_t *rio;
	rio = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));
	rio->opcode = ZVOL_OPCODE_HANDSHAKE;
	rio->io_seq = 0;
	rio->offset = 0;
	char *data = malloc(strlen("vol1")+1);
	rio->len    = strlen("vol1");
	strncpy(data, "vol1", strlen("vol1"));
	
	write(replica->iofd, rio, sizeof(zvol_io_hdr_t));
	write(replica->iofd, data, strlen("vol1"));
	free(rio);
	rio = NULL;
	spec->ready = true;
	return replica;
}

int
zvol_handshake(char *volname, char *replicaip, int replica_port) {
	int rc, iofd;
	spec_t *spec;
	REPLICA_LOG("VOLNAME = %s\n", volname);
	MTX_LOCK(&specq_mtx);
	TAILQ_FOREACH(spec, &spec_q, spec_next) {
		if(strcmp(volname, volname) == 0)  {
			if((iofd = cstor_ops.conn_connect(replicaip, replica_port)) < 0) {
				REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
				exit(EXIT_FAILURE);
			}
			rc = make_socket_non_blocking(iofd);
			if (rc == -1) {
				REPLICA_ERRLOG("make_socket_non_blocking() failed errno:%d\n", errno);
				exit(EXIT_FAILURE);

			}
			create_replica_entry(spec, iofd, replicaip, replica_port);
			break;
		}
	}
	MTX_UNLOCK(&specq_mtx);
	return 0;
}

int
accept_mgmt_conns(int epfd, int sfd) {
	struct epoll_event event;
	int rc, rcount=0;
	spec_t *spec;
	int mgmtfd[MAXREPLICA];
	char *buf = malloc(BUFSIZE);
	zvol_op_code_t mgmt_opcode;
	int mgmt_fd, data_len = 0;

	while (1) {
		struct sockaddr saddr;
		socklen_t slen;
		char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
		slen = sizeof(saddr);
		mgmt_fd = accept(sfd, &saddr, &slen);
		if (mgmt_fd == -1) {
			if((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
				break;
			} else {
				REPLICA_ERRLOG("accept() failed, errrno:%d", errno);
				break;
			}
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
			REPLICA_ERRLOG("make_socket_non_blocking() failed, errno:%d", errno);
			return -1;
		}
		event.data.fd = mgmt_fd;
		event.events = EPOLLIN | EPOLLET;
		rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmt_fd, &event);
		if(rc == -1) {
			REPLICA_ERRLOG("epoll_ctl() failed, errno:%d", errno);
			return -1;
		}
		MTX_LOCK(&specq_mtx);
		TAILQ_FOREACH(spec, &spec_q, spec_next) {
			data_len += snprintf(buf, BUFSIZE, "%s", "vol1") + 1;
		}
		MTX_UNLOCK(&specq_mtx);
		mgmt_opcode = ZVOL_OPCODE_HANDSHAKE;
		send_mgmtio(mgmt_fd, mgmt_opcode, buf, data_len);
		data_len = 0;
		return mgmt_fd;
	}
	return -1;
}

void *
start_replication(void *arg __attribute__((__unused__))) {
	struct epoll_event event, *events;
	int rc, sfd, event_count, i, mgmtfd;
	char *replicaip = malloc(MAXIPLEN);
	int replica_port;
	mgmt_ack_data_t *mgmt_ack_data;
	zvol_op_code_t opcode;
	zvol_io_hdr_t *mgmt_ack = malloc(sizeof(zvol_io_hdr_t));
	int64_t epfd, count = 0;

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
		for(i=0; i< event_count; i++) {
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) 
					|| (!(events[i].events & EPOLLIN))) {
				REPLICA_LOG("epoll error\n");
				close(events[i].data.fd);
				continue;
			} else if (events[i].data.fd == sfd) {
				//Accept management connections from replicas and add the replicas to replica queue
				mgmtfd = accept_mgmt_conns(epfd, sfd);
				continue;
			} else {
				//Negotiate operating parameters with replica and add the replica to spec->rq
					count = read_data(events[i].data.fd, (uint8_t *)mgmt_ack, sizeof(zvol_io_hdr_t));
					if (count == -1 && errno == EAGAIN) {
						//TODO HANDLE DATA for next iteration
						break;
					}
					check_read_error();

					if(mgmt_ack->len) {
						mgmt_ack_data = malloc(sizeof(mgmt_ack_data_t));
						count = read_data(events[i].data.fd, (uint8_t *)mgmt_ack_data, mgmt_ack->len);
						if (count == -1 && errno == EAGAIN) {
							//TODO HANDLE ACK DATA
							break;
						}
						check_read_error();
					}
					opcode = mgmt_ack->opcode;
					strncpy(replicaip, mgmt_ack_data->ip, MAXIPLEN);
					replica_port = mgmt_ack_data->port;
					switch(opcode) {
						case ZVOL_OPCODE_HANDSHAKE:
							zvol_handshake(mgmt_ack_data->volname, replicaip, replica_port);
							break;
						default:
							break;
					}
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
		cmd->completed = 1;
		next_cmd = TAILQ_NEXT(cmd, send_cmd_next);
		TAILQ_REMOVE(&rcommon_sendq, cmd, send_cmd_next);
		cmd = next_cmd;
	}
	cmd = TAILQ_FIRST(&spec->rcommon_waitq);
	while (cmd) {
		cmd->status = -1;
		cmd->completed = 1;
		next_cmd = TAILQ_NEXT(cmd, wait_cmd_next);
		TAILQ_REMOVE(&rcommon_waitq, cmd, wait_cmd_next);
		cmd = next_cmd;
	}
	cmd = TAILQ_FIRST(&spec->rcommon_pendingq);
	while (cmd) {
		cmd->status = -1;
		cmd->completed = 1;
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
	spec->replica_count = 0;
	spec->consistency_count = 0;
	spec->ready = false;
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
