#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <replication.h>
#include <replication_misc.h>
#include <istgt_integration.h>
#include <zrepl_prot.h>
#include <assert.h>

#define	REPL_TEST_ERROR		-1
#define	REPL_TEST_RESTART	-2

#define	ERROR_TYPE_MGMT	(1)
#define	ERROR_TYPE_DATA	(1 << 1)

int start_errored_replica(int replica_count);
void trigger_data_conn_error(void);
void shutdown_errored_replica(void);
void wait_for_spec_ready(void);
static int verify_replica_removal(int replica_mgmt_sport);

extern int replication_initialized;
extern int replication_factor;
extern int consistency_factor;

static int *replica_port_list;
static pthread_t *errored_rthread;
static pthread_key_t err_repl_key;
static int total_replica;
static int active_replica;

static int error_type;
static pthread_mutex_t err_replica_mtx;

volatile int ignore_io_error = 0;

typedef struct {
	int replica_port;
	int mgmtfd;
	int datafd;
	int sfd;
	int mgmt_err_cnt;
	int data_err_cnt;
	int replica_status;
} errored_replica_data_t;

#define	CONNECTION_CLOSE_ERROR_EPOLL(_fd, _epoll_fd, rc, _label, _err_freq, _err_type)				\
do {														\
	errored_replica_data_t *_repl_data;									\
	if (_fd == -1)												\
		break;												\
														\
	if (!check_for_error(_err_type))									\
		break;												\
														\
	_repl_data = (errored_replica_data_t *)pthread_getspecific(err_repl_key);				\
	(void) epoll_ctl(_epoll_fd, EPOLL_CTL_DEL, _fd, NULL);							\
	close(_fd);												\
	(_repl_data->mgmtfd == _fd) && (_repl_data->mgmtfd = -1);						\
	(_repl_data->datafd == _fd) && (_repl_data->datafd = -1);						\
	_fd = -1;												\
														\
	if (_err_type & ERROR_TYPE_MGMT)									\
		_repl_data->mgmt_err_cnt++;									\
	if (_err_type & ERROR_TYPE_DATA)									\
		_repl_data->data_err_cnt++;									\
														\
	REPLICA_ERRLOG("Injecting error at %s:%d.. error count.. mgmt(%d) data(%d) replica(%d)\n",		\
	    __FUNCTION__, __LINE__, _repl_data->mgmt_err_cnt, _repl_data->data_err_cnt,				\
	    _repl_data->replica_port);										\
	rc = REPL_TEST_RESTART;											\
	goto _label;												\
} while (0);

#define	HEADER_BUF_ERROR(iohdr, _err_freq, _err_type)								\
do {														\
	errored_replica_data_t *_repl_data;									\
	int _count;												\
	if (!check_for_error(_err_type))									\
		break;												\
														\
	_count = random() % 5;											\
	iohdr->version = REPLICA_VERSION;									\
	if (_count == 0) {											\
		iohdr->status = ZVOL_OP_STATUS_OK;								\
	} else if (_count == 1) {										\
		iohdr->status = ZVOL_OP_STATUS_FAILED;								\
	} else if (_count == 2) {										\
		iohdr->status = ZVOL_OP_STATUS_VERSION_MISMATCH;						\
	}													\
														\
	if (_count == 3)											\
		iohdr->io_seq = -1;										\
	if (_count == 4)											\
		iohdr->offset = ULONG_MAX;									\
	if (_count == 5)											\
		iohdr->len = ULONG_MAX;										\
														\
	_repl_data = (errored_replica_data_t *)pthread_getspecific(err_repl_key);				\
														\
	if (_err_type & ERROR_TYPE_MGMT)									\
		_repl_data->mgmt_err_cnt++;									\
	if (_err_type & ERROR_TYPE_DATA)									\
		_repl_data->data_err_cnt++;									\
														\
	REPLICA_ERRLOG("Injecting error at %s:%d.. error count.. mgmt(%d) data(%d) replica(%d)\n",		\
	    __FUNCTION__, __LINE__, _repl_data->mgmt_err_cnt, _repl_data->data_err_cnt,				\
	    _repl_data->replica_port);										\
} while (0);

#define	CONNECTION_CLOSE_ERROR(_fd, _rc, _label, _err_freq, _err_type)						\
do {														\
	errored_replica_data_t *_repl_data;									\
	if (_fd == -1)												\
		break;												\
														\
	if (!check_for_error(_err_type))									\
		break;												\
														\
	_repl_data = (errored_replica_data_t *)pthread_getspecific(err_repl_key);				\
	close(_fd);												\
	(_repl_data->mgmtfd == _fd) && (_repl_data->mgmtfd = -1);						\
	(_repl_data->datafd == _fd) && (_repl_data->datafd = -1);						\
	_fd = -1;												\
														\
	_rc = REPL_TEST_RESTART;										\
	if (_err_type & ERROR_TYPE_MGMT)									\
		_repl_data->mgmt_err_cnt++;									\
	if (_err_type & ERROR_TYPE_DATA)									\
		_repl_data->data_err_cnt++;									\
														\
	REPLICA_ERRLOG("Injecting error at %s:%d.. error count.. mgmt(%d) data(%d) replica(%d)\n",		\
	    __FUNCTION__, __LINE__, _repl_data->mgmt_err_cnt, _repl_data->data_err_cnt,				\
	    _repl_data->replica_port);										\
	goto _label;												\
} while (0);

static void
exit_errored_replica(void *arg)
{
	errored_replica_data_t *rdata = (errored_replica_data_t *)arg;

	if (rdata->mgmtfd != -1)
		close(rdata->mgmtfd);

	if (rdata->datafd != -1)
		close(rdata->datafd);

	if (rdata->sfd != -1)
		close(rdata->sfd);

	REPLICA_LOG("Errored Replica(%d) destroyed.. "
	    "total injected errors mgmt(%d) data(%d)", rdata->replica_port,
	    rdata->mgmt_err_cnt, rdata->data_err_cnt);
	free(rdata);
}

static uint64_t
fetch_update_io_buf(zvol_io_hdr_t *io_hdr, uint8_t *user_data,
    uint8_t **resp_data)
{
	uint32_t count = 0;
	uint64_t len = io_hdr->len;
	uint64_t offset = io_hdr->offset;
	uint64_t start = offset;
	uint64_t end = offset + len;
	uint64_t resp_index, data_index;
	uint64_t total_payload_len;
	uint64_t md_io_num = 0, last_md_io_num = 0;
	struct zvol_io_rw_hdr *last_io_rw_hdr;
	uint8_t *resp;

	while (start < end) {
		count++;
		start += 512;
	}

	if (!count)
		count = 1;

	if (!(io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA))
		count = 1;

	total_payload_len = len + count * sizeof(struct zvol_io_rw_hdr);
	*resp_data = malloc(total_payload_len);
	memset(*resp_data, 0, total_payload_len);
	start = offset;

	last_md_io_num = md_io_num = random();
	last_io_rw_hdr = (struct zvol_io_rw_hdr *)*resp_data;
	last_io_rw_hdr->io_num = (io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) ?
	    last_md_io_num : 0;
	resp_index = sizeof (struct zvol_io_rw_hdr);
	resp = *resp_data;
	data_index = 0;
	count = 0;
	while ((io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) && (start < end)) {
		if (md_io_num != last_md_io_num) {
			last_io_rw_hdr->len = count * 512;
			memcpy(resp + resp_index, user_data + data_index,
			    last_io_rw_hdr->len);
			data_index += last_io_rw_hdr->len;
			resp_index += last_io_rw_hdr->len;
			last_io_rw_hdr = (struct zvol_io_rw_hdr *)(resp + resp_index);
			last_io_rw_hdr->io_num = md_io_num;
			resp_index += sizeof (struct zvol_io_rw_hdr);
			last_io_rw_hdr->io_num = last_md_io_num;
			last_md_io_num = md_io_num;
			count = 0;
		}
		count++;
		start += 512;
		md_io_num = random();
	}
	last_io_rw_hdr->len = (io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) ?
	    (count * 512) : len;
	memcpy(resp + resp_index, user_data + data_index, last_io_rw_hdr->len);
	return total_payload_len;
}

static int
verify_replica_removal(int replica_mgmt_sport)
{
	int rc = 0;
#ifdef	DEBUG
	replica_t *r;
	spec_t *spec = NULL;
	int retry_count = 10;
#endif

	if (!replica_mgmt_sport)
		goto error;

#ifdef	DEBUG
	MTX_LOCK(&specq_mtx);
		TAILQ_FOREACH(spec, &spec_q, spec_next) {
			// Since we are supporting single spec per controller
			// we will continue using first spec only       
			break;
		}
	MTX_UNLOCK(&specq_mtx);

	/*
	 * Check if the replica is in spec's list (both normal and waitlist) or not.
	 * There are chances that replica won't be removed immediately, so we will
	 * try to check `retry_count` times if the replica is in spec's list.
	 */
	while (retry_count) {
		rc = 0;

		MTX_LOCK(&spec->rq_mtx);
		TAILQ_FOREACH(r, &(spec->rq), r_next) {
			if (r->replica_mgmt_dport == replica_mgmt_sport) {
				rc = 1;
				goto retry;
			}
		}

		TAILQ_FOREACH(r, &(spec->rwaitq), r_next) {
			if (r->replica_mgmt_dport == replica_mgmt_sport) {
				rc = 1;
				goto retry;
			}
		}

retry:
		MTX_UNLOCK(&spec->rq_mtx);
		retry_count--;

		/* Sleep for 1 second to avoid lock starvation */
		sleep(1);
	}
#else
#warning Debug mode is disabled
#endif

error:
	return rc;
}

static inline int
get_socket_info(int socket)
{
	int rc = -1;
	struct sockaddr_in addr;
	socklen_t len = sizeof (struct sockaddr);

	rc = getsockname(socket, (struct sockaddr *)&addr, &len);
	if (rc == -1) {
		REPLICA_ERRLOG("Failed to get sockinfo for socket(%d)\n", socket);
		goto error;
	}

	REPLICA_DEBUGLOG("socket(%d) bound to %s:%d\n",
	    socket, inet_ntoa(addr.sin_addr), (int) ntohs(addr.sin_port));
	rc = (int) ntohs(addr.sin_port);

error:
	return rc;
}

static int
check_for_error(int err_type)
{
	
	int rc = 0;
	static int num = 1;
	static int old_num = 1;

	MTX_LOCK(&err_replica_mtx);
	if (!(err_type & error_type)) {
		MTX_UNLOCK(&err_replica_mtx);
		goto out;
	}
	MTX_UNLOCK(&err_replica_mtx);

	num--;

	if (num <= 0) {
		MTX_LOCK(&err_replica_mtx);
		if (err_type == ERROR_TYPE_DATA) {
			if (active_replica <= consistency_factor) {
				MTX_UNLOCK(&err_replica_mtx);
				goto out;
			} else {
				active_replica--;
			}
		}
		MTX_UNLOCK(&err_replica_mtx);
		rc = 1;
		num = (int) random() % 30;
		old_num++;
	}

out:
	return rc;
}

static int
send_mgmt_ack(int fd, zvol_io_hdr_t *mgmt_ack_hdr, void *buf, int *zrepl_status_msg_cnt,
    const char *replica_ip, int replica_port)
{
	int i, nbytes = 0;
	int rc = 0, start;
	struct iovec iovec[12];
	int iovec_count;
	zrepl_status_ack_t zrepl_status = { 0 };
	mgmt_ack_t mgmt_ack_data;
	int ret = -1;
	zvol_op_stat_t stats;

	iovec[0].iov_base = mgmt_ack_hdr;
	iovec[0].iov_len = 16;

	iovec[1].iov_base = ((uint8_t *)mgmt_ack_hdr) + 16;
	iovec[1].iov_len = 16;

	iovec[2].iov_base = ((uint8_t *)mgmt_ack_hdr) + 32;
	iovec[2].iov_len = sizeof (zvol_io_hdr_t) - 32;

	switch (mgmt_ack_hdr->opcode) {
		case ZVOL_OPCODE_SNAP_DESTROY:
			iovec[2].iov_base = ((uint8_t *)mgmt_ack_hdr) + 32;
			iovec[2].iov_len = 2;

			iovec[3].iov_base = ((uint8_t *)mgmt_ack_hdr) + 34;
			iovec[3].iov_len = sizeof (zvol_io_hdr_t) - 34;

			iovec_count = 4;
			mgmt_ack_hdr->status = (random() % 2) ? ZVOL_OP_STATUS_FAILED : ZVOL_OP_STATUS_OK;
			mgmt_ack_hdr->len = 0;
			break;

		case ZVOL_OPCODE_SNAP_CREATE:
			iovec_count = 3;
			sleep(random()%3 + 1);
			mgmt_ack_hdr->status = (random() % 5 == 0) ? ZVOL_OP_STATUS_FAILED : ZVOL_OP_STATUS_OK;
			mgmt_ack_hdr->len = 0;
			break;

		case ZVOL_OPCODE_REPLICA_STATUS:
			zrepl_status.state = ZVOL_STATUS_DEGRADED;
//			zrepl_status.state = ZVOL_STATUS_HEALTHY;

			if (((*zrepl_status_msg_cnt) >= 3)) {
//			    (zrepl_status.state != ZVOL_STATUS_HEALTHY)) {
				zrepl_status.state = ZVOL_STATUS_HEALTHY;
				zrepl_status.rebuild_status = ZVOL_REBUILDING_DONE;
				(*zrepl_status_msg_cnt) = 0;
			} else if ((*zrepl_status_msg_cnt) >= 1) {
				zrepl_status.rebuild_status = ZVOL_REBUILDING_SNAP;
			}

			if (zrepl_status.rebuild_status == ZVOL_REBUILDING_SNAP) {
				(*zrepl_status_msg_cnt) += 1;
			}

			mgmt_ack_hdr->len = sizeof (zrepl_status_ack_t);
			iovec_count = 4;
			iovec[3].iov_base = &zrepl_status;
			iovec[3].iov_len = sizeof (zrepl_status_ack_t);
			mgmt_ack_hdr->len = sizeof (zrepl_status_ack_t);
			break;

		case ZVOL_OPCODE_START_REBUILD:
			*zrepl_status_msg_cnt = 1;
			zrepl_status.rebuild_status = ZVOL_REBUILDING_SNAP;
			iovec_count = 3;
			mgmt_ack_hdr->len = 0;
			break;

		case ZVOL_OPCODE_STATS:
			strcpy(stats.label, "used");
			stats.value = 10000;

			iovec[3].iov_base = &stats;
			iovec[3].iov_len = sizeof (zvol_op_stat_t);
			iovec_count = 4;
			mgmt_ack_hdr->len = sizeof (zvol_op_stat_t);
			break;

		case ZVOL_OPCODE_PREPARE_FOR_REBUILD:
		case ZVOL_OPCODE_HANDSHAKE:
			strcpy(mgmt_ack_data.ip, replica_ip);
			strcpy(mgmt_ack_data.volname, buf);
			mgmt_ack_data.port = replica_port;
			mgmt_ack_data.pool_guid = replica_port;
			mgmt_ack_data.zvol_guid = replica_port;

			iovec[3].iov_base = &mgmt_ack_data;
			iovec[3].iov_len = 50;

			iovec[4].iov_base = ((uint8_t *)&mgmt_ack_data) + 50;
			iovec[4].iov_len = 50;

			iovec[5].iov_base = ((uint8_t *)&mgmt_ack_data) + 100;
			iovec[5].iov_len = sizeof (mgmt_ack_t) - 100;
			iovec_count = 6;
			mgmt_ack_hdr->len = sizeof (mgmt_ack_data);
			break;

		default:
			REPLICA_ERRLOG("opcode(%d) is not handled.. program is aborting now..",
			    mgmt_ack_hdr->opcode);
			abort();
			break;
	}

	if(check_for_error(ERROR_TYPE_MGMT)) {
		for (i = 0; i < iovec_count; i++) {
			iovec[iovec_count + i].iov_base = iovec[i].iov_base;
			iovec[iovec_count + i].iov_len = iovec[i].iov_len;
		}
		iovec_count = 2 * iovec_count;
		/*
		 * Here, We are sending two responses to the target.
		 * Due to this, target will disconnect this replica.
		 * This may happen while we are sending seconds response data
		 * to the target.
		 * In this case, we will set return value as REPL_TEST_RESTART.
		 */
		ret = REPL_TEST_RESTART;
	}

	for (start = 0; start < iovec_count; start += 1) {
		nbytes = iovec[start].iov_len;
		while (nbytes) {
			rc = writev(fd, &iovec[start], 1);//Review iovec in this line
			if (rc < 0) {
				goto out;
			}
			nbytes -= rc;
			if (nbytes == 0)
				break;
			/* adjust iovec length */
			for (i = start; i < start + 1; i++) {
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
	}
	ret = 0;
out:
	return ret;
}

static int
send_io_resp(int fd, zvol_io_hdr_t *io_hdr, void *buf)
{
	struct iovec iovec[2];
	int iovcnt, i, nbytes = 0;
	int rc = 0;

	if (fd < 0) {
		REPLICA_ERRLOG("fd is %d!!!\n", fd);
		return REPL_TEST_ERROR;
	}

	HEADER_BUF_ERROR(io_hdr, err_freq, ERROR_TYPE_DATA);

	if(io_hdr->opcode == ZVOL_OPCODE_READ) {
		iovcnt = 2;
		iovec[0].iov_base = io_hdr;
		nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
		iovec[1].iov_base = buf;
		iovec[1].iov_len = io_hdr->len;
		nbytes += io_hdr->len;
	} else if(io_hdr->opcode == ZVOL_OPCODE_WRITE) {
		iovcnt = 1;
		iovec[0].iov_base = io_hdr;
		nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
	} else {
		iovcnt = 1;
		iovec[0].iov_base = io_hdr;
		nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
		io_hdr->len = 0;
	}
	while (nbytes) {
		rc = writev(fd, iovec, iovcnt);//Review iovec in this line
		if (rc < 0) {
			REPLICA_ERRLOG("failed to write on fd errno(%d)\n", errno);
			return REPL_TEST_ERROR;
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

	MTX_LOCK(&err_replica_mtx);
	if (io_hdr->opcode == ZVOL_OPCODE_OPEN)
		active_replica++;
	MTX_UNLOCK(&err_replica_mtx);

	return 0;
}

static void *
errored_replica(void *arg)
{
	int zrepl_status_msg_cnt = -1;
	int ctrl_port = 6060;
	int replica_port = *(int *)arg;
	int replica_mgmt_sport = 0;
	zvol_op_open_data_t *open_ptr;
	int sfd = -1, rc, epfd, event_count, i;
	volatile int mgmtfd = -1, iofd = -1;
	int64_t count;
	struct epoll_event event, *events;
	uint8_t *data;
	zvol_io_hdr_t *io_hdr;
	zvol_io_hdr_t *mgmtio;
	bool read_rem_data = false, read_rem_hdr = false;
	uint64_t recv_len = 0, total_len = 0;
	struct timespec now;
	errored_replica_data_t *rdata;

	ASSERT(replica_port);
	REPLICA_ERRLOG("starting replica(%d)\n", replica_port);

	rdata = malloc(sizeof (errored_replica_data_t));
	memset(rdata, 0, sizeof (errored_replica_data_t));
	rdata->replica_port = replica_port;
	rdata->replica_status = ZVOL_STATUS_DEGRADED;
	pthread_setspecific(err_repl_key, rdata);

	pthread_cleanup_push(exit_errored_replica, rdata);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	io_hdr = malloc(sizeof(zvol_io_hdr_t));
	mgmtio = malloc(sizeof(zvol_io_hdr_t));

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	snprintf(tinfo, 50, "mock_nwrepl%d", replica_port);
        prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	data = NULL;
	epfd = epoll_create1(0);
	
	//Create non-blocking listener for io connections from controller and add to epoll
	if((sfd = replication_listen("127.0.0.1", replica_port, 32, 1)) < 0) {
               	rc = REPL_TEST_ERROR;
		rdata->sfd = sfd = -1;
		goto error;
        }

	rdata->sfd = sfd;

	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &event);
	if (rc == -1) {
	       	rc = REPL_TEST_ERROR;
		goto error;
	}

	events = calloc(MAXEVENTS, sizeof(event));

try_again:
	mgmtfd = rdata->mgmtfd = -1;
	iofd = rdata->datafd = -1;
	read_rem_data = false;
	read_rem_hdr = false;
	zrepl_status_msg_cnt = -1;
	replica_mgmt_sport = 0;

	//Connect to controller to start handshake and connect to epoll
	if((mgmtfd = replication_connect("127.0.0.1", ctrl_port)) < 0) {
		REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
		rc = REPL_TEST_ERROR;
		goto error;
	}

	replica_mgmt_sport = get_socket_info(mgmtfd);
	if (replica_mgmt_sport < 0) {
		REPLICA_ERRLOG("failed to get socket info\n");
		goto error;
	}

	CONNECTION_CLOSE_ERROR(mgmtfd, rc, error, err_freq, ERROR_TYPE_MGMT);

	event.data.fd = mgmtfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmtfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed.. err(%d) replica(%d)", errno, replica_port);
		rc = REPL_TEST_ERROR;
		goto error;
	}

	CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);

	while (1) {
		event_count = epoll_wait(epfd, events, MAXEVENTS, -1);
		for(i=0; i< event_count; i++) {
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN))) {
				REPLICA_ERRLOG("epoll error for replica(%d) event(%d) fd(%d)\n",
				    replica_port, events[i].events, events[i].data.fd);
				if (events[i].data.fd == mgmtfd || events[i].data.fd == iofd) {
					rc = REPL_TEST_RESTART;
					goto error;
				} else {
					REPLICA_ERRLOG("something messy\n");
					abort();
				}
			} else if (events[i].data.fd == mgmtfd) {
				count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)mgmtio, sizeof(zvol_io_hdr_t), READ_IO_RESP_HDR);
				if (count < 0) {
					REPLICA_ERRLOG("Failed to read from fd(%d)\n", events[i].data.fd);
					rc = REPL_TEST_RESTART;
					goto error;
				}

				ASSERT(count == sizeof (zvol_io_hdr_t));

				CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
				CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

				if(mgmtio->len) {
					data = malloc(mgmtio->len);
					count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)data, mgmtio->len, READ_IO_RESP_DATA);
					if (count < 0) {
						rc = REPL_TEST_RESTART;
						goto error;
					}
					ASSERT((uint64_t)count == mgmtio->len);

					CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
					CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);
				}

				rc = send_mgmt_ack(mgmtfd, mgmtio, data, &zrepl_status_msg_cnt, "127.0.0.1", replica_port);
				if (rc == REPL_TEST_ERROR || rc == REPL_TEST_RESTART) {
					REPLICA_ERRLOG("failed to send ack for opcode:%d replica(%d)\n", mgmtio->opcode, replica_port);
					goto error;
				}
				if (zrepl_status_msg_cnt == 0)
					rdata->replica_status = ZVOL_STATUS_HEALTHY;
			} else if (events[i].data.fd == sfd) {
				iofd = accept(sfd, NULL, 0);
				if (iofd == -1) {
					if((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
						break;
					} else {
						REPLICA_ERRLOG("accept() failed, err(%d) replica(%d)", errno, replica_port);
						break;
					}
				}

				rdata->datafd = iofd;

				CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);

				rc = make_socket_non_blocking(iofd);
				if (rc == -1) {
					REPLICA_ERRLOG("make_socket_non_blocking() failed, err(%d)"
					    " replica(%d)", errno, replica_port);
					rc = REPL_TEST_ERROR;
					goto error;
				}

				CONNECTION_CLOSE_ERROR(iofd, rc, error, err_freq, ERROR_TYPE_DATA);

				event.data.fd = iofd;
				event.events = EPOLLIN | EPOLLET;
				rc = epoll_ctl(epfd, EPOLL_CTL_ADD, iofd, &event);
				if(rc == -1) {
					REPLICA_ERRLOG("epoll_ctl() failed, errno:%d replica(%d)", errno, replica_port);
					rc = REPL_TEST_ERROR;
					goto error;
				}
			} else if(events[i].data.fd == iofd) {
				while(1) {
					if (read_rem_data) {
						count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)data + recv_len,
						    total_len - recv_len, READ_IO_RESP_DATA);
						CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);
						CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
						if (count < 0) {
							rc = REPL_TEST_RESTART;
							goto error;
						} else if ((uint64_t)count < (total_len - recv_len)) {
							read_rem_data = true;
							recv_len += count;
							break;
						} else {
							recv_len = 0;
							total_len = 0;
							read_rem_data = false;
							goto execute_io;
						}

					} else if (read_rem_hdr) {
						CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);
						CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
						count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)io_hdr + recv_len,
						    total_len - recv_len, READ_IO_RESP_HDR);
						if (count < 0) {
							rc = REPL_TEST_RESTART;
							goto error;
						} else if ((uint64_t)count < (total_len - recv_len)) {
							read_rem_hdr = true;
							recv_len += count;
							break;
						} else {
							read_rem_hdr = false;
							recv_len = 0;
							total_len = 0;
						}
					} else {
						count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)io_hdr,
						    sizeof (zvol_io_hdr_t), READ_IO_RESP_HDR);
						CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
						CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);
						if (count < 0) {
							rc = REPL_TEST_RESTART;
							goto error;
						} else if ((uint64_t)count < sizeof (zvol_io_hdr_t)) {
							read_rem_hdr = true;
							recv_len = count;
							total_len = sizeof (zvol_io_hdr_t);
							break;
						} else {
							read_rem_hdr = false;
						}
					}

					CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
					CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

					if (io_hdr->opcode == ZVOL_OPCODE_WRITE ||
					    io_hdr->opcode == ZVOL_OPCODE_HANDSHAKE ||
					    io_hdr->opcode == ZVOL_OPCODE_OPEN) {
						if (io_hdr->len) {
							io_hdr->status = ZVOL_OP_STATUS_OK;
							data = malloc(io_hdr->len);

							CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
							CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

							count = perform_read_write_on_fd(events[i].data.fd, (uint8_t *)data,
							    io_hdr->len, READ_IO_RESP_DATA);
							if (count < 0) {
								rc = REPL_TEST_RESTART;
								goto error;
							} else if ((uint64_t)count < io_hdr->len) {
								read_rem_data = true;
								recv_len = count;
								total_len = io_hdr->len;
								break;
							}
							read_rem_data = false;
						}
					}

					if (io_hdr->opcode == ZVOL_OPCODE_OPEN) {
						open_ptr = (zvol_op_open_data_t *)data;
						io_hdr->status = ZVOL_OP_STATUS_OK;

						REPLICA_LOG("Got volname(%s) blocksize(%d) timeout(%d) for replica(%d)\n",
						    open_ptr->volname, open_ptr->tgt_block_size, open_ptr->timeout, replica_port);
					}
execute_io:
					if(io_hdr->opcode == ZVOL_OPCODE_WRITE) {
						CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
						CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

						io_hdr->status = ZVOL_OP_STATUS_OK;
					} else if(io_hdr->opcode == ZVOL_OPCODE_READ) {
						uint8_t *user_data = NULL;
						CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
						CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

						if(io_hdr->len)
							user_data = malloc(io_hdr->len);

						io_hdr->status = ZVOL_OP_STATUS_OK;
						io_hdr->len = fetch_update_io_buf(io_hdr, user_data, &data);

						if (user_data)
							free(user_data);
					}

					CONNECTION_CLOSE_ERROR_EPOLL(mgmtfd, epfd, rc, error, err_freq, ERROR_TYPE_MGMT);
					CONNECTION_CLOSE_ERROR_EPOLL(iofd, epfd, rc, error, err_freq, ERROR_TYPE_DATA);

					rc = send_io_resp(iofd, io_hdr, data);
					if (rc == REPL_TEST_ERROR || rc == REPL_TEST_RESTART) {
						REPLICA_ERRLOG("Failed to send response from replica(%d)\n", replica_port);
						goto error;
					}

					if (data) {
						free(data);
						data = NULL;
					}
				}
			}
		}
	}

error:
	MTX_LOCK(&err_replica_mtx);
	if ((rdata->replica_status == ZVOL_STATUS_HEALTHY) &&
	    (error_type == ERROR_TYPE_DATA))
		ignore_io_error = 1;
	MTX_UNLOCK(&err_replica_mtx);

	if (mgmtfd != -1) {
		(void) epoll_ctl(epfd, EPOLL_CTL_DEL, mgmtfd, NULL);
		close(mgmtfd);
		mgmtfd = -1;
	}

	if (iofd != -1) {
		(void) epoll_ctl(epfd, EPOLL_CTL_DEL, iofd, NULL);
		close(iofd);
		iofd = -1;
	}

	if (data) {
		free(data);
		data = NULL;
	}

	if (rc == REPL_TEST_RESTART) {
		rc = verify_replica_removal(replica_mgmt_sport);
		if (rc) {
			REPLICA_ERRLOG("Replica(%d) is not removed\n", replica_port);
			abort();
		}
		goto try_again;
	}

	if (sfd > 0)
		close(sfd);

	free(io_hdr);
	free(mgmtio);
	free(events);
	REPLICA_ERRLOG("shutting down replica(%d)... \n", replica_port);
	pthread_cleanup_pop(0);
	return NULL;
}

static void
wait_for_replication_initialization(void)
{
	int count = 10;

	while (count) {
		__sync_synchronize();
		usleep(100000);
		if (replication_initialized)
			return;
		count--;
	}

	REPLICA_ERRLOG("replication module is not initialized yet!\n");
	abort();
}

int
start_errored_replica(int replica_count)
{
	int i, rc = 0;

	ASSERT(replica_count > 0);

	wait_for_replication_initialization();

	/*
	 * By default, error will be injected in mgmt connection only.
	 * error_type is proected with `err_replica_mtx`.
	 */
	error_type = ERROR_TYPE_DATA | ERROR_TYPE_MGMT;
	pthread_mutex_init(&err_replica_mtx, NULL);

	errored_rthread = malloc(replica_count * sizeof (pthread_t));
	replica_port_list = malloc(replica_count * sizeof (int));
	total_replica = replica_count;
	pthread_key_create(&err_repl_key, NULL);

	for (i = 0; i < replica_count; i++) {
		replica_port_list[i] = 6061 + i;
		rc = pthread_create(&errored_rthread[i], NULL, &errored_replica, (void *)&replica_port_list[i]);
		if (rc != 0) {
			REPLICA_ERRLOG("Failed to create errored replica(%d) err(%d)\n", replica_port_list[i], rc);
			goto error;
		}
        }

	REPLICA_NOTICELOG("Total %d errored replica started\n", replica_count);

error:
	return rc;
}

void
trigger_data_conn_error(void)
{
	MTX_LOCK(&err_replica_mtx);
	error_type = ERROR_TYPE_DATA;
	MTX_UNLOCK(&err_replica_mtx);
}

void
shutdown_errored_replica(void)
{
	int i;

	for (i = 0; i < total_replica; i++) {
		pthread_cancel(errored_rthread[i]);
	}

	free(errored_rthread);
	free(replica_port_list);
}

void
wait_for_spec_ready(void)
{
	int count = 10;
	spec_t *spec = NULL;
	
	MTX_LOCK(&specq_mtx);
		TAILQ_FOREACH(spec, &spec_q, spec_next) {
			// Since we are supporting single spec per controller
			// we will continue using first spec only       
			break;
		}
	MTX_UNLOCK(&specq_mtx);

	while (count && spec) {
		sleep(5);
		MTX_LOCK(&spec->rq_mtx);
		if (spec->ready) {
			MTX_UNLOCK(&spec->rq_mtx);
			return;
		}
		MTX_UNLOCK(&spec->rq_mtx);

		count--;
	}
	REPLICA_ERRLOG("spec is not ready yet.. \n");
	abort();
}
