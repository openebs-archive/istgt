#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <stdbool.h>
#include "istgt.h"
#include "zrepl_prot.h"
#include "istgt_integration.h"
#include "replication.h"
#include "replication_misc.h"
#include "ring_mempool.h"
#include "assert.h"

#define	READ_PARTIAL	1
#define	READ_COMPLETED	2

#define	WRITE_PARTIAL	1
#define	WRITE_COMPLETED	2

/* default timeout is set to REPLICA_DEFAULT_TIMEOUT seconds */
int replica_timeout = REPLICA_DEFAULT_TIMEOUT;

#define check_for_blockage() {						\
	uint64_t current_lba = cmd->offset;				\
	uint64_t current_lbE = current_lba + cmd->data_len;		\
	uint64_t pending_lba = pending_rcmd->offset;			\
	uint64_t pending_lbE = pending_lba + pending_rcmd->data_len;	\
	if ((current_lbE < pending_lba)  ||				\
	    (pending_lbE < current_lba)) {				\
		cmd_blocked = false;					\
	} else {							\
		cmd_blocked = true;					\
	}								\
}

#define CHECK_BLOCKAGE_IN_Q(queue, next) {				\
	if(cmd_blocked == false) {					\
		TAILQ_FOREACH(pending_rcmd, queue, next) {		\
			check_for_blockage();				\
			if(cmd_blocked == true)				\
				break;					\
		}							\
	}								\
}

#define SEND_ERROR_RESPONSES(head, r, _cond, _cnt, _time_diff, _r, _w)	\
{									\
	_cnt = 0;							\
	memset(&_time_diff, 0, sizeof (_time_diff));			\
	rcmd = TAILQ_FIRST(head);					\
	if (rcmd != NULL) {						\
		struct timespec now;					\
		clock_gettime(CLOCK_MONOTONIC, &now);			\
		timesdiff(CLOCK_MONOTONIC, rcmd->queued_time, 		\
		    now, _time_diff);					\
	}								\
	while (rcmd != NULL) {						\
		next_rcmd = TAILQ_NEXT(rcmd, next);			\
		TAILQ_REMOVE(head, rcmd, next);				\
		idx = rcmd->idx;					\
		rcomm_cmd = rcmd->rcommq_ptr;				\
		_cond = rcomm_cmd->cond_var;				\
									\
		DECREMENT_INFLIGHT_REPLICA_IO_CNT(r, rcomm_cmd->opcode);\
									\
		if (rcomm_cmd->opcode == ZVOL_OPCODE_WRITE)		\
			++_w;						\
		if (rcomm_cmd->opcode == ZVOL_OPCODE_READ)		\
			++_r;						\
									\
		rcomm_cmd->resp_list[idx].io_resp_hdr.status =		\
		    ZVOL_OP_STATUS_FAILED;				\
		rcomm_cmd->resp_list[idx].data_ptr = NULL;		\
		/*							\
		 * cleanup_deadlist thread performs cleanup of 		\
		 * rcomm_cmd. if the response from all replica is 	\
		 * received. Since we are avoiding locking for 		\
		 * rcomm_cmd, we will update response status in 	\
		 * rcomm_cmd at last.					\
		 */							\
		if (rcomm_cmd->state != CMD_EXECUTION_DONE) {		\
			rcomm_cmd->resp_list[idx].status |= 		\
			    RECEIVED_ERR;				\
			pthread_cond_signal(_cond);			\
		} else {						\
			REPLICA_DEBUGLOG("error set for command(%lu)"	\
			    " for replica(%lu)\n",			\
			    rcomm_cmd->io_seq, r->zvol_guid);		\
			rcomm_cmd->resp_list[idx].status |= 		\
			    RECEIVED_ERR;				\
		}							\
		free(rcmd->iov_data);					\
		free(rcmd);						\
		rcmd = next_rcmd;					\
		_cnt++;							\
	}								\
}

#define	CHECK_REPLICA_TIMEOUT(_head, _diff, _ret, _exit_label, etimeout)\
	do {								\
		struct timespec nw;					\
		rcmd_t *pending_cmd;					\
		int ms;							\
		pending_cmd = TAILQ_FIRST(_head);			\
		if (pending_cmd != NULL) {				\
			clock_gettime(CLOCK_MONOTONIC, &nw);		\
			timesdiff(CLOCK_MONOTONIC,			\
			    pending_cmd->queued_time, nw, _diff);	\
			if (_diff.tv_sec >= replica_timeout) {		\
				REPLICA_ERRLOG("timeout happened for "	\
				    "replica(%lu).. delay(%lu sec)\n", 	\
				    r->zvol_guid, _diff.tv_sec); 	\
				_ret = -1;				\
				goto _exit_label;			\
			} else {					\
				ms = _diff.tv_sec * 1000;		\
				ms += _diff.tv_nsec / 1000000;		\
				if (ms >				\
				    (wait_count * polling_timeout)) {	\
					REPLICA_NOTICELOG("replica(%lu)"\
					    " hasn't responded in last "\
					    "%d seconds\n",		\
					    r->zvol_guid, ms / 1000);	\
				}					\
				if (ms > (wait_count * polling_timeout))\
					wait_count =			\
					    (ms / polling_timeout) + 1;	\
				ms = (replica_timeout * 1000) - ms;	\
				if (ms < etimeout)			\
					etimeout = ms;			\
			}						\
		}							\
	} while (0)

/*
 * check replica's blocked command queue.
 * if any blocked command can be unblock then add it to replica's readyq
 */
static bool
unblock_cmds(replica_t *r)
{
	bool cmd_blocked = false;
	rcmd_t *pending_rcmd, *cmd, *next_cmd;
	bool unblocked = false;

	for (cmd = TAILQ_FIRST(&r->blockedq); cmd; cmd = next_cmd) {
		next_cmd = TAILQ_NEXT(cmd, next);
		cmd_blocked = false;
		CHECK_BLOCKAGE_IN_Q(&r->readyq, next);
		if (cmd_blocked == true)
			break;
		CHECK_BLOCKAGE_IN_Q(&r->waitq, next);
		if (cmd_blocked == true)
			break;
		TAILQ_REMOVE(&r->blockedq, cmd, next);
		TAILQ_INSERT_TAIL(&r->readyq, cmd, next);
		unblocked = true;
	}
	return unblocked;
}

/*
 * Check if cmd can be added to replica's ready queue or blocked queue
 */
static void
move_to_blocked_or_ready_q(replica_t *r, rcmd_t *cmd)
{
	bool cmd_blocked = false;
	rcmd_t *pending_rcmd;

	if (!TAILQ_EMPTY(&r->blockedq)) {
		clock_gettime(CLOCK_MONOTONIC, &cmd->queued_time);
		TAILQ_INSERT_TAIL(&r->blockedq, cmd, next);
		goto done;
	}
	CHECK_BLOCKAGE_IN_Q(&r->readyq, next);
	CHECK_BLOCKAGE_IN_Q(&r->waitq, next);
	clock_gettime(CLOCK_MONOTONIC, &cmd->queued_time);
	if (cmd_blocked == true)
		TAILQ_INSERT_TAIL(&r->blockedq, cmd, next);
	else
		TAILQ_INSERT_TAIL(&r->readyq, cmd, next);
done:
	return;
}

/*
 * inform replica's management interface regarding error in
 * replica_thread
 */
void
inform_mgmt_conn(replica_t *r)
{
	uint64_t num = 1;
	r->disconnect_conn = 1;
	if (write(r->mgmt_eventfd1, &num, sizeof (num)) != sizeof (num))
		REPLICA_NOTICELOG("Failed to report err to mgmt_conn for "
		    "replica(%lu) (%s:%d) mgmt_fd%d\n", r->zvol_guid, r->ip,
		    r->port, r->mgmt_fd);
}

/*
 * fetch command from replica's command queue
 */
static rcmd_t *
dequeue_replica_cmdq(replica_t *replica)
{
	unsigned count = 0;

	count = get_num_entries_from_mempool(&(replica->cmdq));
	if (count)
		return get_from_mempool(&(replica->cmdq));
	else
		return NULL;
}

/*
 * perform cleanup for all pending IOs in replica's queue
 */
static void
respond_with_error_for_all_outstanding_ios(replica_t *r)
{
	rcmd_t *rcmd, *next_rcmd;
	int idx;
	rcommon_cmd_t *rcomm_cmd;
	pthread_cond_t *cond_var;
	int wait_cnt, ready_cnt, blocked_cnt;
	struct timespec wait_diff, ready_diff, blocked_diff;
	uint64_t read_cnt = 0, write_cnt = 0;

	ASSERT(r->data_eventfd == -1);

	ASSERT(r->data_eventfd == -1);

	while ((rcmd = dequeue_replica_cmdq(r)) != NULL)
		move_to_blocked_or_ready_q(r, rcmd);

	SEND_ERROR_RESPONSES((&(r->waitq)), r, cond_var, wait_cnt, wait_diff,
	    read_cnt, write_cnt);
	SEND_ERROR_RESPONSES((&(r->readyq)), r, cond_var, ready_cnt,
	    ready_diff, read_cnt, write_cnt);
	SEND_ERROR_RESPONSES((&(r->blockedq)), r, cond_var, blocked_cnt,
	    blocked_diff, read_cnt, write_cnt);

	REPLICA_ERRLOG("IO command set with error for replica(%lu) .."
	    "sent command(count:%d delay:%lu), "
	    "queued command(count:%d delay:%lu), "
	    "blocked command(count:%d delay:%lu).. "
	    " read_error(%lu) write_error(%lu)\n",
	    r->zvol_guid, wait_cnt, wait_diff.tv_sec, ready_cnt,
	    ready_diff.tv_sec, blocked_cnt, blocked_diff.tv_sec,
	    read_cnt, write_cnt);
}

/*
 * handle error in replica_thread (or data_connection)
 */
static int
handle_data_conn_error(replica_t *r)
{
	int fd, data_eventfd, mgmt_eventfd2, epollfd;
	spec_t *spec;
	replica_t *r1 = NULL;

	if (r->iofd == -1) {
		REPLICA_ERRLOG("repl %s %d %p\n", r->ip, r->port, r);
		return -1;
	}

	MTX_LOCK(&r->spec->rq_mtx);
	spec = r->spec;

	TAILQ_FOREACH(r1, &(spec->rq), r_next)
		if (r1 == r)
			break;

	if (r1 == NULL) {
		REPLICA_ERRLOG("replica %s %d not part of rqlist..\n",
		    r->ip, r->port);
		/*
		 * mgmt thread will check mgmt_eventfd2 fd to see if
		 * it needs to wait for replica thread or not.
		 * At this stage, the replica hasn't been added to
		 * spec's rqlist so we need to update mgmt_eventfd2 to -1
		 * here So that mgmt_thread can skip replica thread check.
		 */
		if (r->mgmt_eventfd2 != -1)
			r->mgmt_eventfd2 = -1;
		MTX_UNLOCK(&spec->rq_mtx);
		return -1;
	}

	TAILQ_REMOVE(&spec->rq, r, r_next);

	if (r->state == ZVOL_STATUS_HEALTHY)
		spec->healthy_rcount--;
	else if (r->state == ZVOL_STATUS_DEGRADED)
		spec->degraded_rcount--;

	update_volstate(r->spec);

	mgmt_eventfd2 = r->mgmt_eventfd2;

	epollfd = r->epollfd;

	MTX_UNLOCK(&spec->rq_mtx);

	MTX_LOCK(&r->r_mtx);
	(void) epoll_ctl(r->epollfd, EPOLL_CTL_DEL, r->iofd, NULL);

	fd = r->iofd;
	r->iofd = -1;
	shutdown(fd, SHUT_RDWR);
	close(fd);

	data_eventfd = r->data_eventfd;
	r->data_eventfd = -1;
	close_fd(epollfd, data_eventfd);

	if (r->io_resp_hdr) {
		free(r->io_resp_hdr);
		r->io_resp_hdr = NULL;
	}
	if (r->ongoing_io_buf) {
		free(r->ongoing_io_buf);
		r->ongoing_io_buf = NULL;
	}

	MTX_UNLOCK(&r->r_mtx);

	respond_with_error_for_all_outstanding_ios(r);

	MTX_LOCK(&r->r_mtx);
	r->conn_closed++;
	if (r->conn_closed != 2) {
		inform_mgmt_conn(r);
		pthread_cond_wait(&r->r_cond, &r->r_mtx);
	}
	else
	/*
	 * no one else should signal this cv other than replica thread,
	 * and mgmt_conn thread, which are consumers of mgmt_eventfd2
	 */
		pthread_cond_signal(&r->r_cond);
	r->mgmt_eventfd2 = -1;
	MTX_UNLOCK(&r->r_mtx);

	/* replica might have got destroyed */
	/* shouldn't access any replica related variables */
	close_fd(epollfd, mgmt_eventfd2);
	close(epollfd);
	return 0;
}

/*
 * find a command from replica's waitq according to io_sequence
 */
static rcmd_t *
find_replica_cmd(replica_t *r, uint64_t ioseq)
{
	rcmd_t *cmd;

	TAILQ_FOREACH(cmd, &(r->waitq), next) {
		if (cmd->io_seq == ioseq)
			return cmd;
	}

	return NULL;
}

/*
 * read response read on replica's data connection
 */
static int
read_cmd(replica_t *r)
{
	int fd = r->iofd;
	int state = r->io_state;
	zvol_io_hdr_t *resp_hdr = r->io_resp_hdr;
	uint8_t *resp_data = NULL;
	uint64_t reqlen;
	ssize_t count;
	rcmd_t *cmd;

	switch(state) {
		case READ_IO_RESP_HDR:
			ASSERT(r->io_read < sizeof(zvol_io_hdr_t));
			reqlen = sizeof (zvol_io_hdr_t) - (r->io_read);
			count = perform_read_write_on_fd(fd,
			    ((uint8_t *)resp_hdr) + (r->io_read), reqlen, state);
			if (count == -1)
				return -1;
			r->io_read += count;
			if (count != (ssize_t)reqlen)
				return READ_PARTIAL;

			if (resp_hdr->status != ZVOL_OP_STATUS_OK)
				return -1;

			cmd = find_replica_cmd(r, resp_hdr->io_seq);
			if (cmd == NULL)
				return -1;

			r->ongoing_io = cmd;

			r->io_state = READ_IO_RESP_DATA;
			r->io_read = 0;
			if ((resp_hdr->len == 0) ||
			    (resp_hdr->opcode == ZVOL_OPCODE_WRITE)) {
				r->ongoing_io_len = 0;
				r->ongoing_io_buf = NULL;
			}
			else {
				r->ongoing_io_len = resp_hdr->len;
				r->ongoing_io_buf = malloc(resp_hdr->len);
			}
			/* Fall through */
		case READ_IO_RESP_DATA:
			reqlen = r->ongoing_io_len - (r->io_read);
			resp_data = r->ongoing_io_buf;
			if (reqlen != 0) {
				count = perform_read_write_on_fd(fd,
				    ((uint8_t *)(resp_data)) + (r->io_read), reqlen, state);

				if (count == -1)
					return -1;
				r->io_read += count;
				if (count != (ssize_t)reqlen)
					return READ_PARTIAL;
			}
			return READ_COMPLETED;
		default:
			REPLICA_ERRLOG("got invalid read state(%d) for "
			    "replica(%lu).. aborting..\n", state,
			    r->zvol_guid);
			abort();
			break;
	}
	return -1;
}

static int
write_cmd(int fd, rcmd_t *cmd)
{
	ssize_t rc;
	int err, i;

	ASSERT(cmd->iovcnt > 0);
start:
	rc = writev(fd, cmd->iov, cmd->iovcnt);
	err = errno;
	if (rc < 0) {
		if (err == EINTR)
			goto start;
		if ((err != EAGAIN) && (err != EWOULDBLOCK))
			return -1;
		return WRITE_PARTIAL;
	}

	for (i = 0; i < cmd->iovcnt; i++) {
		if (cmd->iov[i].iov_len != 0 && cmd->iov[i].iov_len > (size_t)rc) {
			cmd->iov[i].iov_base = (void *)(((uint8_t *)cmd->iov[i].iov_base) + rc);
			cmd->iov[i].iov_len -= rc;
			break;
		}
		else {
			rc -= cmd->iov[i].iov_len;
			cmd->iov[i].iov_len = 0;
		}
	}

	if (i == cmd->iovcnt)
		return WRITE_COMPLETED;

	/* This is the case where
	 * - writev wrote less
	 * - there is more data to write
	 * - and writev can write more */
	goto start;
}

int
do_drainfd(int data_eventfd)
{
	uint64_t value;
	int rc, err;
	while (1) {
		rc = read(data_eventfd, &value, sizeof (value));
		err = errno;
		if (rc < 0) {
			if (err == EINTR)
				continue;
			if (err != EAGAIN && err != EWOULDBLOCK)
				return -1;
			break;
		}
		if (rc == 0) {
			return -1;
		}
	}
	return 0;
}

static int
handle_epoll_out_event(replica_t *r)
{
	rcmd_t *cmd;
	int ret;

	while ((cmd = TAILQ_FIRST(&r->readyq)) != NULL) {
		ret = write_cmd(r->iofd, cmd);
		if (ret < 0)
			return -1;
		if (ret == WRITE_COMPLETED) {
			TAILQ_REMOVE(&r->readyq, cmd, next);
			TAILQ_INSERT_TAIL(&r->waitq, cmd, next);
			continue;
		}
		else {
			//ASSERT(ret == WRITE_PARTIAL);
			break;
		}
	}
	return 0;
}

static int
handle_epoll_in_event(replica_t *r)
{
	int ret, idx;
	rcommon_cmd_t *rcomm_cmd;
	bool task_completed = false;
	bool unblocked = false;
	pthread_cond_t *cond_var;

start:
	ret = read_cmd(r);
	if (ret < 0)
		return -1;

	if (ret == READ_COMPLETED) {
		idx = r->ongoing_io->idx;
		TAILQ_REMOVE(&r->waitq, r->ongoing_io, next);
		rcomm_cmd = r->ongoing_io->rcommq_ptr;

		cond_var = rcomm_cmd->cond_var;

		rcomm_cmd->resp_list[idx].io_resp_hdr = *(r->io_resp_hdr);
		rcomm_cmd->resp_list[idx].data_ptr = r->ongoing_io_buf;

		DECREMENT_INFLIGHT_REPLICA_IO_CNT(r, rcomm_cmd->opcode);

		/*
		 * cleanup_deadlist thread performs cleanup of rcomm_cmd.
		 * if the response from all replica is received. Since we are
		 * avoiding locking for rcomm_cmd, we will update response
		 * status in rcomm_cmd at last.
		 */
		if (rcomm_cmd->state != CMD_EXECUTION_DONE) {
			rcomm_cmd->resp_list[idx].status |= RECEIVED_OK;
			pthread_cond_signal(cond_var);
		} else
			rcomm_cmd->resp_list[idx].status |= RECEIVED_OK;

		free(r->ongoing_io->iov_data);
		free(r->ongoing_io);
		r->ongoing_io = NULL;
		r->io_read = 0;
		r->ongoing_io_buf = NULL;
		r->io_state = READ_IO_RESP_HDR;

		task_completed = true;
		goto start;
	}

	ret = 0;
	if (task_completed == true) {
		unblocked = unblock_cmds(r);
		if (unblocked == true)
			ret = handle_epoll_out_event(r);
	}

	return ret;
}

static int
handle_data_eventfd(void *arg)
{
	replica_t *r = (replica_t *)arg;
	rcmd_t *cmd;
	int ret;

	while ((cmd = dequeue_replica_cmdq(r)) != NULL)
		move_to_blocked_or_ready_q(r, cmd);
	ret = handle_epoll_out_event(r);
	return ret;
}

static int
handle_mgmt_eventfd(void *arg)
{
	replica_t *r = (replica_t *) arg;
	if (r->disconnect_conn > 0)
		return -1;
	return 0;
}

void *
replica_thread(void *arg)
{
	int r_data_eventfd = -1, r_mgmt_eventfd = -1, r_epollfd = -1;
	struct epoll_event ev, events[MAXEVENTS];
	int i, nfds, fd, ret = 0;
	void *ptr;
	struct timespec diff_time, last_time, now;
	replica_t *r = (replica_t *)arg;
	int polling_timeout = (replica_timeout / 4) * 1000;
	int epoll_timeout = polling_timeout;
	int wait_count = 1;
	pthread_t self = pthread_self();

	snprintf(tinfo, sizeof tinfo, "r#%d.%lu", (int)(((uint64_t *)self)[0]), r->zvol_guid);

	r_data_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_data_eventfd < 0) {
		REPLICA_ERRLOG("error for replica(%s:%d) data_eventfd:%d\n",
		    r->ip, r->port, r_data_eventfd);
		return NULL;
	}

	r_epollfd = epoll_create1(0);
	if (r_epollfd < 0) {
		REPLICA_ERRLOG("epoll_create error for replica(%s:%d) "
		    "errno(%d)\n", r->ip, r->port, errno);
		goto initialize_error;
	}

	ev.events = EPOLLIN;
	ev.data.fd = r_data_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_data_eventfd, &ev) == -1) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) err(%d)\n",
		    r->ip, r->port, errno);
		goto initialize_error;
	}

	r_mgmt_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_mgmt_eventfd < 0) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) err(%d)\n",
		    r->ip, r->port, errno);
		goto initialize_error;
	}

	ev.data.fd = r_mgmt_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_mgmt_eventfd, &ev) == -1) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) err(%d)\n",
		    r->ip, r->port, errno);
		goto initialize_error;
	}

	ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLET |
	    EPOLLRDHUP;
	ev.data.ptr = NULL;

	MTX_LOCK(&r->r_mtx);

	if ((r->iofd == -1) ||
	    (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r->iofd, &ev) == -1)) {
		MTX_UNLOCK(&r->r_mtx);
		REPLICA_ERRLOG("epoll error for replica(%s:%d) err(%d)\n",
		    r->ip, r->port, errno);
initialize_error:
		if (r_mgmt_eventfd > 0) {
			close (r_mgmt_eventfd);
			r_mgmt_eventfd = -1;
		}

		if ((r_epollfd > 0) && (r_data_eventfd > 0)) {
			/*
			 * epoll_ctl may fail so we are ignoring return
			 * value of epoll_ctl
			 */
			(void) epoll_ctl(r_epollfd, EPOLL_CTL_DEL,
			    r_data_eventfd, NULL);
			close(r_epollfd);
			r->epollfd = -1;
		}

		if (r_data_eventfd) {
			close(r_data_eventfd);
			r->data_eventfd = -1;
		}
		return NULL;
	}

	r->data_eventfd = r_data_eventfd;
	r->epollfd = r_epollfd;
	r->mgmt_eventfd2 = r_mgmt_eventfd;

	MTX_UNLOCK(&r->r_mtx);

	prctl(PR_SET_NAME, "replica", 0, 0, 0);
	clock_gettime(CLOCK_MONOTONIC, &last_time);

	while (1) {
		nfds = epoll_wait(r_epollfd, events, MAXEVENTS, epoll_timeout);
		if (nfds == -1) {
			if (errno == EINTR)
				continue;
			REPLICA_ERRLOG("epoll_wait error for replica(%s:%d) "
			    "err(%d)\n", r->ip, r->port, errno);
			ret = -1;
			goto exit;
		}

		if (r->disconnect_conn) {
			ret = -1;
			REPLICA_ERRLOG("replica disconnected(%s:%d)\n", r->ip, r->port);
			goto exit;
		}

		for (i = 0; i < nfds; i++) {
			fd = events[i].data.fd;
			if ((fd == r_data_eventfd) || (fd == r_mgmt_eventfd)) {
				ret = do_drainfd(fd);
				if (ret == -1)
					goto exit;
				if (fd == r_data_eventfd)
					ret = handle_data_eventfd(arg);
				else
					ret = handle_mgmt_eventfd(arg);
				if (ret == -1)
					goto exit;
				continue;
			}

			ret = -1;
			ptr = events[i].data.ptr;
			if (ptr != NULL)
				goto exit;

			if (events[i].events & (EPOLLERR | EPOLLRDHUP | EPOLLHUP))
				goto exit;

			ret = 0;
			if (events[i].events & EPOLLIN)
				ret = handle_epoll_in_event(arg);

			if (ret == -1)
				goto exit;

			if (events[i].events & EPOLLOUT)
				ret = handle_epoll_out_event(arg);

			if (ret == -1)
				goto exit;
		}

		/*
		 * Here, we are checking if replica are taking much time to
		 * responde than expected time.
		 * Expected time is set to replica_timeout in ms.
		 *
		 * We will check time difference for first IOs from all replica
		 * queues (waitq, readyq and blocked queue) at an interval of
		 * `x` ms.
		 * `x` is set to minimum of (replica_timeout/4) or lowest
		 * time_diff of IOs from all replica queue.
		 */
		clock_gettime(CLOCK_MONOTONIC, &now);
		timesdiff(CLOCK_MONOTONIC, last_time, now, diff_time);
		polling_timeout = (replica_timeout / 4) * 1000;
		if (epoll_timeout > polling_timeout)
			epoll_timeout = polling_timeout;

		if (((diff_time.tv_sec * 1000) +
		    (diff_time.tv_nsec / 1000000)) > epoll_timeout) {
			epoll_timeout = polling_timeout;
			CHECK_REPLICA_TIMEOUT(&r->readyq, diff_time, ret, exit,
			    epoll_timeout);
			CHECK_REPLICA_TIMEOUT(&r->waitq, diff_time, ret, exit,
			    epoll_timeout);
			CHECK_REPLICA_TIMEOUT(&r->blockedq, diff_time, ret,
			    exit, epoll_timeout);

			/*
			 * In CHECK_REPLICA_TIMEOUT macro, we are logging
			 * replica details and time_diff since how long replica
			 * is not responding at an interval of
			 * (replica_timeout/4) ms.
			 */
			if (epoll_timeout < replica_timeout)
				wait_count = 1;
			clock_gettime(CLOCK_MONOTONIC, &last_time);
		}
	}
exit:
	if (ret == -1)
		handle_data_conn_error(r);
	REPLICA_ERRLOG("replica_thread exiting ...\n");
	return NULL;
}
