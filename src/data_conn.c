#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>
#include <stdbool.h>
#include "istgt.h"
#include "zrepl_prot.h"
#include "istgt_integration.h"
#include "replication.h"
#include "replication_misc.h"
#include "ring_mempool.h"

#define	READ_PARTIAL	1
#define	READ_COMPLETED	2

#define	WRITE_PARTIAL	1
#define	WRITE_COMPLETED	2

extern rte_smempool_t rcmd_mempool;
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

#define SEND_ERROR_RESPONSES(head, _cond) {				\
	rcmd = TAILQ_FIRST(head);					\
	while (rcmd != NULL) {						\
		next_rcmd = TAILQ_NEXT(rcmd, next);			\
		TAILQ_REMOVE(head, rcmd, next);				\
		idx = rcmd->idx;					\
		rcomm_cmd = rcmd->rcommq_ptr;				\
		_cond = rcomm_cmd->cond_var;				\
		if (rcomm_cmd->opcode == ZVOL_OPCODE_WRITE)		\
			r->replica_inflight_write_io_cnt -= 1;		\
		rcomm_cmd->resp_list[idx].io_resp_hdr.status =		\
		    ZVOL_OP_STATUS_FAILED;				\
		rcomm_cmd->resp_list[idx].data_ptr = NULL;		\
		rcomm_cmd->resp_list[idx].status |= RECEIVED_ERR;	\
		if (rcomm_cmd->state != CMD_EXECUTION_DONE)		\
			pthread_cond_signal(_cond);			\
		free(rcmd->iov_data);					\
		put_to_mempool(&rcmd_mempool, rcmd);			\
		rcmd = next_rcmd;					\
	}								\
}

#define	CHECK_REPLICA_TIMEOUT(_head, _diff, _ret, _exit_label, etimeout)\
	do {								\
		struct timespec now;					\
		rcmd_t *pending_cmd;					\
		int ms;							\
		pending_cmd = TAILQ_FIRST(_head);			\
		if (pending_cmd != NULL) {				\
			clock_gettime(CLOCK_MONOTONIC, &now);		\
			timesdiff(pending_cmd->queued_time, now, _diff);\
			if (_diff.tv_sec >= replica_timeout) {		\
				REPLICA_ERRLOG("timeout happened for "	\
				    "replica(%s:%d)..time(%lu)\n", 	\
				    r->ip, r->port, _diff.tv_sec); 	\
				_ret = -1;				\
				goto _exit_label;			\
			} else {					\
				ms = _diff.tv_sec * 1000;		\
				ms += _diff.tv_nsec / 1000000;		\
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
			continue;
		CHECK_BLOCKAGE_IN_Q(&r->waitq, next);
		if (cmd_blocked == true)
			continue;
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

	if (cmd->opcode == ZVOL_OPCODE_WRITE)
		r->replica_inflight_write_io_cnt += 1;

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
static void
inform_mgmt_conn(replica_t *r)
{
	uint64_t num = 1;
	r->disconnect_conn = 1;
	if (write(r->mgmt_eventfd1, &num, sizeof (num)) != sizeof (num))
		REPLICA_NOTICELOG("Failed report err to mgmt_conn for replica(%p)\n", r);
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

	while ((rcmd = dequeue_replica_cmdq(r)) != NULL)
		move_to_blocked_or_ready_q(r, rcmd);

	SEND_ERROR_RESPONSES((&(r->waitq)), cond_var);
	SEND_ERROR_RESPONSES((&(r->readyq)), cond_var);
	SEND_ERROR_RESPONSES((&(r->blockedq)), cond_var);
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
	if (epoll_ctl(r->epollfd, EPOLL_CTL_DEL, r->iofd, NULL) == -1) {
		MTX_UNLOCK(&r->r_mtx);
		REPLICA_ERRLOG("epoll error for replica(%s:%d) iofd:%d "
		    "errno(%d)\n", r->ip, r->port, r->iofd, errno);
		return -1;
	}

	fd = r->iofd;
	r->iofd = -1;
	close(fd);

	data_eventfd = r->data_eventfd;
	r->data_eventfd = -1;
	close_fd(epollfd, data_eventfd);

	MTX_UNLOCK(&r->r_mtx);

	if (r->io_resp_hdr)
		free(r->io_resp_hdr);
	if (r->ongoing_io_buf)
		free(r->ongoing_io_buf);

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
	}
	return -1;
}

static int
write_cmd(int fd, rcmd_t *cmd)
{
	ssize_t rc;
	int err, i;

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
		if (rcomm_cmd->opcode == ZVOL_OPCODE_WRITE)
			r->replica_inflight_write_io_cnt -= 1;
		rcomm_cmd->resp_list[idx].status |= RECEIVED_OK;

		if (rcomm_cmd->state != CMD_EXECUTION_DONE) {
			pthread_cond_signal(cond_var);
		}

		free(r->ongoing_io->iov_data);
		put_to_mempool(&rcmd_mempool, r->ongoing_io);
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
	int r_data_eventfd, r_mgmt_eventfd, r_epollfd;
	struct epoll_event ev, events[MAXEVENTS];
	int i, nfds, fd, ret = 0;
	void *ptr;
	struct timespec diff_rq, diff_bq, diff_wq;
	replica_t *r = (replica_t *)arg;
	int polling_timeout = (replica_timeout / 3) * 1000;
	int epoll_timeout = polling_timeout;

	r->data_eventfd = r_data_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_data_eventfd < 0) {
		REPLICA_ERRLOG("error for replica(%s:%d) data_eventfd:%d\n",
		    r->ip, r->port, r_data_eventfd);
		return NULL;
	}

	r->epollfd = r_epollfd = epoll_create1(0);
	if (r_epollfd < 0) {
		REPLICA_ERRLOG("epoll_create error for replica(%s:%d) "
		    "errno(%d)\n", r->ip, r->port, errno);
		return NULL;
	}

	ev.events = EPOLLIN;
	ev.data.fd = r_data_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_data_eventfd, &ev) == -1) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) errno(%d)\n",
		    r->ip, r->port, errno);
		return NULL;
	}

	r->mgmt_eventfd2 = r_mgmt_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_mgmt_eventfd < 0) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) errno(%d)\n",
		    r->ip, r->port, errno);
		return NULL;
	}

	ev.data.fd = r_mgmt_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_mgmt_eventfd, &ev) == -1) {
		REPLICA_ERRLOG("epoll error for replica(%s:%d) errno(%d)\n",
		    r->ip, r->port, errno);
		return NULL;
	}

	ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLET | EPOLLRDHUP;
	ev.data.ptr = NULL;

	MTX_LOCK(&r->r_mtx);
	if ((r->iofd == -1) ||
	    (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r->iofd, &ev) == -1)) {
		MTX_UNLOCK(&r->r_mtx);
		REPLICA_ERRLOG("epoll error for replica(%s:%d) errno(%d)\n",
		    r->ip, r->port, errno);
		return NULL;
	}
	MTX_UNLOCK(&r->r_mtx);

	prctl(PR_SET_NAME, "replica", 0, 0, 0);

	while (1) {
		nfds = epoll_wait(r_epollfd, events, MAXEVENTS, epoll_timeout);
		if (nfds == -1) {
			if (errno == EINTR)
				continue;
			REPLICA_ERRLOG("epoll_wait error for replica(%s:%d) "
			    "errno(%d)\n", r->ip, r->port, errno);
			ret = -1;
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
		 * set epoll timeout to minimum of replica_io_timeout/3 or minimum of last
		 * queued IO time diff in all queues (waitq, readyq and blocked queue)
		 */
		epoll_timeout = polling_timeout;
		CHECK_REPLICA_TIMEOUT(&r->readyq, diff_wq, ret, exit, epoll_timeout);
		CHECK_REPLICA_TIMEOUT(&r->waitq, diff_rq, ret, exit, epoll_timeout);
		CHECK_REPLICA_TIMEOUT(&r->blockedq, diff_bq, ret, exit, epoll_timeout);
	}
exit:
	if (ret == -1)
		handle_data_conn_error(r);
	REPLICA_ERRLOG("replica_thread exiting ...\n");
	return NULL;
}
