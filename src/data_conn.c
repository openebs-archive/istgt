#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>
#include <stdbool.h>
#include "istgt.h"
#include "zrepl_prot.h"
#include "istgt_integration.h"
#include "replication.h"
#include "ring_mempool.h"

#define	READ_PARTIAL	1
#define	READ_COMPLETED	2

#define	WRITE_PARTIAL	1
#define	WRITE_COMPLETED	2

extern rte_smempool_t rcmd_mempool;

bool unblock_cmds(replica_t *r);
void move_to_blocked_or_ready_q(replica_t *r, rcmd_t *cmd);
void respond_with_error_for_all_outstanding_ios(replica_t *r);
int handle_data_conn_error(replica_t *r);
int handle_epoll_out_event(replica_t *r);
static ssize_t perform_read_on_fd(int fd, uint8_t *data, uint64_t len);
rcmd_t * find_replica_cmd(replica_t *r, uint64_t ioseq);
int read_cmd(replica_t *r);
int write_cmd(int fd, rcmd_t *cmd);
int handle_data_eventfd(void *arg);
int do_drainfd(int data_eventfd);
int handle_epoll_in_event(replica_t *r);
void *replica_thread(void *arg);

#if 0
	parsed = 0;
	data_node = TAILQ_FIRST(&cmd->data_list);
	while (data_node != NULL) {
		next_node = TAILQ_NEXT(data_node);
		ret = write_data_segment(data_node, parsed, written);
		if (ret == WRITE_COMPLETED) {
			data_node = next_node;
			continue;
		}
		else
			break;
	}
}

write_status_t
write_data_segment(data_node, parsed, written)
{
	len = data_node->len;
	if ((parsed + len) <= written) {
		parsed += len;
		data_node = next_node;
		continue;
	}
	offset_to_write = (written - parsed);
	len_to_write = (parsed + len - written);
	rc = write(data_node->data_ptr, offset_to_write, len_to_write);
}
#endif

#define check_for_blockage() { \
	uint64_t current_lba = cmd->offset; \
	uint64_t current_lbE = current_lba + cmd->data_len; \
	uint64_t pending_lba = pending_rcmd->offset; \
	uint64_t pending_lbE = pending_lba + pending_rcmd->data_len; \
	if ((current_lbE < pending_lba)  || (pending_lbE < current_lba)) { \
		cmd_blocked = false; \
	} else { \
		cmd_blocked = true; \
	} \
}

#define CHECK_BLOCKAGE_IN_Q(queue, next) { \
	if(cmd_blocked == false) { \
		TAILQ_FOREACH(pending_rcmd, queue, next) { \
			check_for_blockage(); \
			if(cmd_blocked == true) \
				break; \
		} \
	} \
}

#define SEND_ERROR_RESPONSES(head) { \
	cmd = TAILQ_FIRST(head); \
	while (cmd != NULL) { \
		next_cmd = TAILQ_NEXT(cmd, next); \
		TAILQ_REMOVE(head, cmd, next); \
		idx = cmd->idx; \
		rcomm_cmd = cmd->rcommq_ptr; \
		rcomm_cmd->resp_list[idx].io_resp_hdr.status = ZVOL_OP_STATUS_FAILED; \
		rcomm_cmd->resp_list[idx].data_ptr = NULL; \
		rcomm_cmd->resp_list[idx].data_len = 0; \
		rcomm_cmd->resp_list[idx].status = 1; \
		MTX_LOCK(rcomm_cmd->mutex); \
		pthread_cond_signal(rcomm_cmd->cond_var); \
		MTX_UNLOCK(rcomm_cmd->mutex); \
		cmd = next_cmd; \
	} \
}

bool
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

void
move_to_blocked_or_ready_q(replica_t *r, rcmd_t *cmd)
{
	bool cmd_blocked = false;
	rcmd_t *pending_rcmd;

	if (!TAILQ_EMPTY(&r->blockedq)) {
		TAILQ_INSERT_TAIL(&r->blockedq, cmd, next);
		goto done;
	}
	CHECK_BLOCKAGE_IN_Q(&r->readyq, next);
	CHECK_BLOCKAGE_IN_Q(&r->waitq, next);
	if (cmd_blocked == true)
		TAILQ_INSERT_TAIL(&r->blockedq, cmd, next);
	else
		TAILQ_INSERT_TAIL(&r->readyq, cmd, next);
done:
	return;
}

void
inform_mgmt_conn(replica_t *r)
{
	uint64_t num = 1;
	r->disconnect_conn = 1;
	write(r->mgmt_eventfd1, &num, sizeof (num));
}

void *
dequeue_replica_cmdq(replica_t *replica)
{
	unsigned count = 0;

	count = get_num_entries_from_mempool(&(replica->cmdq));
	if (count)
		return get_from_mempool(&(replica->cmdq));
	else
		return NULL;
}

void
respond_with_error_for_all_outstanding_ios(replica_t *r)
{
	rcmd_t *cmd, *next_cmd;
	int idx;
	rcommon_cmd_t *rcomm_cmd;
	pthread_mutex_t *mtx;
	pthread_cond_t *cond_var;

	while ((cmd = dequeue_replica_cmdq(r) != NULL))
		move_to_blocked_or_ready_q(r, cmd);

	SEND_ERROR_RESPONSES((&(r->waitq)));
	SEND_ERROR_RESPONSES((&(r->readyq)));
	SEND_ERROR_RESPONSES((&(r->blockedq)));
}

/*
 * TODO:
 * need spec lock to update replica vars also
 */
int
handle_data_conn_error(replica_t *r)
{
	int fd, data_eventfd, mgmt_eventfd2, epollfd;
	spec_t *spec;
	replica_t *r1 = NULL;

	if (r->iofd == -1)
		return -1;

	MTX_LOCK(&r->spec->rq_mtx);
	spec = r->spec;

	TAILQ_FOREACH(r1, &(spec->rq), r_next)
		if (r1 == r)
			break;

	if (r1 == NULL) {
		REPLICA_ERRLOG("replica %s %d not part of rqlist..\n", r->ip, r->port);
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

	if (epoll_ctl(r->epollfd, EPOLL_CTL_DEL, r->iofd, NULL) == -1) {
		perror("epoll_ctl");
		return -1;
	}

	fd = r->iofd;
	r->iofd = -1;

//TODO check
        if (r->io_resp_hdr)
                free(r->io_resp_hdr);
        if (r->ongoing_io_buf)
                free(r->ongoing_io_buf);

	close(fd);

	data_eventfd = r->data_eventfd;
	r->data_eventfd = -1;
	close(data_eventfd);

	respond_with_error_for_all_outstanding_ios(r);

	MTX_LOCK(&r->r_mtx);
	r->conn_closed++;
	if (r->conn_closed != 2) {
		//ASSERT(r->conn_closed == 1);
		inform_mgmt_conn(r);
		pthread_cond_wait(&r->r_cond, &r->r_mtx);
	}
	else
	/*
	 * no one else should signal this cv other than replica thread,
	 * and mgmt_conn thread, which are consumers of mgmt_eventfd2
	 */
		pthread_cond_signal(&r->r_cond);
	MTX_UNLOCK(&r->r_mtx);

	/* replica might have got destroyed */
	/* shouldn't access any replica related variables */
	close(mgmt_eventfd2);

	close(epollfd);
	return 0;
}

int
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
			//ASSERT(ret == WRITE_PARTIAL); TODO
			break;
		}
	}
	return 0;
}

static ssize_t
perform_read_on_fd(int fd, uint8_t *data, uint64_t len)
{
	ssize_t rc, nbytes = 0;

	while(1) {
		rc = read(fd, data + nbytes, len - nbytes);

		if(rc < 0) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return nbytes;
			return -1;
		} else if (rc == 0) {
			REPLICA_ERRLOG("received EOF on fd %d, closing it..\n", fd);
			/* Return -1 here as entire data is not read, and, connection got closed */
			return -1;
		}

		nbytes += rc;
		if(nbytes == (ssize_t )len)
			break;
	}
	return nbytes;
}

rcmd_t *
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
 * TODO:
 * ongoing_io
 * ongoing_io_len, and ongoing_io_buf
 */
int
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
			count = perform_read_on_fd(fd, ((uint8_t *)resp_hdr) + (r->io_read), reqlen);
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
			if ((resp_hdr->len == 0) || (resp_hdr->opcode == ZVOL_OPCODE_WRITE)) {
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
				count = perform_read_on_fd(fd, ((uint8_t *)(resp_data)) + (r->io_read),
				    reqlen);

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

int
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

#if 0
		if (is_destroy_cmd(cmd)) {
			//handle_data_conn_error(r);
			return -1;
		}
#endif

/*
 * TODO:
 * dequeue_replica_cmdq
 * lockless cmdq in replica
 * blockedq, readyq, waitq
 */
int
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
			//TODO chk whether rc is 0 means EOF on fd
			return -1;
		}
	}
	return 0;
}

/*
 * TODO:
 * idx in rcmd
 * mutex, cond_var in rcommon_cmd_t
 * resp_list[].io_resp_hdr, data_ptr, data_len, status in rcommon_cmd_t
 */
int
handle_epoll_in_event(replica_t *r)
{
	int ret, idx;
	rcommon_cmd_t *rcomm_cmd;
	bool task_completed = false;
	bool unblocked = false;
	pthread_mutex_t *mtx;
	pthread_cond_t *cond_var;

start:
	ret = read_cmd(r);
	if (ret < 0)
		return -1;

	if (ret == READ_COMPLETED) {
		idx = r->ongoing_io->idx;
		TAILQ_REMOVE(&r->waitq, r->ongoing_io, next);
		rcomm_cmd = r->ongoing_io->rcommq_ptr;

		mtx = rcomm_cmd->mutex;
		cond_var = rcomm_cmd->cond_var;

		rcomm_cmd->resp_list[idx].io_resp_hdr = *(r->io_resp_hdr);
		rcomm_cmd->resp_list[idx].data_ptr = r->ongoing_io_buf;
		if (rcomm_cmd->opcode == ZVOL_OPCODE_WRITE)
			rcomm_cmd->resp_list[idx].data_len = rcomm_cmd->data_len;
		else
			rcomm_cmd->resp_list[idx].data_len = r->ongoing_io_len;
		rcomm_cmd->resp_list[idx].status = 1;

		MTX_LOCK(mtx);
		if (rcomm_cmd->status != 5) { //TODO change it to something meaningful
			pthread_cond_signal(cond_var);
		}
		MTX_UNLOCK(mtx);

		free(r->ongoing_io->iov_data);
		put_to_mempool(&rcmd_mempool, r->ongoing_io);
		r->ongoing_io = NULL;
		r->io_read = 0;
		r->ongoing_io_buf = NULL;
		r->io_state = READ_IO_RESP_HDR;

		task_completed = true;
		goto start;
	}
	else {
		//ASSERT(ret == READ_PARTIAL);
		//break;
	}

	ret = 0;
	if (task_completed == true) {
		unblocked = unblock_cmds(r);
		if (unblocked == true)
			ret = handle_epoll_out_event(r);
	}

	return ret;
}

/*
 * TODO:
 * added data_eventfd, mgmt_eventfd, epollfd to replica
 */
#define	MAX_EVENTS	64
void *
replica_thread(void *arg)
{
	int r_data_eventfd, r_mgmt_eventfd, r_epollfd;
	struct epoll_event ev, events[MAX_EVENTS];
	int i, nfds, fd, ret = 0;
	void *ptr;
	replica_t *r = (replica_t *)arg;

	r->data_eventfd = r_data_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_data_eventfd < 0) {
		perror("r_data_eventfd");
		return NULL;
	}

	r->mgmt_eventfd2 = r_mgmt_eventfd = eventfd(0, EFD_NONBLOCK);
	if (r_mgmt_eventfd < 0) {
		perror("r_mgmt_eventfd");
		return NULL;
	}

	r->epollfd = r_epollfd = epoll_create1(0);
	if (r_epollfd < 0) {
		perror("epoll_create");
		return NULL;
	}

	ev.events = EPOLLIN;
	ev.data.fd = r_data_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_data_eventfd, &ev) == -1) {
		perror("epoll_ctl_data_eventfd");
		return NULL;
	}

	ev.data.fd = r_mgmt_eventfd;
	if (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r_mgmt_eventfd, &ev) == -1) {
		perror("epoll_ctl_mgmt_eventfd");
		return NULL;
	}

	ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLET | EPOLLRDHUP;
	ev.data.ptr = NULL;

	MTX_LOCK(&r->r_mtx);
	if ((r->iofd == -1) || (epoll_ctl(r_epollfd, EPOLL_CTL_ADD, r->iofd, &ev) == -1)) {
		MTX_UNLOCK(&r->r_mtx);
		perror("epoll_ctl_data_eventfd");
		return NULL;
	}
	MTX_UNLOCK(&r->r_mtx);

	prctl(PR_SET_NAME, "replica", 0, 0, 0);

	while (1) {
		nfds = epoll_wait(r_epollfd, events, MAX_EVENTS, -1);
		if (nfds == -1) {
			if (errno == EINTR)
				continue;
			perror("epoll_wait");
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
				//else
					//ret = handle_mgmt_eventfd(arg);
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
		/* Need to handle wait IOs that are taking more than IO timeout */
		// handle_data_conn_error(r);
		// return -1;
	}
exit:
	if (ret == -1)
		handle_data_conn_error(r);
	return NULL;
}

