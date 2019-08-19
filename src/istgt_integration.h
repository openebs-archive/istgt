/*
 * Copyright © 2017-2019 The OpenEBS Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _ISTGT_INTEGRATION_H
#define	_ISTGT_INTEGRATION_H

#include <stdbool.h>
#include "istgt_lu.h"
#include "istgt_sock.h"
#include "ring_mempool.h"

typedef int (*cstor_listen)(const char *, int, int, int);
typedef int (*cstor_connect)(const char *, int);

TAILQ_HEAD(, istgt_lu_disk_t) spec_q;
pthread_mutex_t specq_mtx;

typedef ISTGT_QUEUE *cmd_ptr;
typedef struct istgt_lu_disk_t spec_t;
typedef struct rcmd_s rcmd_t;

/*
 * state of commands queued for replica according to send/receive
 */
typedef enum {
	READ_IO_RESP_HDR = 1,	/* to read mgmt IO hdr */
	READ_IO_RESP_DATA = 2,	/* to read handshake msg data */
	WRITE_IO_SEND_HDR = 3,	/* to write mgmt IO hdr */
	WRITE_IO_SEND_DATA = 4,	/* to write mgmt IO data */
} mgmt_cmd_state;

typedef struct mgmt_ack mgmt_ack_data_t;
typedef enum zvol_status replica_state_t;

typedef struct replica_s {
	TAILQ_ENTRY(replica_s) r_next;
	/* For Replicas which are connected with the quorum value as 0 */
	TAILQ_ENTRY(replica_s) r_non_quorum_next;
	TAILQ_ENTRY(replica_s) r_waitnext;
	/* list of IOs queued from spec to replica */
	rte_smempool_t cmdq;
	/* list of IOs (non-blocking IOs) ready to sent to replica */
	TAILQ_HEAD(, rcmd_s) readyq;
	/* list of IOs waiting for the response from replica */
	TAILQ_HEAD(, rcmd_s) waitq;
	/* list of blocked IOs */
	TAILQ_HEAD(, rcmd_s) blockedq;
	/* replica level cond. variable */
	pthread_cond_t r_cond;
	/* replica level mutex lock */
	pthread_mutex_t r_mtx;
	replica_state_t state;
	spec_t *spec;
	int iofd;
	/* management connection descriptor */
	int mgmt_fd;
	uint8_t quorum;
	uint8_t reserved;
	/* replica's IOs server port */
	uint16_t port;
	/* replica's IP */
	char *ip;
	uint64_t pool_guid;
	uint64_t zvol_guid;

	/* payload for current IO response for a replica */
	void *ongoing_io_buf;

	/* payload size for IO response for a replica */
	uint64_t ongoing_io_len;
	/* IO for which we are receiving response from replica */
	rcmd_t *ongoing_io;
	/* data ptr for epoll */
	void *m_event1;
	/* eventfd to notify for a command in replica's mgmt cmd queue */
	int mgmt_eventfd1;
	/* data ptr for epoll */
	void *m_event2;
	/* to inform mgmt interface for an error in data interface */
	int mgmt_eventfd2;
	/* to inform data interface for an error in mgmt interface */
	int disconnect_conn;
	/* To track tear down of replica's interfaces(mgmt and data) */
	int conn_closed;
	/* Epoll descriptor for data interface */
	int epollfd;
	/* eventfd to notify data interface for IOs in replica's cmdq */
	int data_eventfd;
	/* Epoll descriptor for mgmt interface */
	int epfd;

	int dont_free;

	struct timespec create_time;

	/* This is calculated from create time till the queued time into readyQ */
	/* Total time(ns) waited in queue to send read_IO req */
	uint64_t totalread_reqtime;
	/* Total time(ns) waited in queue to send write_IO req */
	uint64_t totalwrite_reqtime;

	/* This is calculated from create time till the entire resp read from wire*/
	/* Total time(ns) to recv read_IO resp */
	uint64_t totalread_resptime;
	/* Total time(ns) to recv write_IO resp */
	uint64_t totalwrite_resptime;
	/*
	 * Following variables should be updated with atomic operation only
	 */
	uint64_t replica_inflight_write_io_cnt;
	uint64_t replica_inflight_read_io_cnt;
	uint64_t replica_inflight_sync_io_cnt;

	/* header recieved on data connection */
	zvol_io_hdr_t *io_resp_hdr;
	/* state of command on data connection */
	int io_state;
	/* amount of IO data read in current IO state for data connection */
	uint32_t io_read;
	/* header recieved on management connection */
	zvol_io_hdr_t *mgmt_io_resp_hdr;
	/* data recieved on management connection */
	void *mgmt_io_resp_data;
	/* mgmt command queue for management connection */
	TAILQ_HEAD(, mgmt_cmd_s) mgmt_cmd_queue;

	uint64_t initial_checkpointed_io_seq;
	/* port from which replica has made mgmt conn. */
	int replica_mgmt_dport;
	REPLICA_STATUS_Ptr status;
} replica_t;

typedef struct cstor_conn_ops {
	cstor_listen conn_listen;
	cstor_connect conn_connect;
} cstor_conn_ops_t;

void *cleanup_deadlist(void *);
int initialize_replication(void);
int handle_write_resp(spec_t *, replica_t *);
int handle_read_resp(spec_t *, replica_t *);
int update_replica_list(int, spec_t *, int);
replica_t *create_replica_entry(spec_t *, int, int);
int update_replica_entry(spec_t *, replica_t *, int);
int handle_read_data_event(replica_t *);
int handle_write_data_event(replica_t *replica);
void update_volstate(spec_t *);
#endif /* _ISTGT_INTEGRATION_H */
