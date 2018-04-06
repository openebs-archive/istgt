
#ifndef ISCSI
#define ISCSI 1
#include "istgt_lu.h"
#include "istgt_sock.h"
#include <stdbool.h>

typedef int (*cstor_listen)(const char *, int, int);
typedef int (*cstor_connect)(const char *, int);
typedef void (*cstor_read)(void *, void *, void *, void *, uint64_t);
typedef void (*cstor_close)(void *, void *, void *, void *, uint64_t);

TAILQ_HEAD(, istgt_lu_disk_t) spec_q;
pthread_mutex_t specq_mtx;

typedef ISTGT_QUEUE *cmd_ptr;
typedef struct istgt_lu_disk_t spec_t;

typedef enum replica_state_s {
	ADDED_TO_SPEC,
	NEED_TO_ADD_TO_EPOLL,
	ADDED_TO_EPOLL,
	REMOVED_FROM_EPOLL,
	NEED_REMOVAL_FROM_EPOLL,
} replica_state_t;

typedef struct replica_s {
	TAILQ_ENTRY(replica_s) r_next;
	TAILQ_HEAD(, rcmd_s) sendq;
	TAILQ_HEAD(, rcmd_s) waitq;
	TAILQ_HEAD(, rcmd_s) blockedq;
	TAILQ_HEAD(, rcmd_s) read_waitq;
	pthread_cond_t r_cond;
	pthread_mutex_t r_mtx;
	replica_state_t state;
	spec_t *spec;
	int id;
	int iofd;
	int mgmtfd;
	int port;
	char *ip;
	uint64_t least_recvd;
	int cur_recvd;
	uint64_t rrio_seq;
	uint64_t wrio_seq;

	zvol_io_hdr_t *io_rsp;
	void *io_rsp_data;
	uint64_t recv_len;
        uint64_t total_len;
	bool read_rem_data;
	bool read_rem_hdr;
	bool removed;
} replica_t;

typedef struct cstor_conn_ops {
	cstor_listen conn_listen;
	cstor_connect conn_connect;
	//	cstor_read conn_read;
	//	cstor_close conn_close;
} cstor_conn_ops_t;

int initialize_volume(spec_t *);
void *replicator(void *);
void *replica_sender(void *);
void *replica_receiver(void *);
int initialize_replication(void);
int handle_write_resp(spec_t *, replica_t *, zvol_io_hdr_t *);
int handle_read_resp(spec_t *, replica_t *, zvol_io_hdr_t *, void *);
int update_replica_list(int, spec_t *, int);
int remove_replica_from_list(spec_t *, int);
void unblock_blocked_cmds(replica_t *);
replica_t *create_replica_entry(spec_t *, int, char *, int);
void update_volstate(spec_t *);
void clear_replica_cmd(spec_t *, replica_t *, rcmd_t *);
#endif
