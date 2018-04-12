
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
typedef struct rcmd_s rcmd_t;

typedef enum replica_state_s {
	REPLICA_DEGRADED = 1, //Every replica is attached in degraded state
	REPLICA_HEALTHY,      //Replica is moved to this state when replica state enquiry returns healthy
	REPLICA_ERRORED,      //When there is an error on any of the fds, replica is moved to this state
} replica_state_t;

/* replica state on mgmt thread for mgmt IOs
 * set in mgmt_io_state
 */

/* to read mgmt IO hdr */
#define READ_IO_RESP_HDR	1

/* to read handshake msg data */
#define READ_IO_RESP_DATA	2

typedef struct zvol_io_hdr_s zvol_io_hdr_t;
typedef struct mgmt_ack_data_s mgmt_ack_data_t;

typedef struct replica_s {
	TAILQ_ENTRY(replica_s) r_next;
	TAILQ_ENTRY(replica_s) r_waitnext;
	TAILQ_HEAD(, rcmd_s) sendq;
	TAILQ_HEAD(, rcmd_s) waitq;
	TAILQ_HEAD(, rcmd_s) blockedq;
	TAILQ_HEAD(, rcmd_s) read_waitq;
	pthread_cond_t r_cond;
	pthread_mutex_t r_mtx;
	replica_state_t state;
	spec_t *spec;
	int id;
	int refcount;
	int iofd;
	int mgmt_fd;
	int sender_epfd;
	int port;
	char *ip;
	uint64_t least_recvd;
	int cur_recvd;
	uint64_t rrio_seq;
	uint64_t wrio_seq;

	zvol_io_hdr_t *io_resp_hdr;
	void *io_resp_data;
	int io_state;
	int io_read; //amount of IO data read in current IO state
	bool removed;
	int mgmt_io_state;
	int mgmt_io_read; //amount of data read in current state
	zvol_io_hdr_t *mgmt_ack;
	mgmt_ack_data_t *mgmt_ack_data;
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
int handle_write_resp(spec_t *, replica_t *);
int handle_read_resp(spec_t *, replica_t *);
int update_replica_list(int, spec_t *, int);
void remove_replica_from_list(spec_t *, replica_t *);
void unblock_blocked_cmds(replica_t *);

replica_t *create_replica_entry(spec_t *, int);
replica_t *update_replica_entry(spec_t *, replica_t *, int, char *, int);

replica_t * get_replica(int mgmt_fd, spec_t **);
void handle_read_data_event(int fd);

void update_volstate(spec_t *);
void clear_replica_cmd(spec_t *, replica_t *, rcmd_t *);
#endif
