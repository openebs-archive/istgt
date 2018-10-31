#include <config.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"

cstor_conn_ops_t cstor_ops = {
	.conn_listen = replication_listen,
	.conn_connect = replication_connect,
};

__thread char  tinfo[50] =  {0};
#define build_mgmt_ack_hdr {\
	mgmt_ack_hdr = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));\
	mgmt_ack_hdr->opcode = opcode;\
	mgmt_ack_hdr->version = REPLICA_VERSION;\
	mgmt_ack_hdr->len = sizeof (mgmt_ack_data_t);\
	mgmt_ack_hdr->status = ZVOL_OP_STATUS_OK;\
}

#define build_mgmt_ack_data {\
	mgmt_ack_data = (mgmt_ack_t *)malloc(sizeof(mgmt_ack_t));\
	strcpy(mgmt_ack_data->ip, replica_ip);\
	strcpy(mgmt_ack_data->volname, buf);\
	mgmt_ack_data->port = replica_port;\
	mgmt_ack_data->pool_guid = replica_port;\
	mgmt_ack_data->checkpointed_io_seq = 1000;\
	mgmt_ack_data->zvol_guid = replica_port;\
}

bool degraded_mode = false;
int error_freq = 0;
void *md_list;
int mdlist_fd = 0;
size_t mdlist_size = 0;
uint64_t read_ios;
uint64_t write_ios;

static void
sig_handler(int sig)
{
	printf("read IOs:%lu write IOs:%lu\n", read_ios, write_ios);
}

static int
init_mdlist(char *vol_name)
{
	char mdpath[MAX_NAME_LEN];
	struct stat sbuf;
	bool create = false;

	if (vol_name == NULL)
		return -1;

	if (stat(vol_name, &sbuf)) {
		REPLICA_ERRLOG("Failed to access %s\n", vol_name);
		return -1;
	}
	mdlist_size = (sbuf.st_size / 512) * 8;

	snprintf(mdpath, MAX_NAME_LEN, "%s.mdfile", vol_name);
	if (stat(mdpath, &sbuf)) {
		create = true;
	}

	mdlist_fd = open(mdpath, O_CREAT|O_RDWR, 0666);
	if (mdlist_fd < 0) {
		REPLICA_ERRLOG("Failed to open metadata file %s err(%d)\n",
		    mdpath, errno);
		return -1;
	}

	if(create) {
		if (truncate(mdpath, mdlist_size)) {
			REPLICA_ERRLOG("Failed to create %s err(%d)\n", mdpath, errno);
			return -1;
		}
	}

	md_list = mmap(NULL, mdlist_size, PROT_READ|PROT_WRITE, MAP_SHARED,
	    mdlist_fd, 0);

	return 0;
}

static void
destroy_mdlist(void)
{
	munmap(md_list, mdlist_size);
	close(mdlist_fd);
}

static void
write_metadata(uint64_t offset, size_t len, uint64_t io_num)
{
	size_t md_offset;
	uint64_t end = offset + len;
	uint64_t *buffer = (uint64_t *) md_list;

	while (offset < end) {
		md_offset = offset / 512;
		buffer[md_offset] = io_num;
		offset += 512;
	}
}

static uint64_t
read_metadata(off_t offset)
{
	return *(uint64_t *)((uint64_t *)md_list + offset/512);
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
	uint64_t md_io_num = 0;
	struct zvol_io_rw_hdr *last_io_rw_hdr;
	uint8_t *resp;

	while (start < end) {
		if (md_io_num != read_metadata(start)) {
			count++;
			md_io_num = read_metadata(start);
		}
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

	md_io_num = read_metadata(start);
	last_io_rw_hdr = (struct zvol_io_rw_hdr *)*resp_data;
	last_io_rw_hdr->io_num = (io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) ?
	    md_io_num : 0;
	resp_index = sizeof (struct zvol_io_rw_hdr);
	resp = *resp_data;
	data_index = 0;
	count = 0;
	while ((io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) && (start < end)) {
		if (md_io_num != read_metadata(start)) {
			last_io_rw_hdr->len = count * 512;
			memcpy(resp + resp_index, user_data + data_index,
			    last_io_rw_hdr->len);
			data_index += last_io_rw_hdr->len;
			resp_index += last_io_rw_hdr->len;
			last_io_rw_hdr = (struct zvol_io_rw_hdr *)(resp + resp_index);
			resp_index += sizeof (struct zvol_io_rw_hdr);
			md_io_num = read_metadata(start);
			last_io_rw_hdr->io_num = md_io_num;
			count = 0;
		}
		count++;
		start += 512;
	}
	last_io_rw_hdr->len = (io_hdr->flags & ZVOL_OP_FLAG_READ_METADATA) ?
	    (count * 512) : len;
	memcpy(resp + resp_index, user_data + data_index, last_io_rw_hdr->len);
	return total_payload_len;
}

static int
check_for_err(zvol_io_hdr_t *io_hdr)
{
	static int io_count;

	io_count++;
	if (io_count == 10)
		io_count = 0;

	if (io_count < error_freq) {
		io_hdr->status = ZVOL_OP_STATUS_FAILED;
		return 1;
	}
	return 0;
}

static int64_t
test_read_data(int fd, uint8_t *data, uint64_t len)
{
	int rc = 0;
	uint64_t nbytes = 0;
	while (1) {
		rc = read(fd, data + nbytes, len - nbytes);
		if(rc < 0) {
			if (errno == EINTR)
				continue;
			else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			} else {
				REPLICA_ERRLOG("received err(%d) on fd(%d)\n", errno, fd);
				return -1;
			}
		} else if (rc == 0) {
			REPLICA_ERRLOG("received EOF on fd(%d)\n", fd);
			return -1;
		}

		nbytes += rc;
		if(nbytes == len) {
			break;
		}
	}
	return nbytes;
}

static int
send_mgmt_ack(int fd, zvol_op_code_t opcode, void *buf, char *replica_ip,
    int replica_port, zrepl_status_ack_t *zrepl_status,
    int *zrepl_status_msg_cnt)
{
	int i, nbytes = 0;
	int rc = 0, start;
	struct iovec iovec[6];
	int iovec_count;
	zvol_io_hdr_t *mgmt_ack_hdr = NULL;
	mgmt_ack_t *mgmt_ack_data = NULL;
	int ret = -1;
	zvol_op_stat_t stats;

	/* Init mgmt_ack_hdr */
	build_mgmt_ack_hdr;
	iovec[0].iov_base = mgmt_ack_hdr;
	iovec[0].iov_len = sizeof (zvol_io_hdr_t);

	if (opcode == ZVOL_OPCODE_SNAP_DESTROY) {

		iovec_count = 1;
		mgmt_ack_hdr->status = (random() % 2) ? ZVOL_OP_STATUS_FAILED : ZVOL_OP_STATUS_OK;
		mgmt_ack_hdr->len = 0;
	} else if (opcode == ZVOL_OPCODE_SNAP_CREATE) {
		iovec_count = 1;
		sleep(random()%3 + 1);
		mgmt_ack_hdr->status = (random() % 5 == 0) ? ZVOL_OP_STATUS_FAILED : ZVOL_OP_STATUS_OK;
		mgmt_ack_hdr->len = 0;
	} else if (opcode == ZVOL_OPCODE_REPLICA_STATUS) {
		if (((*zrepl_status_msg_cnt) >= 2) &&
		    (zrepl_status->state != ZVOL_STATUS_HEALTHY) &&
		    !degraded_mode) {
			zrepl_status->state = ZVOL_STATUS_HEALTHY;
			zrepl_status->rebuild_status = ZVOL_REBUILDING_DONE;
			(*zrepl_status_msg_cnt) = 0;
		}

		if (zrepl_status->rebuild_status == ZVOL_REBUILDING_SNAP) {
			(*zrepl_status_msg_cnt) += 1;
		}

		mgmt_ack_hdr->len = sizeof (zrepl_status_ack_t);
		iovec_count = 2;
		iovec[1].iov_base = zrepl_status;
		iovec[1].iov_len = sizeof (zrepl_status_ack_t);
	} else if (opcode == ZVOL_OPCODE_START_REBUILD) {
		zrepl_status->rebuild_status = ZVOL_REBUILDING_SNAP;
		mgmt_ack_hdr->len = 0;
		iovec_count = 1;
	} else if (opcode == ZVOL_OPCODE_STATS) {
		strcpy(stats.label, "used");
		stats.value = 10000;
		mgmt_ack_hdr->len = sizeof (zvol_op_stat_t);
		iovec[1].iov_base = &stats;
		iovec[1].iov_len = sizeof (zvol_op_stat_t);
		iovec_count = 2;
	} else {
		build_mgmt_ack_data;

		iovec[1].iov_base = mgmt_ack_data;
		iovec[1].iov_len = sizeof (mgmt_ack_t);
		iovec_count = 2;
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
	if (mgmt_ack_hdr)
		free(mgmt_ack_hdr);

	if (mgmt_ack_data)
		free(mgmt_ack_data);

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
		return -1;
	}

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
	return 0;
}

static void
usage(void)
{
	printf("replica_test [options]\n");
	printf("options:\n");
	printf(" -i target ip\n");
	printf(" -p target port\n");
	printf(" -I replica ip\n");
	printf(" -P replica port\n");
	printf(" -r retry if failed to connect\n");
	printf(" -V volume path\n");
	printf(" -n number of IOs to serve before sleeping for 60 seconds\n");
	printf(" -d run in degraded mode only\n");
	printf(" -e error frequency (should be <= 10, default is 0)\n");
	printf(" -t delay in response in seconds\n");
}


int
main(int argc, char **argv)
{
	char ctrl_ip[MAX_IP_LEN];
	zrepl_status_ack_t *zrepl_status;
	int zrepl_status_msg_cnt = 0;
	int ctrl_port = 0;
	char replica_ip[MAX_IP_LEN];
	int replica_port = 0;
	char test_vol[1024];
	int sleeptime = 0;
	struct zvol_io_rw_hdr *io_rw_hdr;
	zvol_op_open_data_t *open_ptr;
	int iofd = -1, mgmtfd, sfd, rc, epfd, event_count, i;
	int64_t count;
	struct epoll_event event, *events;
	uint8_t *data, *data_ptr_cpy;
	uint64_t nbytes = 0;
	int vol_fd;
	zvol_op_code_t opcode;
	zvol_io_hdr_t *io_hdr;
	zvol_io_hdr_t *mgmtio;
	bool read_rem_data = false;
	bool read_rem_hdr = false;
	uint64_t recv_len = 0;
	uint64_t total_len = 0;
	uint64_t io_hdr_len = sizeof(zvol_io_hdr_t);
	int io_cnt = 0;
	int ch;
	int check = 1;
	struct timespec now;
	int delay = 0;
	bool retry = false;

	while ((ch = getopt(argc, argv, "i:p:I:P:V:n:e:t:dr")) != -1) {
		switch (ch) {
			case 'i':
				strncpy(ctrl_ip, optarg, sizeof(ctrl_ip));
				check |= 1 << 1;
				break;
			case 'p':
				ctrl_port = atoi(optarg);
				check |= 1 << 2;
				break;
			case 'I':
				strncpy(replica_ip, optarg, sizeof(replica_ip));
				check |= 1 << 3;
				break;
			case 'P':
				replica_port = atoi(optarg);
				check |= 1 << 4;
				break;
			case 'V':
				strncpy(test_vol, optarg, sizeof(test_vol));
				check |= 1 << 5;
				break;
			case 'n':
				io_cnt = atoi(optarg);
				break;
			case 'd':
				degraded_mode = true;
				break;
			case 'e':
				error_freq = atoi(optarg);
				if (error_freq > 10) {
					usage();
					exit(EXIT_FAILURE);
				}
				break;
			case 'r':
				retry = true;
				break;
			case 't':
				delay = atoi(optarg);
				break;
			default:
				usage();
				exit(EXIT_FAILURE);
		}
	}

	if(check != 63) {
		usage();
	}

	(void) signal(SIGHUP, sig_handler);

	vol_fd = open(test_vol, O_RDWR, 0666);
	io_hdr = malloc(sizeof(zvol_io_hdr_t));
	mgmtio = malloc(sizeof(zvol_io_hdr_t));
	zrepl_status = (zrepl_status_ack_t *)malloc(sizeof (zrepl_status_ack_t));
	zrepl_status->state = ZVOL_STATUS_DEGRADED; 
	zrepl_status->rebuild_status = ZVOL_REBUILDING_INIT; 
	if (init_mdlist(test_vol)) {
		REPLICA_ERRLOG("Failed to initialize mdlist for replica(%d)\n", ctrl_port);
		close(vol_fd);
		exit(EXIT_FAILURE);
	}

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	data = NULL;
	epfd = epoll_create1(0);
	
	//Create listener for io connections from controller and add to epoll
	if((sfd = cstor_ops.conn_listen(replica_ip, replica_port, 32, 1)) < 0) {
                REPLICA_LOG("conn_listen() failed, err:%d replica(%d)", errno, ctrl_port);
		close(vol_fd);
		destroy_mdlist();
                exit(EXIT_FAILURE);
        }
	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, err:%d replica(%d)", errno, ctrl_port);
		close(vol_fd);
		destroy_mdlist();
		exit(EXIT_FAILURE);
	}

	events = calloc(MAXEVENTS, sizeof(event));

again:
	//Connect to controller to start handshake and connect to epoll
	if((mgmtfd = cstor_ops.conn_connect(ctrl_ip, ctrl_port)) < 0) {
		REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
		if (retry) {
			sleep(1);
			goto again;
		}
		close(vol_fd);
		free(events);
		destroy_mdlist();
		exit(EXIT_FAILURE);
	}

	event.data.fd = mgmtfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmtfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, err:%d replica(%d)", errno, ctrl_port);
		exit(EXIT_FAILURE);
	}

	while (1) {
		event_count = epoll_wait(epfd, events, MAXEVENTS, -1);
		for(i=0; i< event_count; i++) {
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN))) {
				fprintf (stderr, "epoll error for replica(%d)\n", ctrl_port);
				continue;
			} else if (events[i].data.fd == mgmtfd) {
				count = test_read_data(events[i].data.fd, (uint8_t *)mgmtio, sizeof(zvol_io_hdr_t));
				if (count < 0) {
					if (retry) {
						REPLICA_ERRLOG("Failed to read from %d\n", events[i].data.fd);
						epoll_ctl(epfd, EPOLL_CTL_DEL, mgmtfd, NULL);
						close(mgmtfd);
						sleep(1);
						goto again;
					} else {
						REPLICA_ERRLOG("Failed to read from %d\n", events[i].data.fd);
						rc = -1;
						goto error;
					}
				}
				if (retry) {
					/*
					 * If connection with target is successfully established then
					 * there is no need to re-connect if error occurs.
					 */
					retry = false;
				}
				if (count != sizeof (zvol_io_hdr_t)) {
					REPLICA_ERRLOG("Failed to read complete header.. got only %ld bytes out of %lu\n",
					    count, sizeof (zvol_io_hdr_t));
					rc = -1;
					goto error;
				}

				if(mgmtio->len) {
					data = data_ptr_cpy = malloc(mgmtio->len);
					count = test_read_data(events[i].data.fd, (uint8_t *)data, mgmtio->len);
					if (count < 0) {
						rc = -1;
						goto error;
					} else if ((uint64_t)count != mgmtio->len) {
						REPLICA_ERRLOG("failed to getch mgmt data.. got only %ld bytes out of %lu\n",
						    count, mgmtio->len);
						rc = -1;
						goto error;
					}
				}
				opcode = mgmtio->opcode;
				send_mgmt_ack(mgmtfd, opcode, data, replica_ip, replica_port, zrepl_status, &zrepl_status_msg_cnt);
			} else if (events[i].data.fd == sfd) {
				struct sockaddr saddr;
				socklen_t slen;
				char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
				slen = sizeof(saddr);
				iofd = accept(sfd, &saddr, &slen);
				if (iofd == -1) {
					if((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
						break;
					} else {
						REPLICA_ERRLOG("accept() failed, err:%d replica(%d)", errno, ctrl_port);
						break;
					}
				}

				rc = getnameinfo(&saddr, slen,
						hbuf, sizeof(hbuf),
						sbuf, sizeof(sbuf),
						NI_NUMERICHOST | NI_NUMERICSERV);
				if (rc == 0) {
					REPLICA_LOG("Accepted connection on descriptor %d "
							"(host=%s, port=%s)\n", iofd, hbuf, sbuf);
				}
				rc = make_socket_non_blocking(iofd);
				if (rc == -1) {
					REPLICA_ERRLOG("make_socket_non_blocking() failed, errno:%d"
					    " replica(%d)", errno, ctrl_port);
					exit(EXIT_FAILURE);
				}
				event.data.fd = iofd;
				event.events = EPOLLIN | EPOLLET;
				rc = epoll_ctl(epfd, EPOLL_CTL_ADD, iofd, &event);
				if(rc == -1) {
					REPLICA_ERRLOG("epoll_ctl() failed, errno:%d replica(%d)", errno, ctrl_port);
					exit(EXIT_FAILURE);
				}
			} else if(events[i].data.fd == iofd) {
				while(1) {
					if (read_rem_data) {
						count = test_read_data(events[i].data.fd, (uint8_t *)data + recv_len, total_len - recv_len);
						if (count < 0) {
							rc = -1;
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
						count = test_read_data(events[i].data.fd, (uint8_t *)io_hdr + recv_len, total_len - recv_len);
						if (count < 0) {
							rc = -1;
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
						count = test_read_data(events[i].data.fd, (uint8_t *)io_hdr, io_hdr_len);
						if (count < 0) {
							rc = -1;
							goto error;
						} else if ((uint64_t)count < io_hdr_len) {
							read_rem_hdr = true;
							recv_len = count;
							total_len = io_hdr_len;
							break;
						} else {
							read_rem_hdr = false;
						}
					}

					if (io_hdr->opcode == ZVOL_OPCODE_WRITE ||
					    io_hdr->opcode == ZVOL_OPCODE_HANDSHAKE ||
					    io_hdr->opcode == ZVOL_OPCODE_OPEN) {
						if (io_hdr->len) {
							io_hdr->status = ZVOL_OP_STATUS_OK;
							data = malloc(io_hdr->len);
							nbytes = 0;
							count = test_read_data(events[i].data.fd, (uint8_t *)data, io_hdr->len);
							if (count < 0) {
								rc = -1;
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
						REPLICA_LOG("Volume name:%s blocksize:%d timeout:%d.. replica(%d)\n",
						    open_ptr->volname, open_ptr->tgt_block_size, open_ptr->timeout, ctrl_port);
					}
execute_io:
					if ((io_cnt > 0) && (io_hdr->opcode == ZVOL_OPCODE_WRITE ||
							io_hdr->opcode == ZVOL_OPCODE_READ)) {
						io_cnt --;
						if (io_cnt == 0) {
							REPLICA_ERRLOG("sleeping for 60 seconds.. replica(%d)\n", ctrl_port);
							sleep(60);
						}
					}
					if(io_hdr->opcode == ZVOL_OPCODE_WRITE) {
						if (delay > 0)
							sleep(delay);

						io_hdr->status = ZVOL_OP_STATUS_OK;
						io_rw_hdr = (struct zvol_io_rw_hdr *)data;
						write_metadata(io_hdr->offset, io_rw_hdr->len, io_rw_hdr->io_num);
						data += sizeof(struct zvol_io_rw_hdr);
						nbytes = 0;
						while((rc = pwrite(vol_fd, data + nbytes, io_rw_hdr->len - nbytes, io_hdr->offset + nbytes))) {
							if(rc == -1 ) {
								if(errno == 11) {
									sleep(1);
									continue;
								}
								break;
							}
							nbytes += rc;
							if(nbytes == io_rw_hdr->len) {
								break;
							}
						}

						if (nbytes != io_rw_hdr->len) {
							REPLICA_ERRLOG("Failed to write data to %s replica(%d)\n", test_vol, ctrl_port);
							goto error;
						}

						data -= sizeof(struct zvol_io_rw_hdr);
						usleep(sleeptime);
						write_ios++;
					} else if(io_hdr->opcode == ZVOL_OPCODE_READ) {
						uint8_t *user_data = NULL;
						if (delay > 0)
							sleep(delay);

						if(io_hdr->len) {
							user_data = malloc(io_hdr->len);
						}
						nbytes = 0;
						io_hdr->status = ZVOL_OP_STATUS_OK;
						rc = check_for_err(io_hdr);
						if(!rc)  {
							while ((rc = pread(vol_fd, user_data + nbytes, io_hdr->len - nbytes, io_hdr->offset + nbytes))) {
								if(rc == -1 ) {
									if(errno == EAGAIN) {
										sleep(1);
										continue;
									}
									break;
								}
								nbytes += rc;
								if(nbytes == io_hdr->len) {
									break;
								}
							}
						}

						if (nbytes != io_hdr->len) {
							REPLICA_ERRLOG("failed to read completed data from %s off:%lu "
							    "req:%lu read:%lu replica(%d)\n",
							    test_vol, io_hdr->offset, io_hdr->len, nbytes, ctrl_port);
							free(user_data);
							goto error;
						}

						nbytes = fetch_update_io_buf(io_hdr, user_data, &data);
						if (user_data)
							free(user_data);
						io_hdr->len = nbytes;
						read_ios++;
					}

					rc = send_io_resp(iofd, io_hdr, data);
					if (rc) {
						REPLICA_ERRLOG("Failed to send response replica(%d)\n", ctrl_port);
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
	REPLICA_ERRLOG("shutting down replica(%s:%d) IOs(read:%lu write:%lu)\n",
	    ctrl_ip, ctrl_port, read_ios, write_ios);
	if (data)
		free(data);
	close(vol_fd);
	destroy_mdlist();
	return rc;
}
