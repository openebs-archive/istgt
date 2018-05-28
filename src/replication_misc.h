#ifndef _REPLICATION_MISC_H
#define	_REPLICATION_MISC_H

int replication_connect(const char *, int);
int replication_listen(const char *, int, int);

/* MACRO from istgt_lu_disk.c */
#define timesdiff(_st, _now, _re)                                       \
{                                                                       \
	clock_gettime(CLOCK_MONOTONIC, &_now);                          \
	if ((_now.tv_nsec - _st.tv_nsec)<0) {                           \
		_re.tv_sec  = _now.tv_sec - _st.tv_sec - 1;             \
		_re.tv_nsec = 1000000000 + _now.tv_nsec - _st.tv_nsec;  \
	} else {                                                        \
		_re.tv_sec  = _now.tv_sec - _st.tv_sec;                 \
		_re.tv_nsec = _now.tv_nsec - _st.tv_nsec;               \
	}								\
}

#endif /* _REPLICATION_MISC_H */
