#ifndef	REPLICATION_LOG_H
#define	REPLICATION_LOG_H

enum replication_log_level {
	LOG_LEVEL_ERR,
	LOG_LEVEL_INFO,
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_MAX,
};

int replication_log_level;

#define REPLICA_LOG(fmt, ...)						\
{									\
	if (replication_log_level >= LOG_LEVEL_INFO)			\
		fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,		\
		    __func__, __LINE__, tinfo, ##__VA_ARGS__);		\
}

#define REPLICA_NOTICELOG(fmt, ...)					\
{									\
	if (replication_log_level >= LOG_LEVEL_INFO)			\
		fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,		\
		    __func__, __LINE__, tinfo, ##__VA_ARGS__);		\
}

#define REPLICA_ERRLOG(fmt, ...)					\
{									\
	if (replication_log_level >= LOG_LEVEL_ERR)			\
		fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,		\
		    __func__, __LINE__, tinfo, ##__VA_ARGS__);		\
}
#define REPLICA_WARNLOG(fmt, ...)					\
{									\
	if (replication_log_level >= LOG_LEVEL_ERR)			\
		fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,		\
		    __func__, __LINE__, tinfo, ##__VA_ARGS__);		\
}

#ifdef	DEBUG
#define REPLICA_DEBUGLOG(fmt, ...)					\
{									\
	if (replication_log_level >= LOG_LEVEL_DEBUG)			\
		fprintf(stderr, "%-18.18s:%4d: %-20.20s: " fmt,		\
		    __func__, __LINE__, tinfo, ##__VA_ARGS__);		\
}
#else
#define REPLICA_DEBUGLOG(fmt, ...)
#endif

#endif /* REPLICATION_LOG_H */
