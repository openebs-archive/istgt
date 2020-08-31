/*
Copyright 2018 The OpenEBS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef	_REPLICATION_ASSERT_H
#define	_REPLICATION_ASSERT_H

#ifdef	REPLICATION
#include <replication.h>
#endif
#include <assert.h>

#ifdef verify
#undef verify
#endif

#define	VERIFY_COND(cond)							\
do {									\
	if (!(cond)) {							\
		fprintf(stderr, "%s(%s:%d): assertion occurred for %s",	\
		    __FILE__, __FUNCTION__, __LINE__, #cond);		\
	    abort();							\
	}								\
} while (0)

#define	verify_cond(cond)							\
do {									\
	if (!(cond)) {							\
		fprintf(stderr, "%s(%s:%d): assertion occurred for %s",\
		    __FILE__, __FUNCTION__, __LINE__, #cond);		\
	    abort();							\
	}								\
} while (0)

#define	VERIFY3_IMPL(LEFT, OP, RIGHT, TYPE)				\
do {									\
	const TYPE __left = (TYPE)(LEFT);				\
	const TYPE __right = (TYPE)(RIGHT);				\
	if (!(__left OP __right)) {					\
		fprintf(stderr, "%s(%s:%d): assrtion occurred .. "	\
		    "%s %s %s (0x%llx %s 0x%llx)", __FILE__, 		\
		    __FUNCTION__, __LINE__, #LEFT, #OP, #RIGHT, 	\
		    (unsigned long long int)__left, #OP, 		\
		    (unsigned long long int)__right);			\
		abort();						\
	}								\
} while (0)

#define	VERIFY3S(a, b, c)	VERIFY3_IMPL(a, b, c, int64_t)
#define	VERIFY3U(a, b, c)	VERIFY3_IMPL(a, b, c, uint64_t)
#define	VERIFY3P(a, b, c)	VERIFY3_IMPL(a, b, c, uintptr_t)
#define	VERIFY0(a)		VERIFY3_IMPL(a, ==, 0, uint64_t)

#ifdef assert
#undef assert
#endif

/* Compile time assert */
#define	CTASSERT_GLOBAL(a)		_CTASSERT(a, __LINE__)
#define	CTASSERT(a)			{ _CTASSERT(a, __LINE__); }
#define	_CTASSERT(a, b)			__CTASSERT(a, b)
#define	__CTASSERT(a, b)						\
	typedef char __attribute__((unused))				\
	__compile_time_assertion__ ## b[(a) ? 1 : -1]

#ifndef DEBUG
#define	ASSERT3S(a, b, c)	((void)0)
#define	ASSERT3U(a, b, c)	((void)0)
#define	ASSERT3P(a, b, c)	((void)0)
#define	ASSERT0(a)		((void)0)
#define	ASSERT(a)		((void)0)
#define	assert(a)		((void)0)
#define	ASSERTV(a)
#define	IMPLY(X, Y)		((void)0)
#define	EQUIV(X, Y)		((void)0)
#else
#define	ASSERT3S(a, b, c)	VERIFY3S(a, b, c)
#define	ASSERT3U(a, b, c)	VERIFY3U(a, b, c)
#define	ASSERT3P(a, b, c)	VERIFY3P(a, b, c)
#define	ASSERT0(a)		VERIFY0(a)
#define	ASSERT(a)		VERIFY_COND(a)
#define	assert(a)		VERIFY_COND(a)
#define	ASSERTV(a)		a
#define	IMPLY(X, Y) \
	((void)(((!(X)) || (Y)) || \
	    REPLICA_ERRLOG("(" %s ") implies (" %s ")", #X, #Y)))
#define	EQUIV(X, Y) \
	((void)((!!(X) == !!(Y)) || \
	    REPLICA_ERRLOG("(" %s ") is equivalent to (" %s ")", #X, #Y)))

#endif  /* DEBUG */

#endif /* _REPLICATION_ASSERT_H */
