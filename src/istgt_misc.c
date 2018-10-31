/*
 * Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdint.h>

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

#include <unistd.h>

#ifdef HAVE_LIBPTHREAD
#include <pthread.h>
#endif
#ifdef HAVE_SCHED
#include <sched.h>
#endif

#include "istgt.h"
#include "istgt_misc.h"

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

#ifndef	REPLICATION
static void fatal(const char *format, ...) __attribute__((__noreturn__,
	__format__(__printf__, 1, 2)));

static void
fatal(const char *format, ...)
{
	char buf[MAX_TMPBUF];
	va_list ap;

	va_start(ap, format);
	vsnprintf(buf, sizeof (buf), format, ap);
	fprintf(stderr, "%s", buf);
	syslog(LOG_ERR, "%s", buf);
	va_end(ap);
	exit(EXIT_FAILURE);
}
#endif

typedef struct mem_hdr {
	uint16_t pIdx;
	uint16_t bIdx;
	uint8_t	 inUse;
	uint8_t  dfree;
	uint16_t line;
	uint32_t alloccnt;
	uint32_t freecnt;
	char	tinf[16];
} mem_hdr_t;

typedef struct memory_pool {
	uint32_t objsize;
	uint64_t frees;
	uint64_t allocs;
	uint32_t fail;

	uint32_t inuse;
	uint32_t maxuse;

	void *startmem;
	void *endmem;

	pthread_mutex_t  mmtx;
	// limits us to 64k objects per pool,
	// should be looking at bits instead of uint8
	uint16_t	mapsize;
	uint16_t bucksize;
	uint8_t *usagemap;
	uint64_t **bucket;
	char	name[12];
} mpool_t;

mpool_t mpools[50];

static int mpool_inited = 0;
static size_t mpool_minSize = 0;
static size_t mpool_maxSize = 0;

// uint8_t PoolMap[MAX_SIZE+1];
/*
 * uint32_t PoolSize[20]	= { 4096, 8192, 12288, 0, 0, 0,
 * 	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
 */
uint32_t PoolSizeActual[50] = {0};

uint32_t poolBuckUsage[50] = { 0 };
/*
 * uint32_t PoolSize[20]	=  { 1024, 2048, 3072, 4096, 5000, 6000, 8192,
 *	12288,	16384, 32768, 65536, 131072,
 *	262144, 307200, 524288, 0, 0, 0, 0, 0};
 * uint32_t poolBuckLimit[20]	=  { 1024,	1024,	1024,	1040,	800,
 *	400,	600,	320,	120,	320,	120,	120,	120,
 *	60,	120,	0,	0,	0,	0,	0};
 */
uint32_t PoolSize[50]	=  { 32,	64,	128,	256,	512,
	1024,	2048,	3072,	4096,	5000,	6000,	8192,	12288,
	16384,	32768,	65536,	131072,	262144,	307200,	524288,	590000,
	856200,	1048680,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0};
uint16_t poolBuckLimit[50]	= { 700,	700,	400,	700,
	4096,	4096,	2080,	1024,	1024,	500,	400,	600,
	220,	220,	120,	220,	220,	220,	160,	50,
	40,	100,	12,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0,	0,	0,
	0,	0,	0,	0,	0,	0};

static int	g_poolCnt = 0;
static uint64_t totalAlloc = 0;

int detectDoubleFree = 0;

static void *gstart = NULL;
static void *gend = NULL;
void
poolinit(void)
{
	int i = 0, j = 0, k = 0, rc;
	uint64_t msize = 1000;

	FILE *fp = fopen("/etc/istgt-mem.conf", "r");
	if (fp) {
		while (!feof(fp)) {
			int psz = 0, plimit = 0;
			int vals = fscanf(fp, "mem%d.%d\n", &psz, &plimit);
			if (vals == 2) {
				if (psz > 0 && psz < 1050000 &&\
						(uint32_t)psz > PoolSize[i-1]) {
					PoolSize[i] = psz;
					if (plimit < 0)
						plimit = 0;
				    else if (plimit > 9000)
						plimit = 9000;
					poolBuckLimit[i] = plimit;
					++i;
				}
			}
			if (i >= 50)
				break;
		}
		fclose(fp);
		fp = NULL; i = 0;
	}

	for (i = 0; i < 50; ++i) {
		if (PoolSize[i] != 0 && poolBuckLimit[i] != 0)
			msize +=  ((PoolSize[i] + (sizeof (mem_hdr_t) * 3))
							 * poolBuckLimit[i]);
	}
	void *globalmem = malloc(msize);
	uint8_t *gptr = globalmem;

	totalAlloc = msize;
	gstart = gptr;
	gend = gptr + (msize - 10);

	for (i = 0, j = 0; j < 50; ++j) {
		if ((PoolSize[j] != 0) && (poolBuckLimit[j] != 0)) {
			snprintf(mpools[i].name, 11, "m:%x", PoolSize[j]);
			mpools[i].objsize = PoolSize[j];
			mpools[i].frees = 0;
			mpools[i].allocs = 0;
			mpools[i].maxuse = 0;
			mpools[i].inuse = 0;
			mpools[i].fail = 0;

			mpools[i].bucksize = poolBuckLimit[j];
			mpools[i].mapsize = mpools[i].bucksize;

			mpools[i].bucket = malloc((sizeof (void *))
						 * (mpools[i].bucksize + 2));
			mpools[i].usagemap = malloc((sizeof (uint8_t))
						 * (mpools[i].mapsize + 2));
			memset(mpools[i].bucket, 0, (sizeof (void *))
						 * (mpools[i].bucksize + 2));
			memset(mpools[i].usagemap, 0, (sizeof (uint8_t))
						 * (mpools[i].mapsize + 2));

			rc = pthread_mutex_init(&mpools[i].mmtx, NULL);
			if (!mpools[i].bucket || !mpools[i].usagemap || rc
									!= 0) {
				ISTGT_ERRLOG("mempool init failed"
				"for %d)size:%d %x/%d [ %p %p %d ]\n", i,
				PoolSize[j], mpools[i].bucksize,
				mpools[i].mapsize, mpools[i].bucket,
				mpools[i].usagemap, rc);
				return;
			}
			mpools[i].startmem = gptr;
			for (k = 0; k < mpools[i].bucksize; ++k) {
				uint64_t *mem = (uint64_t *)gptr;
				gptr += mpools[i].objsize +
					(sizeof (mem_hdr_t) * 3);
				memset(mem, 0, 200);
				mpools[i].bucket[k] = mem;
			}
			mpools[i].endmem = gptr;
			if (gptr > ((uint8_t *)gend)) {
				ISTGT_NOTICELOG("poolmem: %s %x %x overshot"
				"the intial alloc\n", mpools[i].name,
				mpools[i].bucksize, mpools[i].mapsize);
				raise(11);
			}
			PoolSizeActual[i] = PoolSize[j];
			if (mpool_minSize == 0 ||
				mpool_minSize > PoolSizeActual[i])
				mpool_minSize = PoolSizeActual[i] - 1;
			if (mpool_maxSize < PoolSizeActual[i])
				mpool_maxSize = PoolSizeActual[i] + 1;
			++g_poolCnt;
			++i;
		}
	}
	// if we have a smaller pool, lets do all allocs from us
	if (mpool_minSize < 256)
		mpool_minSize = 0;
	mpool_inited = 1;
}


void
poolfini(void)
{
	int i;
	mpool_inited = 0;
	for (i = 0; i < 50; ++i) {
		PoolSizeActual[i] = 0;
		if (mpools[i].bucket != NULL)
			free(mpools[i].bucket);
		if (mpools[i].usagemap != NULL)
			free(mpools[i].usagemap);

		mpools[i].bucksize = 0;
		mpools[i].mapsize = 0;
		mpools[i].bucket = NULL;
		mpools[i].usagemap = NULL;
		mpools[i].maxuse = 0;
		mpools[i].inuse = 0;

		pthread_mutex_destroy(&mpools[i].mmtx);
		mpools[i].startmem = NULL;
		mpools[i].endmem = NULL;

	}

//	if (gstart != NULL)
//		free(gstart);
	gstart = NULL;
	gend = NULL;
}

static void
checkleak(int i)
{
	char buf[1024];
	int rem = 900;
	char *ptr = buf;
	int wn, k, e = 0, jn = 0;
	uint32_t u = 0, pr = 0;
	uint32_t osz = mpools[i].objsize;
	for (k = 0; k < mpools[i].bucksize && u <= mpools[i].inuse; ++k) {
		if (mpools[i].usagemap[k] != 0) {
			uint64_t *m = mpools[i].bucket[k];
			mem_hdr_t *mem = (mem_hdr_t *)m;
			if (mem->inUse == 0) {
				++e; // not expected
			} else {
				++u;
				wn = snprintf(ptr, rem, " %d.%.*s.%d.%d,", k,
					14, mem->tinf, mem->line,
					mem->alloccnt);
				if (wn < 0)
					wn = 0;
				else if (wn > rem)
					wn = rem;
				ptr += wn; rem -= wn;
				if (rem < 40) {
					ISTGT_NOTICELOG("L:%d [%u:%s]", osz,
					u-pr, buf);
					ptr = buf; rem = 900; pr = u;
				}
			}
		} else {
			++jn;
		}
	}
	if (rem != 900) {
		ISTGT_NOTICELOG("L:%d [%u:%s]", osz, u-pr, buf);
	}
}

int
poolprint(char *inbuf __attribute__((__unused__)),
	int len __attribute__((__unused__)))
{
	// char *ptr = inbuf;
	// int wn, i;
	// int rem = len - 3;
	int i;
	ISTGT_NOTICELOG("in: m:%9s  %7s,%5s,%5s %5s   allocs,frees",
		"poolsize", "total", "inuse", "max", "fail");
	for (i = 0; i < 50; ++i) {
		if (mpools[i].bucksize == 0)
			continue;
		ISTGT_NOTICELOG("%2d: m:%9d  %7d,%5d,%5d %5d   %ld,%ld",
		i, mpools[i].objsize, mpools[i].bucksize,
		mpools[i].inuse, mpools[i].maxuse,
		mpools[i].fail, mpools[i].allocs, mpools[i].frees);
		/*
		 * wn = snprintf(ptr, rem, " m:%9d  %7d,%5d,%5d %5d  %ld,%ld",
		 *	mpools[i].objsize, mpools[i].bucksize,
		 *	mpools[i].inuse, mpools[i].maxuse,
		 *	mpools[i].fail, mpools[i].allocs,
		 *	mpools[i].frees);
		 * if (wn < 0)
		 *	wn = 0;
		 * else if (wn > rem)
		 *	wn = rem;
		 * ISTGT_NOTICELOG("%.*s", wn, ptr);
		 * ptr += wn; rem -= wn;
		 * if (rem < 40) {
		 *	ptr = inbuf; rem = len - 3;
		 * }
		 */
	}
	for (i = 0; i < 50; ++i) {
		if (mpools[i].inuse > 0)
			checkleak(i);
	}
	return (0);
	// return (len - rem);
}



int memdebug = 0;

#ifndef	REPLICATION
void *
xmalloci(size_t size, uint16_t line)
{
	void *p;
	uint16_t i;
	uint16_t pIdx = 0xffff;
	uint16_t bIdx = 0xffff;
	uint32_t inuse = -1;
	uint16_t bucksize = -1;
	uint32_t objsz = 0;

	if (size < 1)
		size = 1;

	if (mpool_inited == 1 &&
			(size > mpool_minSize && size < mpool_maxSize)) {
		for	(i = 0; i < g_poolCnt; ++i) {
				// we rely on the increasing size in the array
			if (PoolSizeActual[i] >= size) {
				pIdx = i;
				break;
			}
		}
		if (pIdx != 0xffff) {
			uint16_t umsz = mpools[pIdx].mapsize;
			uint8_t *umap = mpools[pIdx].usagemap;

			pthread_mutex_lock(&mpools[pIdx].mmtx);
			objsz = mpools[pIdx].objsize;
			inuse = mpools[pIdx].inuse;
			bucksize = mpools[pIdx].bucksize;
			if (mpools[pIdx].inuse < mpools[pIdx].bucksize) {
				for (i = 0; i < umsz; ++i) {
					if (umap[i] == 0) {
						umap[i] = 1;
						bIdx = i;
						++mpools[pIdx].allocs;
						++mpools[pIdx].inuse;
						if (mpools[pIdx].inuse >
							mpools[pIdx].maxuse)
							mpools[pIdx].maxuse
							= mpools[pIdx].inuse;
						break;
					}
				}
			}
			if (bIdx == 0xffff) {
				++mpools[pIdx].fail;
				if (mpools[pIdx].inuse <
					mpools[pIdx].bucksize) {
					raise(11);
				}
			}
			pthread_mutex_unlock(&mpools[pIdx].mmtx);
			if (bIdx != 0xffff) {
				uint64_t *m = mpools[pIdx].bucket[bIdx];
				p = (void *)(((uint8_t *)m) +
					sizeof (mem_hdr_t));
				mem_hdr_t *mem = (mem_hdr_t *)m;
				mem->pIdx = pIdx; mem->bIdx = bIdx;
				if ((mem->inUse == 1) ||
					(mem->freecnt != mem->alloccnt)) {
					ISTGT_NOTICELOG("poolalloc:issue %p"
					"inuse:%d.%d  c:%u/%u"
					"ln:%d.%s:%d)  [%d/%d]",
					p, mem->inUse, mem->dfree,
					mem->alloccnt, mem->freecnt,
					mem->line, mem->tinf, line,
					mem->pIdx, mem->bIdx);
					if (detectDoubleFree == 1)
						raise(11);
				}
				++mem->alloccnt;
				mem->inUse = 1;
				memcpy(mem->tinf, tinfo, 16);
				mem->line = line;
				if (memdebug == 1) {
					mem_hdr_t *mx = (mem_hdr_t *)(((uint8_t
						*)p) - sizeof (mem_hdr_t));
					ISTGT_NOTICELOG("poolalloc: %s size:%ld"
					"Idx:%d/%d  %d/%d  %p/%p %p",
					tinfo, size, pIdx, mx->pIdx,
					bIdx, mx->bIdx, m, mx, p);
				}
				return (p);
			}
		}
	}

	p = malloc(size);
	if (p == NULL)
		fatal("no memory\n");

	ISTGT_TRACELOG(ISTGT_TRACE_MEM, "alloc:%p:size:%lu"
	"%s:%d [pool:%u %d/%d]", p, size, tinfo,
	line, objsz, inuse, bucksize);
	return (p);
}

void
xfreei(void *p, uint16_t line)
{
	if (p == NULL)
		return;

	// simple way to check if its our pools
	if (p >= gstart && p < gend) {
		mem_hdr_t *m = (mem_hdr_t *)(((uint8_t *)p)
			- sizeof (mem_hdr_t));
		pthread_mutex_lock(&mpools[m->pIdx].mmtx);
		if ((m->inUse == 0) || ((m->freecnt+1) != m->alloccnt)) {
			uint16_t pi = m->pIdx, bi = m->bIdx, ln = m->line;
			uint8_t df = ++m->dfree;
			if (detectDoubleFree == 1) {
				ISTGT_NOTICELOG("poolfree:issue %s %p(%p)"
				"Idx:%d/%d(freed:%d/%d, %d)",
					tinfo, p, m,  pi, bi, ln, line, df);
				raise(11);
			} else {
				pthread_mutex_unlock(&mpools[m->pIdx].mmtx);
				ISTGT_NOTICELOG("poolfree:issue %s %p"
				"(%p)  Idx:%d/%d(freed:%d/%d, %d)",
					tinfo, p, m,  pi, bi, ln, line, df);
				return;
			}
		}
		mpools[m->pIdx].usagemap[m->bIdx] = 0;
		++mpools[m->pIdx].frees;
		--mpools[m->pIdx].inuse;
		++m->freecnt;
		m->inUse = 0;
		m->line = line;
		pthread_mutex_unlock(&mpools[m->pIdx].mmtx);
		if (memdebug == 1) {
			ISTGT_NOTICELOG("poolfree: %s %p(%p)"
			"Idx:%d/%d", tinfo, p, m,  m->pIdx, m->bIdx);
		}
		return;
	}

	ISTGT_TRACELOG(ISTGT_TRACE_MEM, " free:%p: %s:%d", p, tinfo, line);
	free(p);
}

char *
xstrdupi(const char *s, uint16_t line)
{
	char *p;
	size_t size;

	if (s == NULL)
		return (NULL);
	size = strlen(s) + 1;
	p = xmalloci(size, line);
	memcpy(p, s, size - 1);
	p[size - 1] = '\0';
	return (p);
}
#endif

char *
strlwr(char *s)
{
	char *p;

	if (s == NULL)
		return (NULL);

	p = s;
	while (*p != '\0') {
		*p = tolower((int) *p);
		p++;
	}
	return (s);
}

char *
strupr(char *s)
{
	char *p;

	if (s == NULL)
		return (NULL);

	p = s;
	while (*p != '\0') {
		*p = toupper((int) *p);
		p++;
	}
	return (s);
}

char *
strsepq(char **stringp, const char *delim)
{
	char *p, *q, *r;
	int quoted = 0, bslash = 0;

	p = *stringp;
	if (p == NULL)
		return (NULL);

	r = q = p;
	while (*q != '\0' && *q != '\n') {
		/* eat quoted characters */
		if (bslash) {
			bslash = 0;
			*r++ = *q++;
			continue;
		} else if (quoted) {
			if (quoted == '"' && *q == '\\') {
				bslash = 1;
				q++;
				continue;
			} else if (*q == quoted) {
				quoted = 0;
				q++;
				continue;
			}
			*r++ = *q++;
			continue;
		} else if (*q == '\\') {
			bslash = 1;
			q++;
			continue;
		} else if (*q == '"' || *q == '\'') {
			quoted = *q;
			q++;
			continue;
		}

		/* separator? */
		if (strchr(delim, (int) *q) == NULL) {
			*r++ = *q++;
			continue;
		}

		/* new string */
		q++;
		break;
	}
	*r = '\0';

	/* skip tailer */
	while (*q != '\0' && strchr(delim, (int) *q) != NULL) {
	q++;
	}
	if (*q != '\0') {
		*stringp = q;
	} else {
		*stringp = NULL;
	}

	return (p);
}

char *
trim_string(char *s)
{
	char *p, *q;

	if (s == NULL)
		return (NULL);

	/* remove header */
	p = s;
	while (*p != '\0' && isspace((int) *p)) {
		p++;
	}
	/* remove tailer */
	q = p + strlen(p);
	while (q - 1 >= p && isspace((int) *(q - 1))) {
		q--;
		*q = '\0';
	}
	/* if remove header, move */
	if (p != s) {
		q = s;
		while (*p != '\0') {
			*q++ = *p++;
		}
	}
	return (s);
}

char *
escape_string(const char *s)
{
	const char *p;
	char *q, *r;
	size_t size;

	if (s == NULL)
		return (NULL);

	p = s;
	size = 0;
	while (*p != '\0') {
		if (*p == '"' || *p == '\\' || *p == '\'') {
			size += 2;
		} else {
			size++;
		}
		p++;
	}

	p = s;
	r = q = xmalloc(size + 1);
	while (*p != '\0') {
		if (*p == '"' || *p == '\\' || *p == '\'') {
			*q++ = '\\';
			*q++ = *p++;
		} else {
			*q++ = *p++;
		}
	}
	*q++ = '\0';
	return (r);
}

/* LBA = (M * 60 + S) * 75 + F - 150 */
uint32_t
istgt_msf2lba(uint32_t msf)
{
	uint32_t lba;

	lba = ((msf >> 16) & 0xff) * 60 * 75;
	lba += ((msf >> 8) & 0xff) * 75;
	lba += msf & 0xff;
	lba -= 150;
	return (lba);
}

uint32_t
istgt_lba2msf(uint32_t lba)
{
	uint32_t m, s, f;

	lba += 150;
	m = (lba / 75) / 60;
	s = (lba / 75) % 60;
	f = lba % 75;

	return ((m << 16) | (s << 8) | f);
}

uint8_t
istgt_dget8(const uint8_t *data)
{
	uint8_t value;

	value  = (data[0] & 0xffU) << 0;
	return (value);
}

void
istgt_dset8(uint8_t *data, uint32_t value)
{
	data[0] = (value >> 0) & 0xffU;
}

uint16_t
istgt_dget16(const uint8_t *data)
{
	uint16_t value;

	value  = (data[0] & 0xffU) << 8;
	value |= (data[1] & 0xffU) << 0;
	return (value);
}

void
istgt_dset16(uint8_t *data, uint32_t value)
{
	data[0] = (value >> 8) & 0xffU;
	data[1] = (value >> 0) & 0xffU;
}

uint32_t
istgt_dget24(const uint8_t *data)
{
	uint32_t value;

	value  = (data[0] & 0xffU) << 16;
	value |= (data[1] & 0xffU) << 8;
	value |= (data[2] & 0xffU) << 0;
	return (value);
}

void
istgt_dset24(uint8_t *data, uint32_t value)
{
	data[0] = (value >> 16) & 0xffU;
	data[1] = (value >> 8)  & 0xffU;
	data[2] = (value >> 0)  & 0xffU;
}

uint32_t
istgt_dget32(const uint8_t *data)
{
	uint32_t value;

	value  = (data[0] & 0xffU) << 24;
	value |= (data[1] & 0xffU) << 16;
	value |= (data[2] & 0xffU) << 8;
	value |= (data[3] & 0xffU) << 0;
	return (value);
}

void
istgt_dset32(uint8_t *data, uint32_t value)
{
	data[0] = (value >> 24) & 0xffU;
	data[1] = (value >> 16) & 0xffU;
	data[2] = (value >> 8)  & 0xffU;
	data[3] = (value >> 0)  & 0xffU;
}

uint64_t
istgt_dget48(const uint8_t *data)
{
	uint64_t value;

	value  = (data[0] & 0xffULL) << 40;
	value |= (data[1] & 0xffULL) << 32;
	value |= (data[2] & 0xffULL) << 24;
	value |= (data[3] & 0xffULL) << 16;
	value |= (data[4] & 0xffULL) << 8;
	value |= (data[5] & 0xffULL) << 0;
	return (value);
}

void
istgt_dset48(uint8_t *data, uint64_t value)
{
	data[0] = (value >> 40) & 0xffULL;
	data[1] = (value >> 32) & 0xffULL;
	data[2] = (value >> 24) & 0xffULL;
	data[3] = (value >> 16) & 0xffULL;
	data[4] = (value >> 8)  & 0xffULL;
	data[5] = (value >> 0)  & 0xffULL;
}

uint64_t
istgt_dget64(const uint8_t *data)
{
	uint64_t value;

	value  = (data[0] & 0xffULL) << 56;
	value |= (data[1] & 0xffULL) << 48;
	value |= (data[2] & 0xffULL) << 40;
	value |= (data[3] & 0xffULL) << 32;
	value |= (data[4] & 0xffULL) << 24;
	value |= (data[5] & 0xffULL) << 16;
	value |= (data[6] & 0xffULL) << 8;
	value |= (data[7] & 0xffULL) << 0;
	return (value);
}

void
istgt_dset64(uint8_t *data, uint64_t value)
{
	data[0] = (value >> 56) & 0xffULL;
	data[1] = (value >> 48) & 0xffULL;
	data[2] = (value >> 40) & 0xffULL;
	data[3] = (value >> 32) & 0xffULL;
	data[4] = (value >> 24) & 0xffULL;
	data[5] = (value >> 16) & 0xffULL;
	data[6] = (value >> 8)  & 0xffULL;
	data[7] = (value >> 0)  & 0xffULL;
}

void
istgt_dump(const char *label, const uint8_t *buf, size_t len)
{
	istgt_fdump(stdout, label, buf, len);
}

void
istgt_fdump(FILE *fp, const char *label, const uint8_t *buf, size_t len)
{
	char tmpbuf[MAX_TMPBUF];
	char buf8[8+1] = { 0 };
	size_t total;
	size_t idx;

	fprintf(fp, "%s\n", label);

	total = 0;
	for (idx = 0; idx < len; idx++) {
		if (idx != 0 && idx % 8 == 0) {
			total += snprintf(tmpbuf + total,
				sizeof (tmpbuf) - total, "%s", buf8);
			fprintf(fp, "%s\n", tmpbuf);
			total = 0;
		}
		total += snprintf(tmpbuf + total, sizeof (tmpbuf) - total,
		    "%2.2x ", buf[idx] & 0xff);
		buf8[idx % 8] = isprint(buf[idx]) ? buf[idx] : '.';
	}
	for (; idx % 8 != 0; idx++) {
		total += snprintf(tmpbuf + total,
			sizeof (tmpbuf) - total, "   ");
		buf8[idx % 8] = ' ';
	}
	total += snprintf(tmpbuf + total, sizeof (tmpbuf) - total, "%s", buf8);
	fprintf(fp, "%s\n", tmpbuf);
	fflush(fp);
}

#ifndef HAVE_SRANDOMDEV
#include <time.h>
void
srandomdev(void)
{
	unsigned long seed;
	time_t now;
	pid_t pid;

	pid = getpid();
	now = time(NULL);
	seed = pid ^ now;
	srandom(seed);
}
#endif /* HAVE_SRANDOMDEV */

#ifndef HAVE_ARC4RANDOM
static int istgt_arc4random_initialized = 0;

uint32_t
arc4random(void)
{
	uint32_t r;
	uint32_t r1, r2;

	if (!istgt_arc4random_initialized) {
		srandomdev();
		istgt_arc4random_initialized = 1;
	}
	r1 = (uint32_t) (random() & 0xffff);
	r2 = (uint32_t) (random() & 0xffff);
	r = (r1 << 16) | r2;
	return (r);
}
#endif /* HAVE_ARC4RANDOM */

void
istgt_gen_random(uint8_t *buf, size_t len)
{
#ifdef USE_RANDOM
	long l;
	size_t idx;

	srandomdev();
	for (idx = 0; idx < len; idx++) {
		l = random();
		buf[idx] = (uint8_t) l;
	}
#else
	uint32_t r;
	size_t idx;

	for (idx = 0; idx < len; idx++) {
		r = arc4random();
		buf[idx] = (uint8_t) r;
	}
#endif /* USE_RANDOM */
}

int
istgt_bin2hex(char *buf, size_t len, const uint8_t *data, size_t data_len)
{
	const char *digits = "0123456789ABCDEF";
	size_t total = 0;
	size_t idx;

	if (len < 3)
		return (-1);
	buf[total] = '0';
	total++;
	buf[total] = 'x';
	total++;
	buf[total] = '\0';

	for (idx = 0; idx < data_len; idx++) {
		if (total + 3 > len) {
			buf[total] = '\0';
			return (-1);
		}
		buf[total] = digits[(data[idx] >> 4) & 0x0fU];
		total++;
		buf[total] = digits[data[idx] & 0x0fU];
		total++;
	}
	buf[total] = '\0';
	return (total);
}

int
istgt_hex2bin(uint8_t *data, size_t data_len, const char *str)
{
	const char *digits = "0123456789ABCDEF";
	const char *dp;
	const char *p;
	size_t total = 0;
	int n0, n1;

	p = str;
	if (p[0] != '0' && (p[1] != 'x' && p[1] != 'X'))
		return (-1);
	p += 2;

	while (p[0] != '\0' && p[1] != '\0') {
		if (total >= data_len) {
			return (-1);
		}
		dp = strchr(digits, toupper((int) p[0]));
		if (dp == NULL) {
			return (-1);
		}
		n0 = (int) (dp - digits);
		dp = strchr(digits, toupper((int) p[1]));
		if (dp == NULL) {
			return (-1);
		}
		n1 = (int) (dp - digits);

		data[total] = (uint8_t) (((n0 & 0x0fU) << 4) | (n1 & 0x0fU));
		total++;
		p += 2;
	}
	return (total);
}

void
istgt_yield(void)
{
#if defined(HAVE_PTHREAD_YIELD)
	pthread_yield();
#elif defined(HAVE_SCHED_YIELD)
	sched_yield();
#else
	usleep(0);
#endif
}

#ifndef HAVE_STRLCPY
size_t
strlcpy(char *dst, const char *src, size_t size)
{
	size_t len;

	if (dst == NULL)
		return (0);
	if (size < 1) {
		return (0);
	}
	len = strlen(src);
	if (len > size - 1) {
		len = size - 1;
	}
	memcpy(dst, src, len);
	dst[len] = '\0';
	return (len);
}
#endif /* HAVE_STRLCPY */
