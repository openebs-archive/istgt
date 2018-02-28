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

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include <fcntl.h>
#include <unistd.h>

#include "istgt.h"
#include "istgt_log.h"
#include "istgt_misc.h"
#include "istgt_lu.h"
#include "istgt_proto.h"

#if !defined(__GNUC__)
#undef __attribute__
#define __attribute__(x)
#endif

#ifdef USE_VBOXVD
#define IN_RING3
#include "iprt/buildconfig.h"
#include "VBox/vd.h"

typedef struct istgt_lu_disk_vbox_t {
	PVBOXHDD pDisk;
	PVDINTERFACE pVDIfs;
	PVDINTERFACE pVDIfsImage;
	VDINTERFACE VDIfsDisk;
	VDINTERFACEERROR VDIfError;
	VDTYPE enmType;
	RTUUID uuid;
} ISTGT_LU_DISK_VBOX;

#ifndef O_ACCMODE
#define O_ACCMODE (O_RDONLY|O_WRONLY|O_RDWR)
#endif

static void istgt_lu_disk_vbox_error(void *pvUser, int rc, const char *pszFile, unsigned iLine, const char *pszFunction, const char *pszFormat, va_list va) __attribute__((__format__(__printf__, 6, 0)));

static void istgt_lu_disk_vbox_error(void *pvUser, int rc, const char *pszFile, unsigned iLine, const char *pszFunction, const char *pszFormat, va_list va)
{
	ISTGT_LU_DISK *spec = (ISTGT_LU_DISK*)pvUser;
	char buf[MAX_TMPBUF*2];

	vsnprintf(buf, sizeof buf, pszFormat, va);
	ISTGT_ERRLOG("LU%d: LUN%d: rc=%d, %s:%u:%s: %s", spec->num, spec->lun,
	    rc, pszFile, iLine, pszFunction, buf);
}

static int
istgt_lu_disk_open_vbox(ISTGT_LU_DISK *spec, int flags, int mode __attribute__((__unused__)))
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	PVDINTERFACE pIf;
	uint32_t major, minor, build;
	unsigned uOpenFlags;
	int rc;

	major = RTBldCfgVersionMajor();
	minor = RTBldCfgVersionMinor();
	build = RTBldCfgVersionBuild();
	if (major > 4 || (major == 4 && minor >= 2)) {
		// VBoxDDU >= 4.2
		if (ISTGT_VBOXINC_VERSION_MAJOR < 4 ||
		    (ISTGT_VBOXINC_VERSION_MAJOR == 4 && ISTGT_VBOXINC_VERSION_MINOR < 2)) {
			ISTGT_ERRLOG("VBox library is newer than istgt\n");
			return -1;
		}
	} else {
		// VBoxDDU < 4.2
		if (ISTGT_VBOXINC_VERSION_MAJOR > 4 ||
		    (ISTGT_VBOXINC_VERSION_MAJOR == 4 && ISTGT_VBOXINC_VERSION_MINOR >= 2)) {
			ISTGT_ERRLOG("VBox library is older than istgt\n");
			return -1;
		}
		if (strcasecmp(spec->disktype, "QCOW") == 0
		    || strcasecmp(spec->disktype, "QED") == 0
		    || strcasecmp(spec->disktype, "VHDX") == 0) {
			ISTGT_ERRLOG("VD format(%s) is not supported in this version.\n",
			    spec->disktype);
			return -1;
		}
	}

	if ((flags & O_ACCMODE) == O_RDONLY) {
		uOpenFlags = VD_OPEN_FLAGS_READONLY;
	} else if ((flags & O_ACCMODE) == O_RDWR) {
		uOpenFlags = VD_OPEN_FLAGS_NORMAL;
	} else {
		ISTGT_ERRLOG("not supported mode %x\n", (flags & O_ACCMODE));
		return -1;
	}

	exspec->pDisk = NULL;
	exspec->pVDIfs = NULL;
	exspec->pVDIfsImage = NULL;
	exspec->enmType = VDTYPE_HDD;

#if ((ISTGT_VBOXINC_VERSION_MAJOR > 4) || (ISTGT_VBOXINC_VERSION_MAJOR == 4 && ISTGT_VBOXINC_VERSION_MINOR >= 2))
	exspec->VDIfError.pfnError = istgt_lu_disk_vbox_error;
	exspec->VDIfError.pfnMessage = NULL;
	pIf = (PVDINTERFACE)&exspec->VDIfError;

	rc = VDInterfaceAdd(pIf, "VD interface error", VDINTERFACETYPE_ERROR,
	    spec, sizeof(VDINTERFACEERROR), &exspec->pVDIfs);
#else /* VBox < 4.2 */
	exspec->VDIfError.cbSize = sizeof(VDINTERFACEERROR);
	exspec->VDIfError.enmInterface = VDINTERFACETYPE_ERROR;
	exspec->VDIfError.pfnError = istgt_lu_disk_vbox_error;
	exspec->VDIfError.pfnMessage = NULL;
	pIf = &exspec->VDIfsDisk;

	rc = VDInterfaceAdd(pIf, "VD interface error", VDINTERFACETYPE_ERROR,
	    &exspec->VDIfError, spec, &exspec->pVDIfs);
#endif /* VBox >= 4.2 */
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDInterfaceAdd error\n");
		return -1;
	}

	rc = VDCreate(exspec->pVDIfs, exspec->enmType, &exspec->pDisk);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDCreate error\n");
		VDInterfaceRemove(pIf, &exspec->pVDIfs);
		exspec->pDisk = NULL;
		exspec->pVDIfs = NULL;
		exspec->pVDIfsImage = NULL;
		return -1;
	}
	rc = VDOpen(exspec->pDisk, spec->disktype, spec->file, uOpenFlags,
	    exspec->pVDIfsImage);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDOpen error\n");
		VDDestroy(exspec->pDisk);
		VDInterfaceRemove(pIf, &exspec->pVDIfs);
		exspec->pDisk = NULL;
		exspec->pVDIfs = NULL;
		exspec->pVDIfsImage = NULL;
		return -1;
	}
	return 0;
}

static int
istgt_lu_disk_close_vbox(ISTGT_LU_DISK *spec)
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	PVDINTERFACE pIf;
	bool fDelete = false;
	int rc;

#if ((ISTGT_VBOXINC_VERSION_MAJOR > 4) || (ISTGT_VBOXINC_VERSION_MAJOR == 4 && ISTGT_VBOXINC_VERSION_MINOR >= 2))
	pIf = (PVDINTERFACE)&exspec->VDIfError;
#else /* VBox < 4.2 */
	pIf = &exspec->VDIfsDisk;
#endif /* VBox >= 4.2 */
	rc = VDClose(exspec->pDisk, fDelete);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDClose error\n");
		VDDestroy(exspec->pDisk);
		VDInterfaceRemove(pIf, &exspec->pVDIfs);
		exspec->pDisk = NULL;
		exspec->pVDIfs = NULL;
		exspec->pVDIfsImage = NULL;
		return -1;
	}
	VDDestroy(exspec->pDisk);
	VDInterfaceRemove(pIf, &exspec->pVDIfs);
	exspec->pDisk = NULL;
	exspec->pVDIfs = NULL;
	exspec->pVDIfsImage = NULL;
	return 0;
}

static int64_t
istgt_lu_disk_seek_vbox(ISTGT_LU_DISK *spec, uint64_t offset)
{
	//ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;

	spec->foffset = offset;
	return 0;
}

static int64_t
istgt_lu_disk_read_vbox(ISTGT_LU_DISK *spec, void *buf, uint64_t nbytes)
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	uint64_t offset;
	int rc;

	offset = spec->foffset;
	rc = VDRead(exspec->pDisk, offset, buf, (size_t)nbytes);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDRead error\n");
		return -1;
	}
	spec->foffset += nbytes;
	return (int64_t)nbytes;
}

static int64_t
istgt_lu_disk_write_vbox(ISTGT_LU_DISK *spec, const void *buf, uint64_t nbytes)
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	uint64_t offset;
	int rc;

	offset = spec->foffset;
	rc = VDWrite(exspec->pDisk, offset, buf, (size_t)nbytes);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDWrite error\n");
		return -1;
	}
	spec->foffset += nbytes;
	return (int64_t)nbytes;
}

static int64_t
istgt_lu_disk_sync_vbox(ISTGT_LU_DISK *spec, uint64_t offset __attribute__((__unused__)), uint64_t nbytes __attribute__((__unused__)))
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	int rc;

	rc = VDFlush(exspec->pDisk);
	if (RT_FAILURE(rc)) {
		ISTGT_ERRLOG("VDFlush error\n");
		return -1;
	}
	return 0;
}

static int
istgt_lu_disk_allocate_vbox(ISTGT_LU_DISK *spec __attribute__((__unused__)))
{
	//ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;

	return 0;
}

static int
istgt_lu_disk_setcache_vbox(ISTGT_LU_DISK *spec)
{
	//ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;

	if (spec->read_cache) {
	}
	if (spec->write_cache) {
	}
	return 0;
}

int
istgt_lu_disk_vbox_lun_init(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu)
{
	ISTGT_LU_DISK_VBOX *exspec;
	uint64_t capacity;
	uint64_t fsize;
	int flags;
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_vbox_lun_init\n");

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d for disktype=%s\n",
	    spec->num, spec->lun, spec->disktype);

	spec->open = istgt_lu_disk_open_vbox;
	spec->close = istgt_lu_disk_close_vbox;
	spec->seek = istgt_lu_disk_seek_vbox;
	spec->read = istgt_lu_disk_read_vbox;
	spec->write = istgt_lu_disk_write_vbox;
	spec->sync = istgt_lu_disk_sync_vbox;
	spec->allocate = istgt_lu_disk_allocate_vbox;
	spec->setcache = istgt_lu_disk_setcache_vbox;

	exspec = xmalloc(sizeof *exspec);
	memset(exspec, 0, sizeof *exspec);
	spec->exspec = exspec;

	flags = lu->readonly ? O_RDONLY : O_RDWR;
	rc = spec->open(spec, flags, 0666);
	if (rc < 0) {
		ISTGT_ERRLOG("LU%d: LUN%d: open error(rc=%d)\n",
		    spec->num, spec->lun, rc);
		return -1;
	}

	capacity = VDGetSize(exspec->pDisk, 0);
	fsize = VDGetFileSize(exspec->pDisk, 0);

	spec->size = capacity;
	spec->blocklen = 512;
	spec->blockcnt = spec->size / spec->blocklen;
	if (spec->blockcnt == 0) {
		ISTGT_ERRLOG("LU%d: LUN%d: size zero\n", spec->num, spec->lun);
		spec->close(spec);
		return -1;
	}

#if 0
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "LU%d: LUN%d file=%s, size=%"PRIu64"\n",
	    spec->num, spec->lun, spec->file, spec->size);
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,
	    "LU%d: LUN%d %"PRIu64" blocks, %"
	    PRIu64" bytes/block\n",
	    spec->num, spec->lun, spec->blockcnt, spec->blocklen);
#endif

	printf("LU%d: LUN%d file=%s, size=%"PRIu64"\n",
	    spec->num, spec->lun, spec->file, spec->size);
	printf("LU%d: LUN%d %"PRIu64" blocks, %"PRIu64" bytes/block\n",
	    spec->num, spec->lun, spec->blockcnt, spec->blocklen);

	if (strcasecmp(spec->disktype, "VDI") == 0
	    || strcasecmp(spec->disktype, "VHD") == 0
	    || strcasecmp(spec->disktype, "VMDK") == 0) {
		rc = VDGetUuid(exspec->pDisk, 0, &exspec->uuid);
		if (RT_FAILURE(rc)) {
			ISTGT_ERRLOG("LU%d: LUN%d: uuid error\n", spec->num, spec->lun);
			spec->close(spec);
			return -1;
		}
		printf("LU%d: LUN%d UUID="
		    "%8.8x-%4.4x-%4.4x-%2.2x%2.2x-%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x\n",
		    spec->num, spec->lun,
		    exspec->uuid.Gen.u32TimeLow,
		    exspec->uuid.Gen.u16TimeMid,
		    exspec->uuid.Gen.u16TimeHiAndVersion,
		    exspec->uuid.Gen.u8ClockSeqHiAndReserved,
		    exspec->uuid.Gen.u8ClockSeqLow,
		    exspec->uuid.Gen.au8Node[0],
		    exspec->uuid.Gen.au8Node[1],
		    exspec->uuid.Gen.au8Node[2],
		    exspec->uuid.Gen.au8Node[3],
		    exspec->uuid.Gen.au8Node[4],
		    exspec->uuid.Gen.au8Node[5]);
	}
	return 0;
}

int
istgt_lu_disk_vbox_lun_shutdown(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu __attribute__((__unused__)))
{
	ISTGT_LU_DISK_VBOX *exspec = (ISTGT_LU_DISK_VBOX *)spec->exspec;
	int rc;

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "istgt_lu_disk_vbox_lun_shutdown\n");

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d for disktype=%s\n",
	    spec->num, spec->lun, spec->disktype);

	if (!spec->lu->readonly) {
		rc = spec->sync(spec, 0, spec->size);
		if (rc < 0) {
			//ISTGT_ERRLOG("LU%d: lu_disk_sync() failed\n", lu->num);
			/* ignore error */
		}
	}
	rc = spec->close(spec);
	if (rc < 0) {
		//ISTGT_ERRLOG("LU%d: lu_disk_close() failed\n", lu->num);
		/* ignore error */
	}

	xfree(exspec);
	spec->exspec = NULL;
	return 0;
}
#else /* USE_VBOXVD */
int
istgt_lu_disk_vbox_lun_init(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu __attribute__((__unused__)))
{
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d unsupported virtual disk\n",
	    spec->num, spec->lun);
	return -1;
}

int
istgt_lu_disk_vbox_lun_shutdown(ISTGT_LU_DISK *spec, ISTGT_Ptr istgt __attribute__((__unused__)), ISTGT_LU_Ptr lu __attribute__((__unused__)))
{
	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "LU%d: LUN%d unsupported virtual disk\n",
	    spec->num, spec->lun);
	return -1;
}
#endif /* USE_VBOXVD */
