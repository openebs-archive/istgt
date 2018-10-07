/*
 * Copyright (c) 2003 Silicon Graphics International Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions, and the following disclaimer,
 *    without modification.
 * 2. Redistributions in binary form must reproduce at minimum a disclaimer
 *    substantially similar to the "NO WARRANTY" disclaimer below
 *    ("Disclaimer") and any redistribution must be conditioned upon
 *    including a substantially similar Disclaimer requirement for further
 *    binary redistribution.
 *
 * NO WARRANTY
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * $Id: //depot/users/kenm/FreeBSD-test2/sys/cam/ctl/ctl_ser_table.c#1 $
 * $FreeBSD: stable/9/sys/cam/ctl/ctl_ser_table.c 229997 2012-01-12
 * 00:34:33Z ken $
 */

/*
 * iSCSI Target command serialization table.
 *
 * Author: Kim Le
 */

/* ******************************************************************* */
/*
 *	TABLE    istgtSerTbl
 *
 *  The matrix which drives the serialization algorithm. The major index
 *  (the first) into this table is the command being checked and the minor
 *  index is the command against which the first command is being checked.
 *  i.e., the major index (row) command is ahead of the minor index command
 *  (column) in the queue. This allows the code to optimize by capturing the
 *	 result of the first indexing operation into a pointer.
 *
 *  Whenever a new value is added to the IDX_T type, this matrix must be
 *	expanded by one row AND one column -- Because of this, some effort
 *	should be made to re-use the indexes whenever possible.
 */
/* ******************************************************************** */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdint.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <unistd.h>
#include <sys/param.h>

#include "istgt.h"
#include "istgt_ver.h"
#include "istgt_log.h"
#include "istgt_sock.h"
#include "istgt_misc.h"
#include "istgt_md5.h"
#include "istgt_lu.h"
#include "istgt_iscsi.h"
#include "istgt_proto.h"

#if !defined(__GNUC__)
#undef __attribute__
#define	__attribute__(x)
#endif

#define	sK	ISTGT_SER_SKIP		/* Skip */
#define	pS	ISTGT_SER_PASS		/* pS */
#define	bK	ISTGT_SER_BLOCK		/* Blocked */
#define	xT	ISTGT_SER_EXTENT	/* Extent check */

istgt_serialize_action
istgt_serialize_table[ISTGT_SERIDX_COUNT + 1][ISTGT_SERIDX_COUNT + 1] = {
/*
 * >IDX_ :: 2nd:
 * TU RD WRT UNMP MDS MDS RQS INQ RDC RES REL LSN FMT STR PR PROT MAI INV
 * R              N   L   N       P           S           IN      NIN LD
 */
/* TUR */
{ pS, pS, pS, pS, pS, pS, pS, pS, pS, bK, bK, pS, pS, pS, bK, bK, pS, pS},
/* READ */
{ pS, pS, xT, bK, bK, bK, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* WRITE */
{ pS, xT, xT, bK, bK, bK, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* UNMAP */
{ pS, bK, bK, pS, bK, bK, bK, pS, pS, bK, bK, pS, bK, bK, pS, bK, bK, pS},
/* MD_SNS */
{ pS, bK, bK, bK, pS, bK, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* MD_SEL */
{ pS, bK, bK, bK, bK, bK, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* RQ_SNS */
{ pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS,  pS, pS, pS},
/* INQ */
{ pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS},
/* RD_CAP */
{ pS, pS, pS, pS, pS, pS, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* RESV  */
{ bK, bK, bK, bK, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* REL  */
{ bK, bK, bK, bK, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* LOG_SNS */
{ pS, pS, pS,  pS, pS, bK, bK, pS, pS, bK, bK, pS, bK, bK, bK, bK, bK, pS},
/* FORMAT */
{ pS, bK, bK, bK, bK, bK, pS, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* START */
{ pS, bK, bK, bK, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* PRES_IN */
{ bK, bK, bK, pS, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* PRES_OUT */
{ bK, bK, bK, bK, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, bK, pS},
/* MAIN_IN */
{ pS, bK, bK, bK, bK, bK, bK, pS, bK, bK, bK, bK, bK, bK, bK, bK, pS, pS},
/* INVLD */
{ pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS, pS}
};
