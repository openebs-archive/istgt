/* ****************************************************************************
 *  (C) Copyright 2014 CloudByte, Inc.
 *  All Rights Reserved.
 *
 *  This program is an unpublished copyrighted work which is proprietary
 *  to CloudByte, Inc. and contains confidential information that is not
 *  to be reproduced or disclosed to any other person or entity without
 *  prior written consent from CloudByte, Inc. in each and every instance.
 *
 *  WARNING:  Unauthorized reproduction of this program as well as
 *  unauthorized preparation of derivative works based upon the
 *  program or distribution of copies by sale, rental, lease or
 *  lending are violations of federal copyright laws and state trade
 *  secret laws, punishable by civil and criminal penalties.
 *
 *
 ****************************************************************************/
#include <istgt_iscsi_xcopy.h>
#include <istgt_proto.h>
#include <istgt_misc.h>

#ifdef __FreeBSD__
#include <sys/malloc.h>
#endif

#include <sys/queue.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <unistd.h>

#define getdata(data, lu_cmd) {		\
	if (lu_cmd->iobufindx == -1) {		\
		data = NULL;					\
		ISTGT_ERRLOG("data null\n");	\
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;\
		return -1;							\
	} else {							\
		/*?? malloc and copy over all data to data = lu_c*/\
		uint8_t *dptr; int _i; \
		for (_i = 0; _i <= lu_cmd->iobufindx; _i++) \
			_nb += lu_cmd->iobuf[_i].iov_len;\
		dptr = data = xmalloc(_nb);\
		for (_i = 0; _i <= lu_cmd->iobufindx; (dptr += lu_cmd->iobuf[_i].iov_len), _i++)\
			memcpy(dptr, lu_cmd->iobuf[_i].iov_base, lu_cmd->iobuf[_i].iov_len);\
	}									\
}

#define PR_ALLOW(WE,EA,ALLRR,WERR,EARR) \
	 ((((WE)&1) << 4) | (((EA)&1) << 3) | (((ALLRR)&1) << 2) \
	  | (((WERR)&1) << 1) | (((EARR)&1) << 0))

#define BUILD_SENSE(SK,ASC,ASCQ) istgt_lu_scsi_build_sense_data(lu_cmd, ISTGT_SCSI_SENSE_ ## SK, (ASC), (ASCQ))

int
istgt_lu_disk_receive_copy_results(CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	uint8_t *data = 0;
 	short int service_action;
	int data_len = 0;
	uint8_t *cdb = lu_cmd->cdb;
	uint32_t allocation_len = DGET32(&cdb[10]);

	data_len =  allocation_len;
	if (data_len != 0) {
		if (istgt_lu_disk_transfer_data(conn, lu_cmd, data_len) < 0) {
			ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
			return -1;
		}
	}

	data = xmalloc(200);
	service_action = BGET8W(&cdb[1], 4, 5);

	switch(service_action) {
		case 0x00: /* Copy Status */
		{
			/* ILLEGAL REQUEST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			xfree(data);
			return -1;
		}
		case 0x01: /* Receive data */
		{
			/* ILLEGAL REQUEST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			xfree(data);
			return -1;
		}
		case 0x03: /* Operating parameters */
		{
			DSET32(&data[0], 43);	/* Available data (n-3) */
			BSET8W(&data[4], 0, SNLID);
			DSET16(&data[8], MAX_CSCD_DESCRIPTOR_COUNT);
			DSET16(&data[10], MAX_SEGMENT_DESCRIPTOR_COUNT);
			DSET32(&data[12], MAX_DESCRIPTOR_LIST_LENGTH);
			DSET32(&data[16], MAX_SEGMENT_LENGTH);
			DSET32(&data[20], MAX_INLINE_DATA_LENGTH);
			DSET32(&data[24], HELD_DATA_LIMIT);
			DSET32(&data[28], MAX_STREAM_DEVICE_TRANSFER_SIZE);
			DSET16(&data[34], TOTAL_CONCURRENT_COPIES);
			DSET8(&data[36], MAX_CONCURRENT_COPIES);
			DSET8(&data[37], DATA_SEGMENT_GRANULARITY);
			DSET8(&data[38], INLINE_DATA_GRANULARITY);
			DSET8(&data[39], HELD_DATA_GRANULARITY);
			DSET8(&data[43], IMPLEMENTED_DESCRIPTOR_LIST_LENGTH);
			DSET8(&data[44], SEGMENT_DESCRIPTOR_B2B);
			DSET8(&data[45], SEGMENT_DESCRIPTOR_B2B_OFFSET);
			DSET8(&data[46], CSCD_IDENTIFICATION_DESCRIPTOR);
			break;
		}
		case 0x04: /* Failed segment details */
		{
			/* ILLEGAL REQUEST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			xfree(data);
			return -1;
		}
		default:	/* Illegal Request */
		{
			/* ILLEGAL REQUEST */
			BUILD_SENSE(ILLEGAL_REQUEST, 0x24, 0x00);
			xfree(data);
			return -1;
		}
	}

	lu_cmd->data = data;
	lu_cmd->data_len = allocation_len < 47 ? allocation_len : 47;
	return 0;
}

static uint64_t
istgt_lu_get_lid(uint64_t vid)
{
	uint64_t naa;
	uint64_t enc;

	naa = 0x3; // Locally Assigned

	/* NAA + LOCALLY ADMINISTERED VALUE */
	enc = (naa & 0xfULL) << (64-4); // 4bits
	enc |= vid & 0xfffffffffffffffULL; //60bits

	return enc;
}

static ISTGT_LU_DISK *
istgt_find_xcopy_target(ISTGT_Ptr istgt, uint64_t identifier)
{
	ISTGT_LU_Ptr lu;
	uint64_t LUI;
	uint64_t id;
	int i;

	if (istgt == NULL)
		return NULL;
	for (i = 0; i < MAX_LOGICAL_UNIT; i++) {
			lu = istgt->logical_unit[i];
			if (lu == NULL)
				continue;
			/* Cloudbyte supports single lun for logical group so zero */
			LUI = istgt_get_lui(lu->name, 0 & 0xffffU);
				id = istgt_lu_get_lid(LUI);
				if (id == identifier) {
					return (lu->lun[0].spec);
				}
	}

	return NULL;

}

static ISTGT_XCOPY_TGT *
istgt_get_xcopy_target(ISTGT_LU_DISK *spec, uint8_t *target_descriptor, ISTGT_LU_CMD_Ptr lu_cmd)
{
	ISTGT_XCOPY_TGT  *target;
	uint64_t  identifier = 0;
	uint16_t ripi; /* Relative Initiator Port identifier */
	//uint8_t *dvc_typ_param;
	uint8_t identifier_length;
	uint8_t td_opcode;
	uint8_t pdt; /* Peripheral Device Type */
	int association;
	int code_set;
	int identifier_type;
	int naa;


	target =  xmalloc(sizeof *target);
	memset(target, 0, sizeof *target);
	target->spec = NULL;

	td_opcode = DGET8(&target_descriptor[0]);
	if (td_opcode != 0xE4 ) {
		/* ILLEGAL REQUEST UNSUPPORTED TARGET DESCRIPTOR */
		ISTGT_ERRLOG("TARGET_DESCRIPTOR OPCODE NOT SUPPORTED %x \n", td_opcode);
		BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x07); // Refer  SCSI Primary Commands - 3 (SPC-3)
		xfree(target);
		return NULL;
	}

	pdt = BGET8W(&target_descriptor[1], 4, 5);
	ripi = DGET16(&target_descriptor[2]);

	code_set = BGET8W(&target_descriptor[4], 3, 4);
	if (code_set == 1) {
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Association and Identifier field contains Binary values \n");
	}

	association = BGET8W(&target_descriptor[5], 5, 2);
	identifier_type = BGET8W(&target_descriptor[5], 3, 4);
	identifier_length = DGET8(&target_descriptor[7]);

	ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Association %d, Identifier_type %d, Identifier_length %d\n", association, identifier_type, identifier_length);

	if (identifier_type == NAA_IDENTIFIER) {
		/* Naa based descriptor*/
		naa = BGET8W(&target_descriptor[8], 7, 4);
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Naa %d\n", naa);
		if (naa == NAA_IDENTIFIER) {
			/* Locally Assigned Descriptor */
			ISTGT_TRACELOG(ISTGT_TRACE_DEBUG,"Locally Assigned Descriptor\n");
			identifier = DGET64(&target_descriptor[8]);
		}
	}
	else {
		ISTGT_ERRLOG("Identifier Type not supported \n");
		xfree(target);
		return NULL;
	}

	target->spec = istgt_find_xcopy_target(spec->lu->istgt, identifier);
	if (target->spec == NULL) {
		ISTGT_ERRLOG("Target not found \n");
		xfree(target);
		return NULL;
	}

	if (! (pdt == PDT_DIRECT_ACCESS_BLK_DEV || pdt == PDT_SIMPLIFIED_DIRECT_ACCESS_DEV)) {
		ISTGT_ERRLOG("INVALID peripheral device type %x\n", pdt);
		xfree(target);
		return NULL;
	}

	/* Relative initiator port zero specifies that any port can be used */
	if (ripi == 0) {
		/* use any port */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "Relative Initiator Port %d\n", ripi);
	}

	target->pad = BGET8W(&target_descriptor[28], 2, 1);
	target->block_len = DGET24(&target_descriptor[29]);

	return target;
}

int
istgt_lu_disk_process_xcopy(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, uint8_t *data_buf, int dlen)
{
	ISTGT_XCOPY_TGT *src_tgt = NULL, *dst_tgt= NULL;
	uint64_t src_lba = 0, dst_lba = 0; /* Source and Destination logical block address */
	uint32_t num_of_blks_byts = 0;
	uint8_t *src_td = NULL, *dst_td= NULL; /* Source Destination target descriptor*/
	uint8_t lstid = 0; /* List identifier */
	uint8_t sd_opcode = 0; /* segment Descriptor opcode support only 0x2h, 0xDh, 0xAh */
	uint8_t stdi, dtdi; /* Source target descriptor and Destination target descriptor index */
	off_t src_offset=0, dst_offset=0;
	int rc = 0; /* result */
	int dc = 0, cat = 0;
	int tdll = 0; /* Target and Segment Descriptor List Length */
	int len = 16; /* Actual block starts with the offset 16 */
	int str, list_id_usage, priority; /* sequential striped (str), No Receive Copy Results (NRCR), Priority */
	int num_tar_desc; /* Number of target descriptor */
	//uint32_t transfer_len = 0; /* Transfer length */
	int sdl = 0; /* Segment descriptor length */

	if (spec == NULL) { /* This spec can be source target / destination target, should not be NULL*/
		ISTGT_ERRLOG("c#%d SPEC in the istgt_lu_disk_process_xcopy is NULL\n", conn->id);
		goto fail;
	}


	/* List ID should be zero for XCOPY(LID1), if SNLID = 1
	  * Refer section 6.4.3.2 of spc4r36s */
	lstid = DGET8(&data_buf[0]);
	if (lstid != 0) {
		/*ILLEGAL REQUEST INVALID FIELD IN PARAMETER LIST */
		ISTGT_ERRLOG("c#%d LST_ID is Non-Zero\n", conn->id);
		//BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
		//goto fail;
	}

	priority = BGET8W(&data_buf[1], 2, 3);
	if (priority == 0) {
		/* High priority copy command */
		/* CloudByte: Currently does not support priority on the commands. */
	}

	str = BGET8W(&data_buf[1], 5, 1);
	if (str == 1) {
		/* sequential access */
		}
	else {
		/* May not be sequential */
		}

	list_id_usage = BGET8W(&data_buf[1], 4, 2);
	if (list_id_usage == LIST_ID_USAGE) {
		/* List_id_usage is 11b, which specifies that,
		  * the copy manager need not hold any data for tha application client,
		  * and the Extended copy commands list identifier value should only be zero
		  * Refer 6.4.3.2 of spc4r36s */
		ISTGT_TRACELOG(ISTGT_TRACE_DEBUG, "c#%d List_id_usage %d\n", conn->id, list_id_usage);
	} else {
		ISTGT_ERRLOG("c#%d List_id_usage is not valid \n", conn->id);
		/*ILLEGAL REQUEST INVALID FIELD IN PARAMETER LIST */
		//BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x00);
		//goto fail;
	}

	/* Number of target descriptors must not be more than MAXIMUM TARGET DESCRIPTOR COUNT */
	tdll = DGET16(&data_buf[2]);
	num_tar_desc = tdll/CSCD_DESCRIPTOR_LENGTH;
	if (num_tar_desc != MAX_CSCD_DESCRIPTOR_COUNT) {
		/* ILLEGAL REQUEST: TOO MANY TARGET DESCRIPTORS */
		ISTGT_ERRLOG("c#%d TOO MANY TARGET DESCRIPTORS\n", conn->id);
		BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x06);	/* Refer spc4r36s */
		goto fail;
	}

	DSET32(&data_buf[4], 0); /* Copy manager to ensure bytes 4 through 7 contains zero */

	/* Fetch the segment descriptors to get the index for target descriptor */
	len = PARAMETER_HEADER_DATA + tdll;

	//need to come back to this one later.. if we need to have xcopy support
	//while (((len + (PARAMETER_HEADER_DATA+tdll+(sdll))) <= dlen) && (len < (PARAMETER_HEADER_DATA+tdll+(sdll))))
	while (len +4 <=  dlen)
	{
		sd_opcode = DGET8(&data_buf[len]);
		if (!(sd_opcode == SEGMENT_DESCRIPTOR_B2B || sd_opcode == SEGMENT_DESCRIPTOR_B2B_OFFSET)) {
			ISTGT_ERRLOG("c#%d SD OPCODE NOT SUPPORTED %x  \n", conn->id, sd_opcode);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x09); // Refer  SCSI Primary Commands - 3 (SPC-3) 
			goto fail;
		}

		sdl = DGET16(&data_buf[len+2]);

		if (len+sdl > dlen) {
			ISTGT_ERRLOG("c#%d SD data error we need to send COPY ABORTED %x  \n", conn->id, sd_opcode);
			BUILD_SENSE(ILLEGAL_REQUEST, 0x26, 0x09); // Refer  SCSI Primary Commands - 3 (SPC-3) 
			goto fail;
		}
		stdi = DGET16(&data_buf[len +4]);
		dtdi = DGET16(&data_buf[len+6]);

		num_of_blks_byts = DGET16(&data_buf[len+10]);
		src_lba = DGET64(&data_buf[len+12]);
		dst_lba = DGET64(&data_buf[len+20]);
		if (sd_opcode == 0xA){
			src_offset = DGET16(&data_buf[len+28]);
			dst_offset = DGET16(&data_buf[len+30]);
		}

		cat = BGET8W(&data_buf[len+1], 0, 1); 
		stdi = PARAMETER_HEADER_DATA + (stdi * CSCD_DESCRIPTOR_LENGTH);
		dtdi = PARAMETER_HEADER_DATA + (dtdi * CSCD_DESCRIPTOR_LENGTH);

		src_td = &data_buf[stdi];
		dst_td = &data_buf[dtdi];

		src_tgt = istgt_get_xcopy_target(spec, src_td, lu_cmd);
		if (src_tgt == NULL) {
			ISTGT_ERRLOG("c#%d Could not find the source target \n", conn->id);
			goto fail;
		}

		//if (src_td != dst_td) {
			dst_tgt = istgt_get_xcopy_target(spec, dst_td, lu_cmd);
			if (dst_tgt == NULL) {
				ISTGT_ERRLOG("c#%d Could not find the destination target \n", conn->id);
				goto fail;
			}
		//}else {
			/* Source and Destination Targets are both same */
			//dst_tgt = src_tgt ;
		//}

		if (sd_opcode != 0xA) {
			src_offset = src_lba * src_tgt->block_len;
			dst_offset = dst_lba *dst_tgt->block_len;
			dc = BGET8W(&data_buf[len+1], 1, 1);
		}
		
		src_tgt->lba = src_lba;
		dst_tgt->lba = dst_lba;
		src_tgt->offset = src_offset;
		dst_tgt->offset = dst_offset;

		rc = istgt_lu_disk_lbxcopy(src_tgt, dst_tgt, conn, lu_cmd, dc, cat, num_of_blks_byts, sd_opcode);
		if (rc < 0) {
			ISTGT_ERRLOG("c#%d lu_disk_lbxcopy() failed\n", conn->id); 
			goto fail;	
		}
		len += sdl+4;
		xfree(src_tgt);
		xfree(dst_tgt);
	}

	return 0;

	fail:
		if (src_tgt)
			xfree(src_tgt);
		if (dst_tgt)
			xfree(dst_tgt);
		return -1;
}

int
istgt_lu_disk_xcopy(ISTGT_LU_DISK *spec, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd)
{
	uint32_t i = 0,pll = 0;
	uint8_t *cdb;
	uint8_t *data;
	short int opcode = 0;
	int rc = 0;
	int _nb = 0;

	cdb = lu_cmd->cdb; /* Command Descriptor Block */

	opcode = cdb[0];
	/* Check for the opcode */
	if (opcode != 0x83) {
		ISTGT_ERRLOG("c#%d EXTENDED COPY command opcode seems to be wrong \n", conn->id);
		return -1;
	}

	pll = DGET32(&cdb[10]);
	/* Check for the parameter list length */
	if (pll == 0) {
		/* Prameter list length is zero Nothing to do */
		return 0; /* Success */
	}

	/* Parameter list length truncates the prarameter data */
	if (pll < 2) { 
		/* PARAMETER LIST LENGTH ERROR */
		BUILD_SENSE(ILLEGAL_REQUEST, 0x1A, 0x00);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}

	/* Check whether write bit is set, if not return with error condition */
	if (lu_cmd->W_bit == 0) {
		ISTGT_ERRLOG("W_bit == 0\n");
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}

	/* Data-Out */
	rc = istgt_lu_disk_transfer_data(conn, lu_cmd, pll);
	if (rc < 0) {
		ISTGT_ERRLOG("c#%d lu_disk_transfer_data() failed\n", conn->id);
		lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		return -1;
	}

	getdata(data, lu_cmd)

	/* Process the XCOPY data */
	rc = istgt_lu_disk_process_xcopy(spec, conn, lu_cmd, data, _nb);
	if (rc < 0) {
		ISTGT_ERRLOG("c#%d istgt_lu_disk_process_xcopy() failed\n", conn->id);
		if (lu_cmd->status !=  ISTGT_SCSI_STATUS_RESERVATION_CONFLICT)
			lu_cmd->status = ISTGT_SCSI_STATUS_CHECK_CONDITION;
		if (data != NULL)
			xfree(data);
		return rc;
	}

	/* Reserved bytes should to be zeroed */
	DSET32(&cdb[1], 0);
	DSET32(&cdb[5], 0);
	DSET8(&cdb[9], 0);
	DSET8(&cdb[14], 0);
	for (i= 0; i<=pll; i++) {
		DSET8(&data[i], 0);
	}

	lu_cmd->data = data;
	lu_cmd->data_len = pll;
	return 0;
}

static uint64_t
istgt_obtain_xcopy_nbytes(ISTGT_XCOPY_TGT *src_tgt, ISTGT_XCOPY_TGT *dst_tgt, int dc, uint64_t num_blks_byts, uint8_t sd_opcode)
{
	uint64_t nbytes = 0;
	if (sd_opcode == 0x02) {
		if (num_blks_byts == 0 && dc == 0) {
			/* residual handle */ 
		} else if (num_blks_byts == 0 && dc == 1) {
			/* residual handle */
		} else if (dc == 0) {
			nbytes = num_blks_byts * src_tgt->block_len;
		} else if (dc == 1) {
			nbytes = num_blks_byts * dst_tgt->block_len;
		}
	}else {
		nbytes = num_blks_byts;
	}
	return nbytes;
}
 
int
istgt_lu_disk_lbxcopy(ISTGT_XCOPY_TGT *src_tgt, ISTGT_XCOPY_TGT *dst_tgt, CONN_Ptr conn, ISTGT_LU_CMD_Ptr lu_cmd, int dc, int cat,  uint64_t num_blks_byts, uint8_t sd_opcode)
{
	uint64_t *clone_buf = NULL;
	uint64_t maxlba;
	uint64_t llen;
	uint64_t  nbytes;
	int64_t rc;

	/* Check for the scsi reservation */
	if (src_tgt->spec->rsv_key) {
		rc = istgt_lu_disk_check_pr(src_tgt->spec, conn, PR_ALLOW(1,0,1,1,0));
		if (rc != 0) {
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			return -1;
		}
	}

	/* Check for the scsi reservation */
	if (dst_tgt->spec->rsv_key) {
		rc = istgt_lu_disk_check_pr(dst_tgt->spec, conn, PR_ALLOW(1,0,1,1,0));
		if (rc != 0) {
			lu_cmd->status = ISTGT_SCSI_STATUS_RESERVATION_CONFLICT;
			return -1;
		}
	}

	nbytes = istgt_obtain_xcopy_nbytes(src_tgt, dst_tgt, dc, num_blks_byts, sd_opcode);
	if (nbytes == 0) {
		/* No data to copy */
		lu_cmd->data_len = 0;
		return 0;
	}
		
	maxlba = src_tgt->spec->blockcnt;
	llen = (nbytes / src_tgt->block_len);	
	if (src_tgt->lba >= maxlba || llen > maxlba || src_tgt->lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media\n", conn->id);
		return -1;
	}
	

	MTX_LOCK(&src_tgt->spec->clone_mutex);
	clone_buf = xmalloc(nbytes);
	rc = pread(src_tgt->spec->fd,  clone_buf, nbytes, src_tgt->offset);
	if (rc < 0  || (uint64_t) rc != nbytes) {
		xfree(clone_buf);
		/* CloudByte: TODO FIX, Refer spc4rs36 Table-118, Preserve the residual data
		  * for the next segment processing if the cat bit is being Non-zero */
		MTX_UNLOCK(&src_tgt->spec->clone_mutex);
		if (src_tgt->pad  == 0 && dst_tgt->pad == 0 && cat == 0) {
			BUILD_SENSE(COPY_ABORTED, 0x26, 0x0A);	/* Copy Aborted: Unexpected Inexact Segment */
		}
		ISTGT_ERRLOG("c#%d lu_disk_read() failed, %d read: %ld\n", conn->id, errno, rc);
		return -1;
	}

	MTX_UNLOCK(&src_tgt->spec->clone_mutex);

	maxlba = dst_tgt->spec->blockcnt;
	llen = (nbytes / dst_tgt->block_len);
	if (dst_tgt->lba >= maxlba || llen > maxlba || dst_tgt->lba > (maxlba - llen)) {
		ISTGT_ERRLOG("c#%d end of media\n", conn->id);
		xfree(clone_buf);
		return -1;
	}

	
	if (dst_tgt->spec->lu->readonly) {
		ISTGT_ERRLOG("c#%d LU%d: readonly unit\n", conn->id, dst_tgt->spec->lu->num);
		xfree(clone_buf);
		return -1;
	}

	MTX_LOCK(&dst_tgt->spec->clone_mutex);
	rc = pwrite(dst_tgt->spec->fd, clone_buf, nbytes, dst_tgt->offset);
	if (rc < 0  || (uint64_t) rc != nbytes) {
		xfree(clone_buf);
		/* CloudByte: TODO FIX, Refer sp4r36s Table-118, Preserve the residual data
		  * for the next segment processing if the cat bit being NON-Zero */
		MTX_UNLOCK(&dst_tgt->spec->clone_mutex);
		if (src_tgt->pad  == 0 && dst_tgt->pad == 0 && cat == 0) {
			BUILD_SENSE(COPY_ABORTED, 0x26, 0x0A);	/* Copy Aborted: Unexpected Inexact Segment */
		}
		ISTGT_ERRLOG("c#%d lu_disk_write() failed, %d read: %ld\n", conn->id, errno, rc);
		return -1;
	}
	xfree(clone_buf);
	MTX_UNLOCK(&dst_tgt->spec->clone_mutex);

	return 0;
}

