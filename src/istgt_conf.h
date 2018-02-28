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

#ifndef ISTGT_CONF_H
#define ISTGT_CONF_H

typedef struct config_value_t {
	struct config_value_t *next;
	char *value;
} CF_VALUE;

typedef struct config_item_t {
	struct config_item_t *next;
	char *key;
	CF_VALUE *val;
} CF_ITEM;

typedef enum
{
	ST_INVALID = -1,
	ST_NONE = 0,
	ST_GLOBAL,
	ST_UNITCONTROL,
	ST_PORTALGROUP,
	ST_INITIATORGROUP,
	ST_LOGICAL_UNIT,
	ST_AUTHGROUP,
} CF_SECTION_TYPE;

typedef struct config_section_t
{
	struct config_section_t *next;
	CF_SECTION_TYPE type;
	char *name;
	int num;
	CF_ITEM *item;
} CF_SECTION;

typedef struct config_t
{
	char *file;
	CF_SECTION *current_section;
	CF_SECTION *section;
} CONFIG;

CONFIG *istgt_allocate_config(void);
void istgt_free_config(CONFIG *cp);
void istgt_copy_cf_item(CF_SECTION *sp_dst, CF_SECTION *sp_src);
CF_SECTION *istgt_find_cf_section(CONFIG *cp, const char *name);
CF_ITEM *istgt_find_cf_nitem(CF_SECTION *sp, const char *key, int idx);
CF_ITEM *istgt_find_cf_item(CF_SECTION *sp, const char *key);
int istgt_read_config(CONFIG *cp, const char *file);
int istgt_print_config(CONFIG *cp);

#endif /* ISTGT_CONF_H */
