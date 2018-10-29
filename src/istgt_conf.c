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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "istgt.h"
#include "istgt_misc.h"
#include "istgt_conf.h"

// #define CF_DELIM " \t,;"
#define	CF_DELIM " \t"

static void istgt_free_all_cf_section(CF_SECTION *sp);
static void istgt_free_all_cf_item(CF_ITEM *ip);
static void istgt_free_all_cf_value(CF_VALUE *vp);
static void istgt_append_cf_item(CF_SECTION *sp, CF_ITEM *ip);
static void istgt_append_cf_value(CF_ITEM *ip, CF_VALUE *vp);

CONFIG *
istgt_allocate_config(void)
{
	CONFIG *cp;

	cp = xmalloc(sizeof (*cp));
	memset(cp, 0, sizeof (*cp));
	cp->file = NULL;
	cp->section = NULL;

	return (cp);
}

void
istgt_free_config(CONFIG *cp)
{
	if (cp == NULL)
		return;
	if (cp->section != NULL) {
		istgt_free_all_cf_section(cp->section);
	}
	xfree(cp->file);
	xfree(cp);
}

static CF_SECTION *
istgt_allocate_cf_section(void)
{
	CF_SECTION *sp;

	sp = xmalloc(sizeof (*sp));
	memset(sp, 0, sizeof (*sp));
	sp->next = NULL;
	sp->item = NULL;

	return (sp);
}

static void
istgt_free_cf_section(CF_SECTION *sp)
{
	if (sp == NULL)
		return;
	if (sp->item) {
		istgt_free_all_cf_item(sp->item);
	}
	xfree(sp->name);
	xfree(sp);
}

static void
istgt_free_all_cf_section(CF_SECTION *sp)
{
	CF_SECTION *next;

	if (sp == NULL)
		return;
	while (sp != NULL) {
		next = sp->next;
		istgt_free_cf_section(sp);
		sp = next;
	}
}

static CF_ITEM *
istgt_allocate_cf_item(void)
{
	CF_ITEM *ip;

	ip = xmalloc(sizeof (*ip));
	memset(ip, 0, sizeof (*ip));
	ip->next = NULL;
	ip->key = NULL;
	ip->val = NULL;

	return (ip);
}

static void
istgt_free_cf_item(CF_ITEM *ip)
{
	if (ip == NULL)
		return;
	if (ip->val != NULL) {
		istgt_free_all_cf_value(ip->val);
	}
	xfree(ip->key);
	xfree(ip);
}

static void
istgt_free_all_cf_item(CF_ITEM *ip)
{
	CF_ITEM *next;

	if (ip == NULL)
		return;
	while (ip != NULL) {
		next = ip->next;
		istgt_free_cf_item(ip);
		ip = next;
	}
}

static CF_VALUE *
istgt_allocate_cf_value(void)
{
	CF_VALUE *vp;

	vp = xmalloc(sizeof (*vp));
	memset(vp, 0, sizeof (*vp));
	vp->next = NULL;
	vp->value = NULL;

	return (vp);
}

static void
istgt_free_cf_value(CF_VALUE *vp)
{
	if (vp == NULL)
		return;
	xfree(vp->value);
	xfree(vp);
}

static void
istgt_free_all_cf_value(CF_VALUE *vp)
{
	CF_VALUE *next;

	if (vp == NULL)
		return;
	while (vp != NULL) {
		next = vp->next;
		istgt_free_cf_value(vp);
		vp = next;
	}
}

void
istgt_copy_cf_item(CF_SECTION *sp_dst, CF_SECTION *sp_src)
{
	CF_ITEM *ip, *ip_old;
	CF_VALUE *vp, *vp_old;

	istgt_free_all_cf_item(sp_dst->item);
	sp_dst->item = NULL;

	ip_old = sp_src->item;
	while (ip_old != NULL) {
		ip = istgt_allocate_cf_item();
		istgt_append_cf_item(sp_dst, ip);
		ip->key = xstrdup(ip_old->key);
		ip->val = NULL;

		vp_old = ip_old->val;
		while (vp_old != NULL) {
			vp = istgt_allocate_cf_value();
			istgt_append_cf_value(ip, vp);
			vp->value = xstrdup(vp_old->value);

			vp_old = vp_old->next;
		}
		ip_old = ip_old->next;
	}
}

CF_SECTION *
istgt_find_cf_section(CONFIG *cp, const char *name)
{
	CF_SECTION *sp;

	if (name == NULL || name[0] == '\0')
		return (NULL);

	for (sp = cp->section; sp != NULL; sp = sp->next) {
		if (sp->name != NULL && sp->name[0] == name[0] &&
				strcasecmp(sp->name, name) == 0) {
			return (sp);
		}
	}

	return (NULL);
}

static void
istgt_append_cf_section(CONFIG *cp, CF_SECTION *sp)
{
	CF_SECTION *last;

	if (cp == NULL)
		return;
	if (cp->section == NULL) {
		cp->section = sp;
		return;
	}
	for (last = cp->section; last->next != NULL; last = last->next)
		;
	last->next = sp;
}

CF_ITEM *
istgt_find_cf_nitem(CF_SECTION *sp, const char *key, int idx)
{
	CF_ITEM *ip;
	int i;

	if (key == NULL || key[0] == '\0')
		return (NULL);

	i = 0;
	for (ip = sp->item; ip != NULL; ip = ip->next) {
		if (ip->key != NULL && ip->key[0] == key[0] &&
				strcasecmp(ip->key, key) == 0) {
			if (i == idx) {
				return (ip);
			}
			i++;
		}
	}

	return (NULL);
}

CF_ITEM *
istgt_find_cf_item(CF_SECTION *sp, const char *key)
{
	return (istgt_find_cf_nitem(sp, key, 0));
}

static void
istgt_append_cf_item(CF_SECTION *sp, CF_ITEM *ip)
{
	CF_ITEM *last;

	if (sp == NULL)
		return;
	if (sp->item == NULL) {
		sp->item = ip;
		return;
	}
	for (last = sp->item; last->next != NULL; last = last->next)
		;
	last->next = ip;
}

static void
istgt_append_cf_value(CF_ITEM *ip, CF_VALUE *vp)
{
	CF_VALUE *last;

	if (ip == NULL)
		return;
	if (ip->val == NULL) {
		ip->val = vp;
		return;
	}
	for (last = ip->val; last->next != NULL; last = last->next)
		;
	last->next = vp;
}

static void
istgt_set_cf_section_type(CF_SECTION *sp)
{
	static struct cfst_table_t {
		const char *name;
		CF_SECTION_TYPE type;
	} cfst_table[] = {
		{ "Global", ST_GLOBAL },
		{ "UnitControl", ST_UNITCONTROL },
		{ "PortalGroup", ST_PORTALGROUP },
		{ "InitiatorGroup", ST_INITIATORGROUP },
		{ "LogicalUnit", ST_LOGICAL_UNIT },
		{ "AuthGroup", ST_AUTHGROUP },
		{ NULL, ST_INVALID },
	};
	int i;

	if (sp == NULL || sp->name == NULL)
		return;
	for (i = 0; cfst_table[i].name != NULL; i++) {
		if (sp->name[0] == cfst_table[i].name[0] &&
				strncasecmp(sp->name, cfst_table[i].name,
				strlen(cfst_table[i].name)) == 0) {
			sp->type = cfst_table[i].type;
			return;
		}
	}
	sp->type = ST_NONE;
}

static int
parse_line(CONFIG *cp, char *lp)
{
	CF_SECTION *sp;
	CF_ITEM *ip;
	CF_VALUE *vp;
	char *arg;
	char *key;
	char *val;
	char *p;
	int num;

	arg = trim_string(lp);
	if (arg[0] == '[') {
		/* section */
		arg++;
		key = strsepq(&arg, "]");
		if (key == NULL || arg != NULL) {
			fprintf(stderr, "broken section\n");
			return (-1);
		}
		/* determine section number */
		for (p = key; *p != '\0' && !isdigit((int)*p); p++)
			;
		if (*p != '\0') {
			num = (int)strtol(p, NULL, 10);
		} else {
			num = 0;
		}

		sp = istgt_find_cf_section(cp, key);
		if (sp == NULL) {
			sp = istgt_allocate_cf_section();
			istgt_append_cf_section(cp, sp);
		}
		cp->current_section = sp;
		sp->name = xstrdup(key);
		sp->num = num;
		istgt_set_cf_section_type(sp);
	} else {
		/* parameters */
		sp = cp->current_section;
		if (sp == NULL) {
			fprintf(stderr, "unknown section\n");
			return (-1);
		}
		key = strsepq(&arg, CF_DELIM);
		if (key == NULL) {
			fprintf(stderr, "broken key\n");
			return (-1);
		}

		ip = istgt_allocate_cf_item();
		istgt_append_cf_item(sp, ip);
		ip->key = xstrdup(key);
		ip->val = NULL;
		if (arg != NULL) {
			/* key has value(s) */
			while (arg != NULL) {
				val = strsepq(&arg, CF_DELIM);
				vp = istgt_allocate_cf_value();
				istgt_append_cf_value(ip, vp);
				vp->value = xstrdup(val);
			}
		}
	}

	return (0);
}

static char *
fgets_line(FILE *fp)
{
#define	MAX_TMPBUFX 8192
	char *dst, *p, *tmp;
	size_t total, len;

	dst = p = xmalloc(MAX_TMPBUFX);
	dst[0] = '\0';
	total = 0;

	while (fgets(p, MAX_TMPBUFX, fp) != NULL) {
		len = strlen(p);
		total += len;
		if (len + 1 < MAX_TMPBUFX || dst[total - 1] == '\n') {
			// return realloc(dst, total + 1);
			tmp = xmalloc(total + 5);
			bcopy(dst, tmp, total);
			tmp[total] = '\0';
			xfree(dst);
			dst = tmp;
			tmp = NULL;
			return (dst);
		}
		tmp = xmalloc(total + MAX_TMPBUFX);
		bcopy(dst, tmp, total);
		tmp[total] = '\0';
		xfree(dst);
		dst = tmp;
		tmp = NULL;
		// dst = realloc (dst, total + MAX_TMPBUFX);
		p = dst + total;
	}

	if (feof(fp) && total != 0) {
		tmp = xmalloc(total + 5);
		bcopy(dst, tmp, total);
		tmp[total] = '\n';
		tmp[total + 1] = '\0';
		xfree(dst);
		dst = tmp;
		tmp = NULL;
		// dst = realloc(dst, total + 2);
		// dst[total] = '\n';
		// dst[total + 1] = '\0';
		return (dst);
	}

	xfree(dst);

	return (NULL);
}

int
istgt_read_config(CONFIG *cp, const char *file)
{
	FILE *fp;
	char *lp, *p;
	char *lp2, *q;
	int line;
	int n, n2;

	if (file == NULL || file[0] == '\0')
		return (-1);
	fp = fopen(file, "r");
	if (fp == NULL) {
		fprintf(stderr, "open error: %s\n", file);
		return (-1);
	}
	cp->file = xstrdup(file);

	line = 1;
	while ((lp = fgets_line(fp)) != NULL) {
		/* skip spaces */
		for (p = lp; *p != '\0' && isspace((int)*p); p++)
			;
		/* skip comment, empty line */
		if (p[0] == '#' || p[0] == '\0')
			goto next_line;

		/* concatenate line end with '\' */
		n = strlen(p);
		while (n > 2 && p[n - 1] == '\n' && p[n - 2] == '\\') {
			n -= 2;
			lp2 = fgets_line(fp);
			if (lp2 == NULL)
				break;
			line++;
			n2 = strlen(lp2);
			q = xmalloc(n + n2 + 1);
			memcpy(q, p, n);
			memcpy(q + n, lp2, n2);
			q[n + n2] = '\0';
			xfree(lp2);
			xfree(lp);
			p = lp = q;
			n += n2;
		}

		/* parse one line */
		if (parse_line(cp, p) < 0) {
			fprintf(stderr, "parse error at line %d of %s\n",
				line, cp->file);
		}
	next_line:
		line++;
		xfree(lp);
	}

	fclose(fp);
	return (0);
}

int
istgt_print_config(CONFIG *cp)
{
	CF_SECTION *sp;
	CF_ITEM *ip;
	CF_VALUE *vp;

	if (cp == NULL)
		return (-1);

	/* empty config? */
	sp = cp->section;
	if (sp == NULL)
		return (0);

	while (sp != NULL) {
		printf("Section: %s\n", sp->name);
		ip = sp->item;
		while (ip != NULL) {
			printf("  Item: %s ", ip->key);
			vp = ip->val;
			while (vp != NULL) {
				printf("Val: %s ", vp->value);
				vp = vp->next;
			}
			printf("\n");
			ip = ip->next;
		}
		sp = sp->next;
	}

	return (0);
}
