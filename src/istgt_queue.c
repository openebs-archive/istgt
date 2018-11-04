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

#include <stdlib.h>
#include <string.h>

#include "istgt_misc.h"
#include "istgt_queue.h"
#include "istgt_lu.h"
#include "istgt_log.h"

/* Queues are implemented as Circular Doubly Linked Lists */
int
istgt_queue_init(ISTGT_QUEUE_Ptr head)
{
	if (head == NULL)
		return (-1);
	head->prev = head;
	head->next = head;
	head->elem = NULL;
	head->num = 0;
	return (0);
}

void
istgt_queue_destroyi(ISTGT_QUEUE_Ptr head, uint16_t line)
{
	ISTGT_QUEUE_Ptr qp;
	ISTGT_QUEUE_Ptr next;

	if (head == NULL)
		return;
	for (qp = head->next; qp != NULL && qp != head; qp = next) {
		next = qp->next;
		xfreei(qp, line);
	}
	head->next = head;
	head->prev = head;
}

int
istgt_queue_count(ISTGT_QUEUE_Ptr head)
{
#if 0
	ISTGT_QUEUE_Ptr qp;
	int num;

	if (head == NULL)
		return (0);
	num = 0;
	for (qp = head->next; qp != NULL && qp != head; qp = qp->next) {
		num++;
	}
	return (num);
#else
	if (head == NULL)
		return (0);
	return (head->num);
#endif
}

ISTGT_QUEUE_Ptr
istgt_queue_enqueuei(ISTGT_QUEUE_Ptr head, void *elem, uint16_t line)
{
	ISTGT_QUEUE_Ptr qp;
	ISTGT_QUEUE_Ptr tail;

	if (head == NULL)
		return (NULL);
	qp = xmalloci(sizeof (*qp), line);
	qp->num = head->num;
	qp->elem = elem;

	tail = head->prev;
	if (tail == NULL) {
		head->next = qp;
		head->prev = qp;
		qp->next = head;
		qp->prev = head;
	} else {
		tail->next = qp;
		head->prev = qp;
		qp->next = head;
		qp->prev = tail;
	}
	head->num++;
	return (qp);
}

ISTGT_QUEUE_Ptr
istgt_queue_enqueue_afteri(ISTGT_QUEUE_Ptr head, ISTGT_QUEUE_Ptr current_ptr,
    void *elem, uint16_t line)
{
	ISTGT_QUEUE_Ptr qp, next_ptr;

	if (head == NULL)
		return (NULL);
	if (current_ptr == NULL)
		return (NULL);
	qp = xmalloci(sizeof (*qp), line);
	qp->num = current_ptr->num + 1;
	qp->elem = elem;

	next_ptr = current_ptr->next;

	if (next_ptr == NULL || next_ptr == current_ptr) {
		current_ptr->next = qp;
		current_ptr->prev = qp;
		qp->next = current_ptr;
		qp->prev = current_ptr;
	} else {
		current_ptr->next = qp;
		next_ptr->prev = qp;
		qp->next = next_ptr;
		qp->prev = current_ptr;
	}
	head->num++;
	return (qp);
}

void *
istgt_queue_dequeuei(ISTGT_QUEUE_Ptr head, uint16_t line)
{
	ISTGT_QUEUE_Ptr first;
	ISTGT_QUEUE_Ptr next;
	void *elem;

	if (head == NULL)
		return (NULL);
	first = head->next;
	if (first == NULL || first == head) {
		return (NULL);
	} else {
		elem = first->elem;
		next = first->next;
		xfreei(first, line);
		if (next == NULL) {
			head->next = NULL;
			head->prev = NULL;
		} else {
			head->next = next;
			next->prev = head;
		}
	}
	head->num--;
	return (elem);
}

void *
istgt_queue_dequeue_middlei(ISTGT_QUEUE_Ptr head,
    ISTGT_QUEUE_Ptr complete_queue_ptr, uint16_t line)
{
	ISTGT_QUEUE_Ptr prev = NULL;
	ISTGT_QUEUE_Ptr next = NULL;
	if (head == NULL || complete_queue_ptr == NULL ||
	    complete_queue_ptr == head)
		return (NULL);

	prev = complete_queue_ptr->prev;
	next = complete_queue_ptr->next;

	prev->next = next;
	if (next == NULL)
		head->prev = NULL;
	else
		next->prev = prev;
	xfreei(complete_queue_ptr, line);
	head->num--;
	return (NULL);
}

void *
istgt_queue_first(ISTGT_QUEUE_Ptr head)
{
	ISTGT_QUEUE_Ptr first;
	void *elem;

	if (head == NULL)
		return (NULL);
	first = head->next;
	if (first == NULL || first == head) {
		return (NULL);
	} else {
		elem = first->elem;
	}
	return (elem);
}
ISTGT_QUEUE_Ptr
istgt_queue_enqueue_firsti(ISTGT_QUEUE_Ptr head, void *elem, uint16_t line)
{
	ISTGT_QUEUE_Ptr qp;
	ISTGT_QUEUE_Ptr first;

	if (head == NULL)
		return (NULL);
	qp = xmalloci(sizeof (*qp), line);
	qp->num = head->num;
	qp->elem = elem;

	first = head->next;
	if (first == NULL || first == head) {
		head->next = qp;
		head->prev = qp;
		qp->next = head;
		qp->prev = head;
	} else {
		head->next = qp;
		first->prev = qp;
		qp->next = first;
		qp->prev = head;
	}
	head->num++;
	return (qp);
}

void *
istgt_queue_last(ISTGT_QUEUE_Ptr head, void *elem)
{
	ISTGT_QUEUE_Ptr last;
	if (head == NULL)
		return (NULL);

	last = head->prev;
	if (last == NULL || last == head)
		return (NULL);
	else
		elem = last->elem;
	return (elem);
}

void *
istgt_queue_prev(ISTGT_QUEUE_Ptr head, void * elem)
{
	ISTGT_QUEUE_Ptr qp;
	ISTGT_QUEUE_Ptr first;

	if (head == NULL || elem == NULL)
		return (NULL);

	first = head->next;

	if (first == NULL || first == head)
		return (NULL);

	for (qp = head->next; qp != NULL && qp != head; qp = qp->next) {
		if (qp == elem)
			return (qp->prev);
	}
	return (NULL);
}

ISTGT_QUEUE_Ptr
istgt_get_prev_qptr(void *cookie)
{
	ISTGT_QUEUE_Ptr cp;
	if (cookie == NULL)
		return (NULL);
	cp = (ISTGT_QUEUE_Ptr)cookie;
	return (cp->prev);
}

ISTGT_QUEUE_Ptr
istgt_get_next_qptr(void *cookie)
{
	ISTGT_QUEUE_Ptr cp;
	if (cookie == NULL)
		return (NULL);
	cp = (ISTGT_QUEUE_Ptr)cookie;
	return (cp->next);
}

/*
 * Queue walk when we do not expect the queue to change
 * Uses a cookie at the caller to maintain the position of
 * the next element.
 *
 * Usage:
 * void *cookie=NULL;
 * while(elem=istgt_queue_walk(some_queue,&cookie)) {
 *       do_something(elem);
 * }
 */
void *
istgt_queue_walk(ISTGT_QUEUE_Ptr head, void ** cookie)
{
	ISTGT_QUEUE_Ptr cp;

	/* Empty queue */
	if (head == NULL) {
		*cookie = NULL;
		return (NULL);
	}

	if (head->next == NULL) {
		*cookie = NULL;
		return (NULL);
	}

	if (*cookie == head) {
		return (NULL);
	}

	if (head == head->next) {
		*cookie = NULL;
		return (NULL);
	}

	if (*cookie == NULL) {
		cp = head->next;
	} else {
		cp = (ISTGT_QUEUE_Ptr)*cookie;
	}

	*cookie = cp->next;
	if (unlikely(*cookie == NULL))
		ISTGT_ERRLOG("cookie is NULL!!!!!!\n");

	return (*cookie == NULL ? NULL :cp->elem);
}

void *
istgt_queue_reverse_walk(ISTGT_QUEUE_Ptr head, void ** cookie)
{
	ISTGT_QUEUE_Ptr cp;

	/* Empty queue */
	if (head == NULL) {
		*cookie = NULL;
		return (NULL);
	}

	if (head->next == NULL) {
		*cookie = NULL;
		return (NULL);
	}

	if (*cookie == head) {
		return (NULL);
	}

	if (head == head->next) {
		*cookie = NULL;
		return (NULL);
	}

	if (*cookie == NULL) {
		cp = head->prev;
	} else {
		cp = (ISTGT_QUEUE_Ptr)*cookie;
	}

	*cookie = cp->prev;
	if (unlikely(*cookie == NULL))
		ISTGT_ERRLOG("cookie is NULL!!!!!!\n");

	return (*cookie == NULL ? NULL :cp->elem);
}
