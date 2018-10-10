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

#ifndef ISTGT_QUEUE_H
#define	ISTGT_QUEUE_H

#include <stddef.h>

typedef struct istgt_queue_t {
	struct istgt_queue_t *prev;
	struct istgt_queue_t *next;
	void *elem;
	int num;
} ISTGT_QUEUE;
typedef ISTGT_QUEUE *ISTGT_QUEUE_Ptr;

int istgt_queue_init(ISTGT_QUEUE_Ptr head);
#define	istgt_queue_destroy(head) istgt_queue_destroyi(head, __LINE__)
void istgt_queue_destroyi(ISTGT_QUEUE_Ptr head, uint16_t line);
int istgt_queue_count(ISTGT_QUEUE_Ptr head);

#define	istgt_queue_enqueue(head, elem)	\
	istgt_queue_enqueuei(head, elem, __LINE__)
#define	istgt_queue_enqueue_after(head, current_ptr,  elem)	\
	istgt_queue_enqueue_afteri(head, current_ptr, elem, __LINE__)
#define	istgt_queue_dequeue(head)  istgt_queue_dequeuei(head, __LINE__)
#define	istgt_queue_dequeue_middle(head, ptr)	\
	istgt_queue_dequeue_middlei(head, ptr, __LINE__)
#define	istgt_queue_enqueue_first(head, elem)	\
	istgt_queue_enqueue_firsti(head, elem, __LINE__)

ISTGT_QUEUE_Ptr istgt_get_next_qptr(void *cookie);
ISTGT_QUEUE_Ptr istgt_get_prev_qptr(void *cookie);
ISTGT_QUEUE_Ptr istgt_queue_enqueuei(ISTGT_QUEUE_Ptr head,
					void *elem,
					uint16_t line);
ISTGT_QUEUE_Ptr istgt_queue_enqueue_afteri(ISTGT_QUEUE_Ptr head,
						ISTGT_QUEUE_Ptr current_ptr,
						void *elem, uint16_t line);
void *istgt_queue_dequeuei(ISTGT_QUEUE_Ptr head, uint16_t line);
void *istgt_queue_dequeue_middlei(ISTGT_QUEUE_Ptr head,
					ISTGT_QUEUE_Ptr complete_queue_ptr,
					uint16_t line);
ISTGT_QUEUE_Ptr istgt_queue_enqueue_firsti(ISTGT_QUEUE_Ptr head,
						void *elem,
						uint16_t line);

void *istgt_queue_first(ISTGT_QUEUE_Ptr head);
void *istgt_queue_last(ISTGT_QUEUE_Ptr head, void *elem);
void *istgt_queue_prev(ISTGT_QUEUE_Ptr head, void *elem);
void * istgt_queue_walk(ISTGT_QUEUE_Ptr head, void ** cookie);
void * istgt_queue_reverse_walk(ISTGT_QUEUE_Ptr head, void ** cookie);

#endif /* ISTGT_QUEUE_H */

#ifdef __linux__
#define	TAILQ_FOREACH_SAFE(var, head, field, tvar)                      \
	for ((var) = TAILQ_FIRST((head));                               \
		(var) && ((tvar) = TAILQ_NEXT((var), field), 1);            \
		(var) = (tvar))
#endif
