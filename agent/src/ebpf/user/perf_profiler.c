/*
 * Copyright (c) 2022 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <bcc/perf_reader.h>
#include "config.h"
#include "common.h"
#include "mem.h"
#include "log.h"
#include "types.h"
#include "vec.h"
#include "tracer.h"
#include "perf_profiler.h"
#include "elf.h"
#include "load.h"
#include "../kernel/include/perf_profiler.h"
#include "perf_reader.h"
#include "bihash_8_8.h"
#include "stringifier.h"
#include "table.h"

#include "perf_profiler_bpf_common.c"

#define LOG_CP_TAG	"[CP] "
#define CP_TRACER_NAME	"continuous_profiler"
#define CP_PERF_PG_NUM	16

extern int major, minor;
extern char linux_release[128];
extern __thread uword thread_index;

struct stack_trace_key_t *raw_stack_data;
static u64 stack_trace_lost;
static struct bpf_tracer *profiler_tracer;

/*
 * The iteration count causes BPF to switch buffers with each iteration.
 */
static u64 transfer_count;
static volatile u64 reader_exit;
static void print_profiler_status(struct bpf_tracer *t, u64 iter_count,
				  stack_str_hash_t *h,
				  stack_trace_msg_hash_t *msg_h);
static void print_cp_status(struct bpf_tracer *t);

static void reader_lost_cb(void *t, u64 lost)
{
	struct bpf_tracer *tracer = (struct bpf_tracer *)t;
	atomic64_add(&tracer->lost, lost);
}

static void reader_raw_cb(void *t, void *raw, int raw_size)
{
	struct stack_trace_key_t *v;
	struct bpf_tracer *tracer = (struct bpf_tracer *)t;
	v = (struct stack_trace_key_t *)raw;

	int ret = VEC_OK;
	vec_add1(raw_stack_data, *v, ret);
	if (ret != VEC_OK) {
		ebpf_warning("vec add failed\n");
	}

	atomic64_add(&tracer->recv, 1);
}

static int relase_profiler(struct bpf_tracer *tracer)
{
	tracer_reader_lock(tracer);

	/* detach perf event */
	tracer_hooks_detach(tracer);

	/* free all readers */
	free_all_readers(tracer);

	/* release object */
	release_object(tracer->obj);
	reader_exit = 1;
	CLIB_MEMORY_STORE_BARRIER();
	tracer_reader_unlock(tracer);

	return ETR_OK;
}

static void set_msg_kvp(stack_trace_msg_kv_t * kvp,
			struct stack_trace_key_t *v,
			u64 stime,
			void *msg_value)
{
	kvp->k.pid = (u64) v->pid;
	kvp->k.stime = stime;
	kvp->k.u_stack_id = (u32) v->userstack;
	kvp->k.k_stack_id = (u32) v->kernstack;
	kvp->msg_ptr = pointer_to_uword(msg_value);
}

static int init_stack_trace_msg_hash(stack_trace_msg_hash_t *h,
				     const char *name)
{
	memset(h, 0, sizeof(*h));
	u32 nbuckets = STACK_TRACE_MSG_HASH_BUCKETS_NUM;
	u64 hash_memory_size = STACK_TRACE_MSG_HASH_MEM_SZ;
	return stack_trace_msg_hash_init(h, (char *)name,
					 nbuckets, hash_memory_size);
}

static int push_and_free_msg_kvp_cb(stack_trace_msg_hash_kv *kv, void *ctx)
{
	stack_trace_msg_kv_t *msg_kv = (stack_trace_msg_kv_t *)kv;
	if (msg_kv->msg_ptr != 0) {
		stack_trace_msg_t *msg = (stack_trace_msg_t *)msg_kv->msg_ptr;
#ifdef CP_DEBUG
		ebpf_debug("tiemstamp %lu pid %u stime %lu u_stack_id %lu k_statck_id"
			   "%lu cpu %u count %u comm %s datalen %u data %s\n",
			   msg->time_stamp, msg->pid, msg->stime, msg->u_stack_id,
			   msg->k_stack_id, msg->cpu, msg->count, msg->comm,
			   msg->data_len, msg->data);
#endif
		tracer_callback_t fun = profiler_tracer->process_fn;
		/*
 		 * Execute callback function to hand over the data to the
 		 * higher level for processing. The higher level will se-
 		 * nd the data to the server for storage as required.
 		 */ 
		fun(msg);
		clib_mem_free((void *)msg);
		(*(u64 *) ctx)++;
	}

	return BIHASH_WALK_CONTINUE;
}

static void push_and_release_stack_trace_msg(stack_trace_msg_hash_t *h)
{
	ASSERT(profiler_tracer != NULL);

	u64 elem_count = 0;
	stack_trace_msg_hash_foreach_key_value_pair(h, push_and_free_msg_kvp_cb,
						    (void *)&elem_count);
	stack_trace_msg_hash_free(h);
	//ebpf_info("stack_trace_msg hashmap clear %lu elems.\n", elem_count);
}

static void aggregate_stack_traces(struct bpf_tracer *t,
				   const char *stack_map_name,
				   u64 sample_cnt_idx)
{
	u32 count = 0;
	struct stack_trace_key_t *v;

	/*
	 * Why use stack_str_hash?
	 *
	 * When the stringizer encounters a stack-ID for the first time in
	 * the stack trace table, it clears it. If a stack-ID is reused by
	 * different stack trace keys, the stringizer returns its memoized
	 * stack trace string. Since stack IDs are unstable between profile
	 * iterations, we create and destroy the stringizer in each profile
	 * iteration.
	 */
	stack_str_hash_t stack_str_hash;
	if (init_stack_str_hash(&stack_str_hash, "profile_stack_str")) {
		ebpf_warning("init_stack_str_hash() failed.\n");
		return;
	}

	/*
	 * During each transmission iteration, we have a hashmap structure in
	 * place for the following purposes:
	 *
	 * 1 Pushing the data of this iteration to the higher-level processing.
	 * 2 Performing data statistics based on the stack trace data, using the
	 *   combination of "pid + pid_start_time + k_stack_id + u_stack_id" as
	 *   the key.
	 *
	 * Here is the key-value pair structure of the hashmap:
	 * ```
	 * typedef struct {
	 *         struct {
	 *         u64 pid;
	 *         u64 stime;
	 *         u32 u_stack_id;
	 *         u32 k_stack_id;
	 *         } k;
	 *         u64 msg_ptr; Store perf profiler data
	 * } stack_trace_msg_kv_t;
	 * ```
	 *
	 * This is the final form of the data. If the current stack trace message
	 * is a match, we only need to increment the count field in the correspon-
	 * ding value, thus avoiding duplicate parsing.
	 */
	stack_trace_msg_hash_t msg_hash;
	if (init_stack_trace_msg_hash(&msg_hash, "stack_trace_msg")) {
		ebpf_warning("init_stack_trace_msg_hash() failed.\n");
		return;
	}

	vec_foreach(v, raw_stack_data) {
		if (v == NULL)
			break;

		/* -EEXIST: Hash bucket collision in the stack trace table */
		if (v->kernstack == -EEXIST)
			stack_trace_lost++;

		if (v->userstack == -EEXIST)
			stack_trace_lost++;

		count++;

		/*
		 * Firstly, search the stack-trace-msg hash to see if the
		 * stack trace messages has already been stored. 
		 */
		stack_trace_msg_kv_t kv; // stack_trace_msg_hash_kv
		set_msg_kvp(&kv, v, get_pid_stime(v->pid), (void *)0);
		if (stack_trace_msg_hash_search(&msg_hash, (stack_trace_msg_hash_kv *)&kv,
						(stack_trace_msg_hash_kv *)&kv) == 0) {
			__sync_fetch_and_add(&msg_hash.hit_stack_msg_hash_count, 1);
			((stack_trace_msg_t *)kv.msg_ptr)->count++;

			continue;
		}

		/*
		 * Folded stack trace string and generate stack trace messages.
		 *
		 * Folded stack trace string (taken from a performance profiler test):
		 * main;xxx();yyy()
		 * It is a list of symbols corresponding to addresses in the underlying
		 * stack trace, separated by ';'.
		 */

		stack_trace_msg_t *msg =
			resolve_and_gen_stack_trace_msg(t, v, stack_map_name,
							&stack_str_hash);
		if (msg) {
			stack_trace_msg_kv_t msg_kvp;
			set_msg_kvp(&msg_kvp, v, msg->stime, (void *)msg);
			if (stack_trace_msg_hash_add_del(&msg_hash,
							 (stack_trace_msg_hash_kv *)&msg_kvp,
							 1 /* is_add */ )) {
				ebpf_warning("stack_trace_msg_hash_add_del() failed.\n");
			}
		}
	}

	/*
	 * Clear any kernel stack-ids, that were potentially not already
	 * cleared, out of the stack traces table.
	 */

	/* Now that we've consumed the data, reset the sample count in BPF. */
	u64 sample_cnt_val = 0;
	bpf_table_set_value(t, MAP_PROFILER_STATE_MAP,
			    sample_cnt_idx, &sample_cnt_val);

	vec_free(raw_stack_data);

	print_profiler_status(t, count, &stack_str_hash, &msg_hash);

	/* free all elems and free stack_str_hash */
	release_stack_strs(&stack_str_hash);

	/* Push messages and free stack_trace_msg_hash */
	push_and_release_stack_trace_msg(&msg_hash);
}

static void process_bpf_stacktraces(struct bpf_tracer *t,
				    struct bpf_perf_reader *r_a,
				    struct bpf_perf_reader *r_b)
{
	struct bpf_perf_reader *r;
	const char *stack_map_name;
	bool using_map_set_a = (transfer_count % 2 == 0);
	r = using_map_set_a ? r_a : r_b;
	stack_map_name = using_map_set_a ? MAP_STACK_A_NAME : MAP_STACK_B_NAME;
	const u64 sample_count_idx =
	    using_map_set_a ? SAMPLE_CNT_A_IDX : SAMPLE_CNT_B_IDX;

	struct pollfd pfds[r->readers_count];
	bool has_event = reader_poll_wait(r, pfds);

	transfer_count++;
	/* update map MAP_PROFILER_STATE_MAP */
	if (bpf_table_set_value(t, MAP_PROFILER_STATE_MAP,
				TRANSFER_CNT_IDX, &transfer_count) == false) {
		transfer_count--;
	}

	if (has_event) {
		/* 
		 * If there is data, the reader's callback
		 * function will be called.
		 */
		reader_event_read(r, pfds);
	}

	/*
	 * After the reader completes data reading, the work of
	 * data aggregation will be blocked if there is no data.
	 */
	aggregate_stack_traces(t, stack_map_name, sample_count_idx);
}

static void cp_reader_work(void *arg)
{
	thread_index = THREAD_PROFILER_READER_IDX;
	struct bpf_tracer *t = (struct bpf_tracer *)arg;
	struct bpf_perf_reader *reader_a, *reader_b;
	reader_a = &t->readers[0];
	reader_b = &t->readers[1];

	for (;;) {
		tracer_reader_lock(t);
		if (reader_exit)
			goto exit;
		process_bpf_stacktraces(t, reader_a, reader_b);
		tracer_reader_unlock(t);
	}

exit:
	tracer_reader_unlock(t);

	/* clear thread */
	t->perf_worker[0] = 0;
	ebpf_info(LOG_CP_TAG "perf profiler reader-thread exit.\n");

	/* resouce share release */
	release_symbol_caches();

	print_cp_status(t);

	pthread_exit(NULL);
}

static int create_profiler(struct bpf_tracer *tracer)
{
	int ret;

	profiler_tracer = tracer;

	/* load ebpf perf profiler */
	if (tracer_bpf_load(tracer))
		return ETR_LOAD;
	/*
	 * create reader for read eBPF-profiler data.
	 * To implement eBPF perf-profiler double buffering output,
	 * it is necessary to create two readers to correspond to
	 * the double buffering structure design.
	 */
	struct bpf_perf_reader *reader_a, *reader_b;
	reader_a = create_perf_buffer_reader(tracer,
					     MAP_PERF_PROFILER_BUF_A_NAME,
					     reader_raw_cb,
					     reader_lost_cb,
					     PROFILE_PG_CNT_DEF, -1);
	if (reader_a == NULL)
		return ETR_NORESOURCE;

	reader_b = create_perf_buffer_reader(tracer,
					     MAP_PERF_PROFILER_BUF_B_NAME,
					     reader_raw_cb,
					     reader_lost_cb,
					     PROFILE_PG_CNT_DEF, -1);
	if (reader_b == NULL) {
		free_perf_buffer_reader(reader_a);
		return ETR_NORESOURCE;
	}

	/* attach perf event */
	tracer_hooks_attach(tracer);

	/* syms_cache_hash maps from pid to BCC symbol cache.
	 * Use of void* is inherited from the BCC library. */
	create_and_init_symbolizer_caches();

	/*
	 * Start a new thread to execute the data
	 * reading of perf buffer.
	 */
	ret = enable_tracer_reader_work("cp_reader", tracer,
					(void *)&cp_reader_work);

	if (ret) {
		relase_profiler(tracer);
		return ETR_INVAL;
	}

	return ETR_OK;
}

int stop_continuous_profiler(void)
{
	release_bpf_tracer(CP_TRACER_NAME);
	profiler_tracer = NULL;
	return (0);
}

static void print_cp_status(struct bpf_tracer *t)
{
	u64 alloc_b, free_b;
	get_mem_stat(&alloc_b, &free_b);
	ebpf_info("\n\n----------------------------\nrecv envent:\t%lu\n"
		  "kern_lost:\t%lu\nstack_trace_lost:\t%lu\n"
		  "ransfer_count:\t%lu iter_count_avg:\t%.2lf\nall"
		  "oc_b:\t%lu bytes free_b:\t%lu bytes use:\t%lu bytes\n"
		  "----------------------------\n\n",
		  atomic64_read(&t->recv), atomic64_read(&t->lost),
		  stack_trace_lost, transfer_count,
		  ((double)atomic64_read(&t->recv) / (double)transfer_count),
		  alloc_b, free_b, alloc_b - free_b);
}

static void print_profiler_status(struct bpf_tracer *t, u64 iter_count,
				  stack_str_hash_t *h,
				  stack_trace_msg_hash_t *msg_h)
{
	u64 alloc_b, free_b;
	get_mem_stat(&alloc_b, &free_b);
	ebpf_debug("\n\n----------------------------\nrecv envent:\t%lu\n"
		   "kern_lost:\t%lu\nstack_trace_lost:\t%lu\n"
		   "ransfer_count:\t%lu iter_count:\t%lu\nall"
		   "oc_b:\t%lu bytes free_b:\t%lu bytes use:\t%lu bytes\n"
		   "stack_str_hash.hit_count %lu\nstack_trace_msg_hash hit %lu\n",
		   atomic64_read(&t->recv), atomic64_read(&t->lost),
		   stack_trace_lost, transfer_count, iter_count,
		   alloc_b, free_b, alloc_b - free_b,
		   h->hit_stack_str_hash_count, msg_h->hit_stack_msg_hash_count);
}

/*
 * start continuous profiler
 * @freq sample frequency, Hertz. (e.g. 99 profile stack traces at 99 Hertz)
 * @report_period How often is the data reported.
 * @callback Profile data processing callback interface
 * @returns 0 on success, < 0 on error
 */
int start_continuous_profiler(int freq,
			      int report_period, tracer_callback_t callback)
{
	char bpf_load_buffer_name[NAME_LEN];
	void *bpf_bin_buffer;
	uword buffer_sz;

	// REQUIRES: Linux 4.9+ (BPF_PROG_TYPE_PERF_EVENT support).
	if (check_kernel_version(4, 9) != 0) {
		ebpf_warning
		    (LOG_CP_TAG
		     "Currnet linux %d.%d, not support, require Linux 4.9+\n",
		     major, minor);

		return (-1);
	}

	snprintf(bpf_load_buffer_name, NAME_LEN, "continuous_profiler");
	bpf_bin_buffer = (void *)perf_profiler_common_ebpf_data;
	buffer_sz = sizeof(perf_profiler_common_ebpf_data);

	struct bpf_tracer *tracer =
	    setup_bpf_tracer(CP_TRACER_NAME, bpf_load_buffer_name,
			     bpf_bin_buffer, buffer_sz, NULL, 0,
			     relase_profiler, create_profiler,
			     (void *)callback, freq);
	if (tracer == NULL)
		return (-1);

	tracer->state = TRACER_RUNNING;
	return (0);
}
