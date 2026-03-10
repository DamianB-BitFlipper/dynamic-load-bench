#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef __linux__

#include <sched.h>

typedef struct Node {
    uint32_t next;
    uint8_t padding[60];
} Node;

_Static_assert(sizeof(Node) == 64, "Node must occupy one cache line");

typedef struct Config {
    int cpu_count;
    int streamer_count;
    int stream_only;
    uint64_t warmup_ms;
    uint64_t chase_steps;
    uint64_t stream_iterations;
    size_t stream_array_bytes;
    size_t chase_bytes;
    double scalar;
    const char *output_path;
} Config;

typedef struct StreamWorker {
    int thread_index;
    int cpu_id;
    size_t element_count;
    double *a;
    double *b;
    double *c;
    double checksum;
    uint64_t bytes_moved;
    double elapsed_sec;
} StreamWorker;

typedef struct LatencyWorker {
    int cpu_id;
    const Node *nodes;
    uint32_t start_index;
    uint64_t steps;
    uint32_t final_index;
    uint64_t elapsed_ns;
} LatencyWorker;

typedef struct TrialResult {
    int active_streamers;
    uint32_t final_index;
    uint64_t latency_ns;
    double total_bandwidth_gib;
} TrialResult;

typedef struct StreamOnlyResult {
    int active_streamers;
    uint64_t iterations;
    uint64_t total_bytes;
    uint64_t elapsed_ns;
    double total_bandwidth_gib;
} StreamOnlyResult;

typedef struct SharedState {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int total_threads;
    int ready_threads;
    int start_streamers;
    int fixed_iterations_mode;
    atomic_int start_latency;
    atomic_int stop_streamers;
    atomic_int abort_run;
    uint64_t stream_iterations;
    double scalar;
} SharedState;

typedef struct StreamThreadArgs {
    SharedState *shared;
    StreamWorker *worker;
} StreamThreadArgs;

typedef struct LatencyThreadArgs {
    SharedState *shared;
    LatencyWorker *worker;
} LatencyThreadArgs;

static uint64_t monotonic_ns(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }

    return (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
}

static void sleep_ms(uint64_t ms) {
    struct timespec ts;

    ts.tv_sec = (time_t) (ms / 1000ull);
    ts.tv_nsec = (long) ((ms % 1000ull) * 1000000ull);

    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static int pin_thread_to_cpu(int cpu_id) {
    cpu_set_t set;

    CPU_ZERO(&set);
    CPU_SET(cpu_id, &set);

    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

static void *aligned_alloc_or_die(size_t alignment, size_t bytes) {
    void *ptr = NULL;
    int rc;

    rc = posix_memalign(&ptr, alignment, bytes);
    if (rc != 0) {
        errno = rc;
        perror("posix_memalign");
        exit(EXIT_FAILURE);
    }

    return ptr;
}

static uint64_t parse_u64(const char *text, const char *flag) {
    char *end = NULL;
    unsigned long long value;

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        fprintf(stderr, "invalid value for %s: %s\n", flag, text);
        exit(EXIT_FAILURE);
    }

    return (uint64_t) value;
}

static size_t mib_to_bytes(uint64_t mib, const char *flag) {
    if (mib == 0 || mib > (UINT64_MAX / (1024ull * 1024ull))) {
        fprintf(stderr, "invalid MiB value for %s: %" PRIu64 "\n", flag, mib);
        exit(EXIT_FAILURE);
    }

    return (size_t) (mib * 1024ull * 1024ull);
}

static uint64_t clamp_u64(uint64_t value, uint64_t min_value, uint64_t max_value) {
    if (value < min_value) {
        return min_value;
    }
    if (value > max_value) {
        return max_value;
    }
    return value;
}

static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;

    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static void build_random_cycle(Node *nodes, size_t node_count) {
    uint32_t *order;
    uint64_t seed;
    size_t i;

    order = malloc(node_count * sizeof(*order));
    if (order == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    for (i = 0; i < node_count; ++i) {
        order[i] = (uint32_t) i;
    }

    seed = ((uint64_t) time(NULL) << 32) ^ (uint64_t) getpid() ^ (uint64_t) node_count;
    if (seed == 0) {
        seed = 0x9e3779b97f4a7c15ull;
    }

    for (i = node_count - 1; i > 0; --i) {
        size_t j = (size_t) (xorshift64(&seed) % (i + 1));
        uint32_t tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }

    for (i = 0; i + 1 < node_count; ++i) {
        nodes[order[i]].next = order[i + 1];
    }
    nodes[order[node_count - 1]].next = order[0];

    free(order);
}

static uint32_t run_pointer_chase(const Node *nodes, uint32_t start_index, uint64_t steps) {
    uint32_t index = start_index;
    uint64_t i;

    for (i = 0; i < steps; ++i) {
        index = nodes[index].next;
    }

    return index;
}

static void initialize_stream_arrays(StreamWorker *worker) {
    size_t i;

    for (i = 0; i < worker->element_count; ++i) {
        worker->a[i] = 0.0;
        worker->b[i] = 1.0 + (double) worker->thread_index;
        worker->c[i] = 0.5 + (double) (i & 7u);
    }
}

static void reset_stream_worker_metrics(StreamWorker *worker) {
    worker->checksum = 0.0;
    worker->bytes_moved = 0;
    worker->elapsed_sec = 0.0;
}

static void *stream_thread_main(void *opaque) {
    StreamThreadArgs *args = opaque;
    SharedState *shared = args->shared;
    StreamWorker *worker = args->worker;
    uint64_t start_ns;
    uint64_t end_ns;
    uint64_t iteration;
    size_t i;
    double checksum = 0.0;
    double scalar = shared->scalar;

    if (pin_thread_to_cpu(worker->cpu_id) != 0) {
        perror("pthread_setaffinity_np");
        pthread_mutex_lock(&shared->mutex);
        shared->ready_threads += 1;
        atomic_store_explicit(&shared->abort_run, 1, memory_order_release);
        pthread_cond_broadcast(&shared->cond);
        pthread_mutex_unlock(&shared->mutex);
        return (void *) (intptr_t) EXIT_FAILURE;
    }

    pthread_mutex_lock(&shared->mutex);
    shared->ready_threads += 1;
    pthread_cond_broadcast(&shared->cond);
    while (!shared->start_streamers && atomic_load_explicit(&shared->abort_run, memory_order_acquire) == 0) {
        pthread_cond_wait(&shared->cond, &shared->mutex);
    }
    pthread_mutex_unlock(&shared->mutex);

    if (atomic_load_explicit(&shared->abort_run, memory_order_acquire) != 0) {
        return (void *) (intptr_t) EXIT_FAILURE;
    }

    start_ns = monotonic_ns();
    if (shared->fixed_iterations_mode) {
        for (iteration = 0; iteration < shared->stream_iterations; ++iteration) {
            for (i = 0; i < worker->element_count; ++i) {
                worker->a[i] = worker->b[i] + scalar * worker->c[i];
            }

            checksum += worker->a[(size_t) worker->thread_index % worker->element_count];
            worker->bytes_moved += (uint64_t) worker->element_count * 3ull * sizeof(double);
        }
    } else {
        while (atomic_load_explicit(&shared->stop_streamers, memory_order_relaxed) == 0) {
            for (i = 0; i < worker->element_count; ++i) {
                worker->a[i] = worker->b[i] + scalar * worker->c[i];
            }

            checksum += worker->a[(size_t) worker->thread_index % worker->element_count];
            worker->bytes_moved += (uint64_t) worker->element_count * 3ull * sizeof(double);
        }
    }
    end_ns = monotonic_ns();

    worker->checksum = checksum;
    worker->elapsed_sec = (double) (end_ns - start_ns) / 1e9;
    return NULL;
}

static void *latency_thread_main(void *opaque) {
    LatencyThreadArgs *args = opaque;
    SharedState *shared = args->shared;
    LatencyWorker *worker = args->worker;
    uint64_t start_ns;
    uint64_t end_ns;

    if (pin_thread_to_cpu(worker->cpu_id) != 0) {
        perror("pthread_setaffinity_np");
        pthread_mutex_lock(&shared->mutex);
        shared->ready_threads += 1;
        atomic_store_explicit(&shared->abort_run, 1, memory_order_release);
        pthread_cond_broadcast(&shared->cond);
        pthread_mutex_unlock(&shared->mutex);
        return (void *) (intptr_t) EXIT_FAILURE;
    }

    pthread_mutex_lock(&shared->mutex);
    shared->ready_threads += 1;
    pthread_cond_broadcast(&shared->cond);
    while (!shared->start_streamers && atomic_load_explicit(&shared->abort_run, memory_order_acquire) == 0) {
        pthread_cond_wait(&shared->cond, &shared->mutex);
    }
    pthread_mutex_unlock(&shared->mutex);

    if (atomic_load_explicit(&shared->abort_run, memory_order_acquire) != 0) {
        return (void *) (intptr_t) EXIT_FAILURE;
    }

    while (atomic_load_explicit(&shared->start_latency, memory_order_acquire) == 0) {
        if (atomic_load_explicit(&shared->abort_run, memory_order_acquire) != 0) {
            return (void *) (intptr_t) EXIT_FAILURE;
        }
    }

    start_ns = monotonic_ns();
    worker->final_index = run_pointer_chase(worker->nodes, worker->start_index, worker->steps);
    end_ns = monotonic_ns();
    worker->elapsed_ns = end_ns - start_ns;
    return NULL;
}

static void choose_defaults(Config *cfg) {
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    uint64_t phys_bytes = 0;
    uint64_t memory_budget;
    uint64_t default_chase = 0;
    uint64_t per_streamer_budget;
    uint64_t per_array;
    uint64_t worker_count;

    worker_count = (uint64_t) (cfg->stream_only ? cfg->cpu_count : cfg->streamer_count);
    if (worker_count == 0) {
        worker_count = 1;
    }

    if (pages > 0 && page_size > 0) {
        phys_bytes = (uint64_t) pages * (uint64_t) page_size;
    }

    if (phys_bytes == 0) {
        if (cfg->stream_array_bytes == 0) {
            cfg->stream_array_bytes = 16ull * 1024ull * 1024ull;
        }
        if (!cfg->stream_only && cfg->chase_bytes == 0) {
            cfg->chase_bytes = 128ull * 1024ull * 1024ull;
        }
        return;
    }

    memory_budget = phys_bytes / 4ull;
    if (memory_budget < 128ull * 1024ull * 1024ull) {
        memory_budget = 128ull * 1024ull * 1024ull;
    }

    if (!cfg->stream_only) {
        default_chase = clamp_u64(memory_budget / 8ull,
                                  64ull * 1024ull * 1024ull,
                                  256ull * 1024ull * 1024ull);
        if (default_chase >= memory_budget) {
            default_chase = 16ull * 1024ull * 1024ull;
        }
    }

    per_streamer_budget = (memory_budget - default_chase) / worker_count;
    per_array = per_streamer_budget / 3ull;
    per_array = clamp_u64(per_array,
                          4ull * 1024ull * 1024ull,
                          64ull * 1024ull * 1024ull);

    if (cfg->stream_array_bytes == 0) {
        cfg->stream_array_bytes = (size_t) per_array;
    }
    if (!cfg->stream_only && cfg->chase_bytes == 0) {
        cfg->chase_bytes = (size_t) default_chase;
    }
}

static void usage(const char *program_name) {
    fprintf(stderr,
            "Usage: %s [--stream-only -i iterations] [-w warmup_ms] [-n chase_steps] [-s stream_mib] [-p chase_mib] [-o output_csv]\n"
            "  The benchmark sweeps streaming threads from 0 to N-1 while CPU 0 runs pointer chasing.\n"
            "  --stream-only  run only the streaming kernel on all CPUs\n"
            "  -i  streaming iterations per CPU in stream-only mode (default: 100)\n"
            "  -w  warmup duration before latency timing (default: 1000)\n"
            "  -n  dependent pointer-chase loads to measure (default: 10000000)\n"
            "  -s  MiB per streaming array, per streaming thread (default: auto)\n"
            "  -p  MiB for the pointer-chase structure (default: auto)\n"
            "  -o  CSV file written after the full sweep (default: mem_bw_latency_results.csv)\n",
            program_name);
}

static void parse_args(int argc, char **argv, Config *cfg) {
    int opt;
    static const struct option long_options[] = {
        {"stream-only", no_argument, NULL, 'm'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "w:n:s:p:o:i:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'm':
                cfg->stream_only = 1;
                break;
            case 'w':
                cfg->warmup_ms = parse_u64(optarg, "-w");
                break;
            case 'n':
                cfg->chase_steps = parse_u64(optarg, "-n");
                break;
            case 'i':
                cfg->stream_iterations = parse_u64(optarg, "-i");
                break;
            case 's':
                cfg->stream_array_bytes = mib_to_bytes(parse_u64(optarg, "-s"), "-s");
                break;
            case 'p':
                cfg->chase_bytes = mib_to_bytes(parse_u64(optarg, "-p"), "-p");
                break;
            case 'o':
                cfg->output_path = optarg;
                break;
            case 'h':
                usage(argv[0]);
                exit(EXIT_SUCCESS);
            default:
                usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }
}

static void print_config(const Config *cfg, size_t node_count) {
    printf("Benchmark configuration\n");
    printf("  mode                   : %s\n", cfg->stream_only ? "stream-only" : "latency-sweep");
    printf("  online_cpus            : %d\n", cfg->cpu_count);
    if (cfg->stream_only) {
        printf("  streaming_threads      : %d\n", cfg->cpu_count);
        printf("  stream_iterations      : %" PRIu64 "\n", cfg->stream_iterations);
    } else {
        printf("  max_streaming_threads  : %d\n", cfg->streamer_count);
        printf("  sweep_range            : 0..%d\n", cfg->streamer_count);
        printf("  latency_cpu            : 0\n");
        printf("  warmup_ms              : %" PRIu64 "\n", cfg->warmup_ms);
        printf("  chase_steps            : %" PRIu64 "\n", cfg->chase_steps);
    }
    printf("  stream_array_bytes     : %zu\n", cfg->stream_array_bytes);
    printf("  stream_working_set     : %zu per streamer\n",
           (size_t) (cfg->stream_array_bytes * 3ull));
    if (!cfg->stream_only) {
        printf("  chase_bytes            : %zu\n", cfg->chase_bytes);
        printf("  chase_nodes            : %zu\n", node_count);
    }
    printf("  output_file            : %s\n", cfg->output_path);
    printf("\n");
}

static int write_results_csv(const char *path, const Config *cfg, const TrialResult *results,
                             size_t count) {
    FILE *fp;
    size_t i;

    fp = fopen(path, "w");
    if (fp == NULL) {
        perror("fopen");
        return -1;
    }

    if (fprintf(fp,
                "streamers,total_gib_per_sec,ns_per_load,final_index,warmup_ms,chase_steps,stream_array_bytes,chase_bytes\n") < 0) {
        perror("fprintf");
        fclose(fp);
        return -1;
    }

    for (i = 0; i < count; ++i) {
        if (fprintf(fp,
                    "%d,%.6f,%.6f,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%zu,%zu\n",
                    results[i].active_streamers,
                    results[i].total_bandwidth_gib,
                    (double) results[i].latency_ns / (double) cfg->chase_steps,
                    results[i].final_index,
                    cfg->warmup_ms,
                    cfg->chase_steps,
                    cfg->stream_array_bytes,
                    cfg->chase_bytes) < 0) {
            perror("fprintf");
            fclose(fp);
            return -1;
        }
    }

    if (fclose(fp) != 0) {
        perror("fclose");
        return -1;
    }

    return 0;
}

static int write_stream_only_csv(const char *path, const Config *cfg,
                                 const StreamOnlyResult *result) {
    FILE *fp;

    fp = fopen(path, "w");
    if (fp == NULL) {
        perror("fopen");
        return -1;
    }

    if (fprintf(fp,
                "mode,streamers,iterations,total_bytes,elapsed_ns,throughput_gib_per_sec,stream_array_bytes\n") < 0) {
        perror("fprintf");
        fclose(fp);
        return -1;
    }

    if (fprintf(fp,
                "stream-only,%d,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.6f,%zu\n",
                result->active_streamers,
                result->iterations,
                result->total_bytes,
                result->elapsed_ns,
                result->total_bandwidth_gib,
                cfg->stream_array_bytes) < 0) {
        perror("fprintf");
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        perror("fclose");
        return -1;
    }

    return 0;
}

static int run_stream_only_trial(const Config *cfg, StreamWorker *streamers,
                                 int active_streamers, StreamOnlyResult *result) {
    SharedState shared;
    StreamThreadArgs *stream_args = NULL;
    pthread_t *stream_threads = NULL;
    uint64_t start_ns;
    uint64_t end_ns;
    int i;
    int created_stream_threads = 0;
    int failed = 0;
    int mutex_initialized = 0;
    int cond_initialized = 0;

    memset(&shared, 0, sizeof(shared));
    memset(result, 0, sizeof(*result));
    result->active_streamers = active_streamers;
    result->iterations = cfg->stream_iterations;

    if (active_streamers <= 0) {
        fprintf(stderr, "stream-only mode requires at least one streaming thread\n");
        return -1;
    }

    for (i = 0; i < active_streamers; ++i) {
        reset_stream_worker_metrics(&streamers[i]);
    }

    atomic_init(&shared.start_latency, 0);
    atomic_init(&shared.stop_streamers, 0);
    atomic_init(&shared.abort_run, 0);
    shared.total_threads = active_streamers;
    shared.ready_threads = 0;
    shared.start_streamers = 0;
    shared.fixed_iterations_mode = 1;
    shared.stream_iterations = cfg->stream_iterations;
    shared.scalar = cfg->scalar;

    if (pthread_mutex_init(&shared.mutex, NULL) != 0) {
        perror("pthread_mutex_init");
        failed = 1;
        goto cleanup;
    }
    mutex_initialized = 1;

    if (pthread_cond_init(&shared.cond, NULL) != 0) {
        perror("pthread_cond_init");
        failed = 1;
        goto cleanup;
    }
    cond_initialized = 1;

    stream_args = calloc((size_t) active_streamers, sizeof(*stream_args));
    stream_threads = calloc((size_t) active_streamers, sizeof(*stream_threads));
    if (stream_args == NULL || stream_threads == NULL) {
        perror("calloc");
        failed = 1;
        goto join_partial;
    }

    for (i = 0; i < active_streamers; ++i) {
        stream_args[i].shared = &shared;
        stream_args[i].worker = &streamers[i];
        if (pthread_create(&stream_threads[i], NULL, stream_thread_main, &stream_args[i]) != 0) {
            perror("pthread_create");
            failed = 1;
            goto join_partial;
        }
        created_stream_threads += 1;
    }

    pthread_mutex_lock(&shared.mutex);
    while (shared.ready_threads < shared.total_threads &&
           atomic_load_explicit(&shared.abort_run, memory_order_acquire) == 0) {
        pthread_cond_wait(&shared.cond, &shared.mutex);
    }
    if (atomic_load_explicit(&shared.abort_run, memory_order_acquire) != 0) {
        pthread_mutex_unlock(&shared.mutex);
        failed = 1;
        goto join_partial;
    }
    start_ns = monotonic_ns();
    shared.start_streamers = 1;
    pthread_cond_broadcast(&shared.cond);
    pthread_mutex_unlock(&shared.mutex);

join_partial:
    if (failed) {
        atomic_store_explicit(&shared.abort_run, 1, memory_order_release);
        if (mutex_initialized && cond_initialized) {
            pthread_mutex_lock(&shared.mutex);
            shared.start_streamers = 1;
            pthread_cond_broadcast(&shared.cond);
            pthread_mutex_unlock(&shared.mutex);
        }
    }

    for (i = 0; i < created_stream_threads; ++i) {
        if (pthread_join(stream_threads[i], NULL) != 0) {
            perror("pthread_join");
            failed = 1;
        }
    }
    end_ns = monotonic_ns();

    if (!failed) {
        result->elapsed_ns = end_ns - start_ns;
        result->total_bytes = 0;
        for (i = 0; i < active_streamers; ++i) {
            result->total_bytes += streamers[i].bytes_moved;
        }
        result->total_bandwidth_gib =
            ((double) result->total_bytes / ((double) result->elapsed_ns / 1e9)) /
            (1024.0 * 1024.0 * 1024.0);
    }

cleanup:
    if (cond_initialized) {
        pthread_cond_destroy(&shared.cond);
    }
    if (mutex_initialized) {
        pthread_mutex_destroy(&shared.mutex);
    }
    free(stream_args);
    free(stream_threads);
    return failed ? -1 : 0;
}

static int run_trial(const Config *cfg, StreamWorker *streamers, const Node *nodes,
                     int active_streamers, TrialResult *result) {
    SharedState shared;
    StreamThreadArgs *stream_args = NULL;
    pthread_t *stream_threads = NULL;
    pthread_t latency_thread;
    LatencyThreadArgs latency_args;
    LatencyWorker latency_worker;
    int i;
    int created_stream_threads = 0;
    int latency_thread_created = 0;
    int failed = 0;
    int mutex_initialized = 0;
    int cond_initialized = 0;

    memset(&shared, 0, sizeof(shared));
    memset(result, 0, sizeof(*result));
    result->active_streamers = active_streamers;

    if (active_streamers < 0 || active_streamers > cfg->streamer_count) {
        fprintf(stderr, "invalid active streamer count: %d\n", active_streamers);
        return -1;
    }

    for (i = 0; i < active_streamers; ++i) {
        reset_stream_worker_metrics(&streamers[i]);
    }

    latency_worker.cpu_id = 0;
    latency_worker.nodes = nodes;
    latency_worker.start_index = 0;
    latency_worker.steps = cfg->chase_steps;
    latency_worker.final_index = 0;
    latency_worker.elapsed_ns = 0;

    atomic_init(&shared.start_latency, 0);
    atomic_init(&shared.stop_streamers, 0);
    atomic_init(&shared.abort_run, 0);
    shared.total_threads = active_streamers + 1;
    shared.ready_threads = 0;
    shared.start_streamers = 0;
    shared.fixed_iterations_mode = 0;
    shared.stream_iterations = 0;
    shared.scalar = cfg->scalar;

    if (pthread_mutex_init(&shared.mutex, NULL) != 0) {
        perror("pthread_mutex_init");
        failed = 1;
        goto cleanup;
    }
    mutex_initialized = 1;

    if (pthread_cond_init(&shared.cond, NULL) != 0) {
        perror("pthread_cond_init");
        failed = 1;
        goto cleanup;
    }
    cond_initialized = 1;

    if (active_streamers > 0) {
        stream_args = calloc((size_t) active_streamers, sizeof(*stream_args));
        stream_threads = calloc((size_t) active_streamers, sizeof(*stream_threads));
        if (stream_args == NULL || stream_threads == NULL) {
            perror("calloc");
            failed = 1;
            goto join_partial;
        }
    }

    for (i = 0; i < active_streamers; ++i) {
        stream_args[i].shared = &shared;
        stream_args[i].worker = &streamers[i];
        if (pthread_create(&stream_threads[i], NULL, stream_thread_main, &stream_args[i]) != 0) {
            perror("pthread_create");
            failed = 1;
            goto join_partial;
        }
        created_stream_threads += 1;
    }

    latency_args.shared = &shared;
    latency_args.worker = &latency_worker;
    if (pthread_create(&latency_thread, NULL, latency_thread_main, &latency_args) != 0) {
        perror("pthread_create");
        failed = 1;
        goto join_partial;
    }
    latency_thread_created = 1;

    pthread_mutex_lock(&shared.mutex);
    while (shared.ready_threads < shared.total_threads &&
           atomic_load_explicit(&shared.abort_run, memory_order_acquire) == 0) {
        pthread_cond_wait(&shared.cond, &shared.mutex);
    }
    if (atomic_load_explicit(&shared.abort_run, memory_order_acquire) != 0) {
        pthread_mutex_unlock(&shared.mutex);
        failed = 1;
        goto join_partial;
    }
    shared.start_streamers = 1;
    pthread_cond_broadcast(&shared.cond);
    pthread_mutex_unlock(&shared.mutex);

    sleep_ms(cfg->warmup_ms);
    atomic_store_explicit(&shared.start_latency, 1, memory_order_release);

    if (pthread_join(latency_thread, NULL) != 0) {
        perror("pthread_join");
        failed = 1;
    }
    latency_thread_created = 0;

    atomic_store_explicit(&shared.stop_streamers, 1, memory_order_release);

join_partial:
    if (failed) {
        atomic_store_explicit(&shared.abort_run, 1, memory_order_release);
        atomic_store_explicit(&shared.start_latency, 1, memory_order_release);
        atomic_store_explicit(&shared.stop_streamers, 1, memory_order_release);
        if (mutex_initialized && cond_initialized) {
            pthread_mutex_lock(&shared.mutex);
            shared.start_streamers = 1;
            pthread_cond_broadcast(&shared.cond);
            pthread_mutex_unlock(&shared.mutex);
        }
    }

    if (latency_thread_created) {
        if (pthread_join(latency_thread, NULL) != 0) {
            perror("pthread_join");
            failed = 1;
        }
    }

    for (i = 0; i < created_stream_threads; ++i) {
        if (pthread_join(stream_threads[i], NULL) != 0) {
            perror("pthread_join");
            failed = 1;
        }
    }

    if (!failed) {
        result->final_index = latency_worker.final_index;
        result->latency_ns = latency_worker.elapsed_ns;
        result->total_bandwidth_gib = 0.0;

        for (i = 0; i < active_streamers; ++i) {
            result->total_bandwidth_gib +=
                ((double) streamers[i].bytes_moved / streamers[i].elapsed_sec) /
                (1024.0 * 1024.0 * 1024.0);
        }
    }

cleanup:
    if (cond_initialized) {
        pthread_cond_destroy(&shared.cond);
    }
    if (mutex_initialized) {
        pthread_mutex_destroy(&shared.mutex);
    }
    free(stream_args);
    free(stream_threads);
    return failed ? -1 : 0;
}

int main(int argc, char **argv) {
    Config cfg;
    TrialResult *results = NULL;
    StreamWorker *streamers = NULL;
    StreamOnlyResult stream_only_result;
    Node *nodes = NULL;
    size_t node_count = 0;
    int i;
    int worker_count;
    int cpu_base;
    size_t result_count = 0;
    int failed = 0;

    memset(&cfg, 0, sizeof(cfg));
    cfg.warmup_ms = 1000;
    cfg.chase_steps = 10000000ull;
    cfg.stream_iterations = 100ull;
    cfg.scalar = 3.0;
    cfg.output_path = "mem_bw_latency_results.csv";
    cfg.cpu_count = (int) sysconf(_SC_NPROCESSORS_ONLN);
    if (cfg.cpu_count < 1) {
        fprintf(stderr, "need at least 1 online CPU; found %d\n", cfg.cpu_count);
        return EXIT_FAILURE;
    }

    cfg.streamer_count = cfg.cpu_count - 1;
    parse_args(argc, argv, &cfg);
    choose_defaults(&cfg);

    if (!cfg.stream_only && cfg.cpu_count < 2) {
        fprintf(stderr, "latency sweep mode requires at least 2 online CPUs; found %d\n", cfg.cpu_count);
        return EXIT_FAILURE;
    }

    if (cfg.stream_array_bytes < 1024ull * 1024ull) {
        fprintf(stderr, "stream array size must be at least 1 MiB\n");
        return EXIT_FAILURE;
    }
    if (cfg.stream_only && cfg.stream_iterations == 0) {
        fprintf(stderr, "stream-only mode requires -i to be at least 1\n");
        return EXIT_FAILURE;
    }
    if (!cfg.stream_only && cfg.chase_bytes < sizeof(Node) * 1024ull) {
        fprintf(stderr, "pointer-chase size must be at least %zu bytes\n",
                (size_t) (sizeof(Node) * 1024ull));
        return EXIT_FAILURE;
    }

    if (!cfg.stream_only) {
        node_count = cfg.chase_bytes / sizeof(Node);
        if (node_count < 2) {
            fprintf(stderr, "pointer-chase region is too small\n");
            return EXIT_FAILURE;
        }
        cfg.chase_bytes = node_count * sizeof(Node);
    }
    cfg.stream_array_bytes = (cfg.stream_array_bytes / sizeof(double)) * sizeof(double);

    print_config(&cfg, node_count);

    worker_count = cfg.stream_only ? cfg.cpu_count : cfg.streamer_count;
    cpu_base = cfg.stream_only ? 0 : 1;

    if (!cfg.stream_only) {
        result_count = (size_t) cfg.streamer_count + 1ull;
        results = calloc(result_count, sizeof(*results));
        if (results == NULL) {
            perror("calloc");
            failed = 1;
            goto cleanup;
        }
    }

    streamers = calloc((size_t) worker_count, sizeof(*streamers));
    if (streamers == NULL) {
        perror("calloc");
        failed = 1;
        goto cleanup;
    }

    if (!cfg.stream_only) {
        nodes = aligned_alloc_or_die(64, cfg.chase_bytes);
        build_random_cycle(nodes, node_count);
    }

    for (i = 0; i < worker_count; ++i) {
        StreamWorker *worker = &streamers[i];

        worker->thread_index = i;
        worker->cpu_id = cpu_base + i;
        worker->element_count = cfg.stream_array_bytes / sizeof(double);
        worker->a = aligned_alloc_or_die(64, cfg.stream_array_bytes);
        worker->b = aligned_alloc_or_die(64, cfg.stream_array_bytes);
        worker->c = aligned_alloc_or_die(64, cfg.stream_array_bytes);
        initialize_stream_arrays(worker);
    }

    if (cfg.stream_only) {
        if (run_stream_only_trial(&cfg, streamers, worker_count, &stream_only_result) != 0) {
            failed = 1;
            goto cleanup;
        }

        printf("Stream-only results\n");
        printf("  streamers              : %d\n", stream_only_result.active_streamers);
        printf("  iterations             : %" PRIu64 "\n", stream_only_result.iterations);
        printf("  total_bytes            : %" PRIu64 "\n", stream_only_result.total_bytes);
        printf("  elapsed_ns             : %" PRIu64 "\n", stream_only_result.elapsed_ns);
        printf("  total_GiB_per_sec      : %.3f\n", stream_only_result.total_bandwidth_gib);

        if (write_stream_only_csv(cfg.output_path, &cfg, &stream_only_result) != 0) {
            failed = 1;
            goto cleanup;
        }
        printf("\nResults written to %s\n", cfg.output_path);
    } else {
        printf("Sweep results\n");
        printf("  streamers total_GiB_per_sec ns_per_load final_index\n");
        for (i = 0; i <= cfg.streamer_count; ++i) {
            if (run_trial(&cfg, streamers, nodes, i, &results[i]) != 0) {
                failed = 1;
                break;
            }

            printf("  %9d %17.3f %11.3f %" PRIu32 "\n",
                   results[i].active_streamers,
                   results[i].total_bandwidth_gib,
                   (double) results[i].latency_ns / (double) cfg.chase_steps,
                   results[i].final_index);
        }

        if (!failed) {
            if (write_results_csv(cfg.output_path, &cfg, results, result_count) != 0) {
                failed = 1;
                goto cleanup;
            }
            printf("\nResults written to %s\n", cfg.output_path);
        }
    }

cleanup:
    if (streamers != NULL) {
        for (i = 0; i < worker_count; ++i) {
            free(streamers[i].a);
            free(streamers[i].b);
            free(streamers[i].c);
        }
    }

    free(nodes);
    free(results);
    free(streamers);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}

#else

int main(void) {
    fprintf(stderr, "This benchmark is Linux-specific and requires pthread CPU affinity APIs.\n");
    return EXIT_FAILURE;
}

#endif
