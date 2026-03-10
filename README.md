# mem_bw_latency

`mem_bw_latency` is a Linux-focused C benchmark for stressing memory bandwidth and measuring memory latency.

It supports two modes:

- Default `latency-sweep` mode: runs pointer chasing on a selected CPU from the current affinity mask and varies the number of streaming threads on the remaining allowed CPUs.
- `--stream-only` mode: runs the streaming kernel on all CPUs allowed by the current affinity mask for a fixed number of iterations and reports aggregate memory throughput.

## Files

- `mem_bw_latency.c` - benchmark source
- `Makefile` - build, run, stream-only run, and clean targets

## Build

```sh
make
```

This produces the `mem_bw_latency` executable.

## Run

Default latency sweep:

```sh
make run
./mem_bw_latency
```

Stream-only throughput run on all allowed CPUs:

```sh
make run-stream-only
./mem_bw_latency --stream-only -i 1000
```

Show usage:

```sh
./mem_bw_latency -h
```

## Options

- `--stream-only` - run only the streaming kernel on all CPUs allowed by the current affinity mask
- `--sweep <start:end:step>` - latency-mode streamer counts to test, inclusive; defaults to `0:max:1`
- `--chaser-cpu <cpu_id>` - latency-mode pointer-chaser CPU ID from the allowed affinity mask
- `-i <iterations>` - streaming iterations per CPU in stream-only mode
- `-w <ms>` - warmup time before pointer-chase timing in latency-sweep mode
- `-n <steps>` - number of dependent pointer-chase loads in latency-sweep mode
- `-s <MiB>` - MiB per streaming array for each streaming thread
- `-p <MiB>` - MiB used by the pointer-chase structure in latency-sweep mode
- `-o <path>` - CSV output path

Examples:

```sh
./mem_bw_latency -w 2000 -n 20000000 -s 32 -p 128 -o sweep.csv
./mem_bw_latency --sweep 0:0:1 -o no_load.csv
./mem_bw_latency --sweep 0:8:2 --chaser-cpu 4 -o partial_sweep.csv
./mem_bw_latency --stream-only -i 5000 -s 64 -o stream_only.csv
```

## Output

Latency-sweep mode prints:

- Benchmark configuration
- One row per sweep point with active streaming threads, total GiB/s, and pointer-chase nanoseconds per load

Stream-only mode prints:

- Benchmark configuration
- Streaming thread count, iteration count, total bytes moved, elapsed nanoseconds, and aggregate GiB/s

Both modes write CSV results at the end. The default output file is `results.csv` unless `-o` is provided.

## Notes

- The benchmark is intended for Linux systems with pthread CPU affinity support.
- CPU selection follows the process affinity mask from `sched_getaffinity()`, so sparse masks like `0,2,4,6` work correctly.
- In latency mode, `--sweep 0:0:1` measures pointer chasing with no streaming load.
- On non-Linux platforms, the program builds but exits with a message explaining that Linux affinity APIs are required.
- `stream_array_bytes` is per array, per thread; each streaming worker uses three arrays.
- Use `make clean` to remove the compiled binary.
