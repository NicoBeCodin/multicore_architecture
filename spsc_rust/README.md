# SPSC queue

A bounded queue for one producer and one consumer. The two ends can run on separate threads. The queue allocates its storage when it is created; pushing and popping do not allocate unless the value being passed does.

This is a library in `src/lib.rs`. `src/main.rs` is the benchmark program.

## Use

```rust
use spsc::Spsc;

let (mut tx, mut rx) = Spsc::<String>::new(8);

tx.push("hello".to_owned()).unwrap();
assert_eq!(rx.pop().as_deref(), Some("hello"));
```

The capacity must be a nonzero power of two. `push` returns the value in `Err` if the queue is full. `pop` returns `None` if it is empty. Neither call waits. The caller decides whether to retry, yield, or do other work.

The endpoints have `is_consumer_alive` and `is_producer_alive` checks. These are snapshots: the other end can be dropped immediately after a check. A failed `push` or empty `pop` does not, by itself, tell you whether the other end has gone away.

## Batches

For `Copy` values, `push_slice` and `pop_slice` move as many items as currently fit. Both return the number transferred; a call may process only part of a slice.

```rust
use spsc::Spsc;

let (mut tx, mut rx) = Spsc::<u64>::new(8);
assert_eq!(tx.push_slice(&[10, 20, 30]), 3);

let mut output = [0; 3];
assert_eq!(rx.pop_slice(&mut output), 3);
assert_eq!(output, [10, 20, 30]);
```

`try_reserve` stages a batch directly in the ring. Each call to the reservation's `push` writes one value and returns a mutable reference to it. `commit` publishes the staged values together. Dropping the reservation without committing drops those values.

`try_read` borrows a batch as up to two slices, since the ring may wrap. The reservation's `pop` moves values out. `discard` drops values that are no longer needed. For `Copy` values, `advance` releases inspected values without copying them out. Dropping a read reservation leaves any unconsumed values in the queue.

## Run the benchmarks

From this directory:

```sh
cargo test --release
cargo run --release -- batch
SPSC_CORES=0,2 cargo run --release --example compare
```

`batch` measures scalar transfers, slice copies, and reservations at several batch sizes. `compare` sends scalar `u64` values through this queue, `rtrb`, and `ringbuf` using the same workload. On Linux, `SPSC_CORES` pins the producer and consumer to the given logical CPUs. Check `lscpu -e=CPU,CORE` to choose separate physical cores. Run benchmarks one at a time; results change with scheduling and CPU power state.

## Status

The queue currently uses heap storage and the standard library. It has tests for ordering, wraparound, batch operations, and dropping queued values. It has not yet been model-checked or audited for production use.
