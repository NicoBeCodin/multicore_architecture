//! Same scalar workload for three SPSC implementations.
//! Run with: cargo run --release --example compare

use ringbuf::{
    traits::{Consumer as _, Producer as _, Split as _},
    HeapRb,
};
use std::{
    hint::{black_box, spin_loop},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

const MESSAGES: u64 = 5_000_000;
const RUNS: usize = 7;

#[cfg(target_os = "linux")]
fn pin_thread(cpu: usize) {
    assert!(cpu < libc::CPU_SETSIZE as usize, "CPU index is too large");
    // SAFETY: cpu_set_t is initialized before use, and the syscall receives
    // the exact size of the set. PID 0 means the calling thread on Linux.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        assert_eq!(
            rc,
            0,
            "sched_setaffinity: {}",
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_thread(_: usize) {
    panic!("SPSC_CORES requires Linux");
}

fn measure<P, C>(producer: P, consumer: C, n: u64, cores: Option<[usize; 2]>) -> Duration
where
    P: FnOnce() + Send + 'static,
    C: FnOnce() -> u64 + Send + 'static,
{
    let gate = Arc::new(Barrier::new(3));
    let pg = gate.clone();
    let cg = gate.clone();
    let p = thread::spawn(move || {
        if let Some([cpu, _]) = cores {
            pin_thread(cpu);
        }
        pg.wait();
        producer();
    });
    let c = thread::spawn(move || {
        if let Some([_, cpu]) = cores {
            pin_thread(cpu);
        }
        cg.wait();
        consumer()
    });
    let start = Instant::now();
    gate.wait();
    p.join().unwrap();
    let sum = c.join().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(sum, (n as u128 * (n as u128 - 1) / 2) as u64);
    elapsed
}

fn one(kind: &str, capacity: usize, n: u64, cores: Option<[usize; 2]>) -> Duration {
    match kind {
        "spsc" => {
            let (mut tx, mut rx) = spsc::Spsc::<u64>::new(capacity);
            measure(
                move || {
                    for i in 0..n {
                        let mut value = black_box(i);
                        loop {
                            match tx.push(value) {
                                Ok(()) => break,
                                Err(v) => {
                                    value = v;
                                    spin_loop();
                                }
                            }
                        }
                    }
                },
                move || {
                    let mut sum = 0u64;
                    for _ in 0..n {
                        let value = loop {
                            if let Some(v) = rx.pop() {
                                break v;
                            }
                            spin_loop();
                        };
                        sum = sum.wrapping_add(black_box(value));
                    }
                    sum
                },
                n,
                cores,
            )
        }
        "rtrb" => {
            let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(capacity);
            measure(
                move || {
                    for i in 0..n {
                        let mut value = black_box(i);
                        loop {
                            match tx.push(value) {
                                Ok(()) => break,
                                Err(rtrb::PushError::Full(v)) => {
                                    value = v;
                                    spin_loop();
                                }
                            }
                        }
                    }
                },
                move || {
                    let mut sum = 0u64;
                    for _ in 0..n {
                        let value = loop {
                            if let Ok(v) = rx.pop() {
                                break v;
                            }
                            spin_loop();
                        };
                        sum = sum.wrapping_add(black_box(value));
                    }
                    sum
                },
                n,
                cores,
            )
        }
        "ringbuf" => {
            let (mut tx, mut rx) = HeapRb::<u64>::new(capacity).split();
            measure(
                move || {
                    for i in 0..n {
                        let mut value = black_box(i);
                        loop {
                            match tx.try_push(value) {
                                Ok(()) => break,
                                Err(v) => {
                                    value = v;
                                    spin_loop();
                                }
                            }
                        }
                    }
                },
                move || {
                    let mut sum = 0u64;
                    for _ in 0..n {
                        let value = loop {
                            if let Some(v) = rx.try_pop() {
                                break v;
                            }
                            spin_loop();
                        };
                        sum = sum.wrapping_add(black_box(value));
                    }
                    sum
                },
                n,
                cores,
            )
        }
        _ => unreachable!(),
    }
}

fn main() {
    let cores = std::env::var("SPSC_CORES").ok().map(|value| {
        let mut parts = value.split(',');
        let producer = parts
            .next()
            .unwrap()
            .parse::<usize>()
            .expect("invalid producer CPU");
        let consumer = parts
            .next()
            .expect("missing consumer CPU")
            .parse::<usize>()
            .expect("invalid consumer CPU");
        assert!(
            parts.next().is_none(),
            "expected SPSC_CORES=producer,consumer"
        );
        assert_ne!(producer, consumer, "use two different logical CPUs");
        [producer, consumer]
    });
    if let Some([producer, consumer]) = cores {
        // Validate both CPU choices before spawning threads and entering barriers.
        pin_thread(producer);
        pin_thread(consumer);
    }
    println!("Scalar u64 transfer, {MESSAGES} messages, median of {RUNS} runs");
    println!("Each producer sends 0..N; each consumer verifies the sum.");
    if let Some([producer, consumer]) = cores {
        println!("Pinned producer to CPU {producer}, consumer to CPU {consumer}");
    } else {
        println!("Threads are not pinned; set SPSC_CORES=producer,consumer on Linux");
    }
    for capacity in [64, 1024, 16_384] {
        println!("\ncapacity={capacity}");
        // Rotate the order to reduce systematic warm-up or thermal bias.
        let kinds = ["spsc", "rtrb", "ringbuf"];
        let mut samples: [Vec<Duration>; 3] = std::array::from_fn(|_| Vec::with_capacity(RUNS));
        for run in 0..RUNS {
            for offset in 0..kinds.len() {
                let index = (run + offset) % kinds.len();
                samples[index].push(one(kinds[index], capacity, MESSAGES, cores));
            }
        }
        for (kind, times) in kinds.into_iter().zip(samples.iter_mut()) {
            times.sort_unstable();
            let median = times[RUNS / 2];
            let mps = MESSAGES as f64 / median.as_secs_f64() / 1e6;
            let slow = MESSAGES as f64 / times[RUNS - 1].as_secs_f64() / 1e6;
            let fast = MESSAGES as f64 / times[0].as_secs_f64() / 1e6;
            println!("  {kind:<8} {mps:>9.2} M/s  range {slow:>9.2}..{fast:>9.2}");
        }
    }
}
