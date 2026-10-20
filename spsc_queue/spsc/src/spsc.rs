use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[repr(align(64))]
struct CachePadded<T>(T);

pub struct Spsc<T> {
    queue: Vec<UnsafeCell<MaybeUninit<T>>>,
    producer_index: CachePadded<AtomicUsize>,
    consumer_index: CachePadded<AtomicUsize>,
    size: usize,
}

impl<T: Copy + Default> Spsc<T> {
    pub fn new(size: usize) -> (Producer<T>, Consumer<T>) {
        let mut queue = Vec::new();
        if size == 0 {
            panic!("Zero capacity");
        }
        queue.resize_with(size, || UnsafeCell::new(MaybeUninit::uninit()));

        let shared = Arc::new(Spsc {
            queue,
            producer_index: CachePadded(AtomicUsize::new(0)),
            consumer_index: CachePadded(AtomicUsize::new(0)),
            size,
        });
        (
            Producer {
                shared: shared.clone(),
            },
            Consumer {
                shared: shared.clone(),
            },
        )
    }
}
pub struct Producer<T> {
    shared: Arc<Spsc<T>>,
}

pub struct Consumer<T> {
    shared: Arc<Spsc<T>>,
}

impl<T> Producer<T> {
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let producer_index = self.shared.producer_index.0.load(Ordering::Relaxed);
        let consumer_index = self.shared.consumer_index.0.load(Ordering::Acquire);
        if producer_index.wrapping_sub(consumer_index) == self.shared.size {
            return Err(value);
        }
        let ptr = self.shared.queue[producer_index % self.shared.size].get();
        unsafe {
            (*ptr).write(value);
        }

        let _ = self
            .shared
            .producer_index
            .0
            .store(producer_index.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    pub fn pop(&mut self) -> Option<T> {
        let producer_index = self.shared.producer_index.0.load(Ordering::Acquire);
        let consumer_index = self.shared.consumer_index.0.load(Ordering::Relaxed);

        if consumer_index == producer_index {
            return None;
        }
        let ptr: *mut MaybeUninit<T> = self.shared.queue[consumer_index % self.shared.size].get();
        let value = unsafe { (*ptr).assume_init_read() };
        let _ = self
            .shared
            .consumer_index
            .0
            .store(consumer_index.wrapping_add(1), Ordering::Release);

        Some(value)
    }
}

unsafe impl<T: Send> Send for Spsc<T> {}
unsafe impl<T: Send> Sync for Spsc<T> {}

#[cfg(test)]
mod tests {
    use super::Spsc;
    use std::thread;

    #[test]
    fn empty_queue() {
        let (_, mut receiver) = Spsc::<u64>::new(4);

        assert_eq!(receiver.pop(), None);
    }

    #[test]
    fn fifo_order() {
        let (mut producer, mut receiver) = Spsc::<u64>::new(4);

        assert_eq!(producer.push(10), Ok(()));
        assert_eq!(producer.push(20), Ok(()));
        assert_eq!(producer.push(30), Ok(()));

        assert_eq!(receiver.pop(), Some(10));
        assert_eq!(receiver.pop(), Some(20));
        assert_eq!(receiver.pop(), Some(30));
        assert_eq!(receiver.pop(), None);
    }

    #[test]
    fn full_queue() {
        let (mut producer, mut receiver) = Spsc::<u64>::new(2);

        assert_eq!(producer.push(10), Ok(()));
        assert_eq!(producer.push(20), Ok(()));
        assert_eq!(producer.push(30), Err(30));

        assert_eq!(receiver.pop(), Some(10));
        assert_eq!(producer.push(30), Ok(()));

        assert_eq!(receiver.pop(), Some(20));
        assert_eq!(receiver.pop(), Some(30));
    }

    #[test]
    fn concurrent_transfer() {
        const N: u64 = 100_000;
        const CAPACITY: usize = 8;

        let (mut producer, mut receiver) = Spsc::<u64>::new(CAPACITY);

        let producer_thread = thread::spawn(move || {
            for i in 0..N {
                while producer.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let receiver_thread = thread::spawn(move || {
            for expected in 0..N {
                let value = loop {
                    if let Some(value) = receiver.pop() {
                        break value;
                    }

                    thread::yield_now();
                };

                assert_eq!(value, expected);
            }
        });

        producer_thread.join().unwrap();
        receiver_thread.join().unwrap();
    }

    #[test]
    fn wraparound_under_contention() {
        const N: u64 = 100_000;

        let (mut producer, mut receiver) = Spsc::<u64>::new(2);

        let producer_thread = thread::spawn(move || {
            for i in 0..N {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let receiver_thread = thread::spawn(move || {
            for expected in 0..N {
                let value = loop {
                    if let Some(value) = receiver.pop() {
                        break value;
                    }

                    std::hint::spin_loop();
                };

                assert_eq!(value, expected);
            }
        });

        producer_thread.join().unwrap();
        receiver_thread.join().unwrap();
    }
}
