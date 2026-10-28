use std::{
    cell::{Cell, UnsafeCell},
    cmp::min,
    mem::MaybeUninit,
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

#[repr(align(128))]
struct CachePadded<T>(T);

pub struct Spsc<T> {
    queue: Vec<UnsafeCell<MaybeUninit<T>>>,
    producer_index: CachePadded<AtomicUsize>,
    consumer_index: CachePadded<AtomicUsize>,
    size: usize,
    producer_alive: AtomicBool,
    consumer_alive: AtomicBool,
}

impl<T> Spsc<T> {
    /// Constructs a ring and returns its unique producer and consumer endpoints.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(size: usize) -> (Producer<T>, Consumer<T>) {
        let mut queue = Vec::new();
        if size == 0 {
            panic!("Zero capacity");
        }
        assert!(size.is_power_of_two(), "Size must be a power of two");
        queue.resize_with(size, || UnsafeCell::new(MaybeUninit::uninit()));

        let shared = Arc::new(Spsc {
            queue,
            producer_index: CachePadded(AtomicUsize::new(0)),
            consumer_index: CachePadded(AtomicUsize::new(0)),
            size,
            producer_alive: AtomicBool::new(true),
            consumer_alive: AtomicBool::new(true),
        });
        (
            Producer {
                shared: shared.clone(),
                write_position: 0,
                cached_read_position: 0,
            },
            Consumer {
                shared: shared.clone(),
                read_position: 0,
                cached_write_position: 0,
            },
        )
    }
}
pub struct Producer<T> {
    shared: Arc<Spsc<T>>,
    write_position: usize,
    cached_read_position: usize,
}

pub struct Consumer<T> {
    shared: Arc<Spsc<T>>,
    read_position: usize,
    cached_write_position: usize,
}

impl<T> Producer<T> {
    /// Returns whether the consumer endpoint still exists.
    /// A concurrent drop may occur immediately after this check.
    pub fn is_consumer_alive(&self) -> bool {
        self.shared.consumer_alive.load(Ordering::Acquire)
    }

    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let producer_index = self.write_position;
        let consumer_index = self.cached_read_position;
        if producer_index.wrapping_sub(consumer_index) == self.shared.size {
            self.cached_read_position = self.shared.consumer_index.0.load(Ordering::Acquire);
            if self.write_position.wrapping_sub(self.cached_read_position) == self.shared.size {
                return Err(value);
            }
        }

        let index = producer_index & (self.shared.size - 1);
        let ptr = self.shared.queue[index].get();
        unsafe {
            (*ptr).write(value);
        }
        self.write_position = self.write_position.wrapping_add(1);

        self.shared
            .producer_index
            .0
            .store(self.write_position, Ordering::Release);
        Ok(())
    }

    /// Reserves up to `max` vacant slots. Values written through the guard
    /// become visible together when `commit` is called. Dropping the guard
    /// without committing drops its staged values.
    #[inline]
    pub fn try_reserve(&mut self, max: usize) -> Option<WriteReservation<'_, T>> {
        if max == 0 {
            return None;
        }
        let mut free =
            self.shared.size - self.write_position.wrapping_sub(self.cached_read_position);
        if free < max {
            self.cached_read_position = self.shared.consumer_index.0.load(Ordering::Acquire);
            free = self.shared.size - self.write_position.wrapping_sub(self.cached_read_position);
        }
        let len = min(max, free);
        (len != 0).then_some(WriteReservation {
            producer: self,
            len,
            initialized: 0,
        })
    }
}

impl<T> Consumer<T> {
    /// Returns whether the producer endpoint still exists.
    /// Already queued values can remain after it returns false.
    pub fn is_producer_alive(&self) -> bool {
        self.shared.producer_alive.load(Ordering::Acquire)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let producer_index = self.cached_write_position;
        let consumer_index = self.read_position;
        if consumer_index == producer_index {
            self.cached_write_position = self.shared.producer_index.0.load(Ordering::Acquire);
            if self.cached_write_position == consumer_index {
                return None;
            }
        }
        let index = consumer_index & (self.shared.size - 1);
        let ptr: *mut MaybeUninit<T> = self.shared.queue[index].get();
        let value = unsafe { (*ptr).assume_init_read() };
        self.read_position = self.read_position.wrapping_add(1);
        self.shared
            .consumer_index
            .0
            .store(self.read_position, Ordering::Release);

        Some(value)
    }

    /// Borrows up to `max` readable items without copying them. The guard
    /// releases only the items explicitly popped or discarded through it.
    #[inline]
    pub fn try_read(&mut self, max: usize) -> Option<ReadReservation<'_, T>> {
        if max == 0 {
            return None;
        }
        let mut available = self.cached_write_position.wrapping_sub(self.read_position);
        if available < max {
            self.cached_write_position = self.shared.producer_index.0.load(Ordering::Acquire);
            available = self.cached_write_position.wrapping_sub(self.read_position);
        }
        let len = min(max, available);
        (len != 0).then_some(ReadReservation {
            consumer: self,
            len,
            consumed: 0,
            not_sync: std::marker::PhantomData,
        })
    }
}

impl<T: Copy> Producer<T> {
    #[inline]
    pub fn push_slice(&mut self, values: &[T]) -> usize {
        if values.is_empty() {
            return 0;
        }
        let producer_index = self.write_position;
        let mut available_queue =
            self.shared.size - self.write_position.wrapping_sub(self.cached_read_position);

        if available_queue < values.len() {
            self.cached_read_position = self.shared.consumer_index.0.load(Ordering::Acquire);

            available_queue =
                self.shared.size - self.write_position.wrapping_sub(self.cached_read_position);
        }

        let min_size = min(values.len(), available_queue);

        if min_size == 0 {
            return 0;
        }
        let index = producer_index & (self.shared.size - 1);
        let first = min(min_size, self.shared.size - index);
        let base = self.shared.queue.as_ptr();
        // SAFETY: the producer owns these vacant slots. The Vec is contiguous,
        // T is Copy, and neither span crosses the allocation boundary.
        unsafe {
            ptr::copy_nonoverlapping(
                values.as_ptr(),
                base.add(index).cast::<T>() as *mut T,
                first,
            );
            ptr::copy_nonoverlapping(
                values.as_ptr().add(first),
                base.cast::<T>() as *mut T,
                min_size - first,
            );
        }
        self.write_position = self.write_position.wrapping_add(min_size);
        self.shared
            .producer_index
            .0
            .store(self.write_position, Ordering::Release);
        min_size
    }
}

impl<T: Copy> Consumer<T> {
    #[inline]
    pub fn pop_slice(&mut self, output: &mut [T]) -> usize {
        if output.is_empty() {
            return 0;
        }
        let consumer_index = self.read_position;

        let mut available_queue = self.cached_write_position.wrapping_sub(self.read_position);

        if available_queue < output.len() {
            self.cached_write_position = self.shared.producer_index.0.load(Ordering::Acquire);
            available_queue = self.cached_write_position.wrapping_sub(self.read_position);
            if available_queue == 0 {
                return 0;
            }
        }

        let index = consumer_index & (self.shared.size - 1);

        let count = min(available_queue, output.len());

        let first = min(count, self.shared.size - index);
        let base = self.shared.queue.as_ptr();
        // SAFETY: the consumer owns these initialized slots. T is Copy, and
        // both source spans stay within the contiguous allocation.
        unsafe {
            ptr::copy_nonoverlapping(base.add(index).cast::<T>(), output.as_mut_ptr(), first);
            ptr::copy_nonoverlapping(
                base.cast::<T>(),
                output.as_mut_ptr().add(first),
                count - first,
            );
        }
        self.read_position = self.read_position.wrapping_add(count);
        self.shared
            .consumer_index
            .0
            .store(self.read_position, Ordering::Release);
        count
    }
}

/// An unpublished producer batch. Written items are dropped if it is abandoned.
pub struct WriteReservation<'a, T> {
    producer: &'a mut Producer<T>,
    len: usize,
    initialized: usize,
}

impl<T> WriteReservation<'_, T> {
    pub fn capacity(&self) -> usize {
        self.len
    }

    /// Adds an item and returns it for direct mutation in ring storage.
    pub fn push(&mut self, value: T) -> Result<&mut T, T> {
        if self.initialized == self.len {
            return Err(value);
        }
        let index = self.producer.write_position.wrapping_add(self.initialized)
            & (self.producer.shared.size - 1);
        // SAFETY: this slot is vacant and only the producer can write it.
        let item = unsafe { (*self.producer.shared.queue[index].get()).write(value) };
        self.initialized += 1;
        Ok(item)
    }

    /// Publishes all values written through this reservation at once.
    pub fn commit(mut self) -> usize {
        let count = self.initialized;
        if count != 0 {
            self.producer.write_position = self.producer.write_position.wrapping_add(count);
            self.producer
                .shared
                .producer_index
                .0
                .store(self.producer.write_position, Ordering::Release);
            self.initialized = 0;
        }
        count
    }
}

impl<T> Drop for WriteReservation<'_, T> {
    fn drop(&mut self) {
        for i in 0..self.initialized {
            let index =
                self.producer.write_position.wrapping_add(i) & (self.producer.shared.size - 1);
            // SAFETY: these staged items were initialized by push and are not published.
            unsafe {
                (*self.producer.shared.queue[index].get()).assume_init_drop();
            }
        }
    }
}

/// A borrowed consumer batch. Popped items become writable when it is dropped.
pub struct ReadReservation<'a, T> {
    consumer: &'a mut Consumer<T>,
    len: usize,
    consumed: usize,
    not_sync: std::marker::PhantomData<Cell<()>>,
}

impl<T> ReadReservation<'_, T> {
    pub fn len(&self) -> usize {
        self.len - self.consumed
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the remaining items as at most two contiguous slices.
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let index = self.consumer.read_position.wrapping_add(self.consumed)
            & (self.consumer.shared.size - 1);
        let first = min(self.len(), self.consumer.shared.size - index);
        let base = self.consumer.shared.queue.as_ptr();
        // SAFETY: these slots were published by the producer and cannot be
        // reused until this guard publishes their consumption.
        unsafe {
            (
                std::slice::from_raw_parts(base.add(index).cast::<T>(), first),
                std::slice::from_raw_parts(base.cast::<T>(), self.len() - first),
            )
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let index = self.consumer.read_position.wrapping_add(self.consumed)
            & (self.consumer.shared.size - 1);
        // SAFETY: this is the next initialized, unconsumed slot.
        let value = unsafe { (*self.consumer.shared.queue[index].get()).assume_init_read() };
        self.consumed += 1;
        Some(value)
    }

    /// Drops the first `n` remaining values and releases their slots.
    pub fn discard(mut self, n: usize) {
        assert!(n <= self.len());
        for _ in 0..n {
            // Increment consumed before running user Drop code. If Drop
            // panics, this guard still publishes exactly the consumed prefix.
            drop(self.pop().unwrap());
        }
    }
}

impl<T: Copy> ReadReservation<'_, T> {
    /// Releases `n` inspected items with one cursor publication.
    pub fn advance(mut self, n: usize) {
        assert!(n <= self.len());
        self.consumed += n;
    }
}

impl<T> Drop for ReadReservation<'_, T> {
    fn drop(&mut self) {
        if self.consumed != 0 {
            self.consumer.read_position = self.consumer.read_position.wrapping_add(self.consumed);
            self.consumer
                .shared
                .consumer_index
                .0
                .store(self.consumer.read_position, Ordering::Release);
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.producer_alive.store(false, Ordering::Release);
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_alive.store(false, Ordering::Release);
    }
}

impl<T> Drop for Spsc<T> {
    fn drop(&mut self) {
        let read = self.consumer_index.0.load(Ordering::Acquire);
        let write = self.producer_index.0.load(Ordering::Acquire);
        let remaining = write.wrapping_sub(read);
        debug_assert!(remaining <= self.size);
        for i in 0..remaining {
            let index = read.wrapping_add(i) & (self.size - 1);
            // SAFETY: after the last Arc is dropped there are no active
            // endpoints. Exactly the published, unconsumed prefix is initialized.
            unsafe {
                (*self.queue[index].get()).assume_init_drop();
            }
        }
    }
}

unsafe impl<T: Send> Send for Spsc<T> {}
unsafe impl<T: Send> Sync for Spsc<T> {}

#[cfg(test)]
mod tests {
    use super::Spsc;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
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

    #[test]
    fn batch_partial_and_wraparound() {
        let (mut tx, mut rx) = Spsc::<u64>::new(4);
        let mut out = [u64::MAX; 8];

        // Empty queue
        assert_eq!(rx.pop_slice(&mut out), 0);
        assert_eq!(out, [u64::MAX; 8]);

        // Empty input
        assert_eq!(tx.push_slice(&[]), 0);

        // Fill three slots
        assert_eq!(tx.push_slice(&[10, 11, 12]), 3);

        // Consume two
        assert_eq!(rx.pop_slice(&mut out[..2]), 2);
        assert_eq!(&out[..2], &[10, 11]);

        // One item remains, so only three slots are free.
        // This write also wraps around the physical buffer.
        assert_eq!(tx.push_slice(&[13, 14, 15, 16]), 3);

        // The FIFO order must be preserved.
        assert_eq!(rx.pop_slice(&mut out[..4]), 4);
        assert_eq!(&out[..4], &[12, 13, 14, 15]);

        assert_eq!(rx.pop_slice(&mut out), 0);

        // A batch larger than total capacity
        assert_eq!(tx.push_slice(&[20, 21, 22, 23, 24]), 4);
        assert_eq!(rx.pop_slice(&mut out), 4);
        assert_eq!(&out[..4], &[20, 21, 22, 23]);
    }

    struct Counted(usize, Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn non_copy_items_are_dropped_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = Spsc::new(4);
        tx.push(Counted(10, drops.clone()))
            .unwrap_or_else(|_| panic!("full"));
        tx.push(Counted(20, drops.clone()))
            .unwrap_or_else(|_| panic!("full"));
        let first = rx.pop().unwrap();
        assert_eq!(first.0, 10);
        drop(first);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(tx);
        assert!(!rx.is_producer_alive());
        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn abandoned_write_reservation_drops_staged_items() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = Spsc::new(2);
        {
            let mut batch = tx.try_reserve(2).unwrap();
            assert_eq!(batch.capacity(), 2);
            batch
                .push(Counted(1, drops.clone()))
                .unwrap_or_else(|_| panic!("full"));
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(rx.pop().is_none());
        let mut batch = tx.try_reserve(2).unwrap();
        batch
            .push(Counted(2, drops.clone()))
            .unwrap_or_else(|_| panic!("full"));
        batch
            .push(Counted(3, drops.clone()))
            .unwrap_or_else(|_| panic!("full"));
        assert_eq!(batch.commit(), 2);
        assert_eq!(rx.pop().unwrap().0, 2);
        drop(tx);
        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn read_reservation_wraps_and_releases_consumed_prefix() {
        let (mut tx, mut rx) = Spsc::<String>::new(4);
        tx.push("a".into()).unwrap();
        tx.push("b".into()).unwrap();
        tx.push("c".into()).unwrap();
        assert_eq!(rx.pop().as_deref(), Some("a"));
        assert_eq!(rx.pop().as_deref(), Some("b"));
        tx.push("d".into()).unwrap();
        tx.push("e".into()).unwrap();
        tx.push("f".into()).unwrap();
        {
            let mut read = rx.try_read(4).unwrap();
            let (first, second) = read.as_slices();
            assert_eq!(first, ["c", "d"]);
            assert_eq!(second, ["e", "f"]);
            assert_eq!(read.pop().as_deref(), Some("c"));
        }
        assert_eq!(tx.push("g".into()), Ok(()));
        let read = rx.try_read(4).unwrap();
        assert_eq!(read.len(), 4);
        read.discard(3);
        assert_eq!(rx.pop().as_deref(), Some("g"));
        assert_eq!(rx.pop(), None);
    }

    #[test]
    fn copy_read_reservation_advances_without_copying() {
        let (mut tx, mut rx) = Spsc::<u64>::new(2);
        assert_eq!(tx.push_slice(&[10, 20]), 2);
        let batch = rx.try_read(2).unwrap();
        let (first, second) = batch.as_slices();
        assert_eq!(first, &[10, 20]);
        assert!(second.is_empty());
        batch.advance(2);
        assert_eq!(tx.push_slice(&[30, 40]), 2);
        assert_eq!(rx.pop(), Some(30));
        assert_eq!(rx.pop(), Some(40));
    }

    #[test]
    fn endpoint_drop_is_visible() {
        let (tx, rx) = Spsc::<u64>::new(1);
        drop(rx);
        assert!(!tx.is_consumer_alive());
        drop(tx);
        let (tx, rx) = Spsc::<u64>::new(1);
        drop(tx);
        assert!(!rx.is_producer_alive());
    }

    #[test]
    fn read_guard_releases_prefix_if_item_drop_panics() {
        struct PanickingDrop {
            panic: bool,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for PanickingDrop {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                assert!(!self.panic, "intentional drop panic");
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = Spsc::new(2);
        tx.push(PanickingDrop {
            panic: true,
            drops: drops.clone(),
        })
        .unwrap_or_else(|_| panic!("full"));
        tx.push(PanickingDrop {
            panic: false,
            drops: drops.clone(),
        })
        .unwrap_or_else(|_| panic!("full"));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rx.try_read(2).unwrap().discard(2);
        }));
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(rx);
        drop(tx);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
