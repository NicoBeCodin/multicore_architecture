//! Bounded single-producer, single-consumer queue.
//!
//! `Spsc::new` creates one producer and one consumer. Scalar operations are
//! nonblocking; a failed `push` returns its value and an empty `pop` returns
//! `None`. The endpoints are moved to separate threads for concurrent use.
//!
//! ```
//! use spsc::Spsc;
//! let (mut tx, mut rx) = Spsc::<String>::new(4);
//! tx.push("hello".into()).unwrap();
//! assert_eq!(rx.pop().as_deref(), Some("hello"));
//! ```

pub mod spsc;

pub use spsc::{Consumer, Producer, ReadReservation, Spsc, WriteReservation};
