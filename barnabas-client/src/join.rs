//! Run several futures at once, with no runtime and no `Send` bound.
//!
//! `futures::join_all` would do this, but it is the only thing this crate would
//! want from that dependency, and the version here is short enough to read in
//! one sitting.
//!
//! Why it exists at all: a client holding partitions on three brokers used to
//! send its three `Fetch` requests one after another, waiting for each. That is
//! invisible against a single broker — there is only ever one request — and it
//! cost roughly a factor of three the moment there were three. Requests to
//! *different* brokers have nothing to do with each other and belong in flight
//! together.
//!
//! Every pending future is polled on each wake rather than tracking which waker
//! fired. That is O(n) per wake, and n here is the number of brokers a core
//! talks to — a handful. A general-purpose combinator would not make that
//! trade; this one can.

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;

/// Drive every future to completion concurrently, returning their outputs in
/// the order the futures were given.
pub(crate) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Option<Pin<Box<F>>>> =
        futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut done: Vec<Option<F::Output>> = (0..pending.len()).map(|_| None).collect();
    let mut left = pending.len();

    poll_fn(move |cx| {
        for (slot, out) in pending.iter_mut().zip(done.iter_mut()) {
            let Some(fut) = slot.as_mut() else { continue };
            if let Poll::Ready(value) = fut.as_mut().poll(cx) {
                *out = Some(value);
                // Dropped as soon as it finishes, so a long-running sibling
                // does not hold a finished request's buffers.
                *slot = None;
                left -= 1;
            }
        }
        if left == 0 {
            Poll::Ready(
                std::mem::take(&mut done)
                    .into_iter()
                    .map(|o| o.expect("every slot filled before left reached zero"))
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Context;

    /// Order of the results follows the order of the inputs, not the order in
    /// which they finished — callers index into it.
    #[test]
    fn results_keep_input_order() {
        let out = futures_lite::future::block_on(join_all(vec![
            std::future::ready(1),
            std::future::ready(2),
            std::future::ready(3),
        ]));
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn empty_input_completes_immediately() {
        let out: Vec<()> = futures_lite::future::block_on(join_all(Vec::<
            std::future::Ready<()>,
        >::new()));
        assert!(out.is_empty());
    }

    /// The property the whole file exists for: the second future starts before
    /// the first has finished. A sequential loop would fail this.
    #[test]
    fn futures_make_progress_together() {
        let log = Rc::new(Cell::new(String::new()));
        let push = |log: &Rc<Cell<String>>, c: char| {
            let mut s = log.take();
            s.push(c);
            log.set(s);
        };

        // Each yields once, so a sequential runner would produce "aabb" and a
        // concurrent one "abab".
        let a = {
            let log = Rc::clone(&log);
            async move {
                push(&log, 'a');
                yield_once().await;
                push(&log, 'a');
            }
        };
        let b = {
            let log = Rc::clone(&log);
            async move {
                push(&log, 'b');
                yield_once().await;
                push(&log, 'b');
            }
        };

        futures_lite::future::block_on(join_all(vec![
            Box::pin(a) as Pin<Box<dyn Future<Output = ()>>>,
            Box::pin(b),
        ]));
        assert_eq!(log.take(), "abab");
    }

    async fn yield_once() {
        let mut yielded = false;
        poll_fn(move |cx: &mut Context<'_>| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}
