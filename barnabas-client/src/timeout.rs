//! A timeout with no runtime.
//!
//! Every async runtime has one of these, and none of them can be named here —
//! that is the point of [`Transport`](crate::Transport). So it is built from
//! the two things the seam does provide: the future being waited on, and
//! `Transport::sleep`.
//!
//! No dependency, no executor, no `Send` bound. The future is polled first on
//! every wake, so a request that completes in the same poll as its deadline
//! wins — a timeout that fired on a *finished* request would drop a perfectly
//! good connection.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::Transport;

/// Run `fut`, giving up after `dur`.
///
/// Returns `None` on timeout.
pub(crate) async fn with_timeout<T, F>(dur: Duration, fut: F) -> Option<F::Output>
where
    T: Transport,
    F: Future,
{
    Race {
        fut: Box::pin(fut),
        sleep: Box::pin(T::sleep(dur)),
    }
    .await
}

// Generic over the sleep rather than boxed as `dyn`: a trait object would
// impose `'static` on the transport's timer future, which is a bound the seam
// never asked for.
struct Race<F: Future, S: Future<Output = ()>> {
    fut: Pin<Box<F>>,
    sleep: Pin<Box<S>>,
}

impl<F: Future, S: Future<Output = ()>> Future for Race<F, S> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The work first, deliberately: if both are ready, the request won.
        if let Poll::Ready(out) = self.fut.as_mut().poll(cx) {
            return Poll::Ready(Some(out));
        }
        if self.sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}
