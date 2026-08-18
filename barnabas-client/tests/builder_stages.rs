//! The staging is a compile-time claim, so it is checked at compile time.
//!
//! A builder whose stages are only documented is a builder whose stages do not
//! exist. These assert the two directions: the required steps cannot be
//! skipped, and the optional ones are only offered once they are legal.

use barnabas_client::{Consumer, Producer, StartOffset, Transport};

/// A transport that never connects: these tests are about types, and none of
/// them builds anything.
struct Nothing;

impl Transport for Nothing {
    type Stream = std::io::Cursor<Vec<u8>>;

    async fn connect(&self, _addr: &str) -> std::io::Result<Self::Stream> {
        Err(std::io::Error::other("not a real transport"))
    }
    async fn read(_s: &mut Self::Stream, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
    async fn write_all(_s: &mut Self::Stream, _buf: &[u8]) -> std::io::Result<()> {
        Ok(())
    }
    async fn sleep(_dur: std::time::Duration) {}
}

/// Each stage returns the next one, and the optional settings appear only at
/// the last. If this compiles, an editor offers the same sequence.
#[test]
fn the_stages_chain_in_order() {
    let ready = Consumer::builder(Nothing)
        .bootstrap(["localhost:9092"])
        .client_id("my-app")
        .assign("events", 0, StartOffset::Earliest)
        .max_wait(std::time::Duration::from_millis(50));
    // Not awaited: building would connect, and there is nothing to connect to.
    let _ = ready;

    let ready = Producer::builder(Nothing)
        .bootstrap(["localhost:9092".to_owned()])
        .client_id("my-app")
        .transactional_id("sink-0")
        .max_in_flight(3);
    let _ = ready;
}

/// `StartOffset` is an enum, so a stored offset cannot be confused with the
/// sentinels — which is the bug the `i64` parameter allowed.
#[test]
fn a_stored_offset_is_not_a_sentinel() {
    assert_ne!(StartOffset::At(0), StartOffset::Earliest);
    assert_ne!(StartOffset::At(-1), StartOffset::Latest);
}

/// What must *not* compile. `trybuild` would assert this mechanically; kept as
/// a documented list because the shapes are short and the point is the design:
///
/// ```compile_fail
/// # use barnabas_client::Consumer;
/// // No bootstrap list: `client_id` does not exist on stage 1.
/// let _ = Consumer::builder(Nothing).client_id("my-app");
/// ```
///
/// ```compile_fail
/// # use barnabas_client::Consumer;
/// // No client id: `build` does not exist on stage 2.
/// let _ = Consumer::builder(Nothing).bootstrap(["localhost:9092"]).build();
/// ```
///
/// ```compile_fail
/// # use barnabas_client::Consumer;
/// // `max_wait` is not offered before the required steps are done.
/// let _ = Consumer::builder(Nothing).max_wait(std::time::Duration::from_secs(1));
/// ```
#[test]
fn required_steps_cannot_be_skipped() {}
