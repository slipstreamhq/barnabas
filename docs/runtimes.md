# Choosing a runtime

[← barnabas](../README.md)

**By depending on a binding, not by setting a feature.** There is no `cfg` in
this workspace selecting a runtime, and no default one to override.

`barnabas-client` is generic over one trait:

```rust
pub trait Transport: 'static {
    type Stream: 'static;
    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>>;
    fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>>;
    fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> impl Future<Output = io::Result<()>>;
    fn sleep(dur: Duration) -> impl Future<Output = ()>;
}
```

A binding is that trait plus three type aliases. `barnabas-glommio` is 64 lines;
`barnabas-tokio` is 67. So the choice is which line you put in `Cargo.toml`:

```toml
# thread-per-core, io_uring
barnabas-glommio = "0.1"
```
```toml
# work-stealing
barnabas-tokio = "0.1"
```

and which name you import:

```rust
use barnabas_glommio::{Consumer, Glommio, EARLIEST};   // or
use barnabas_tokio::{Consumer, Tokio, EARLIEST};
```

Each binding re-exports everything shared — `Error`, `Result`, `RecordRef`,
`ConsumerRecords`, `ProducerRecord`, `IsolationLevel`, `Partitioner`,
`CompressionCodec`, `EARLIEST`, `LATEST` — so **only those two lines differ**
between the two programs below.

### Why not a feature flag

Cargo features are additive: if two crates in a build ask for different
runtimes, a feature-selected client gets both and has to pick one, and the
program that did not ask for the winner is the one that breaks. Types do not
unify that way — `Consumer<Glommio>` and `Consumer<Tokio>` are different types,
so a binary can hold both at once, on different threads, and the compiler keeps
them apart.

It also means the `Send`-ness is the *binding's* property. `glommio::net::TcpStream`
belongs to the core that opened it, so everything on `Glommio` is `!Send` and
stays on its executor; the tokio side is `Send` from the same client code
because `barnabas-client` places no `Send` bound anywhere. A trait that demanded
`Send` would have forbidden the per-core side outright.

Selecting per build is then the *caller's* job, and a `cfg` in one place does
it. Slipstream, for instance, does this in its Kafka crate:

```rust
#[cfg(feature = "glommio")]
use barnabas_glommio as barnabas;
#[cfg(not(feature = "glommio"))]
use barnabas_tokio as barnabas;
```

### Writing another binding

Implement the four functions. `barnabas-tokio/src/lib.rs` is the shortest
complete example at 67 lines, and nothing above the transport needs to change.
