//! TLS over tokio, as a second [`Transport`](kestrel_client::Transport).
//!
//! Nothing in `kestrel-client` changes to support this: a TLS connection is
//! just a different `Transport::Stream`, so `Consumer<TokioTls>` is the same
//! client as `Consumer<Glommio>` over a different socket. That is what the seam
//! was for.
//!
//! The glommio file is the same twenty lines against `futures-rustls`; the
//! difference is which rustls wrapper the runtime wants, which is exactly the
//! kind of thing a binding is for.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};

use crate::Tokio;

/// A TLS transport for tokio.
///
/// Cheap to clone: the `ClientConfig` is shared, which is how rustls wants it —
/// session tickets and the certificate store are per-config, not per-connection.
#[derive(Clone)]
pub struct TokioTls {
    connector: TlsConnector,
}

impl TokioTls {
    /// TLS with the platform's usual public roots (`webpki-roots`).
    ///
    /// **Certificate verification is on**, which is the only reason to use TLS
    /// at all; there is deliberately no "skip verification" switch. A caller
    /// with a private CA supplies it through [`Self::with_roots`].
    #[must_use]
    pub fn new() -> Self {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Self::with_roots(roots)
    }

    /// TLS trusting exactly `roots` — a private CA, or a pinned certificate.
    #[must_use]
    pub fn with_roots(roots: RootCertStore) -> Self {
        // Name the provider explicitly rather than relying on an installed
        // default — the same trap G4 recorded for the DFS client.
        let config = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring provides the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();

        Self {
            connector: TlsConnector::from(Arc::new(config)),
        }
    }

    /// Build from a fully-specified rustls config — client certificates,
    /// custom verifiers, ALPN.
    #[must_use]
    pub fn from_config(config: Arc<ClientConfig>) -> Self {
        Self {
            connector: TlsConnector::from(config),
        }
    }
}

impl Default for TokioTls {
    fn default() -> Self {
        Self::new()
    }
}

impl kestrel_client::Transport for TokioTls {
    type Stream = TlsStream<tokio::net::TcpStream>;

    async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
        // The certificate is verified against the *host* the caller named, not
        // the address it resolved to — otherwise DNS decides who we trust.
        let host = addr
            .rsplit_once(':')
            .map_or(addr, |(host, _)| host)
            .to_owned();
        let server_name = ServerName::try_from(host)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let tcp = <Tokio as kestrel_client::Transport>::connect(&Tokio, addr).await?;
        self.connector.connect(server_name, tcp).await
    }

    async fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> io::Result<usize> {
        stream.read(buf).await
    }

    async fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> io::Result<()> {
        stream.write_all(buf).await?;
        stream.flush().await
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

/// A cluster handle over TLS.
pub type TlsCluster = kestrel_client::Cluster<TokioTls>;
/// An assign-only consumer over TLS.
pub type TlsConsumer = kestrel_client::Consumer<TokioTls>;
/// A producer over TLS.
pub type TlsProducer = kestrel_client::Producer<TokioTls>;
