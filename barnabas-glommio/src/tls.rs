//! TLS over glommio, as a second [`Transport`](barnabas_client::Transport).
//!
//! Nothing in `barnabas-client` changes to support this: a TLS connection is
//! just a different `Transport::Stream`, so `Consumer<GlommioTls>` is the same
//! client as `Consumer<Glommio>` over a different socket. That is what the seam
//! was for.
//!
//! `futures-rustls` works over glommio's streams unmodified — proven before
//! this was written, in `spikes/glommio-rustls`.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::{AsyncReadExt as _, AsyncWriteExt as _};
use futures_rustls::client::TlsStream;
use futures_rustls::TlsConnector;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};

use crate::Glommio;

/// A TLS transport for glommio.
///
/// Cheap to clone: the `ClientConfig` is shared, which is how rustls wants it —
/// session tickets and the certificate store are per-config, not per-connection.
#[derive(Clone)]
pub struct GlommioTls {
    connector: TlsConnector,
}

impl GlommioTls {
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
        // Name the provider explicitly. rustls will not pick one from features
        // alone, and the failure is a runtime panic in whichever binary forgot
        // to install a default — recorded the hard way in G4.
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

impl Default for GlommioTls {
    fn default() -> Self {
        Self::new()
    }
}

impl barnabas_client::Transport for GlommioTls {
    type Stream = TlsStream<glommio::net::TcpStream>;

    async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
        // The certificate is verified against the *host* the caller named, not
        // the address it resolved to — otherwise DNS decides who we trust.
        let host = addr
            .rsplit_once(':')
            .map_or(addr, |(host, _)| host)
            .to_owned();
        let server_name = ServerName::try_from(host)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let tcp = <Glommio as barnabas_client::Transport>::connect(&Glommio, addr).await?;
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
        glommio::timer::sleep(dur).await;
    }
}

/// A cluster handle over TLS.
pub type TlsCluster = barnabas_client::Cluster<GlommioTls>;
/// An assign-only consumer over TLS.
pub type TlsConsumer = barnabas_client::Consumer<GlommioTls>;
/// A producer over TLS.
pub type TlsProducer = barnabas_client::Producer<GlommioTls>;
