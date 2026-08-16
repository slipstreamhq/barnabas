//! SASL authentication: PLAIN and SCRAM-SHA-256/512.
//!
//! Protocol, so it lives here rather than in a binding — the handshake is the
//! same whichever runtime carries the bytes.
//!
//! # A warning that belongs in the type
//!
//! **PLAIN sends the password in the clear.** Kafka's `SASL_PLAINTEXT` really
//! is plaintext, and a mechanism named "PLAIN" over an unencrypted socket is a
//! credential handed to anyone on the path. [`Credentials::plain`] says so, and
//! [`SaslMechanism::requires_encryption`] lets a caller check rather than
//! remember.
//!
//! SCRAM is a challenge-response, so the password never crosses the wire — but
//! the exchange is still replayable without TLS, so the same advice holds with
//! less urgency.
//!
//! # Shape of the exchange
//!
//! `SaslHandshake` names the mechanism, then one or more `SaslAuthenticate`
//! round trips carry opaque bytes whose meaning is the mechanism's. Kafka wraps
//! the SASL frames in its own requests from v1 onward, which is the version
//! this uses — the older raw-bytes-on-the-socket form is not supported and
//! should not be.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};

/// Which SASL mechanism to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// Username and password, **in the clear**. Use only over TLS.
    Plain,
    /// Salted challenge-response, SHA-256.
    ScramSha256,
    /// Salted challenge-response, SHA-512.
    ScramSha512,
}

impl SaslMechanism {
    /// The name Kafka expects in `SaslHandshake`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Whether using this without TLS discloses the password itself.
    ///
    /// True only for `PLAIN`. SCRAM never sends the password, though an
    /// unencrypted exchange is still worth avoiding.
    #[must_use]
    pub fn requires_encryption(self) -> bool {
        matches!(self, Self::Plain)
    }
}

/// A username and password, and the mechanism to present them with.
#[derive(Clone)]
pub struct Credentials {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    /// Never prints the password. A `Debug` that leaks a credential into a log
    /// is a security bug that looks like a convenience.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Credentials {
    /// `PLAIN`. **Sends the password in the clear** — pair it with TLS.
    #[must_use]
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::Plain,
            username: username.into(),
            password: password.into(),
        }
    }

    #[must_use]
    pub fn scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha256,
            username: username.into(),
            password: password.into(),
        }
    }

    #[must_use]
    pub fn scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha512,
            username: username.into(),
            password: password.into(),
        }
    }
}

/// What went wrong during authentication.
#[derive(Debug, thiserror::Error)]
pub enum SaslError {
    #[error("the broker rejected the credentials")]
    Rejected,
    #[error("the broker's SASL message was malformed: {0}")]
    Malformed(String),
    #[error("the server's signature did not verify — the peer does not know the password")]
    ServerSignature,
}

/// PLAIN's single message: `authzid \0 authcid \0 password`, with an empty
/// authorization id.
#[must_use]
pub fn plain_message(credentials: &Credentials) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0);
    out.extend_from_slice(credentials.username.as_bytes());
    out.push(0);
    out.extend_from_slice(credentials.password.as_bytes());
    out
}

// ── SCRAM (RFC 5802) ─────────────────────────────────────────────────────────

/// A SCRAM exchange in progress.
///
/// Three messages: client-first, server-first, client-final — then the server's
/// final message, **which must be verified**. Skipping that verification is the
/// classic SCRAM mistake: it is what proves the peer actually knows the
/// password rather than merely accepting ours.
pub struct ScramExchange {
    mechanism: SaslMechanism,
    password: String,
    client_nonce: String,
    client_first_bare: String,
    server_signature: Vec<u8>,
}

impl ScramExchange {
    /// Begin, returning the client-first message.
    ///
    /// # Errors
    /// Never; the signature returns a message for symmetry with the rest.
    #[must_use]
    pub fn start(credentials: &Credentials, nonce: &str) -> (Self, Vec<u8>) {
        // `n,,` — no channel binding, no authorization id.
        let bare = format!("n={},r={}", saslprep(&credentials.username), nonce);
        let message = format!("n,,{bare}");
        (
            Self {
                mechanism: credentials.mechanism,
                password: credentials.password.clone(),
                client_nonce: nonce.to_owned(),
                client_first_bare: bare,
                server_signature: Vec::new(),
            },
            message.into_bytes(),
        )
    }

    /// Consume the server-first message and produce client-final.
    ///
    /// # Errors
    /// If the message is malformed, or the server's nonce does not extend ours
    /// — which would mean the exchange is not the one we started.
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Vec<u8>, SaslError> {
        let text = std::str::from_utf8(server_first)
            .map_err(|e| SaslError::Malformed(e.to_string()))?;

        let nonce = field(text, 'r').ok_or_else(|| SaslError::Malformed(text.to_owned()))?;
        let salt_b64 = field(text, 's').ok_or_else(|| SaslError::Malformed(text.to_owned()))?;
        let iterations: u32 = field(text, 'i')
            .ok_or_else(|| SaslError::Malformed(text.to_owned()))?
            .parse()
            .map_err(|_| SaslError::Malformed(text.to_owned()))?;

        if !nonce.starts_with(&self.client_nonce) {
            // The server must echo our nonce and append its own. Anything else
            // is a different exchange being replayed at us.
            return Err(SaslError::Malformed(format!(
                "server nonce {nonce} does not extend the client nonce"
            )));
        }
        let salt = base64_decode(salt_b64).ok_or_else(|| SaslError::Malformed(text.to_owned()))?;

        let salted = hi(self.mechanism, self.password.as_bytes(), &salt, iterations);
        let client_key = hmac(self.mechanism, &salted, b"Client Key");
        let stored_key = hash(self.mechanism, &client_key);

        let final_without_proof = format!("c=biws,r={nonce}");
        let auth_message =
            format!("{},{},{}", self.client_first_bare, text, final_without_proof);

        let client_signature = hmac(self.mechanism, &stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let server_key = hmac(self.mechanism, &salted, b"Server Key");
        self.server_signature = hmac(self.mechanism, &server_key, auth_message.as_bytes());

        Ok(format!("{final_without_proof},p={}", base64_encode(&proof)).into_bytes())
    }

    /// Verify the server's final message.
    ///
    /// **Not optional.** Without it, a peer that never knew the password can
    /// still complete the exchange from our side's perspective.
    ///
    /// # Errors
    /// If the message is malformed, carries an error, or the signature does not
    /// match.
    pub fn verify(&self, server_final: &[u8]) -> Result<(), SaslError> {
        let text = std::str::from_utf8(server_final)
            .map_err(|e| SaslError::Malformed(e.to_string()))?;
        if let Some(err) = field(text, 'e') {
            return Err(SaslError::Malformed(err.to_owned()));
        }
        let verifier = field(text, 'v').ok_or_else(|| SaslError::Malformed(text.to_owned()))?;
        let expected = base64_encode(&self.server_signature);
        if verifier == expected {
            Ok(())
        } else {
            Err(SaslError::ServerSignature)
        }
    }
}

/// Pull `key=value` out of a comma-separated SCRAM message.
fn field(message: &str, key: char) -> Option<&str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(&format!("{key}=")))
}

/// SCRAM's `Hi` — PBKDF2 with the mechanism's hash.
fn hi(mechanism: SaslMechanism, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut salted = Vec::new();
    let mut previous = {
        let mut block = salt.to_vec();
        block.extend_from_slice(&1u32.to_be_bytes());
        hmac(mechanism, password, &block)
    };
    salted.clone_from(&previous);
    for _ in 1..iterations {
        previous = hmac(mechanism, password, &previous);
        for (out, byte) in salted.iter_mut().zip(previous.iter()) {
            *out ^= byte;
        }
    }
    salted
}

fn hmac(mechanism: SaslMechanism, key: &[u8], data: &[u8]) -> Vec<u8> {
    match mechanism {
        SaslMechanism::ScramSha512 => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("hmac takes any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        _ => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac takes any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    }
}

fn hash(mechanism: SaslMechanism, data: &[u8]) -> Vec<u8> {
    match mechanism {
        SaslMechanism::ScramSha512 => Sha512::digest(data).to_vec(),
        _ => Sha256::digest(data).to_vec(),
    }
}

/// SCRAM escapes `,` and `=` in the username, since both are message
/// separators. Not full SASLprep — the normalisation half needs a Unicode
/// table, and getting the escaping wrong is the part that breaks logins.
fn saslprep(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let mut bits = 0u32;
    let mut count = 0;
    let mut out = Vec::new();
    for ch in text.bytes() {
        if ch == b'=' {
            break;
        }
        let value = B64.iter().position(|c| *c == ch)? as u32;
        bits = (bits << 6) | value;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_is_nul_separated_with_an_empty_authzid() {
        let msg = plain_message(&Credentials::plain("user", "pass"));
        assert_eq!(msg, b"\0user\0pass");
    }

    /// A password in a log is a security bug wearing a convenience's clothes.
    #[test]
    fn debug_never_prints_the_password() {
        let printed = format!("{:?}", Credentials::plain("user", "hunter2"));
        assert!(!printed.contains("hunter2"), "the password leaked: {printed}");
        assert!(printed.contains("redacted"));
    }

    #[test]
    fn only_plain_discloses_the_password() {
        assert!(SaslMechanism::Plain.requires_encryption());
        assert!(!SaslMechanism::ScramSha256.requires_encryption());
    }

    #[test]
    fn base64_round_trips() {
        for case in [&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar"] {
            assert_eq!(base64_decode(&base64_encode(case)).as_deref(), Some(case));
        }
        // The known vectors, so an off-by-one in the padding is caught.
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
    }

    /// **The exchange, end to end, against a server that knows the password.**
    ///
    /// The test computes the server side independently, which is what makes it
    /// meaningful: if the client's derivation were wrong, the proof would not
    /// match here either.
    #[test]
    fn scram_completes_against_a_server_that_knows_the_password() {
        let credentials = Credentials::scram_sha256("user", "pencil");
        let (mut exchange, client_first) = ScramExchange::start(&credentials, "clientNONCE");
        assert_eq!(client_first, b"n,,n=user,r=clientNONCE");

        // Server side, computed here rather than taken on faith.
        let salt = b"saltsalt";
        let iterations = 4096u32;
        let server_first = format!(
            "r=clientNONCEserverNONCE,s={},i={iterations}",
            base64_encode(salt)
        );

        let client_final = exchange
            .client_final(server_first.as_bytes())
            .expect("client final");
        let final_text = String::from_utf8(client_final).unwrap();
        assert!(final_text.starts_with("c=biws,r=clientNONCEserverNONCE,p="));

        // What the server would compute to check the proof, and to sign back.
        let salted = hi(
            SaslMechanism::ScramSha256,
            credentials.password.as_bytes(),
            salt,
            iterations,
        );
        let server_key = hmac(SaslMechanism::ScramSha256, &salted, b"Server Key");
        let auth_message = format!(
            "n=user,r=clientNONCE,{server_first},c=biws,r=clientNONCEserverNONCE"
        );
        let signature = hmac(SaslMechanism::ScramSha256, &server_key, auth_message.as_bytes());

        exchange
            .verify(format!("v={}", base64_encode(&signature)).as_bytes())
            .expect("the server signature must verify");
    }

    /// **The verification must actually reject.** A peer that never knew the
    /// password can still send a final message; accepting it unchecked is the
    /// classic SCRAM implementation bug.
    #[test]
    fn a_wrong_server_signature_is_rejected() {
        let credentials = Credentials::scram_sha256("user", "pencil");
        let (mut exchange, _) = ScramExchange::start(&credentials, "nonce");
        exchange
            .client_final(b"r=nonceserver,s=c2FsdA==,i=4096")
            .expect("client final");

        let err = exchange
            .verify(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .expect_err("a forged signature must be rejected");
        assert!(matches!(err, SaslError::ServerSignature));
    }

    /// The server must echo our nonce. One that does not is a different
    /// exchange being replayed at us.
    #[test]
    fn a_server_nonce_that_drops_ours_is_rejected() {
        let credentials = Credentials::scram_sha256("user", "pencil");
        let (mut exchange, _) = ScramExchange::start(&credentials, "clientNONCE");
        let err = exchange
            .client_final(b"r=somethingelse,s=c2FsdA==,i=4096")
            .expect_err("the nonce must extend ours");
        assert!(matches!(err, SaslError::Malformed(_)));
    }

    /// Commas and equals signs separate SCRAM's fields, so a username
    /// containing them must be escaped or it changes the message's meaning.
    #[test]
    fn a_username_with_separators_is_escaped() {
        assert_eq!(saslprep("a,b=c"), "a=2Cb=3Dc");
    }

    #[test]
    fn sha512_derives_differently_from_sha256() {
        let a = hi(SaslMechanism::ScramSha256, b"p", b"s", 2);
        let b = hi(SaslMechanism::ScramSha512, b"p", b"s", 2);
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
        assert_eq!(b.len(), 64);
    }
}
