use crate::error::{CoreError, Result};
use quinn::{ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

/// QUIC transport for LAN and internet peer connections.
///
/// TLS is used only as QUIC's required transport-layer wrapper; it uses
/// a self-signed cert generated fresh per process and we skip standard
/// certificate-chain verification entirely (`SkipServerVerification`
/// below). Actual peer identity verification happens one layer up, via
/// the Noise_XX handshake in `noise_session.rs` and the out-of-band
/// safety-code comparison during pairing. Do not rely on this QUIC/TLS
/// layer for authentication -- it exists purely to get an encrypted,
/// multiplexed, congestion-controlled pipe between two known IPs.
pub struct QuicTransport {
    pub endpoint: Endpoint,
    pub local_addr: SocketAddr,
}

impl QuicTransport {
    /// Bind a QUIC endpoint that can both accept incoming connections
    /// (server role) and dial out to peers (client role). Binding to
    /// port 0 lets the OS pick a free port; the actual bound port is
    /// what gets advertised via mDNS/UDP broadcast in discovery.rs.
    pub fn bind(bind_addr: &str) -> Result<Self> {
        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| CoreError::InvalidState(format!("invalid bind addr: {e}")))?;

        let (server_config, _cert_der) = build_self_signed_server_config()?;
        let mut endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| CoreError::InvalidState(format!("quic endpoint bind failed: {e}")))?;

        endpoint.set_default_client_config(build_insecure_client_config()?);

        let local_addr = endpoint
            .local_addr()
            .map_err(|e| CoreError::InvalidState(format!("could not read local addr: {e}")))?;

        Ok(Self {
            endpoint,
            local_addr,
        })
    }

    /// Connect out to a peer at the given address (learned via discovery).
    /// The "server_name" passed to quinn is unused for verification since
    /// we skip cert validation, but quinn's API requires one -- we pass
    /// the device_id so it shows up in logs/diagnostics.
    pub async fn connect(&self, addr: SocketAddr, peer_device_id: &str) -> Result<quinn::Connection> {
        let connecting = self
            .endpoint
            .connect(addr, peer_device_id)
            .map_err(|e| CoreError::InvalidState(format!("quic connect setup failed: {e}")))?;
        connecting
            .await
            .map_err(|e| CoreError::InvalidState(format!("quic connect failed: {e}")))
    }

    /// Accept the next incoming connection. Call in a loop from a
    /// background task to handle multiple simultaneous peers.
    pub async fn accept(&self) -> Option<Result<quinn::Connection>> {
        let incoming = self.endpoint.accept().await?;
        Some(
            incoming
                .await
                .map_err(|e| CoreError::InvalidState(format!("quic accept failed: {e}"))),
        )
    }
}

/// Open a new bidirectional stream for one logical transfer (e.g. one
/// chunk-range worker). Each parallel chunk worker gets its own stream --
/// QUIC multiplexes them over the same connection/congestion-control
/// state, which is what gives us parallel chunk throughput without
/// opening multiple sockets.
pub async fn open_stream(conn: &quinn::Connection) -> Result<(SendStream, RecvStream)> {
    conn.open_bi()
        .await
        .map_err(|e| CoreError::InvalidState(format!("failed to open stream: {e}")))
}

pub async fn accept_stream(conn: &quinn::Connection) -> Result<(SendStream, RecvStream)> {
    conn.accept_bi()
        .await
        .map_err(|e| CoreError::InvalidState(format!("failed to accept stream: {e}")))
}

fn build_self_signed_server_config() -> Result<(ServerConfig, Vec<u8>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["zaop2p.local".to_string()])
        .map_err(|e| CoreError::Crypto(format!("cert gen failed: {e}")))?;
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.key_pair.serialize_der();

    let cert_chain = vec![cert.cert.der().clone()];
    let priv_key = rustls::pki_types::PrivateKeyDer::try_from(key_der)
        .map_err(|e| CoreError::Crypto(format!("key parse failed: {e:?}")))?;

    let server_config = ServerConfig::with_single_cert(cert_chain, priv_key)
        .map_err(|e| CoreError::Crypto(format!("server config failed: {e}")))?;

    Ok((server_config, cert_der))
}

/// Skips certificate verification entirely. Safe in this design ONLY
/// because real peer authentication happens via Noise_XX + safety-code
/// pairing at the application layer above this transport. This transport
/// must never be used standalone as a security boundary.
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn build_insecure_client_config() -> Result<ClientConfig> {
    let mut tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"zaop2p".to_vec()];

    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
        .map_err(|e| CoreError::Crypto(format!("quic client config failed: {e}")))?;

    Ok(ClientConfig::new(Arc::new(quic_client_config)))
}
