use crate::error::{CoreError, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::UdpSocket;
use tokio::time::{timeout, Duration};

/// Minimal STUN (RFC 5389) client -- just enough to send a Binding
/// Request and parse the XOR-MAPPED-ADDRESS attribute out of the
/// response, which is all "discover my public IP:port" needs. Hand-rolled
/// rather than depending on a STUN crate: the wire format is a fixed
/// 20-byte header plus TLV attributes, small enough to implement
/// correctly and reviewably here, and this avoids taking on an
/// additional crate's exact API surface/version risk in an environment
/// where I can't compile to verify it.
///
/// Public STUN servers (Google's, widely used and free) are queried by
/// default -- these are commodity infrastructure used the same way by
/// most WebRTC/VoIP software; not Zao-specific servers.
const DEFAULT_STUN_SERVERS: &[&str] = &[
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
];

const STUN_MAGIC_COOKIE: u32 = 0x2112A442;
const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_BINDING_RESPONSE: u16 = 0x0101;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001; // older/alternate attribute some servers still send

/// Query a STUN server to discover the public IP:port this device's
/// locally-bound UDP socket maps to through any NAT in front of it.
/// Tries each server in `DEFAULT_STUN_SERVERS` in turn, returning the
/// first successful response -- a single unreachable STUN server
/// (blocked port, transient DNS failure) shouldn't block internet mode
/// entirely.
pub async fn discover_public_address(local_socket: &UdpSocket) -> Result<SocketAddr> {
    let mut last_error = None;
    for server in DEFAULT_STUN_SERVERS {
        match query_stun_server(local_socket, server).await {
            Ok(addr) => return Ok(addr),
            Err(e) => last_error = Some(e),
        }
    }
    Err(last_error.unwrap_or_else(|| CoreError::InvalidState("no STUN servers configured".into())))
}

async fn query_stun_server(local_socket: &UdpSocket, server: &str) -> Result<SocketAddr> {
    let server_addr = tokio::net::lookup_host(server)
        .await
        .map_err(|e| CoreError::InvalidState(format!("STUN DNS lookup failed: {e}")))?
        .next()
        .ok_or_else(|| CoreError::InvalidState(format!("no address resolved for {server}")))?;

    let transaction_id = generate_transaction_id();
    let request = build_binding_request(&transaction_id);

    local_socket
        .send_to(&request, server_addr)
        .await
        .map_err(CoreError::Io)?;

    let mut buf = [0u8; 512];
    let (len, _from) = timeout(Duration::from_secs(3), local_socket.recv_from(&mut buf))
        .await
        .map_err(|_| CoreError::InvalidState("STUN request timed out".into()))?
        .map_err(CoreError::Io)?;

    parse_binding_response(&buf[..len], &transaction_id)
}

fn generate_transaction_id() -> [u8; 12] {
    use rand::Rng;
    let mut id = [0u8; 12];
    rand::thread_rng().fill(&mut id);
    id
}

fn build_binding_request(transaction_id: &[u8; 12]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(20);
    msg.extend_from_slice(&STUN_BINDING_REQUEST.to_be_bytes());
    msg.extend_from_slice(&0u16.to_be_bytes()); // message length: no attributes in the request
    msg.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    msg.extend_from_slice(transaction_id);
    msg
}

fn parse_binding_response(data: &[u8], expected_transaction_id: &[u8; 12]) -> Result<SocketAddr> {
    if data.len() < 20 {
        return Err(CoreError::InvalidState("STUN response too short".into()));
    }

    let message_type = u16::from_be_bytes([data[0], data[1]]);
    if message_type != STUN_BINDING_RESPONSE {
        return Err(CoreError::InvalidState(format!(
            "unexpected STUN message type: {message_type:#06x}"
        )));
    }

    let message_length = u16::from_be_bytes([data[2], data[3]]) as usize;
    let magic_cookie = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    if magic_cookie != STUN_MAGIC_COOKIE {
        return Err(CoreError::InvalidState("STUN magic cookie mismatch".into()));
    }

    let transaction_id = &data[8..20];
    if transaction_id != expected_transaction_id {
        return Err(CoreError::InvalidState(
            "STUN transaction ID mismatch (response to a different request?)".into(),
        ));
    }

    let attrs_end = (20 + message_length).min(data.len());
    let mut offset = 20;
    let mut mapped_address = None;
    let mut xor_mapped_address = None;

    // Walk the TLV attribute list: [type: u16][length: u16][value, padded to 4 bytes]
    while offset + 4 <= attrs_end {
        let attr_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let attr_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        let value_start = offset + 4;
        let value_end = value_start + attr_len;
        if value_end > data.len() {
            break; // malformed/truncated attribute, stop parsing defensively
        }
        let value = &data[value_start..value_end];

        match attr_type {
            ATTR_XOR_MAPPED_ADDRESS => {
                if let Some(addr) = parse_xor_mapped_address(value, expected_transaction_id) {
                    xor_mapped_address = Some(addr);
                }
            }
            ATTR_MAPPED_ADDRESS => {
                if let Some(addr) = parse_mapped_address(value) {
                    mapped_address = Some(addr);
                }
            }
            _ => {}
        }

        // Attributes are padded to a 4-byte boundary.
        let padded_len = attr_len.div_ceil(4) * 4;
        offset = value_start + padded_len;
    }

    xor_mapped_address
        .or(mapped_address)
        .ok_or_else(|| CoreError::InvalidState("STUN response had no mapped address".into()))
}

/// XOR-MAPPED-ADDRESS: same layout as MAPPED-ADDRESS, but the port and
/// address are XORed with the magic cookie (and transaction ID, for
/// IPv6) to survive certain NAT/ALG address rewriting. IPv4-only here --
/// sufficient for LAN/home-NAT scenarios this app targets first.
fn parse_xor_mapped_address(value: &[u8], transaction_id: &[u8; 12]) -> Option<SocketAddr> {
    if value.len() < 8 || value[1] != 0x01 {
        return None; // family byte must be 0x01 (IPv4); IPv6 not handled here
    }
    let cookie_bytes = STUN_MAGIC_COOKIE.to_be_bytes();

    let xport = u16::from_be_bytes([value[2], value[3]]);
    let port = xport ^ u16::from_be_bytes([cookie_bytes[0], cookie_bytes[1]]);

    let mut addr_bytes = [0u8; 4];
    for i in 0..4 {
        let xor_byte = if i < 4 {
            cookie_bytes[i]
        } else {
            transaction_id[i - 4]
        };
        addr_bytes[i] = value[4 + i] ^ xor_byte;
    }
    let ip = Ipv4Addr::from(addr_bytes);
    Some(SocketAddr::new(IpAddr::V4(ip), port))
}

fn parse_mapped_address(value: &[u8]) -> Option<SocketAddr> {
    if value.len() < 8 || value[1] != 0x01 {
        return None;
    }
    let port = u16::from_be_bytes([value[2], value[3]]);
    let ip = Ipv4Addr::new(value[4], value[5], value[6], value[7]);
    Some(SocketAddr::new(IpAddr::V4(ip), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_request_has_correct_header() {
        let tid = [1u8; 12];
        let req = build_binding_request(&tid);
        assert_eq!(req.len(), 20);
        assert_eq!(u16::from_be_bytes([req[0], req[1]]), STUN_BINDING_REQUEST);
        assert_eq!(
            u32::from_be_bytes([req[4], req[5], req[6], req[7]]),
            STUN_MAGIC_COOKIE
        );
        assert_eq!(&req[8..20], &tid);
    }

    #[test]
    fn parse_xor_mapped_address_roundtrip() {
        // Construct a synthetic XOR-MAPPED-ADDRESS attribute value for
        // 192.0.2.1:12345 and verify it decodes back correctly.
        let tid = [0u8; 12];
        let cookie_bytes = STUN_MAGIC_COOKIE.to_be_bytes();
        let real_port: u16 = 12345;
        let real_ip = [192, 0, 2, 1];

        let xport = real_port ^ u16::from_be_bytes([cookie_bytes[0], cookie_bytes[1]]);
        let mut value = vec![0u8, 0x01];
        value.extend_from_slice(&xport.to_be_bytes());
        for i in 0..4 {
            value.push(real_ip[i] ^ cookie_bytes[i]);
        }

        let addr = parse_xor_mapped_address(&value, &tid).unwrap();
        assert_eq!(addr, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 12345));
    }

    #[test]
    fn rejects_wrong_message_type() {
        let mut data = vec![0u8; 20];
        data[0..2].copy_from_slice(&0x0002u16.to_be_bytes()); // not a binding response
        let result = parse_binding_response(&data, &[0u8; 12]);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_transaction_id_mismatch() {
        let mut data = vec![0u8; 20];
        data[0..2].copy_from_slice(&STUN_BINDING_RESPONSE.to_be_bytes());
        data[4..8].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        data[8..20].copy_from_slice(&[9u8; 12]); // different transaction id
        let result = parse_binding_response(&data, &[0u8; 12]);
        assert!(result.is_err());
    }
}
