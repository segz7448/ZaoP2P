use crate::error::{CoreError, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The mDNS service type all Zao devices advertise under.
/// Format follows RFC 6763: `_servicename._proto.local.`
const SERVICE_TYPE: &str = "_zaop2p._tcp.local.";

/// UDP broadcast fallback port, used when mDNS/multicast is blocked by
/// the network (some corporate/guest WiFi and some Android configs
/// restrict multicast). Broadcast (255.255.255.255) is more likely to
/// get through than multicast in those cases.
const UDP_BROADCAST_PORT: u16 = 51820;
const UDP_MAGIC: &[u8] = b"ZAOP2P1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    pub device_id: String,
    pub display_name: String,
    pub addr: String, // "ip:port" -- the QUIC listen address
    pub via: DiscoveryMethod,
    pub last_seen_unix: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiscoveryMethod {
    Mdns,
    UdpBroadcast,
}

/// Holds live discovery state. One instance per running app.
/// Wraps both discovery mechanisms behind a single peer list so the
/// Transport layer doesn't need to know which one found a given peer.
pub struct Discovery {
    mdns: ServiceDaemon,
    peers: Arc<Mutex<HashMap<String, DiscoveredPeer>>>,
    service_type: String,
}

impl Discovery {
    /// Start advertising this device and browsing for others.
    /// `device_id` / `display_name` are advertised in mDNS TXT records.
    /// `quic_port` is the port this device's QUIC listener is bound to --
    /// peers will connect back to `<their_ip>:<quic_port>`.
    pub fn start(device_id: &str, display_name: &str, quic_port: u16) -> Result<Self> {
        let mdns = ServiceDaemon::new()
            .map_err(|e| CoreError::InvalidState(format!("mdns daemon start failed: {e}")))?;

        let mut properties = HashMap::new();
        properties.insert("device_id".to_string(), device_id.to_string());
        properties.insert("display_name".to_string(), display_name.to_string());

        // "." host name is resolved by mdns-sd from local interfaces;
        // we pass 0.0.0.0 sentinel semantics via my_ip lookups internally.
        let instance_name = device_id;
        let host_name = format!("{}.local.", device_id);

        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            instance_name,
            &host_name,
            "", // empty IP list = let mdns-sd auto-detect local interface IPs
            quic_port,
            Some(properties),
        )
        .map_err(|e| CoreError::InvalidState(format!("mdns service info failed: {e}")))?
        .enable_addr_auto();

        mdns.register(service_info)
            .map_err(|e| CoreError::InvalidState(format!("mdns register failed: {e}")))?;

        let peers = Arc::new(Mutex::new(HashMap::new()));

        // Spawn the mDNS browse loop.
        let receiver = mdns
            .browse(SERVICE_TYPE)
            .map_err(|e| CoreError::InvalidState(format!("mdns browse failed: {e}")))?;
        let peers_clone = peers.clone();
        let self_device_id = device_id.to_string();
        std::thread::spawn(move || {
            while let Ok(event) = receiver.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    if let Some(peer) = peer_from_service_info(&info, DiscoveryMethod::Mdns) {
                        if peer.device_id != self_device_id {
                            peers_clone
                                .lock()
                                .unwrap()
                                .insert(peer.device_id.clone(), peer);
                        }
                    }
                }
            }
        });

        // Spawn the UDP broadcast fallback (both announcing and listening).
        spawn_udp_broadcast_announcer(device_id.to_string(), display_name.to_string(), quic_port);
        spawn_udp_broadcast_listener(peers.clone(), device_id.to_string());

        Ok(Self {
            mdns,
            peers,
            service_type: SERVICE_TYPE.to_string(),
        })
    }

    /// Snapshot of currently known peers, pruned of stale entries
    /// (not seen in the last 30 seconds).
    pub fn known_peers(&self) -> Vec<DiscoveredPeer> {
        let now = unix_now();
        let mut guard = self.peers.lock().unwrap();
        guard.retain(|_, p| now.saturating_sub(p.last_seen_unix) < 30);
        guard.values().cloned().collect()
    }

    pub fn stop(&self) -> Result<()> {
        self.mdns
            .shutdown()
            .map_err(|e| CoreError::InvalidState(format!("mdns shutdown failed: {e}")))?;
        Ok(())
    }
}

fn peer_from_service_info(info: &ServiceInfo, via: DiscoveryMethod) -> Option<DiscoveredPeer> {
    let device_id = info.get_property_val_str("device_id")?.to_string();
    let display_name = info
        .get_property_val_str("display_name")
        .unwrap_or("Unknown Device")
        .to_string();
    let ip = info.get_addresses().iter().next()?;
    let addr = SocketAddr::new(*ip, info.get_port());

    Some(DiscoveredPeer {
        device_id,
        display_name,
        addr: addr.to_string(),
        via,
        last_seen_unix: unix_now(),
    })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Serialize, Deserialize)]
struct BroadcastAnnouncement {
    device_id: String,
    display_name: String,
    port: u16,
}

fn spawn_udp_broadcast_announcer(device_id: String, display_name: String, quic_port: u16) {
    std::thread::spawn(move || {
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(s) => s,
            Err(_) => return, // no UDP available; mDNS-only for this device
        };
        let _ = socket.set_broadcast(true);

        let announcement = BroadcastAnnouncement {
            device_id,
            display_name,
            port: quic_port,
        };
        let payload = match serde_json::to_vec(&announcement) {
            Ok(p) => p,
            Err(_) => return,
        };

        let mut packet = UDP_MAGIC.to_vec();
        packet.extend_from_slice(&payload);

        let broadcast_addr: SocketAddr = ([255, 255, 255, 255], UDP_BROADCAST_PORT).into();

        loop {
            let _ = socket.send_to(&packet, broadcast_addr);
            std::thread::sleep(Duration::from_secs(5));
        }
    });
}

fn spawn_udp_broadcast_listener(peers: Arc<Mutex<HashMap<String, DiscoveredPeer>>>, self_id: String) {
    std::thread::spawn(move || {
        let socket = match UdpSocket::bind(("0.0.0.0", UDP_BROADCAST_PORT)) {
            Ok(s) => s,
            Err(_) => return, // port in use or blocked; mDNS-only fallback path
        };
        let mut buf = [0u8; 2048];

        loop {
            let (len, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if len <= UDP_MAGIC.len() || &buf[..UDP_MAGIC.len()] != UDP_MAGIC {
                continue; // not our protocol, ignore silently
            }
            let payload = &buf[UDP_MAGIC.len()..len];
            let announcement: BroadcastAnnouncement = match serde_json::from_slice(payload) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if announcement.device_id == self_id {
                continue;
            }

            let ip: IpAddr = src.ip();
            let addr = SocketAddr::new(ip, announcement.port);
            let peer = DiscoveredPeer {
                device_id: announcement.device_id.clone(),
                display_name: announcement.display_name,
                addr: addr.to_string(),
                via: DiscoveryMethod::UdpBroadcast,
                last_seen_unix: unix_now(),
            };

            let mut guard = peers.lock().unwrap();
            // Prefer mDNS-sourced entries if we already have one for this
            // peer; UDP broadcast is only the fallback/supplement.
            guard
                .entry(announcement.device_id)
                .and_modify(|existing| {
                    if existing.via != DiscoveryMethod::Mdns {
                        *existing = peer.clone();
                    } else {
                        existing.last_seen_unix = unix_now();
                    }
                })
                .or_insert(peer);
        }
    });
}
