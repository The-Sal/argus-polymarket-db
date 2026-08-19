/*
Tailnet Sync (argus-polymarket-db)

This is a builtin server that allows every argus-polymarket-db across an entire tailnet mesh to coordinate with one another and allow
fast sharing of data. When a fresh instance is initiated, it can query the entire tailnet for databases and pull from the latest available
database. It can then check that the database's last refresh time is within this db's refresh window. If it is, it will clone that mesh db's
data here, otherwise it will build the database from scratch.

Discovery is done automatically by querying tailscale status --json iterating for all the Peers and their AllowedIPs, then
attempting to connect to all IPs at the fixed db port and request product number checking if the correct services are running.
There is no authentication within this system as the tailnet is assumed to be secure, moreover, there is no remote code execution risk
because the database is never executed its json-derivative.

Raw databases can range ~1-2GB on disk, during transmission they are automatically compressed with level 9 compression.

 */
use std::process::Command;

pub(crate) struct TailnetFns {}

impl TailnetFns {
    pub(crate) fn tailscale_available() -> bool{
        let cmd = Command::new("tailscale").arg("status").output();
        if cmd.is_err() {
            return false;
        }
        true
    }

    pub(crate) fn get_my_address() -> Option<String>{
        let cmd = Command::new("tailscale").arg("ip").arg("-4").output();
        if cmd.is_ok() {
            let output = cmd.unwrap();
            let output_str = String::from_utf8_lossy(&output.stdout);
            return Some(output_str.trim().to_string());
        }
        None
    }

    pub(crate) fn tailscale_status() -> Option<serde_json::Value>{
        let cmd = Command::new("tailscale").arg("status").arg("--json").output();
        if cmd.is_err() {
            return None;
        }
        let output = cmd.unwrap();
        let output_str = String::from_utf8_lossy(&output.stdout);
        // Malformed/empty output (e.g. not logged in, daemon not running)
        // must not panic — this is now reachable from main()'s boot path via
        // mesh_sync, which needs "tailscale is unusable" to just mean
        // "no peers," never a crash.
        serde_json::from_str(&output_str).ok()
    }

    pub(crate) fn get_peers() -> Result<Vec<String>, ()>{
        let status = TailnetFns::tailscale_status();
        if status.is_none() {
            return Err(());
        }
        let status_json = status.unwrap();
        // `Peer` can be legitimately absent (a tailnet with no other nodes
        // yet) or shaped unexpectedly by a `tailscale` CLI version this
        // wasn't tested against — either way, mesh_sync's boot path must see
        // "no peers," not a panic that takes the whole daemon down before it
        // ever reaches the local crawl fallback.
        let peers_dict = match status_json.get("Peer").and_then(|v| v.as_object()) {
            Some(d) => d,
            None => return Ok(vec![]),
        };
        // Peers with the local node's own address must never be dialed as if
        // they were a remote peer — the hostname skiplist below doesn't cover
        // every way the local node could show up in `Peer` (e.g. a subnet
        // router advertising the same address), so this is a second,
        // address-based guard.
        let my_address = TailnetFns::get_my_address();
        let mut peers_vec: Vec<String> = vec![];
        for (_, peer_json) in peers_dict {
            let Some(host_name) = peer_json.get("HostName").and_then(|v| v.as_str()) else {
                continue;
            };
            if vec!["funnel-ingress-node", "ip-172-31-20-232", "localhost"].contains(&host_name) {
                continue;
            }
            let Some(allowed_ips_array) = peer_json.get("AllowedIPs").and_then(|v| v.as_array()) else {
                continue;
            };
            let allowed_ip: Vec<&str> = allowed_ips_array
                .iter()
                .filter_map(|ip| ip.as_str())
                .filter(|ip| ip.contains("/32"))
                .collect();
            if allowed_ip.len() == 1 {
                // `AllowedIPs` entries are CIDRs (e.g. "100.64.1.2/32");
                // `get_my_address()` and every socket-address consumer here
                // expect a bare IP, so the suffix must be stripped before the
                // address leaves this function.
                let bare_ip = allowed_ip[0].split('/').next().unwrap_or(allowed_ip[0]).to_string();
                if my_address.as_deref() == Some(bare_ip.as_str()) {
                    continue;
                }
                println!("Peer {} has IP {}", host_name, bare_ip);
                peers_vec.push(bare_ip);
            }
        }

        Ok(peers_vec)
    }


}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub(crate) fn test_tailscale_available(){
        assert!(TailnetFns::tailscale_available());
        println!("Tailscale is available");
    }
    #[test]
    pub(crate) fn test_get_my_address(){
        let my_address = TailnetFns::get_my_address();
        if my_address.is_some() {
            println!("My address is {}", my_address.unwrap());
        }else{
            panic!("Tailscale is not available");
        }
    }
    #[test]
    pub(crate) fn test_tailscale_status(){
        let status = TailnetFns::tailscale_status();
        assert!(status.is_some());
        println!("Tailscale status: {}", status.unwrap());
    }
    #[test]
    pub(crate) fn test_get_peers(){
        let peers = TailnetFns::get_peers();
        match peers {
            Ok(peers_vec) => {
                if peers_vec.len() == 0 {
                    panic!("No peers found");
                }
            }
            Err(_) => {
                panic!("Failed to get peers");
            }
        }
    }
}