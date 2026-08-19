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
use serde_json::Value;
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
        let output_json: serde_json::Value = serde_json::from_str(&output_str).unwrap();
        Some(output_json)
    }

    pub(crate) fn get_peers() -> Result<Vec<String>, ()>{
        let status = TailnetFns::tailscale_status();
        if status.is_none() {
            return Err(());
        }
        let status_json = status.unwrap();
        let peers_dict = status_json.get("Peer").unwrap().as_object().unwrap();
        let mut peers_vec: Vec<String> = vec![];
        for (_, peer_json) in peers_dict {
            let host_name = peer_json.get("HostName").unwrap().as_str().unwrap();
            if vec!["funnel-ingress-node", "ip-172-31-20-232", "localhost"].contains(&host_name) {
                continue;
            }
            let allowed_ips_array = peer_json.get("AllowedIPs").unwrap().as_array().unwrap();
            let allowed_ip = allowed_ips_array.iter().filter(|ip| ip.as_str().unwrap().contains("/32")).collect::<Vec<&Value>>();
            if allowed_ip.len() == 1 {
                println!("Peer {} has IP {}", host_name, allowed_ip[0].as_str().unwrap());
                peers_vec.push(allowed_ip[0].as_str().unwrap().to_string());
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