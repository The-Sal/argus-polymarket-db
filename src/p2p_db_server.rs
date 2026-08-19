use crate::tailnet_fns::TailnetFns;
use std::net::TcpListener;

struct P2pDbServer {
    port: u16,
    db_path: String,
    listener: TcpListener,
}

impl P2pDbServer {
    fn new(port: u16, db_path: String) -> P2pDbServer {
        let bind_addr = format!(
            "{}:{}",
            TailnetFns::get_my_address().expect("Failed to get my tailscale address"),
            port
        );
        let listener = TcpListener::bind(&bind_addr).unwrap();
        P2pDbServer {
            port,
            db_path,
            listener,
        }
    }
}
