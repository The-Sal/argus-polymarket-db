use std::thread;
use std::sync::OnceLock;
use std::time::Duration;
use ureq::{Agent, Proxy};

static AGENT: OnceLock<Agent> = OnceLock::new();

fn is_null_disabled() -> bool{
    let null_disabled = std::env::var("NULL_DISABLED").unwrap_or_else(|_| "false".to_string());
    null_disabled.parse::<bool>().unwrap()
}

fn proxy_addrs()  -> Vec<String>{
    _ = dotenvy::from_filename(".env");
    let socks5_addrs = std::env::var("SOCKS5_ADDRS").unwrap_or_else(|_| "".to_string());
    let mut addrs = socks5_addrs.split(',').map(|s| s.to_string()).collect::<Vec<String>>();
    addrs.push("null".to_string());
    addrs.insert(0, "null".to_string());
    if is_null_disabled(){
        addrs.remove(0);
    }

    addrs
}

fn find_proxy_addr(proxy_addrs: Vec<String>) -> String{
    let addr_to_check = "https://polymarket.com/api/geoblock";
    let (tx, rx) = std::sync::mpsc::channel::<String>();

    for addr in proxy_addrs{
        let tx = tx.clone();
        _ = thread::spawn(move || {
            let proxy: Option<Proxy> = match addr.as_str(){
                "null" => None,
                _ => Some(Proxy::new(&addr).unwrap())
            };

            let agent: Agent = Agent::config_builder()
                .proxy(proxy)
                .build()
                .into();

            println!("Checking {}", addr);
            let response = agent.get(addr_to_check).call();
            if let Ok(mut response) = response {
                let string_body = response.body_mut().read_to_string().unwrap();
                println!("{}: {}", addr, string_body);
                tx.send(addr).unwrap();
                return
            } else {
                return
            }
        })
    }

    let addr = rx.recv_timeout(Duration::from_secs(5));
    if addr.is_err(){
        println!("No proxy address found");
        "null".to_string()
    }else{
        let unwrapped_addr = addr.unwrap();
        unwrapped_addr
    }

}

pub(crate) fn get_proxy_agent() -> Agent{
    AGENT.get_or_init(|| {
        let proxy_addr = find_proxy_addr(proxy_addrs());
        let proxy: Option<Proxy>;
        match proxy_addr.as_str() {
            "null" => {proxy = None;},
            _ => {proxy = Some(Proxy::new(&proxy_addr).unwrap());}
        }
        let agent: Agent = Agent::config_builder()
            .proxy(proxy)
            .build()
            .into();
        agent
    }).clone()
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proxy_addrs_test(){
        let addrs = proxy_addrs();
        for addr in addrs{
            println!("{}", addr);
        }
    }

    #[test]
    fn find_proxy_addr_test(){
        let proxy_addrs = proxy_addrs();
        find_proxy_addr(proxy_addrs);
        println!("Agent {:?}", get_proxy_agent());
    }

    #[test]
    fn find_proxy_addr_no_null(){
        unsafe { std::env::set_var("NULL_DISABLED", "true"); }
        let addrs = proxy_addrs();
        find_proxy_addr(addrs);
        let agent = get_proxy_agent();
        println!("Agent {:?}", agent);
        if agent.config().proxy().is_none(){
            // this is an error
            assert!(false);
        }
    }
}