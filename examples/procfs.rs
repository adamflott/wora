use wora::o11y::{Host, HostInfo};

fn main() {
    let h = Host::new().unwrap();
    println!("{:?}", h.stats().cpu());

    let hi = HostInfo::new(h.sys()).unwrap();
    println!("{:?}", hi.os_type());
    #[cfg(target_os = "linux")]
    println!("{:?}", hi.current_process_tcp());
    #[cfg(target_os = "linux")]
    println!("{:?}", hi.current_process_udp());
}
