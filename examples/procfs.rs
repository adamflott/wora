use wora::metrics::{Host, HostInfo};

fn main() {
    let h = Host::new().unwrap();
    println!("{:?}", h.stats().cpu());

    let hi = HostInfo::new(h.sys()).unwrap();
    println!("{:?}", hi.os_type());
    println!("{:?}", hi.current_process_tcp());
    println!("{:?}", hi.current_process_udp());
}
