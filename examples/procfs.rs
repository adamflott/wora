use wora::o11y::{Host, HostInfo};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let h = Host::new()?;
    println!("{:?}", h.stats().cpu());

    let hi = HostInfo::new(h.sys())?;
    println!("{:?}", hi.os_type());
    #[cfg(target_os = "linux")]
    println!("{:?}", hi.current_process_tcp());
    #[cfg(target_os = "linux")]
    println!("{:?}", hi.current_process_udp());

    Ok(())
}
