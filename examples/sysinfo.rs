use sysinfo::{Disks, Networks, System};

fn main() {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let osinfo = os_info::get();
    println!("name: {:?}", System::name());
    println!("os version: {:?}", System::os_version());
    println!("long os version: {:?}", System::long_os_version());
    println!("kernel version: {:?}", System::kernel_version());
    println!("distribution id: {:?}", System::distribution_id());
    println!("cpus: {:?}", sys.cpus());
    println!("cpu count: {:?}", sys.cpus().len());
    for cpu in sys.cpus() {
        println!(
            "cpu name:{} brand:{} freq:{} usage:{:?}",
            cpu.name(),
            cpu.brand(),
            cpu.frequency(),
            cpu.cpu_usage()
        );
    }
    println!("physical core count: {:?}", sys.physical_core_count());
    println!("architecture: {:?}", osinfo.architecture());
    println!("os type: {:?}", osinfo.os_type());
    println!("os version: {:?}", osinfo.version());
    println!("os edition: {:?}", osinfo.edition());
    println!("os code name: {:?}", osinfo.codename());
    println!("os bitness: {:?}", osinfo.bitness());
    let disks = Disks::new_with_refreshed_list();
    for disk in &disks {
        println!(
            "{:?}: {:?} {:?} {:?} {:?}",
            disk.name(),
            disk.kind(),
            disk.available_space(),
            disk.file_system(),
            disk.mount_point()
        );
    }
    let networks = Networks::new_with_refreshed_list();
    println!("{:?}", &networks);
    for (n, d) in &networks {
        println!("Network: {:?} {:?}", n, d);
        println!("  MAC: {:?}", d.mac_address());
    }
    for (pid, process) in sys.processes() {
        let disk_usage = process.disk_usage();
        println!(
            "pid [{}] read bytes   : new/total => {}/{} B",
            pid, disk_usage.read_bytes, disk_usage.total_read_bytes,
        );
        println!(
            "pid [{}] written bytes: new/total => {}/{} B",
            pid, disk_usage.written_bytes, disk_usage.total_written_bytes,
        );
    }
}
