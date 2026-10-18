use super::linux_mm::{MemoryTraceParser, MemoryTraceRecord, MemoryTracepoint};
use super::{O11yEvent, O11yEventKind, O11yPipeline, O11yService, O11yServiceState, O11yServiceStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc::Sender;

/// Explicit opt-in configuration for an owned tracefs memory-event stream.
///
/// Requires suitable tracefs privileges and existing kernel tracing support.
/// Creates a private instance, uses the monotonic trace clock, enables selected
/// events there, and removes the instance on normal shutdown or task cancellation.
/// No tracefs mount or global tracing settings are changed. SIGKILL/crashes can
/// leave a named instance behind; its path is emitted in the startup record.
/// Records are read from `<tracefs_root>/instances/<owned-instance>/trace_pipe`;
/// [`MemoryTracepoint::events`] lists the kernel event paths. Trace payloads
/// retain native units and timestamps use monotonic trace-clock nanoseconds.
#[derive(Clone, Debug)]
pub struct LinuxMemoryTraceOptions {
    /// Existing tracefs mount, default `/sys/kernel/tracing`.
    pub tracefs_root: PathBuf,
    /// Selected event families. All four supported families are selected by default.
    pub events: BTreeSet<MemoryTracepoint>,
    /// Per-CPU ring buffer size in KiB, default 64; valid range 1..=4096.
    pub buffer_size_kib: u32,
}

impl Default for LinuxMemoryTraceOptions {
    fn default() -> Self {
        Self {
            tracefs_root: "/sys/kernel/tracing".into(),
            events: BTreeSet::from([
                MemoryTracepoint::OomVictim,
                MemoryTracepoint::DirectReclaim,
                MemoryTracepoint::Compaction,
                MemoryTracepoint::Migration,
            ]),
            buffer_size_kib: 64,
        }
    }
}

impl LinuxMemoryTraceOptions {
    /// Configure all supported memory event families; performs no I/O.
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.events.is_empty() || !(1..=4096).contains(&self.buffer_size_kib) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory tracing requires events and a 1..=4096 KiB per-CPU buffer",
            ));
        }
        Ok(())
    }
}

impl<T: Send + Sync + 'static> O11yPipeline<T> {
    /// Attach a runner-owned Linux memory trace service before starting the runner.
    ///
    /// Setup/read failures use existing infrastructure-service failure handling.
    /// Unsupported kernel events are reported in a startup record; at least one
    /// event must be available. Event loss and unrecognized records remain visible.
    pub fn with_linux_memory_trace(mut self, options: LinuxMemoryTraceOptions) -> io::Result<Self> {
        options.validate()?;
        let name = "linux-memory-trace";
        if self.services.iter().any(|service| service.name == name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Linux memory trace service already attached"));
        }
        self.service_statuses.lock().map_err(|_| io::Error::other("service status poisoned"))?.insert(
            name.into(),
            O11yServiceStatus {
                name: name.into(),
                state: O11yServiceState::Pending,
            },
        );
        let sender = self.sender();
        self.services.push(O11yService {
            name: name.into(),
            future: Box::pin(async move { run_trace(options, sender).await.map_err(|e| e.to_string()) }),
        });
        Ok(self)
    }
}

struct TraceInstance {
    path: PathBuf,
}

impl TraceInstance {
    fn create(options: &LinuxMemoryTraceOptions) -> io::Result<Self> {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let path = options
            .tracefs_root
            .join("instances")
            .join(format!("wora-memory-{}-{}", std::process::id(), NONCE.fetch_add(1, Ordering::Relaxed)));
        // Single-syscall ownership acquisition cannot be interrupted between an
        // async mkdir completing on a worker and installing this cleanup guard.
        fs::create_dir(&path)?;
        Ok(Self { path })
    }
}

impl Drop for TraceInstance {
    fn drop(&mut self) {
        // Short synchronous control writes are intentional: a cancelled future
        // must not leave detached blocking writes that re-enable a removed instance.
        let _ = write_control(self.path.join("tracing_on"), "0");
        let _ = write_control(self.path.join("events/enable"), "0");
        if let Err(error) = fs::remove_dir(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "could not remove owned memory trace instance");
        }
    }
}

async fn run_trace<T: Send + Sync + 'static>(options: LinuxMemoryTraceOptions, sender: Sender<O11yEvent<T>>) -> io::Result<()> {
    options.validate()?;
    let instance = TraceInstance::create(&options)?;
    write_control(instance.path.join("tracing_on"), "0")?;
    write_control(instance.path.join("events/enable"), "0")?;
    write_control(instance.path.join("trace_clock"), "mono")?;
    write_control(instance.path.join("buffer_size_kb"), &options.buffer_size_kib.to_string())?;
    let mut enabled = Vec::new();
    let mut unavailable = BTreeMap::new();
    for event in options.events.iter().flat_map(|family| family.events()) {
        let path = instance.path.join("events").join(event).join("enable");
        // Open without create/truncate: only kernel-provided controls qualify.
        match write_control(&path, "1") {
            Ok(()) => enabled.push((*event).into()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                unavailable.insert((*event).into(), error.to_string());
            }
            Err(error) => return Err(error),
        }
    }
    if enabled.is_empty() {
        return Err(io::Error::other("no selected memory tracepoints available"));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(instance.path.join("trace_pipe"))?;
    let reader = register_owned_file(file)?;
    emit(
        &sender,
        MemoryTraceRecord::Started {
            instance: instance.path.display().to_string(),
            enabled,
            unavailable,
        },
    )
    .await?;
    write_control(instance.path.join("tracing_on"), "1")?;
    stream_trace(reader, sender).await
}

async fn emit<T>(sender: &Sender<O11yEvent<T>>, record: MemoryTraceRecord) -> io::Result<()> {
    sender
        .send(O11yEvent {
            timestamp: chrono::Utc::now(),
            kind: O11yEventKind::MemoryTrace(Box::new(record)),
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "memory trace observability channel closed"))
}

async fn stream_trace<T>(reader: AsyncFd<File>, sender: Sender<O11yEvent<T>>) -> io::Result<()> {
    let mut parser = MemoryTraceParser::new();
    let mut pending = Vec::new();
    let mut bytes = [0u8; 16384];
    loop {
        let mut ready = reader.readable().await?;
        let length = match ready.try_io(|reader| {
            let mut file = reader.get_ref();
            file.read(&mut bytes)
        }) {
            Ok(result) => result?,
            Err(_) => continue,
        };
        if length == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "memory trace stream ended"));
        }
        pending.extend_from_slice(&bytes[..length]);
        while let Some(end) = pending.iter().position(|b| *b == b'\n') {
            let line: Vec<_> = pending.drain(..=end).collect();
            let line = std::str::from_utf8(&line).map_err(io::Error::other)?;
            if let Some(record) = parser.parse_line(line).map_err(|e| io::Error::other(format!("{e:?}")))? {
                emit(&sender, record).await?;
            }
        }
        if pending.len() > 65536 {
            return Err(io::Error::other("memory trace line exceeds 64 KiB"));
        }
    }
}

fn write_control(path: impl AsRef<std::path::Path>, value: &str) -> io::Result<()> {
    use std::io::Write;
    let path = path.as_ref();
    OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|mut file| file.write_all(value.as_bytes()))
        .map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", path.display())))
}

// The owned File keeps a stable descriptor until AsyncFd drops it. Tokio's
// replacement generic registration API requires unsafe, which this crate forbids.
#[allow(deprecated, reason = "Owned File guarantees a stable descriptor for the entire registration lifetime")]
fn register_owned_file(file: File) -> io::Result<AsyncFd<File>> {
    AsyncFd::with_interest(file, tokio::io::Interest::READABLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;

    #[tokio::test]
    async fn nonblocking_stream_forwards_records_and_cancels_without_events() -> Result<(), Box<dyn std::error::Error>> {
        let (socket, mut writer) = UnixStream::pair()?;
        socket.set_nonblocking(true)?;
        let file = File::from(OwnedFd::from(socket));
        let reader = register_owned_file(file)?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<O11yEvent<()>>(2);
        let task = tokio::spawn(stream_trace(reader, sender));
        writer.write_all(b" trigger-9 [000] .... 13.000001: mark_victim: pid=77\nCPU:0 [LOST 5 EVENTS]\n")?;
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await?
            .ok_or("no event")?;
        assert!(matches!(event.kind, O11yEventKind::MemoryTrace(record) if matches!(*record, MemoryTraceRecord::Event { .. })));
        let event = receiver.recv().await.ok_or("no loss notification")?;
        assert!(matches!(event.kind, O11yEventKind::MemoryTrace(record) if matches!(*record, MemoryTraceRecord::Lost { .. })));
        task.abort();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2), task).await?.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn setup_failure_removes_only_owned_instance_and_is_reported() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        fs::create_dir(root.path().join("instances"))?;
        fs::write(root.path().join("tracing_on"), "unchanged")?;
        let options = LinuxMemoryTraceOptions {
            tracefs_root: root.path().into(),
            ..Default::default()
        };
        let (sender, _) = tokio::sync::mpsc::channel::<O11yEvent<()>>(1);
        assert!(run_trace(options.clone(), sender).await.is_err());
        assert_eq!(fs::read_dir(root.path().join("instances"))?.count(), 0);
        assert_eq!(fs::read_to_string(root.path().join("tracing_on"))?, "unchanged");
        let pipeline = O11yPipeline::<()>::builder()
            .sink("capture", super::super::O11yMemorySink::new(Default::default()))
            .build()?
            .with_linux_memory_trace(options.clone())?;
        assert!(pipeline.with_linux_memory_trace(options).is_err());
        Ok(())
    }
}
