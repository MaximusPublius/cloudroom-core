//! Open agent-controlled files under the agent identity, then retain the opened inode.
use std::{
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::net::UnixStream,
    },
    path::Path,
    process::Stdio,
};
use tokio::process::Command;

pub(super) type Identity = Option<(u32, u32)>;

pub(super) fn open(root: &Path, path: &Path, identity: Identity) -> io::Result<File> {
    open_entry(root, path, identity, false)
}

pub(super) fn open_directory(root: &Path, path: &Path, identity: Identity) -> io::Result<File> {
    open_entry(root, path, identity, true)
}

fn open_entry(root: &Path, path: &Path, identity: Identity, directory: bool) -> io::Result<File> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let root = root.canonicalize()?;
    // Normalize platform aliases (e.g. macOS /var), never resolve the final file link.
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("file has no parent"))?;
    let path = parent.canonicalize()?.join(
        path.file_name()
            .ok_or_else(|| io::Error::other("file has no name"))?,
    );
    let relative = path.strip_prefix(&root).map_err(|_| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "file is outside its allowed directory",
        )
    })?;
    let (mut socket, child_socket) = UnixStream::pair()?;
    socket.set_read_timeout(Some(super::SHUTDOWN_GRACE))?;
    let mut command = Command::new("python3");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .current_dir("/")
        .args(["-I", "-c", include_str!("../sync/files.py")])
        .stdin(Stdio::piped())
        .stdout(Stdio::from(OwnedFd::from(child_socket)))
        .stderr(Stdio::null());
    if let Some((uid, gid)) = identity {
        super::linux::as_agent(&mut command, uid, gid, None)?;
    }
    let mut child = command.as_std_mut().spawn()?;
    drop(command);
    let result = (|| {
        let request = serde_json::json!({"op":"open", "tree":{"root":root}, "path":relative, "directory":directory});
        let mut input = child.stdin.take().unwrap();
        writeln!(input, "{request}")?;
        drop(input);
        let file = receive(&mut socket)?;
        let metadata = file.metadata()?;
        if if directory {
            !metadata.is_dir()
        } else {
            !metadata.is_file()
        } {
            return Err(io::Error::other("unexpected file type"));
        }
        Ok(file)
    })();
    // The file is already open; no helper or pathname is needed while tailing it.
    let _ = child.kill();
    let _ = child.wait();
    result
}

#[cfg(target_os = "linux")]
type Length = usize;
#[cfg(not(target_os = "linux"))]
type Length = u32;

#[repr(C)]
struct IoVec {
    base: *mut u8,
    len: usize,
}
#[repr(C)]
struct Message {
    name: *mut u8,
    name_len: u32,
    iov: *mut IoVec,
    iov_len: Length,
    control: *mut Rights,
    control_len: Length,
    flags: i32,
}
#[repr(C)]
struct Rights {
    len: Length,
    level: i32,
    kind: i32,
    fd: i32,
}

fn receive(socket: &mut UnixStream) -> io::Result<File> {
    unsafe extern "C" {
        fn recvmsg(fd: i32, message: *mut Message, flags: i32) -> isize;
        fn fcntl(fd: i32, command: i32, argument: i32) -> i32;
    }
    #[cfg(target_os = "linux")]
    const SOCKET_LEVEL: i32 = 1;
    #[cfg(not(target_os = "linux"))]
    const SOCKET_LEVEL: i32 = 0xffff;
    let mut bytes = [0u8; 4096];
    let mut iov = IoVec {
        base: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    let mut rights = Rights {
        len: 0,
        level: 0,
        kind: 0,
        fd: -1,
    };
    let mut message = Message {
        name: std::ptr::null_mut(),
        name_len: 0,
        iov: &mut iov,
        iov_len: 1,
        control: &mut rights,
        control_len: size_of::<Rights>() as Length,
        flags: 0,
    };
    // Linux can set CLOEXEC atomically; macOS needs fcntl on the received descriptor.
    let flags = if cfg!(target_os = "linux") {
        0x40000000
    } else {
        0
    };
    // These C layouts match msghdr/iovec/cmsghdr on the supported Linux/macOS targets.
    let count = unsafe { recvmsg(socket.as_raw_fd(), &mut message, flags) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    if rights.level == SOCKET_LEVEL && rights.kind == 1 && rights.fd >= 0 {
        // The private socket has exactly one trusted sender and one SCM_RIGHTS fd.
        let file = unsafe { File::from_raw_fd(rights.fd) };
        if unsafe { fcntl(file.as_raw_fd(), 2, 1) } < 0 {
            return Err(io::Error::last_os_error());
        }
        return Ok(file);
    }
    let mut error = bytes[..count as usize].to_vec();
    socket.take(4096).read_to_end(&mut error)?;
    let error: serde_json::Value = serde_json::from_slice(&error)
        .map_err(|_| io::Error::other("agent file opener returned no file"))?;
    Err(error["errno"]
        .as_i64()
        .map(|code| io::Error::from_raw_os_error(code as i32))
        .unwrap_or_else(|| io::Error::other("agent file path is invalid")))
}
