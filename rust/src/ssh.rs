//! Native SFTP backend using russh + russh-sftp, with auth and host-key
//! verification via the russh::keys shelf helpers.
//!
//! Runs SFTP I/O off-GIL in tokio. Connections (one authenticated SFTP session
//! each) are pooled by (user@host:port, auth).
//!
//! Connection parameters arrive as flat keyword arguments mirroring
//! ``SSHPath._native_kwargs`` — the same ``**_native_kwargs`` spread convention
//! used by every other backend — so there is a single source of connection
//! truth on the Python side.

use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use russh::client::{self, Handle};
#[cfg(unix)]
use russh::keys::agent::client::AgentClient;
use russh::keys::known_hosts::{known_host_keys_path, learn_known_hosts_path};
use russh::keys::{
    HashAlg, PrivateKeyWithHashAlg, PublicKey, check_known_hosts_path, load_secret_key,
};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use ssh2_config::{ParseRule, SshConfig};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use std::sync::Mutex;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Connection descriptor, parsed from the ``**self._native_kwargs`` every
// SSHPath op spreads into its native call.
// ---------------------------------------------------------------------------

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
struct ConnParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    host_key_policy: String,
    known_hosts: String,
}

const CONN_FIELDS: usize = 9;

impl ConnParams {
    fn from_kwargs(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let kwargs = kwargs.ok_or_else(|| PyTypeError::new_err("missing SSH connection kwargs"))?;
        if kwargs.len() != CONN_FIELDS {
            return Err(PyTypeError::new_err(format!(
                "expected {CONN_FIELDS} SSH connection kwargs, got {}",
                kwargs.len()
            )));
        }
        kwargs.extract()
    }

    fn auth(&self) -> AuthSpec {
        AuthSpec {
            password: self.password.clone(),
            key_path: self.key_path.clone(),
            key_passphrase: self.key_passphrase.clone(),
            use_agent: self.use_agent,
        }
    }
}

// ---------------------------------------------------------------------------
// russh client handler — verifies the server key against a known_hosts file
// following OpenSSH's StrictHostKeyChecking semantics.
// ---------------------------------------------------------------------------

/// OpenSSH `StrictHostKeyChecking`, plus `Disabled` for an explicit opt-out
/// (`known_hosts=()` / `SSH_KNOWN_HOSTS=none`) that never touches any file.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HostKeyPolicy {
    /// Unknown and changed keys are refused.
    Yes,
    /// As `Yes`; the Python layer prompts for unknown hosts when interactive.
    Ask,
    /// Unknown keys are recorded and accepted; changed keys are refused.
    AcceptNew,
    /// Unknown keys are recorded; changed keys are accepted.
    No,
    Disabled,
}

impl HostKeyPolicy {
    fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "yes" | "true" => Ok(Self::Yes),
            "ask" => Ok(Self::Ask),
            "accept-new" => Ok(Self::AcceptNew),
            "no" | "off" | "false" => Ok(Self::No),
            "disabled" => Ok(Self::Disabled),
            other => Err(format!("unsupported host key policy {other:?}")),
        }
    }
}

fn host_label(host: &str, port: u16) -> String {
    if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    }
}

fn describe_key(key: &PublicKey) -> String {
    format!(
        "{} key fingerprint is {}",
        key.algorithm(),
        key.fingerprint(HashAlg::Sha256)
    )
}

/// Host names that can be written to known_hosts as-is: no separators,
/// patterns or markers that would make the entry match other hosts.
fn is_plain_host(host: &str) -> bool {
    !host.is_empty()
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':'))
}

struct Handler {
    host: String,
    port: u16,
    policy: HostKeyPolicy,
    known_hosts: String,
    /// Why the server key was refused, reported instead of russh's generic error.
    rejected: Arc<Mutex<Option<String>>>,
    /// The server key, kept for `ssh_scan_host_key`.
    seen: Arc<Mutex<Option<PublicKey>>>,
    /// Set when `StrictHostKeyChecking no` let a changed key through; like
    /// OpenSSH, password authentication is then refused.
    key_changed: Arc<std::sync::atomic::AtomicBool>,
}

impl Handler {
    fn reject(&self, reason: String) -> Result<bool, anyhow::Error> {
        *self.rejected.lock().unwrap() = Some(reason);
        Ok(false)
    }
}

impl client::Handler for Handler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        if let russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } = server_public_key {
            *self.seen.lock().unwrap() = Some(key.clone());
        }
        if self.policy == HostKeyPolicy::Disabled {
            return Ok(true);
        }
        let russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } = server_public_key else {
            return self.reject("host certificates are not supported".to_string());
        };
        let label = host_label(&self.host, self.port);
        match check_known_hosts_path(&self.host, self.port, key, &self.known_hosts) {
            Ok(true) => Ok(true),
            Ok(false) => match self.policy {
                HostKeyPolicy::AcceptNew | HostKeyPolicy::No if !is_plain_host(&self.host) => self
                    .reject(format!(
                        "refusing to record a host key for {label:?}: not a plain host name"
                    )),
                HostKeyPolicy::AcceptNew | HostKeyPolicy::No => {
                    learn_known_hosts_path(&self.host, self.port, key, &self.known_hosts)?;
                    Ok(true)
                }
                _ => self.reject(format!(
                    "no host key is known for {label} in {}; {}",
                    self.known_hosts,
                    describe_key(key)
                )),
            },
            Err(russh::keys::Error::KeyChanged { .. }) if self.policy == HostKeyPolicy::No => {
                self.key_changed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            Err(russh::keys::Error::KeyChanged { line }) => self.reject(format!(
                "REMOTE HOST IDENTIFICATION HAS CHANGED for {label}: {} does not match {}:{line}",
                describe_key(key),
                self.known_hosts
            )),
            Err(e) => Err(e.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Authentication (russh::keys shelf: agent, key files, password)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AuthSpec {
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
}

impl AuthSpec {
    /// Pool-key component: a digest of every credential, so a pooled session
    /// is only reused by callers presenting the same credentials.
    fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for part in [
            Some(if self.use_agent { "agent" } else { "" }),
            self.key_path.as_deref(),
            self.key_passphrase.as_deref(),
            self.password.as_deref(),
        ] {
            let bytes = part.map(str::as_bytes).unwrap_or_default();
            hasher.update([part.is_some() as u8]);
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        }
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

async fn authenticate(
    handle: &mut Handle<Handler>,
    user: &str,
    auth: &AuthSpec,
) -> Result<(), String> {
    if auth.use_agent {
        #[cfg(unix)]
        {
            let sock = std::env::var("SSH_AUTH_SOCK")
                .map_err(|_| "SSH_AUTH_SOCK not set for agent auth".to_string())?;
            let stream = tokio::net::UnixStream::connect(&sock)
                .await
                .map_err(|e| format!("agent connect: {e}"))?;
            let mut agent = AgentClient::connect(stream);
            let identities = agent
                .request_identities()
                .await
                .map_err(|e| format!("agent identities: {e}"))?;
            for identity in identities {
                let russh::keys::agent::AgentIdentity::PublicKey { key, .. } = identity else {
                    continue;
                };
                let authed = handle
                    .authenticate_publickey_with(user, key, None, &mut agent)
                    .await
                    .map_err(|e| format!("agent auth: {e}"))?
                    .success();
                if authed {
                    return Ok(());
                }
            }
            return Err("agent auth: no identity accepted".to_string());
        }
        #[cfg(not(unix))]
        {
            return Err(
                "SSH agent auth (SSH_AUTH_SOCK) is not supported on this platform".to_string(),
            );
        }
    }

    if let Some(path) = &auth.key_path {
        let key = load_secret_key(path, auth.key_passphrase.as_deref())
            .map_err(|e| format!("load key {path}: {e}"))?;
        let authed = handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), None))
            .await
            .map_err(|e| format!("pubkey auth: {e}"))?
            .success();
        return if authed {
            Ok(())
        } else {
            Err("pubkey auth failed".to_string())
        };
    }

    if let Some(password) = &auth.password {
        let authed = handle
            .authenticate_password(user, password)
            .await
            .map_err(|e| format!("password auth: {e}"))?
            .success();
        if authed {
            return Ok(());
        }
        // Many servers run password auth as keyboard-interactive (PAM); answer
        // the challenge prompts with the supplied password.
        return keyboard_interactive(handle, user, password).await;
    }

    Err("no authentication method provided".to_string())
}

/// Keyboard-interactive auth, answering every prompt with `password`.
/// Covers PAM / password-via-kbdint; true multi-prompt OTP would need a
/// Python callback bridge.
async fn keyboard_interactive(
    handle: &mut Handle<Handler>,
    user: &str,
    password: &str,
) -> Result<(), String> {
    use russh::client::KeyboardInteractiveAuthResponse as Resp;
    let mut resp = handle
        .authenticate_keyboard_interactive_start(user, None)
        .await
        .map_err(|e| format!("kbdint start: {e}"))?;
    loop {
        match resp {
            Resp::Success => return Ok(()),
            Resp::Failure { .. } => return Err("keyboard-interactive auth failed".to_string()),
            Resp::InfoRequest { prompts, .. } => {
                let answers = prompts.iter().map(|_| password.to_string()).collect();
                resp = handle
                    .authenticate_keyboard_interactive_respond(answers)
                    .await
                    .map_err(|e| format!("kbdint respond: {e}"))?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection pool: one SFTP session per (user@host:port, auth)
// ---------------------------------------------------------------------------

struct SftpConn {
    #[allow(dead_code)]
    handle: Handle<Handler>,
    sftp: SftpSession,
}

type Pool = Mutex<HashMap<String, Arc<SftpConn>>>;

static POOL_CELL: std::sync::OnceLock<Pool> = std::sync::OnceLock::new();

fn sftp_pool() -> &'static Pool {
    POOL_CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn session_for(conn: &ConnParams) -> Result<Arc<SftpConn>, String> {
    let auth = conn.auth();
    let policy = HostKeyPolicy::parse(&conn.host_key_policy)?;
    let key = format!(
        "{}@{}:{}#{}#{}#{}",
        conn.user,
        conn.host,
        conn.port,
        auth.fingerprint(),
        conn.host_key_policy.to_ascii_lowercase(),
        conn.known_hosts
    );
    {
        let guard = sftp_pool().lock().unwrap();
        if let Some(existing) = guard.get(&key) {
            return Ok(existing.clone());
        }
    }

    let config = Arc::new(client::Config::default());
    let rejected = Arc::new(Mutex::new(None));
    let key_changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handler = Handler {
        host: conn.host.clone(),
        port: conn.port,
        policy,
        known_hosts: conn.known_hosts.clone(),
        rejected: rejected.clone(),
        seen: Arc::new(Mutex::new(None)),
        key_changed: key_changed.clone(),
    };
    let mut handle = match client::connect(config, (conn.host.as_str(), conn.port), handler).await {
        Ok(handle) => handle,
        Err(e) => {
            if let Some(reason) = rejected.lock().unwrap().take() {
                return Err(format!("Host key verification failed: {reason}"));
            }
            return Err(format!("ssh connect {}:{}: {e}", conn.host, conn.port));
        }
    };
    if key_changed.load(std::sync::atomic::Ordering::Relaxed) && auth.password.is_some() {
        return Err(format!(
            "Host key for {} has changed; password authentication is disabled \
             to avoid a man-in-the-middle attack (StrictHostKeyChecking no)",
            host_label(&conn.host, conn.port)
        ));
    }
    authenticate(&mut handle, &conn.user, &auth).await?;
    let channel = handle
        .channel_open_session()
        .await
        .map_err(|e| format!("ssh channel: {e}"))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| format!("sftp subsystem: {e}"))?;
    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| format!("sftp init: {e}"))?;

    let built = Arc::new(SftpConn { handle, sftp });
    sftp_pool().lock().unwrap().insert(key, built.clone());
    Ok(built)
}

/// Close and drop all pooled SFTP sessions (dropping each `Handle` disconnects).
#[pyfunction]
pub fn ssh_disconnect_all() {
    if let Some(pool) = POOL_CELL.get() {
        pool.lock().unwrap().clear();
    }
}

// ---------------------------------------------------------------------------
// Python-exposed async functions. Connection kwargs mirror
// SSHPath._native_kwargs and are accepted positionally-or-by-keyword so the
// Python side can call e.g. ``ssh_read(path=..., **self._native_kwargs)``.
// ---------------------------------------------------------------------------

/// Read a remote file fully. Returns bytes.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_read<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let data = session.sftp.read(&path).await.map_err(err)?;
        Python::attach(|py| Ok(PyBytes::new(py, &data).into_any().unbind()))
    })
}

/// Read a byte range [start, end] (inclusive). Returns bytes (short at EOF).
#[pyfunction]
#[pyo3(signature = (path, start, end, **conn))]
pub fn ssh_read_range<'py>(
    py: Python<'py>,
    path: String,
    start: u64,
    end: u64,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let mut file = session.sftp.open(&path).await.map_err(err)?;
        file.seek(SeekFrom::Start(start)).await.map_err(err)?;
        let len = (end.saturating_sub(start) + 1) as usize;
        let mut buf = vec![0u8; len];
        let mut filled = 0;
        while filled < len {
            let n = file.read(&mut buf[filled..]).await.map_err(err)?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        buf.truncate(filled);
        Python::attach(|py| Ok(PyBytes::new(py, &buf).into_any().unbind()))
    })
}

/// Write bytes to a remote file (create + truncate). Returns the byte count.
#[pyfunction]
#[pyo3(signature = (path, data, **conn))]
pub fn ssh_write<'py>(
    py: Python<'py>,
    path: String,
    data: Vec<u8>,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let n = data.len();
        let mut file = session
            .sftp
            .open_with_flags(
                &path,
                OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
            )
            .await
            .map_err(err)?;
        file.write_all(&data).await.map_err(err)?;
        file.shutdown().await.map_err(err)?;
        Ok(n)
    })
}

/// Write one chunk of a streamed upload. The first chunk (`truncate=true`)
/// resets the file; later chunks open with APPEND so no offset tracking is
/// needed. Returns the byte count written.
#[pyfunction]
#[pyo3(signature = (path, data, truncate, **conn))]
pub fn ssh_write_chunk<'py>(
    py: Python<'py>,
    path: String,
    data: Vec<u8>,
    truncate: bool,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let n = data.len();
        let flags = if truncate {
            OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE
        } else {
            OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::APPEND
        };
        let mut file = session
            .sftp
            .open_with_flags(&path, flags)
            .await
            .map_err(err)?;
        file.write_all(&data).await.map_err(err)?;
        file.shutdown().await.map_err(err)?;
        Ok(n)
    })
}

/// Check whether a remote path exists.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_exists<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let exists = session.sftp.try_exists(&path).await.map_err(err)?;
        Ok(exists)
    })
}

/// Return (size, mtime, atime, uid, gid, permissions) for a remote path.
/// Fields are `None` when the server omits them — mirrors asyncssh SFTPAttrs.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_stat<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let md = session.sftp.metadata(&path).await.map_err(err)?;
        Ok((md.size, md.mtime, md.atime, md.uid, md.gid, md.permissions))
    })
}

/// Like `ssh_stat` but does not follow symlinks (SFTP `lstat`).
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_lstat<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let md = session.sftp.symlink_metadata(&path).await.map_err(err)?;
        Ok((md.size, md.mtime, md.atime, md.uid, md.gid, md.permissions))
    })
}

/// Set remote file metadata (SFTP `setstat`). Only the provided fields are
/// changed — covers chmod (permissions), chown (uid/gid) and utime (atime/mtime).
#[pyfunction]
#[pyo3(signature = (path, permissions, uid, gid, atime, mtime, **conn))]
#[allow(clippy::too_many_arguments)]
pub fn ssh_setstat<'py>(
    py: Python<'py>,
    path: String,
    permissions: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    atime: Option<u32>,
    mtime: Option<u32>,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let attrs = FileAttributes {
            permissions,
            uid,
            gid,
            atime,
            mtime,
            ..Default::default()
        };
        session.sftp.set_metadata(&path, attrs).await.map_err(err)?;
        Ok(())
    })
}

/// List a remote directory. Returns
/// (name, size, mtime, atime, uid, gid, permissions) per entry — the same
/// attribute shape as `ssh_stat`, so children can cache them without a restat.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_list<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    type Entry = (
        String,
        Option<u64>,
        Option<u32>,
        Option<u32>,
        Option<u32>,
        Option<u32>,
        Option<u32>,
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let dir = session.sftp.read_dir(&path).await.map_err(err)?;
        let entries: Vec<Entry> = dir
            .map(|e| {
                let md = e.metadata();
                (
                    e.file_name(),
                    md.size,
                    md.mtime,
                    md.atime,
                    md.uid,
                    md.gid,
                    md.permissions,
                )
            })
            .collect();
        Ok(entries)
    })
}

/// Create a remote directory (optionally creating parents).
#[pyfunction]
#[pyo3(signature = (path, parents, **conn))]
pub fn ssh_mkdir<'py>(
    py: Python<'py>,
    path: String,
    parents: bool,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        if parents {
            let mut prefix = String::new();
            for part in path.split('/').filter(|p| !p.is_empty()) {
                prefix.push('/');
                prefix.push_str(part);
                // Ignore "already exists" while building the chain.
                let _ = session.sftp.create_dir(&prefix).await;
            }
        } else {
            session.sftp.create_dir(&path).await.map_err(err)?;
        }
        Ok(())
    })
}

/// Remove an empty remote directory.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_rmdir<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.remove_dir(&path).await.map_err(err)?;
        Ok(())
    })
}

/// Remove a remote file.
#[pyfunction]
#[pyo3(signature = (path, **conn))]
pub fn ssh_unlink<'py>(
    py: Python<'py>,
    path: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.remove_file(&path).await.map_err(err)?;
        Ok(())
    })
}

/// Rename a remote path.
#[pyfunction]
#[pyo3(signature = (src, dst, **conn))]
pub fn ssh_rename<'py>(
    py: Python<'py>,
    src: String,
    dst: String,
    conn: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = ConnParams::from_kwargs(conn)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.rename(&src, &dst).await.map_err(err)?;
        Ok(())
    })
}

/// Resolve an OpenSSH client-config alias. Parses the given config files (in
/// order, first-obtained-value wins, matching OpenSSH) and returns the host's
/// (HostName, Port, User, first IdentityFile). Replaces the asyncssh config
/// parser so SSHPath needs no Python SSH library at runtime.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn ssh_resolve_config(
    host: String,
    config_paths: Vec<String>,
) -> PyResult<(
    Option<String>,
    Option<u16>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    let mut combined = String::new();
    for path in &config_paths {
        if let Ok(content) = std::fs::read_to_string(path) {
            combined.push_str(&content);
            combined.push('\n');
        }
    }
    if combined.trim().is_empty() {
        return Ok((None, None, None, None, None, None));
    }
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(combined.into_bytes()));
    let config = SshConfig::default()
        .parse(&mut reader, ParseRule::ALLOW_UNSUPPORTED_FIELDS)
        .map_err(|e| PyRuntimeError::new_err(format!("ssh_config parse: {e}")))?;
    let params = config.query(&host);
    let identity = params
        .identity_file
        .and_then(|files| files.into_iter().next())
        .map(|p| p.to_string_lossy().into_owned());
    let first = |field: &str| {
        params
            .unsupported_fields
            .get(field)
            .and_then(|args| args.first())
            .cloned()
    };
    Ok((
        params.host_name,
        params.port,
        params.user,
        identity,
        first("stricthostkeychecking"),
        first("userknownhostsfile"),
    ))
}

/// True when `known_hosts` records any key for `host:port`.
#[pyfunction]
pub fn ssh_known_host(host: String, port: u16, known_hosts: String) -> PyResult<bool> {
    known_host_keys_path(&host, port, &known_hosts)
        .map(|keys| !keys.is_empty())
        .map_err(err)
}

/// Connect without authenticating and return the server's host key as
/// `(description, openssh_public_key)` for an interactive trust prompt.
#[pyfunction]
pub fn ssh_scan_host_key(py: Python<'_>, host: String, port: u16) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let seen = Arc::new(Mutex::new(None));
        let handler = Handler {
            host: host.clone(),
            port,
            policy: HostKeyPolicy::Disabled,
            known_hosts: String::new(),
            rejected: Arc::new(Mutex::new(None)),
            seen: seen.clone(),
            key_changed: Default::default(),
        };
        let config = Arc::new(client::Config::default());
        let connected = client::connect(config, (host.as_str(), port), handler).await;
        drop(connected);
        let key =
            seen.lock().unwrap().take().ok_or_else(|| {
                PyRuntimeError::new_err(format!("ssh {host}:{port}: no host key"))
            })?;
        let openssh = key.to_openssh().map_err(err)?;
        Ok((describe_key(&key), openssh))
    })
}

/// Append `host:port`'s key (OpenSSH public-key format) to `known_hosts`.
#[pyfunction]
pub fn ssh_learn_host_key(
    host: String,
    port: u16,
    public_key: String,
    known_hosts: String,
) -> PyResult<()> {
    if !is_plain_host(&host) {
        return Err(PyRuntimeError::new_err(format!(
            "refusing to record a host key for {host:?}: not a plain host name"
        )));
    }
    let key = PublicKey::from_openssh(&public_key).map_err(err)?;
    learn_known_hosts_path(&host, port, &key, &known_hosts).map_err(err)
}
