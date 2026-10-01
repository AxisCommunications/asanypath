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

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use russh::client::{self, Handle};
use russh::keys::agent::client::AgentClient;
use russh::keys::{PrivateKeyWithHashAlg, check_known_hosts, load_secret_key};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Internal connection descriptor (built from the flat per-op kwargs)
// ---------------------------------------------------------------------------

struct ConnParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
}

/// Build a `ConnParams` from the flat connection kwargs spread by
/// `SSHPath._native_kwargs`. Repeated verbatim at the top of each op.
macro_rules! conn {
    ($host:ident, $port:ident, $user:ident, $password:ident, $key_path:ident,
     $key_passphrase:ident, $use_agent:ident, $strict_host_key:ident) => {
        ConnParams {
            host: $host,
            port: $port,
            user: $user,
            password: $password,
            key_path: $key_path,
            key_passphrase: $key_passphrase,
            use_agent: $use_agent,
            strict_host_key: $strict_host_key,
        }
    };
}

impl ConnParams {
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
// russh client handler — verifies the server key against ~/.ssh/known_hosts
// unless strict checking is disabled.
// ---------------------------------------------------------------------------

struct Handler {
    host: String,
    port: u16,
    strict_host_key: bool,
}

impl client::Handler for Handler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        if !self.strict_host_key {
            return Ok(true);
        }
        match server_public_key {
            russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } => {
                // Ok(true)=known-good, Ok(false)=unknown host, Err=key mismatch.
                Ok(check_known_hosts(&self.host, self.port, key)?)
            }
            russh::keys::PublicKeyOrCertificate::Certificate(_) => Ok(false),
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
    fn fingerprint(&self) -> String {
        format!(
            "agent={};key={};pw={}",
            self.use_agent,
            self.key_path.as_deref().unwrap_or(""),
            self.password.is_some(),
        )
    }
}

async fn authenticate(
    handle: &mut Handle<Handler>,
    user: &str,
    auth: &AuthSpec,
) -> Result<(), String> {
    if auth.use_agent {
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
        return if authed {
            Ok(())
        } else {
            Err("password auth failed".to_string())
        };
    }

    Err("no authentication method provided".to_string())
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
    let key = format!(
        "{}@{}:{}#{}",
        conn.user,
        conn.host,
        conn.port,
        auth.fingerprint()
    );
    {
        let guard = sftp_pool().lock().await;
        if let Some(existing) = guard.get(&key) {
            return Ok(existing.clone());
        }
    }

    let config = Arc::new(client::Config::default());
    let handler = Handler {
        host: conn.host.clone(),
        port: conn.port,
        strict_host_key: conn.strict_host_key,
    };
    let mut handle = client::connect(config, (conn.host.as_str(), conn.port), handler)
        .await
        .map_err(|e| format!("ssh connect {}:{}: {e}", conn.host, conn.port))?;
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
    sftp_pool().lock().await.insert(key, built.clone());
    Ok(built)
}

// ---------------------------------------------------------------------------
// Python-exposed async functions. Connection kwargs mirror
// SSHPath._native_kwargs and are accepted positionally-or-by-keyword so the
// Python side can call e.g. ``ssh_read(path=..., **self._native_kwargs)``.
// ---------------------------------------------------------------------------

/// Read a remote file fully. Returns bytes.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_read<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let data = session.sftp.read(&path).await.map_err(err)?;
        Python::attach(|py| Ok(PyBytes::new(py, &data).into_any().unbind()))
    })
}

/// Read a byte range [start, end] (inclusive). Returns bytes (short at EOF).
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_read_range<'py>(
    py: Python<'py>,
    path: String,
    start: u64,
    end: u64,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
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
#[allow(clippy::too_many_arguments)]
pub fn ssh_write<'py>(
    py: Python<'py>,
    path: String,
    data: Vec<u8>,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
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

/// Check whether a remote path exists.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_exists<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let exists = session.sftp.try_exists(&path).await.map_err(err)?;
        Ok(exists)
    })
}

/// Return (size, mtime, atime, uid, gid, permissions) for a remote path.
/// Fields are `None` when the server omits them — mirrors asyncssh SFTPAttrs.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_stat<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        let md = session.sftp.metadata(&path).await.map_err(err)?;
        Ok((md.size, md.mtime, md.atime, md.uid, md.gid, md.permissions))
    })
}

/// List a remote directory. Returns
/// (name, size, mtime, atime, uid, gid, permissions) per entry — the same
/// attribute shape as `ssh_stat`, so children can cache them without a restat.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_list<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
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
#[allow(clippy::too_many_arguments)]
pub fn ssh_mkdir<'py>(
    py: Python<'py>,
    path: String,
    parents: bool,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
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
#[allow(clippy::too_many_arguments)]
pub fn ssh_rmdir<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.remove_dir(&path).await.map_err(err)?;
        Ok(())
    })
}

/// Remove a remote file.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_unlink<'py>(
    py: Python<'py>,
    path: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.remove_file(&path).await.map_err(err)?;
        Ok(())
    })
}

/// Rename a remote path.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn ssh_rename<'py>(
    py: Python<'py>,
    src: String,
    dst: String,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    key_passphrase: Option<String>,
    use_agent: bool,
    strict_host_key: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let conn = conn!(
        host,
        port,
        user,
        password,
        key_path,
        key_passphrase,
        use_agent,
        strict_host_key
    );
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = session_for(&conn).await.map_err(PyRuntimeError::new_err)?;
        session.sftp.rename(&src, &dst).await.map_err(err)?;
        Ok(())
    })
}
