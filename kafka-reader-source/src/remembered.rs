//! Per-profile, current-user Windows DPAPI credential storage.
use crate::{
    model::Properties,
    workspace::{Server, Store, Workspace, atomic_write},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

const MAX_BLOB: usize = 2 * 1024 * 1024;
const CHANGED: &str = "The remembered credential changed. Reopen the server before using it.";
const UNREADABLE: &str =
    "Could not unlock the remembered credential for this Windows account and connection.";

pub enum RememberChange<'a> {
    Keep,
    Forget,
    Replace(&'a Properties),
}

#[derive(Serialize, Deserialize)]
enum Secret {
    Password { username: String, password: String },
    RefreshToken(String),
}

impl Secret {
    fn for_server(server: &Server, credentials: &Properties) -> Result<Self, String> {
        let property = |key: &str| server.properties.get(key).map(String::as_str).unwrap_or("");
        let value = |key: &str| credentials.get(key).filter(|s| !s.is_empty()).cloned();
        if server.offline
            || !matches!(
                property("security.protocol").to_ascii_uppercase().as_str(),
                "SASL_SSL" | "SASL_PLAINTEXT"
            )
        {
            return Err("Choose SASL authentication to remember a credential".into());
        }
        let mechanism = server
            .properties
            .get("sasl.mechanism")
            .or_else(|| server.properties.get("sasl.mechanisms"))
            .map(|v| v.to_ascii_uppercase());
        match mechanism.as_deref().unwrap_or("PLAIN") {
            "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" => Ok(Self::Password {
                username: value("sasl.username")
                    .ok_or("Enter the Kafka username to remember the password")?,
                password: value("sasl.password").ok_or("Enter the password to remember")?,
            }),
            "OAUTHBEARER" => {
                let endpoint = property("oauth.token.endpoint.uri");
                if !property("security.protocol").eq_ignore_ascii_case("SASL_SSL")
                    || crate::oauth::validate_endpoint(endpoint).is_err()
                    || endpoint.contains('?')
                    || property("oauth.client.id").trim().is_empty()
                    || credentials
                        .get("oauth.token.endpoint.uri")
                        .is_some_and(|v| v != endpoint)
                {
                    return Err("Remembering a refresh token requires SASL_SSL, a client ID and a saved HTTPS endpoint without a query".into());
                }
                Ok(Self::RefreshToken(
                    value("oauth.refresh.token").ok_or("Enter the refresh token to remember")?,
                ))
            }
            _ => Err("Unsupported mechanism for a remembered credential".into()),
        }
    }

    fn into_properties(self) -> Properties {
        match self {
            Self::Password { username, password } => Properties::from([
                ("sasl.username".into(), username),
                ("sasl.password".into(), password),
            ]),
            Self::RefreshToken(token) => Properties::from([("oauth.refresh.token".into(), token)]),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RememberedCredential {
    directory: PathBuf,
    server_id: u64,
    reference: String,
    connection: Properties,
}

pub(crate) fn valid_reference(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn connection(server: &Server) -> Properties {
    let mut props = server.properties.clone();
    props.retain(|key, value| !crate::workspace::session_only(key, value));
    // A renewed token keeps its destination and TLS policy when changing its refresh schedule.
    props.remove("oauth.max.token.expiry.seconds");
    props
}

fn blob_path(directory: &Path, reference: &str) -> PathBuf {
    debug_assert!(valid_reference(reference));
    directory.join(format!("credential-{reference}.dpapi"))
}

impl RememberedCredential {
    fn new(directory: &Path, server: &Server, reference: &str) -> Option<Self> {
        (valid_reference(reference) && !server.offline).then(|| Self {
            directory: directory.into(),
            server_id: server.id,
            reference: reference.into(),
            connection: connection(server),
        })
    }

    pub fn same_target(&self, server: &Server) -> bool {
        self.server_id == server.id && !server.offline && self.connection == connection(server)
    }

    fn entropy(&self) -> Vec<u8> {
        // DPAPI authenticates this context together with the ciphertext. Repointing a
        // profile, editing its trust settings, or swapping blobs cannot redirect a secret.
        serde_json::to_vec(&(
            "Kafka Reader DPAPI v1",
            self.server_id,
            &self.reference,
            &self.connection,
        ))
        .expect("serializing strings into a vector cannot fail")
    }

    fn active_lock(&self) -> Result<File, String> {
        let store = Store::new(Some(self.directory.clone()));
        let lock = store.lock_file()?;
        lock.lock()
            .map_err(|_| "Could not lock remembered credentials")?;
        let workspace = store.load()?;
        if workspace.remembered.get(&self.server_id) != Some(&self.reference)
            || !workspace
                .server(self.server_id)
                .is_some_and(|s| self.same_target(s))
        {
            return Err(CHANGED.into());
        }
        Ok(lock)
    }

    fn secret(&self) -> Result<Secret, String> {
        let mut ciphertext = Vec::new();
        File::open(blob_path(&self.directory, &self.reference))
            .and_then(|f| f.take(MAX_BLOB as u64 + 1).read_to_end(&mut ciphertext))
            .map_err(|_| UNREADABLE)?;
        if ciphertext.is_empty() || ciphertext.len() > MAX_BLOB {
            return Err(UNREADABLE.into());
        }
        let bytes = platform::crypt(&ciphertext, &self.entropy(), false)?;
        serde_json::from_slice(&bytes).map_err(|_| UNREADABLE.into())
    }

    fn sealed(&self, value: &Secret) -> Result<Vec<u8>, String> {
        let bytes =
            serde_json::to_vec(value).map_err(|_| "Could not encode remembered credential")?;
        if bytes.len() > MAX_BLOB {
            return Err("Credential is too large to remember".into());
        }
        let encrypted = platform::crypt(&bytes, &self.entropy(), true)?;
        if encrypted.len() > MAX_BLOB {
            return Err("Credential is too large to remember".into());
        }
        Ok(encrypted)
    }

    pub fn load(&self) -> Result<Properties, String> {
        let _lock = self.active_lock()?;
        Ok(self.secret()?.into_properties())
    }

    /// Serialize spending a single-use token across instances. Only the OAuth
    /// worker holds this file across HTTPS. Profile edits use workspace.lock.
    pub fn lock_refresh(&self) -> Result<File, String> {
        let _workspace_lock = self.active_lock()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(blob_path(&self.directory, &self.reference).with_extension("refresh.lock"))
            .map_err(|_| "Could not lock the remembered refresh token")?;
        file.try_lock()
            .map_err(|_| "Another session is refreshing this saved token. Retry the connection.")?;
        Ok(file)
    }

    pub fn rotate(&self, expected: &str, next: &str) -> Result<(), String> {
        let _lock = self.active_lock()?;
        let Secret::RefreshToken(current) = self.secret()? else {
            return Err(CHANGED.into());
        };
        if current == next {
            return Ok(());
        }
        if current != expected || next.is_empty() {
            return Err(CHANGED.into());
        }
        let bytes = self.sealed(&Secret::RefreshToken(next.into()))?;
        atomic_write(&blob_path(&self.directory, &self.reference), &bytes)
    }
}

impl Store {
    pub fn remembered_credential(
        &self,
        workspace: &Workspace,
        id: u64,
    ) -> Option<RememberedCredential> {
        RememberedCredential::new(
            &self.directory,
            workspace.server(id)?,
            workspace.remembered.get(&id)?,
        )
    }

    pub fn save_with_credential(
        &self,
        proposed: &mut Workspace,
        id: u64,
        change: RememberChange<'_>,
    ) -> Result<(), String> {
        let _lock = self.lock()?;
        let previous = Store::new(Some(self.directory.clone())).load()?;
        let old = self.remembered_credential(&previous, id);
        let server = proposed.server(id).ok_or("Server is absent")?;
        if matches!(change, RememberChange::Keep) {
            if old.as_ref().is_some_and(|h| !h.same_target(server)) {
                return Err(
                    "Enter the credential again or forget it after changing the connection".into(),
                );
            }
            return self.write_workspace_locked(proposed);
        }
        let mut staged = None;
        let mut updated = proposed.clone();
        if let RememberChange::Replace(properties) = change {
            let secret = Secret::for_server(server, properties)?;
            let reference = platform::reference()?;
            let handle = RememberedCredential::new(&self.directory, server, &reference)
                .ok_or("Could not create remembered credential")?;
            let bytes = handle.sealed(&secret)?;
            let path = blob_path(&self.directory, &reference);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| e.to_string())?;
            let write = file.write_all(&bytes).and_then(|()| file.sync_all());
            drop(file);
            if let Err(e) = write {
                let _ = fs::remove_file(&path);
                return Err(e.to_string());
            }
            staged = Some(path);
            updated.remembered.insert(id, reference);
        } else {
            updated.remembered.remove(&id);
        }
        if let Err(e) = self.write_workspace_locked(&updated) {
            if let Some(path) = staged {
                let _ = fs::remove_file(path);
            }
            return Err(e);
        }
        if let Some(old) = old {
            let old_path = blob_path(&self.directory, &old.reference);
            let deletion = fs::remove_file(&old_path);
            if let Err(e) = deletion
                && e.kind() != ErrorKind::NotFound
            {
                // The old record still exists. Restore the prior workspace before the
                // lock is released so a failed Forget/Replace can be retried.
                self.write_workspace_locked(&previous)?;
                if let Some(path) = staged {
                    let _ = fs::remove_file(path);
                }
                return Err("Could not remove the previous remembered credential".into());
            }
            // This lock contains no credential material. Obsolete handles can
            // never acquire a fresh lock or recreate the forgotten generation.
            let _ = fs::remove_file(old_path.with_extension("refresh.lock"));
        }
        *proposed = updated;
        Ok(())
    }
}

#[cfg(windows)]
mod platform {
    use super::UNREADABLE;
    use std::{ptr, slice};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            BCRYPT_USE_SYSTEM_PREFERRED_RNG, BCryptGenRandom, CRYPT_INTEGER_BLOB,
            CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    };

    pub(super) fn reference() -> Result<String, String> {
        let mut bytes = [0u8; 16];
        // SAFETY: system-selected RNG writes exactly the supplied buffer length.
        if unsafe {
            BCryptGenRandom(
                ptr::null_mut(),
                bytes.as_mut_ptr(),
                bytes.len() as u32,
                BCRYPT_USE_SYSTEM_PREFERRED_RNG,
            )
        } != 0
        {
            return Err("Windows could not allocate a credential reference".into());
        }
        Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }

    pub(super) fn crypt(bytes: &[u8], entropy: &[u8], protect: bool) -> Result<Vec<u8>, String> {
        fn input(s: &[u8]) -> Result<CRYPT_INTEGER_BLOB, String> {
            Ok(CRYPT_INTEGER_BLOB {
                cbData: s
                    .len()
                    .try_into()
                    .map_err(|_| "Credential data exceeds the DPAPI limit")?,
                pbData: s.as_ptr().cast_mut(),
            })
        }
        let input_blob = input(bytes)?;
        let context = input(entropy)?;
        let mut output = CRYPT_INTEGER_BLOB::default();
        // SAFETY: both input slices live through the call and DPAPI only reads them.
        // No machine-wide flag or UI is permitted. Successful output is LocalAlloc'd.
        let ok = unsafe {
            if protect {
                CryptProtectData(
                    &input_blob,
                    ptr::null(),
                    &context,
                    ptr::null(),
                    ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    &mut output,
                )
            } else {
                CryptUnprotectData(
                    &input_blob,
                    ptr::null_mut(),
                    &context,
                    ptr::null(),
                    ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    &mut output,
                )
            }
        };
        if ok == 0 || output.pbData.is_null() {
            return Err(if protect {
                "Windows could not protect the credential for this account"
            } else {
                UNREADABLE
            }
            .into());
        }
        // SAFETY: DPAPI returned a valid allocation of cbData bytes. Copy into Rust
        // ownership before releasing with the allocator specified by its API.
        unsafe {
            let data = slice::from_raw_parts(output.pbData, output.cbData as usize).to_vec();
            LocalFree(output.pbData.cast());
            Ok(data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Store, Workspace, Properties) {
        let store = Store::for_gui(None, true).unwrap();
        let mut workspace = Workspace::default();
        let props = Properties::from([
            ("security.protocol".into(), "SASL_SSL".into()),
            ("sasl.mechanism".into(), "OAUTHBEARER".into()),
            ("bootstrap.servers".into(), "example.invalid:9093".into()),
            (
                "oauth.token.endpoint.uri".into(),
                "https://sso.invalid/token".into(),
            ),
            ("oauth.client.id".into(), "kafka-cli".into()),
        ]);
        workspace.servers.push(Server {
            id: 1,
            name: "Unit test".into(),
            offline: false,
            properties: props,
            topics: vec![],
        });
        workspace.selected_server = 1;
        let credentials = Properties::from([
            ("oauth.refresh.token".into(), "REFRESH_SENTINEL".into()),
            ("ssl.key.password".into(), "TLS_SENTINEL".into()),
            ("sasl.password".into(), "PASSWORD_SENTINEL".into()),
            ("sasl.username".into(), "USERNAME_SENTINEL".into()),
        ]);
        (store, workspace, credentials)
    }
    #[test]
    fn credential_references_cannot_select_arbitrary_paths() {
        let (_, mut ws, _) = fixture();
        ws.remembered.insert(1, "../workspace.json".into());
        assert!(ws.validate().is_err());
        ws.remembered.clear();
        ws.remembered.insert(99, "a".repeat(32));
        assert!(ws.validate().is_err());
    }
    #[cfg(windows)]
    #[test]
    fn dpapi_restore_rotation_and_forget_are_scoped_to_the_saved_generation() {
        let (store, mut ws, secrets) = fixture();
        store
            .save_with_credential(&mut ws, 1, RememberChange::Replace(&secrets))
            .unwrap();
        let reopened = Store::new(Some(store.directory.clone()));
        let reloaded = reopened.load().unwrap();
        let slot = reopened.remembered_credential(&reloaded, 1).unwrap();
        assert_eq!(
            slot.load().unwrap(),
            Properties::from([("oauth.refresh.token".into(), "REFRESH_SENTINEL".into())])
        );
        let path = blob_path(&store.directory, &slot.reference);
        let grant_lock = slot.lock_refresh().unwrap();
        assert!(slot.lock_refresh().is_err());
        assert!(slot.load().is_ok());
        drop(grant_lock);
        assert!(slot.lock_refresh().is_ok());
        for data in [fs::read(&path).unwrap(), fs::read(store.path()).unwrap()] {
            assert!(!data.windows(8).any(|window| window == b"SENTINEL"));
        }
        slot.rotate("REFRESH_SENTINEL", "rotated").unwrap();
        slot.rotate("REFRESH_SENTINEL", "rotated").unwrap();
        assert!(slot.rotate("REFRESH_SENTINEL", "stale").is_err());
        assert_eq!(slot.load().unwrap()["oauth.refresh.token"], "rotated");
        ws.servers[0].name = "Renamed".into();
        store
            .save_with_credential(&mut ws, 1, RememberChange::Keep)
            .unwrap();
        assert_eq!(slot.load().unwrap()["oauth.refresh.token"], "rotated");
        let mut other = ws.clone();
        other.servers[0]
            .properties
            .insert("bootstrap.servers".into(), "other.invalid:9092".into());
        assert!(!slot.same_target(&other.servers[0]));
        assert!(
            store
                .save_with_credential(&mut other, 1, RememberChange::Keep)
                .is_err()
        );

        ws.servers[0]
            .properties
            .insert("sasl.mechanism".into(), "SCRAM-SHA-512".into());
        store
            .save_with_credential(&mut ws, 1, RememberChange::Replace(&secrets))
            .unwrap();
        assert!(!path.exists());
        assert!(slot.lock_refresh().is_err());
        assert!(!path.with_extension("refresh.lock").exists());
        assert!(slot.rotate("rotated", "late").is_err());
        let password = store.remembered_credential(&ws, 1).unwrap();
        assert_eq!(
            password.load().unwrap(),
            Properties::from([
                ("sasl.username".into(), "USERNAME_SENTINEL".into()),
                ("sasl.password".into(), "PASSWORD_SENTINEL".into()),
            ])
        );
        assert!(
            reopened
                .save_with_credential(&mut reloaded.clone(), 1, RememberChange::Forget)
                .is_err()
        );
        assert!(
            password.load().is_ok(),
            "A stale save must preserve the new credential"
        );
        let password_path = blob_path(&store.directory, &password.reference);
        fs::write(&password_path, b"damaged").unwrap();
        assert!(password.load().is_err());
        store
            .save_with_credential(&mut ws, 1, RememberChange::Forget)
            .unwrap();
        assert!(!password_path.exists());
        assert!(store.load().unwrap().remembered.is_empty());
        assert!(password.load().is_err());
    }
    #[cfg(windows)]
    #[test]
    fn dpapi_modified_connection_cannot_unlock_existing_blob() {
        let (store, mut ws, secrets) = fixture();
        store
            .save_with_credential(&mut ws, 1, RememberChange::Replace(&secrets))
            .unwrap();
        ws.servers[0].properties.insert(
            "oauth.token.endpoint.uri".into(),
            "https://other.invalid/token".into(),
        );
        // Simulate an out-of-band edit of the public workspace that leaves the same reference in place.
        store.save(&ws).unwrap();
        let handle = store.remembered_credential(&ws, 1).unwrap();
        assert!(handle.load().is_err());
        store
            .save_with_credential(&mut ws, 1, RememberChange::Forget)
            .unwrap();
    }
    #[cfg(not(windows))]
    #[test]
    fn non_windows_does_not_write_a_plaintext_fallback() {
        let (store, mut ws, secrets) = fixture();
        assert!(
            store
                .save_with_credential(&mut ws, 1, RememberChange::Replace(&secrets))
                .is_err()
        );
        assert!(ws.remembered.is_empty());
        assert!(!store.path().exists());
    }
}

#[cfg(not(windows))]
mod platform {
    const WINDOWS_ONLY: &str = "Remembered credentials require Windows DPAPI";
    pub(super) fn reference() -> Result<String, String> {
        Err(WINDOWS_ONLY.into())
    }
    pub(super) fn crypt(_: &[u8], _: &[u8], _: bool) -> Result<Vec<u8>, String> {
        Err(WINDOWS_ONLY.into())
    }
}
