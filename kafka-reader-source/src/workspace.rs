//! Local workspace persistence. Session credentials never enter the serialized model.
use crate::language::LanguagePreference;
use crate::model::{Properties, sensitive_property};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub const MAX_TABS: usize = 8;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub language: LanguagePreference,
    pub dark: bool,
    pub compact_rows: bool,
    pub font_size: f32,
    pub max_records: usize,
    pub seconds: u64,
    pub recent_window: i64,
    pub retained_records: usize,
    pub retained_mib: usize,
    pub utc: bool,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            language: LanguagePreference::System,
            dark: true,
            compact_rows: false,
            font_size: 14.,
            max_records: 2000,
            seconds: 60,
            recent_window: 100,
            retained_records: 10_000,
            retained_mib: 64,
            utc: true,
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<(), String> {
        if !self.font_size.is_finite() || !(11.0..=20.0).contains(&self.font_size) {
            return Err("Font size must be between 11 and 20 points".into());
        }
        if !(1..=1_000_000).contains(&self.max_records)
            || !(1..=86_400).contains(&self.seconds)
            || !(1..=1_000_000).contains(&self.recent_window)
        {
            return Err("Use 1–1,000,000 records/window positions and 1–86,400 seconds".into());
        }
        if !(100..=50_000).contains(&self.retained_records)
            || !(1..=512).contains(&self.retained_mib)
        {
            return Err(
                "Retention must be 100–50,000 records and 1–512 MiB across all topic tabs".into(),
            );
        }
        Ok(())
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_mib * 1024 * 1024
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    #[serde(default)]
    pub favorite: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Server {
    pub id: u64,
    pub name: String,
    #[serde(default)]
    pub offline: bool,
    #[serde(default)]
    pub properties: Properties,
    #[serde(default)]
    pub topics: Vec<Topic>,
}
impl Server {
    pub fn register_topic(&mut self, name: &str) -> Result<(), String> {
        validate_topic(name)?;
        if !self.topics.iter().any(|t| t.name == name) {
            if self.topics.len() >= 500 {
                return Err("A server can register up to 500 exact topic names".into());
            }
            self.topics.push(Topic {
                name: name.into(),
                favorite: false,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tab {
    pub server: u64,
    pub topic: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Workspace {
    pub version: u32,
    pub servers: Vec<Server>,
    /// Opaque references to per-user DPAPI records. Credentials never enter workspace.json.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remembered: BTreeMap<u64, String>,
    pub selected_server: u64,
    pub active_tab: Option<Tab>,
    pub tabs: Vec<Tab>,
    pub settings: Settings,
}
impl Default for Workspace {
    fn default() -> Self {
        Self {
            version: 2,
            servers: vec![],
            remembered: BTreeMap::new(),
            selected_server: 0,
            active_tab: None,
            tabs: vec![],
            settings: Settings::default(),
        }
    }
}
impl Workspace {
    /// Synthetic data exists only for the explicit GUI verification harness.
    /// Normal startup never adds a sample server; Store also excludes it from disk.
    #[doc(hidden)]
    pub fn qa_fixture() -> Self {
        let topic = "agreement.events".to_string();
        let tab = Tab {
            server: 1,
            topic: topic.clone(),
        };
        Self {
            version: 2,
            servers: vec![Server {
                id: 1,
                name: "QA sample".into(),
                offline: true,
                properties: Properties::new(),
                topics: [
                    "agreement.events",
                    "payments.events",
                    "shipments.events",
                    "identity.audit",
                    "agreement.dlq",
                ]
                .into_iter()
                .map(|name| Topic {
                    name: name.into(),
                    favorite: name == topic,
                })
                .collect(),
            }],
            selected_server: 1,
            remembered: BTreeMap::new(),
            active_tab: Some(tab.clone()),
            tabs: vec![tab],
            settings: Settings::default(),
        }
    }
    /// Remove legacy built-in sample servers using their explicit offline marker,
    /// never their names or IDs, so user-created connections remain intact.
    pub fn remove_offline_servers(&mut self) {
        let removed: BTreeSet<u64> = self
            .servers
            .iter()
            .filter(|server| server.offline)
            .map(|server| server.id)
            .collect();
        if removed.is_empty() {
            return;
        }
        self.servers.retain(|server| !server.offline);
        self.remembered.retain(|id, _| !removed.contains(id));
        self.tabs.retain(|tab| !removed.contains(&tab.server));
        if self
            .active_tab
            .as_ref()
            .is_some_and(|tab| removed.contains(&tab.server))
        {
            self.active_tab = self.tabs.first().cloned();
        }
        if removed.contains(&self.selected_server) {
            self.selected_server = self
                .active_tab
                .as_ref()
                .map(|tab| tab.server)
                .or_else(|| self.servers.first().map(|server| server.id))
                .unwrap_or(0);
        }
    }
    pub fn server(&self, id: u64) -> Option<&Server> {
        self.servers.iter().find(|s| s.id == id)
    }
    pub fn server_mut(&mut self, id: u64) -> Option<&mut Server> {
        self.servers.iter_mut().find(|s| s.id == id)
    }
    pub fn next_id(&self) -> u64 {
        self.servers.iter().map(|s| s.id).max().unwrap_or(0) + 1
    }
    pub fn open_tab(&mut self, tab: Tab, reader_active: bool) -> Result<(), String> {
        if reader_active && self.active_tab.as_ref() != Some(&tab) {
            return Err("Stop the active reader before switching topics or servers".into());
        }
        if !self
            .server(tab.server)
            .is_some_and(|s| s.topics.iter().any(|t| t.name == tab.topic))
        {
            return Err("Register the exact topic name on this server first".into());
        }
        if !self.tabs.contains(&tab) {
            if self.tabs.len() >= MAX_TABS {
                return Err("Eight topic tabs are open. Close one before opening another".into());
            }
            self.tabs.push(tab.clone());
        }
        self.selected_server = tab.server;
        self.active_tab = Some(tab);
        Ok(())
    }
    pub fn close_tab(&mut self, tab: &Tab, reader_active: bool) -> Result<(), String> {
        if reader_active && self.active_tab.as_ref() == Some(tab) {
            return Err("Stop the active reader before closing its topic tab".into());
        }
        let index = self
            .tabs
            .iter()
            .position(|t| t == tab)
            .ok_or("This tab is already closed")?;
        self.tabs.remove(index);
        if self.active_tab.as_ref() == Some(tab) {
            self.active_tab = self
                .tabs
                .get(index.saturating_sub(1))
                .or(self.tabs.last())
                .cloned();
            if let Some(t) = &self.active_tab {
                self.selected_server = t.server;
            }
        }
        Ok(())
    }
    pub fn sanitize(&mut self) {
        for server in &mut self.servers {
            server.properties.retain(|k, v| !session_only(k, v));
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 2 {
            return Err("Unsupported workspace version".into());
        }
        self.settings.validate()?;
        if self.servers.len() > 50 {
            return Err("Workspace can have up to 50 server profiles".into());
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for server in &self.servers {
            if !ids.insert(server.id)
                || server.name.trim().is_empty()
                || server.name.len() > 100
                || !names.insert(server.name.to_lowercase())
            {
                return Err(
                    "Server names must be unique and contain 1–100 characters; IDs must be unique"
                        .into(),
                );
            }
            if !server.offline
                && server
                    .properties
                    .get("bootstrap.servers")
                    .is_none_or(|s| s.trim().is_empty())
            {
                return Err(format!("{} needs bootstrap servers", server.name));
            }
            if server.topics.len() > 500 {
                return Err("Too many registered topics".into());
            }
            let mut topics = BTreeSet::new();
            for topic in &server.topics {
                validate_topic(&topic.name)?;
                if !topics.insert(&topic.name) {
                    return Err("Duplicate registered topic".into());
                }
            }
        }
        let mut references = BTreeSet::new();
        if self.remembered.iter().any(|(id, reference)| {
            !ids.contains(id)
                || !crate::remembered::valid_reference(reference)
                || !references.insert(reference)
        }) {
            return Err("Invalid remembered credential reference".into());
        }
        if !(ids.contains(&self.selected_server)
            || (self.servers.is_empty() && self.selected_server == 0))
        {
            return Err("Selected server is absent".into());
        }
        if self.tabs.len() > MAX_TABS {
            return Err("Too many open tabs".into());
        }
        let mut tabs = BTreeSet::new();
        for t in &self.tabs {
            if !tabs.insert(t)
                || !self
                    .server(t.server)
                    .is_some_and(|s| s.topics.iter().any(|topic| topic.name == t.topic))
            {
                return Err("An open tab refers to an absent or duplicate topic".into());
            }
        }
        if self
            .active_tab
            .as_ref()
            .is_some_and(|t| !self.tabs.contains(t))
        {
            return Err("Active tab is absent".into());
        }
        Ok(())
    }
}

pub fn session_only(key: &str, value: &str) -> bool {
    sensitive_property(key, value)
}
pub fn validate_topic(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 249
        || matches!(name, "." | "..")
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        Err(
            "Use an exact Kafka topic name: 1–249 letters, digits, dots, underscores or hyphens"
                .into(),
        )
    } else {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Store {
    pub directory: PathBuf,
    last_read: RefCell<Option<Vec<u8>>>,
    _temporary: Option<Arc<SmokeDirectory>>,
}

struct SmokeDirectory(PathBuf);

impl Drop for SmokeDirectory {
    fn drop(&mut self) {
        // Only implicit smoke directories belong to the app. Leave unrelated files alone.
        let _ = fs::remove_file(self.0.join("workspace.json"));
        let _ = fs::remove_file(self.0.join("workspace.lock"));
        if let Ok(entries) = fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name
                    .to_str()
                    .and_then(|s| s.strip_prefix("credential-"))
                    .and_then(|s| {
                        s.strip_suffix(".dpapi")
                            .or_else(|| s.strip_suffix(".refresh.lock"))
                    })
                    .is_some_and(crate::remembered::valid_reference)
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        let _ = fs::remove_dir(&self.0);
    }
}

impl Store {
    pub fn new(directory: Option<PathBuf>) -> Self {
        let directory = directory.unwrap_or_else(|| {
            let base = if cfg!(windows) {
                std::env::var_os("APPDATA").map(PathBuf::from)
            } else {
                std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
            };
            base.unwrap_or_else(|| PathBuf::from("."))
                .join("kafka-reader-rust")
        });
        Self {
            directory,
            last_read: RefCell::default(),
            _temporary: None,
        }
    }
    pub fn for_gui(directory: Option<PathBuf>, smoke: bool) -> Result<Self, String> {
        if !smoke {
            return Ok(Self::new(directory));
        }
        if let Some(directory) = directory {
            let store = Self::new(Some(directory));
            return match fs::symlink_metadata(store.path()) {
                Ok(_) => Err(
                    "GUI smoke requires a data directory without an existing workspace.json".into(),
                ),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(store),
                Err(e) => Err(e.to_string()),
            };
        }
        let root = std::env::temp_dir();
        let path = loop {
            let path = root.join(format!("kafka-reader-smoke-{}", temporary_suffix()));
            match workspace_directory(false).create(&path) {
                Ok(()) => break path,
                Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.to_string()),
            }
        };
        let mut store = Self::new(Some(path.clone()));
        store._temporary = Some(Arc::new(SmokeDirectory(path)));
        Ok(store)
    }
    pub fn path(&self) -> PathBuf {
        self.directory.join("workspace.json")
    }
    fn read_bytes(&self) -> Result<Option<Vec<u8>>, String> {
        let file = match fs::File::open(self.path()) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() > 1024 * 1024 {
            return Err("Workspace exceeds 1 MiB".into());
        }
        Ok(Some(bytes))
    }
    pub fn load(&self) -> Result<Workspace, String> {
        let Some(bytes) = self.read_bytes()? else {
            *self.last_read.borrow_mut() = None;
            return Ok(Workspace::default());
        };
        let mut value: Workspace =
            serde_json::from_slice(&bytes).map_err(|e| format!("Cannot load workspace: {e}"))?;
        value.sanitize();
        value.validate()?;
        value.remove_offline_servers();
        value.validate()?;
        *self.last_read.borrow_mut() = Some(bytes);
        Ok(value)
    }
    pub(crate) fn write_workspace_locked(&self, workspace: &Workspace) -> Result<(), String> {
        let mut safe = workspace.clone();
        safe.sanitize();
        safe.validate()?;
        safe.remove_offline_servers();
        safe.validate()?;
        let bytes = serde_json::to_vec_pretty(&safe).map_err(|e| e.to_string())?;
        if bytes.len() > 1024 * 1024 {
            return Err("Workspace exceeds 1 MiB".into());
        }
        if self.read_bytes()?.as_ref() != self.last_read.borrow().as_ref() {
            return Err(
                "The workspace changed on disk. Reopen it before saving this session.".into(),
            );
        }
        atomic_write(&self.path(), &bytes)?;
        *self.last_read.borrow_mut() = Some(bytes);
        Ok(())
    }
    pub fn save(&self, workspace: &Workspace) -> Result<(), String> {
        let _lock = self.lock()?;
        self.write_workspace_locked(workspace)
    }

    pub(crate) fn lock(&self) -> Result<fs::File, String> {
        let file = self.lock_file()?;
        file.try_lock()
            .map_err(|e| format!("Could not lock the workspace for saving: {e}"))?;
        Ok(file)
    }
    pub(crate) fn lock_file(&self) -> Result<fs::File, String> {
        workspace_directory(true)
            .create(&self.directory)
            .map_err(|e| e.to_string())?;
        // Closing this stable file handle releases its OS lock, even on process exit.
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join("workspace.lock"))
            .map_err(|e| e.to_string())
    }
}

fn workspace_directory(recursive: bool) -> fs::DirBuilder {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(recursive);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
}
fn temporary_suffix() -> String {
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let (temporary, mut file) = loop {
        let temporary = path.with_extension(format!("tmp-{}", temporary_suffix()));
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.to_string()),
        }
    };
    let result = (|| {
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        drop(file);
        fs::rename(&temporary, path).map_err(|e| e.to_string())?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn workspace_with_server() -> Workspace {
        let mut workspace = Workspace::qa_fixture();
        let server = &mut workspace.servers[0];
        server.name = "Test connection".into();
        server.offline = false;
        server
            .properties
            .insert("bootstrap.servers".into(), "localhost:9092".into());
        workspace
    }
    fn store() -> Store {
        Store::for_gui(None, true).unwrap()
    }
    #[test]
    fn parallel_smoke_runs_get_distinct_directories() {
        let barrier = std::sync::Barrier::new(8);
        let stores = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        store()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let paths: BTreeSet<_> = stores.iter().map(|store| &store.directory).collect();
        assert_eq!(paths.len(), stores.len());
        assert!(paths.iter().all(|p| p.is_dir()));
    }

    #[test]
    fn smoke_storage_is_isolated_and_refuses_an_existing_workspace() {
        let temporary = Store::for_gui(None, true).unwrap();
        let directory = temporary.directory.clone();
        assert_ne!(directory, Store::new(None).directory);
        let explicit = Store::for_gui(Some(directory.clone()), true).unwrap();
        temporary.save(&workspace_with_server()).unwrap();
        let original = fs::read(temporary.path()).unwrap();
        assert!(Store::for_gui(Some(directory.clone()), true).is_err());
        // Another instance created the workspace after this smoke store was prepared.
        assert!(explicit.save(&Workspace::qa_fixture()).is_err());
        assert_eq!(fs::read(temporary.path()).unwrap(), original);
        drop(explicit);
        assert!(directory.exists());
        drop(temporary);
        assert!(!directory.exists());
    }

    #[test]
    fn competing_saves_keep_the_winning_snapshot_and_report_the_conflict() {
        let owner = Store::for_gui(None, true).unwrap();
        owner.save(&workspace_with_server()).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let saved = std::thread::scope(|scope| {
            let mut handles = vec![];
            for name in ["First edit", "Second edit"] {
                let store = Store::new(Some(owner.directory.clone()));
                let mut workspace = store.load().unwrap();
                workspace.servers[0].name = name.into();
                let barrier = &barrier;
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    (name, store.save(&workspace))
                }));
            }
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let winners: Vec<_> = saved.iter().filter(|(_, result)| result.is_ok()).collect();
        assert_eq!(winners.len(), 1);
        let stored = owner.load().unwrap();
        assert_eq!(stored.servers[0].name, winners[0].0);
        owner.save(&stored).unwrap();
    }

    #[test]
    fn stale_sessions_cannot_overwrite_another_instances_edits() {
        let owner = Store::for_gui(None, true).unwrap();
        owner.save(&workspace_with_server()).unwrap();
        let other = Store::new(Some(owner.directory.clone()));
        let mut stale = other.load().unwrap();
        let mut updated = owner.load().unwrap();
        updated.servers[0].register_topic("new.events").unwrap();
        owner.save(&updated).unwrap();
        stale.servers[0].topics[0].favorite = false;
        assert!(other.save(&stale).is_err());
        assert_eq!(other.load().unwrap().servers, updated.servers);
    }
    #[test]
    fn secrets_never_reach_disk_and_workspace_roundtrips() {
        let store = store();
        let mut w = workspace_with_server();
        let p = &mut w.servers[0].properties;
        for key in [
            "sasl.username",
            "sasl.password",
            "oauth.token",
            "ssl.key.pem",
            "ssl.key.password",
        ] {
            p.insert(key.into(), "QA_PRIVATE_SENTINEL".into());
        }
        p.insert("ssl.key.location".into(), "C:/certs/key.pem".into());
        w.settings.compact_rows = true;
        store.save(&w).unwrap();
        assert!(
            !fs::read_to_string(store.path())
                .unwrap()
                .contains("QA_PRIVATE_SENTINEL")
        );
        let loaded = store.load().unwrap();
        assert!(loaded.settings.compact_rows);
        assert_eq!(
            loaded.servers[0].properties["ssl.key.location"],
            "C:/certs/key.pem"
        );
        assert_eq!(loaded.active_tab, w.active_tab);
        fs::remove_dir_all(store.directory).unwrap();
    }
    #[test]
    fn invalid_save_does_not_replace_valid_file() {
        let store = store();
        let mut w = Workspace::default();
        store.save(&w).unwrap();
        let before = fs::read(store.path()).unwrap();
        w.settings.retained_mib = 0;
        assert!(store.save(&w).is_err());
        assert_eq!(fs::read(store.path()).unwrap(), before);
        fs::remove_dir_all(store.directory).unwrap();
    }
    #[test]
    fn malformed_workspace_is_reported_without_overwrite() {
        let store = store();
        fs::create_dir_all(&store.directory).unwrap();
        fs::write(store.path(), b"{broken").unwrap();
        assert!(store.load().is_err());
        assert_eq!(fs::read(store.path()).unwrap(), b"{broken");
        fs::remove_dir_all(store.directory).unwrap();
    }
    #[test]
    fn topics_deduplicate_and_favorites_persist() {
        let store = store();
        let mut w = workspace_with_server();
        let count = w.servers[0].topics.len();
        w.servers[0].register_topic("agreement.events").unwrap();
        assert_eq!(w.servers[0].topics.len(), count);
        w.servers[0].topics[1].favorite = true;
        store.save(&w).unwrap();
        assert!(store.load().unwrap().servers[0].topics[1].favorite);
        fs::remove_dir_all(store.directory).unwrap();
    }
    #[test]
    fn tabs_are_functional_and_reader_guard_is_contextual() {
        let mut w = workspace_with_server();
        let first = w.active_tab.clone().unwrap();
        let second = Tab {
            server: 1,
            topic: "payments.events".into(),
        };
        assert!(w.open_tab(second.clone(), true).is_err());
        w.open_tab(second.clone(), false).unwrap();
        w.open_tab(second.clone(), false).unwrap();
        assert_eq!(w.tabs.len(), 2);
        w.close_tab(&first, true).unwrap();
        assert_eq!(w.active_tab, Some(second.clone()));
        assert!(w.close_tab(&second, true).is_err());
        w.close_tab(&second, false).unwrap();
        assert!(w.active_tab.is_none());
    }
    #[test]
    fn draft_cancel_does_not_mutate_original() {
        let original = workspace_with_server();
        let mut draft = original.servers[0].clone();
        draft.name = "Changed".into();
        draft.topics.clear();
        drop(draft);
        assert_eq!(original.servers[0].name, "Test connection");
        assert_eq!(original.servers[0].topics.len(), 5);
    }
    #[test]
    fn settings_validate_actual_resource_bounds() {
        let mut settings = Settings::default();
        settings.font_size = f32::NAN;
        assert!(settings.validate().is_err());
        settings = Settings::default();
        settings.retained_records = 0;
        assert!(settings.validate().is_err());
        settings.retained_records = 100;
        settings.retained_mib = 1;
        assert!(settings.validate().is_ok());
        assert_eq!(settings.retained_bytes(), 1024 * 1024);
    }

    #[test]
    fn a_new_workspace_is_empty_and_can_be_saved() {
        let store = store();
        let workspace = store.load().unwrap();
        assert!(workspace.servers.is_empty());
        assert!(workspace.tabs.is_empty());
        assert!(workspace.active_tab.is_none());
        assert_eq!(workspace.selected_server, 0);
        store.save(&workspace).unwrap();
        assert!(store.load().unwrap().servers.is_empty());
        fs::remove_dir_all(store.directory).unwrap();
    }

    #[test]
    fn legacy_sample_is_removed_without_changing_real_servers_or_preferences() {
        let store = store();
        let mut legacy = Workspace::qa_fixture();
        legacy.servers[0].name = "Offline workspace".into();
        let mut real = workspace_with_server().servers.remove(0);
        real.id = 2;
        real.name = "Development".into();
        real.topics[1].favorite = true;
        legacy.servers.push(real.clone());
        let real_tab = Tab {
            server: 2,
            topic: "payments.events".into(),
        };
        legacy.tabs.push(real_tab.clone());
        legacy.settings.dark = false;
        legacy.settings.font_size = 16.;
        fs::create_dir_all(&store.directory).unwrap();
        let original = serde_json::to_vec(&legacy).unwrap();
        fs::write(store.path(), &original).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.servers, vec![real]);
        assert_eq!(loaded.tabs, vec![real_tab.clone()]);
        assert_eq!(loaded.active_tab, Some(real_tab));
        assert_eq!(loaded.selected_server, 2);
        assert_eq!(loaded.settings, legacy.settings);
        assert_eq!(fs::read(store.path()).unwrap(), original);

        store.save(&loaded).unwrap();
        let saved = fs::read_to_string(store.path()).unwrap();
        assert!(!saved.contains("Offline workspace"));
        assert_eq!(store.load().unwrap().servers, loaded.servers);
        fs::remove_dir_all(store.directory).unwrap();
    }

    #[test]
    fn sample_only_workspace_becomes_empty_and_qa_sample_never_persists() {
        let store = store();
        let fixture = Workspace::qa_fixture();
        fs::create_dir_all(&store.directory).unwrap();
        fs::write(store.path(), serde_json::to_vec(&fixture).unwrap()).unwrap();
        let migrated = store.load().unwrap();
        assert!(migrated.servers.is_empty() && migrated.tabs.is_empty());
        assert!(migrated.active_tab.is_none());
        assert_eq!(migrated.selected_server, 0);

        store.save(&fixture).unwrap();
        assert!(store.load().unwrap().servers.is_empty());
        assert!(
            !fs::read_to_string(store.path())
                .unwrap()
                .contains("QA sample")
        );
        fs::remove_dir_all(store.directory).unwrap();
    }

    #[test]
    fn real_server_with_a_sample_name_is_preserved() {
        let store = store();
        let mut workspace = workspace_with_server();
        workspace.servers[0].name = "Offline workspace".into();
        store.save(&workspace).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.servers, workspace.servers);
        assert_eq!(loaded.tabs, workspace.tabs);
        assert_eq!(loaded.active_tab, workspace.active_tab);
        fs::remove_dir_all(store.directory).unwrap();
    }
}
