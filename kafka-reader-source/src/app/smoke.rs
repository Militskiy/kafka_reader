//! Deterministic UI exercise hooks. They use the same application actions as the UI.
//! Checkpoints let an independent X11 observer verify actual top-level windows.
use super::*;
use kafka_reader_rust::language::{Language, LanguagePreference};
use std::fs;
const SECRET: &str = "KAFKA_UI_QA_SECRET_2026";

pub(super) struct Smoke {
    stage: usize,
    entered: Instant,
    phase: String,
    kind: String,
    window_title: String,
    announced: bool,
    phase_started: Instant,
    captured: bool,
    checks: usize,
    paused_count: usize,
    pub native_window_seen: bool,
    done: bool,
}
impl Default for Smoke {
    fn default() -> Self {
        Self {
            stage: 0,
            entered: Instant::now(),
            phase: String::new(),
            kind: String::new(),
            window_title: String::new(),
            announced: false,
            phase_started: Instant::now(),
            captured: false,
            checks: 0,
            paused_count: 0,
            native_window_seen: false,
            done: false,
        }
    }
}
impl ReaderApp {
    fn step(&mut self, stage: usize) {
        self.smoke.stage = stage;
        self.smoke.entered = Instant::now();
    }
    fn phase(&mut self, name: &str, kind: &str, title: &str) {
        self.smoke.phase = name.into();
        self.smoke.kind = kind.into();
        self.smoke.window_title = title.into();
        self.smoke.announced = false;
        self.smoke.phase_started = Instant::now();
        self.smoke.captured = false;
        if let Some(folder) = self
            .options
            .screenshot_dir
            .as_ref()
            .or(self.options.reopen.as_ref())
        {
            fs::create_dir_all(folder).expect("Create GUI QA output directory");
        }
    }
    fn announce_phase(&mut self) {
        if self.smoke.announced {
            return;
        }
        self.smoke.announced = true;
        if let Some(folder) = self
            .options
            .screenshot_dir
            .as_ref()
            .or(self.options.reopen.as_ref())
        {
            fs::create_dir_all(folder).expect("Create GUI QA output directory");
            atomic_write(
                &folder.join("gui-phase.json"),
                serde_json::to_string_pretty(
                    &serde_json::json!({"phase":self.smoke.phase,"windowTitle":self.smoke.window_title,"kind":self.smoke.kind}),
                )
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
        }
    }
    pub(super) fn automate(&mut self, ctx: &egui::Context) {
        if self.options.reopen.is_some() {
            if !self.smoke.done && self.created.elapsed() > Duration::from_millis(650) {
                assert!(
                    self.session.is_none(),
                    "Reopening must never connect or read"
                );
                assert!(
                    self.view.buffer.records.is_empty(),
                    "Message bodies must not reload"
                );
                assert_eq!(
                    self.workspace.servers.len(),
                    1,
                    "Only the saved real server reloads"
                );
                let server = &self.workspace.servers[0];
                assert_eq!(server.name, "QA development");
                if cfg!(windows) {
                    assert!(self.workspace.remembered.contains_key(&server.id));
                    assert_eq!(
                        self.credentials.get(&server.id),
                        Some(&Properties::from([
                            ("sasl.username".into(), "qa-reader".into()),
                            ("sasl.password".into(), SECRET.into()),
                        ]))
                    );
                } else {
                    assert!(self.credentials.is_empty());
                }
                assert!(self.credential_errors.is_empty());
                assert!(!server.offline, "The synthetic QA server must not persist");
                assert!(
                    server
                        .topics
                        .iter()
                        .any(|t| t.name == "orders.events" && t.favorite)
                );
                assert!(server.topics.iter().any(|t| t.name == "manual.audit"));
                assert!(self.workspace.settings.compact_rows);
                assert_eq!(self.workspace.settings.retained_records, 120);
                assert_eq!(self.workspace.settings.max_records, 333);
                assert_eq!(self.workspace.settings.seconds, 45);
                assert_eq!(
                    self.workspace.settings.language,
                    LanguagePreference::Russian
                );
                assert_eq!(self.language(), Language::Russian);
                assert_eq!(self.workspace.tabs.len(), 2);
                assert!(self.workspace.tabs.iter().all(|t| t.server == server.id));
                assert!(self.active().is_some_and(|t| t.server == server.id));
                self.smoke.checks = if cfg!(windows) { 19 } else { 18 };
                self.smoke.done = true;
                self.phase("reopened", "main", "Kafka Reader");
                let report = serde_json::json!({"guiReopen":"PASS","checks":self.smoke.checks,"language":self.workspace.settings.language,"sessionStarted":false,"credentialsRestored":cfg!(windows),"sessionOnlyCredentialsRestored":false,"recordsRestored":0,"offlineServersRestored":0,"realServersRestored":1,"appVersion":env!("CARGO_PKG_VERSION")});
                let directory = self.options.reopen.as_ref().unwrap();
                atomic_write(
                    &directory.join("gui-reopen-report.json"),
                    report.to_string().as_bytes(),
                )
                .unwrap();
                println!("{report}");
            }
            if self.smoke.done
                && self.smoke.captured
                && self.smoke.phase_started.elapsed() > Duration::from_millis(1200)
            {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            return;
        }
        if !self.options.smoke {
            return;
        }
        let elapsed = self.smoke.entered.elapsed();
        match self.smoke.stage {
            0 if self.view.buffer.records.len() >= 45 && !self.view.formatted.is_empty() => {
                self.smoke.checks += 2;
                let active = self.active().cloned().unwrap();
                let other = Tab {
                    server: 1,
                    topic: "payments.events".into(),
                };
                assert!(self.activate(other).is_err());
                assert_eq!(self.active(), Some(&active));
                self.smoke.checks += 1;
                assert!(self.close_tab(&active).is_err());
                self.smoke.checks += 1;
                self.session.as_ref().unwrap().toggle_pause();
                self.step(1);
            }
            1 if elapsed > Duration::from_millis(450) => {
                self.smoke.paused_count = self.view.buffer.records.len();
                self.step(2);
            }
            2 if elapsed > Duration::from_millis(400) => {
                assert_eq!(self.view.buffer.records.len(), self.smoke.paused_count);
                self.smoke.checks += 1;
                self.view.filter = "no-match-ui-qa-938423".into();
                assert!(self.view.visible().is_empty());
                self.smoke.checks += 1;
                self.view.filter.clear();
                assert_eq!(self.view.visible().len(), self.smoke.paused_count);
                self.smoke.checks += 1;
                self.toggle_favorite(1, "payments.events");
                assert!(
                    self.workspace
                        .server(1)
                        .unwrap()
                        .topics
                        .iter()
                        .find(|t| t.name == "payments.events")
                        .unwrap()
                        .favorite
                );
                self.smoke.checks += 1;
                self.session.as_ref().unwrap().toggle_pause();
                self.step(3);
            }
            3 if elapsed > Duration::from_millis(500) => {
                assert!(self.view.buffer.records.len() > self.smoke.paused_count);
                self.smoke.checks += 1;
                self.stop();
                self.step(4);
            }
            4 if !self.running() => {
                let count = self.view.buffer.records.len();
                assert!(count > 0);
                self.smoke.checks += 1;
                let arrival_order: Vec<_> = self
                    .view
                    .buffer
                    .records
                    .iter()
                    .map(|r| (r.partition, r.offset))
                    .collect();
                let selected = self.view.selected.clone().unwrap();
                assert_eq!(self.view.order.column, SortColumn::Time);
                assert!(self.view.order.descending);
                self.smoke.checks += 1;
                for column in [SortColumn::Time, SortColumn::Partition, SortColumn::Offset] {
                    self.view.order.select(column);
                    if self.view.order.descending {
                        self.view.order.select(column);
                    }
                    for descending in [false, true] {
                        let records = self.view.visible();
                        let keys: Vec<i64> = records
                            .iter()
                            .map(|r| match column {
                                SortColumn::Time => r.timestamp,
                                SortColumn::Partition => i64::from(r.partition),
                                SortColumn::Offset => r.offset,
                            })
                            .collect();
                        assert!(keys.windows(2).all(|pair| if descending {
                            pair[0] >= pair[1]
                        } else {
                            pair[0] <= pair[1]
                        }));
                        assert_eq!(self.view.order.descending, descending);
                        self.smoke.checks += 1;
                        if !descending {
                            self.view.order.select(column);
                        }
                    }
                }
                assert!(Arc::ptr_eq(self.view.selected.as_ref().unwrap(), &selected));
                assert_eq!(
                    self.view
                        .buffer
                        .records
                        .iter()
                        .map(|r| (r.partition, r.offset))
                        .collect::<Vec<_>>(),
                    arrival_order
                );
                self.smoke.checks += 2;
                let original = Tab {
                    server: 1,
                    topic: "agreement.events".into(),
                };
                let other = Tab {
                    server: 1,
                    topic: "payments.events".into(),
                };
                self.activate(other.clone()).unwrap();
                assert!(self.view.buffer.records.is_empty() && self.session.is_none());
                assert_eq!(self.view.order.column, SortColumn::Time);
                assert!(self.view.order.descending);
                self.smoke.checks += 2;
                self.activate(original.clone()).unwrap();
                assert_eq!(self.view.buffer.records.len(), count);
                assert_eq!(self.view.order.column, SortColumn::Offset);
                assert!(self.view.order.descending);
                self.smoke.checks += 2;
                self.view.order = MessageOrder::default();
                let tabs = self.workspace.tabs.len();
                self.activate(original.clone()).unwrap();
                assert_eq!(self.workspace.tabs.len(), tabs);
                self.smoke.checks += 1;
                self.close_tab(&other).unwrap();
                assert_eq!(self.active(), Some(&original));
                self.smoke.checks += 1;
                self.activate(other).unwrap();
                self.activate(original).unwrap();
                let record = self
                    .view
                    .visible()
                    .into_iter()
                    .find(|record| {
                        record.key.as_ref().is_some_and(|bytes| !bytes.is_empty())
                            && record.value.as_ref().is_some_and(|bytes| {
                                serde_json::from_slice::<serde_json::Value>(bytes).is_ok()
                            })
                    })
                    .expect("The fixture contains a text key and JSON value");
                self.view.inspector_tab = 1;
                self.view.key_format = PayloadFormat::Auto;
                self.select(record);
                self.step(40);
            }
            40 if elapsed > Duration::from_millis(150) && !self.view.pending_preview => {
                let selected = self.view.selected.as_ref().unwrap();
                let expected = std::str::from_utf8(selected.key.as_deref().unwrap()).unwrap();
                assert_eq!(self.view.formatted, expected);
                assert!(!self.view.formatted.contains("KEY · HEX"));
                self.smoke.checks += 2;
                self.phase("key-text", "main", "Kafka Reader");
                self.step(41);
            }
            41 if elapsed > Duration::from_millis(1200) => {
                self.view.key_format = PayloadFormat::Hex;
                self.select(self.view.selected.clone().unwrap());
                self.step(42);
            }
            42 if elapsed > Duration::from_millis(150) && !self.view.pending_preview => {
                assert!(self.view.formatted.starts_with("00000000"));
                assert!(self.view.formatted.contains("41 47 52 2D"));
                self.smoke.checks += 1;
                self.phase("key-hex", "main", "Kafka Reader");
                self.step(43);
            }
            43 if elapsed > Duration::from_millis(1200) => {
                self.view.inspector_tab = 0;
                self.view.value_format = PayloadFormat::Auto;
                self.select(self.view.selected.clone().unwrap());
                self.step(44);
            }
            44 if elapsed > Duration::from_millis(150) && !self.view.pending_preview => {
                assert!(self.view.formatted.starts_with('{'));
                assert!(self.view.formatted.contains("\n"));
                assert_eq!(self.view.key_format, PayloadFormat::Hex);
                self.smoke.checks += 2;
                self.view.key_format = PayloadFormat::Auto;
                self.phase("main", "main", "Kafka Reader");
                self.step(5);
            }
            5 if self.smoke.phase_started.elapsed() > Duration::from_millis(1200) => {
                self.open_server(None);
                let editor = self.server_editor.as_mut().unwrap();
                editor.name = "QA development".into();
                editor.brokers = "localhost:9093".into();
                editor.protocol = "SASL_SSL".into();
                editor.mechanism = "SCRAM-SHA-512".into();
                editor.test_topic = "orders.events".into();
                editor.username = "qa-reader".into();
                editor.password = SECRET.into();
                editor.remember = cfg!(windows);
                editor.key_password = SECRET.into();
                editor.ca = "C:/certs/ca.pem".into();
                editor.certificate = "C:/certs/client.pem".into();
                editor.key = "C:/certs/client-key.pem".into();
                editor.advanced = "isolation.level=read_committed\nclient.id=kafka-reader".into();
                self.phase(
                    "connection",
                    "server",
                    dialogs::server_title(true, self.language()),
                );
                self.step(6);
            }
            6 if elapsed > Duration::from_millis(1200) => {
                self.server_editor.as_mut().unwrap().tab = 1;
                self.phase(
                    "authentication",
                    "server",
                    dialogs::server_title(true, self.language()),
                );
                self.step(7);
            }
            7 if elapsed > Duration::from_millis(1200) => {
                let editor = self.server_editor.as_mut().unwrap();
                editor.mechanism = "OAUTHBEARER".into();
                editor.oauth_endpoint =
                    "https://sso.example.org/realms/test/protocol/openid-connect/token".into();
                editor.refresh_token = SECRET.into();
                self.phase(
                    "oauth",
                    "server",
                    dialogs::server_title(true, self.language()),
                );
                self.step(45);
            }
            45 if elapsed > Duration::from_millis(1200) => {
                self.server_editor.as_mut().unwrap().mechanism = "SCRAM-SHA-512".into();
                self.server_editor.as_mut().unwrap().tab = 2;
                self.phase(
                    "tls",
                    "server",
                    dialogs::server_title(true, self.language()),
                );
                self.step(8);
            }
            8 if elapsed > Duration::from_millis(1200) => {
                self.server_editor.as_mut().unwrap().tab = 3;
                self.phase(
                    "advanced",
                    "server",
                    dialogs::server_title(true, self.language()),
                );
                self.step(9);
            }
            9 if elapsed > Duration::from_millis(1200) => {
                let mut editor = self.server_editor.take().unwrap();
                editor.advanced.push_str(&format!(
                    "\ncustom.token={SECRET}\nssl.key.pem=PRIVATE KEY {SECRET}"
                ));
                let (built, secrets) = editor.build(self.language()).unwrap();
                assert_eq!(built.properties["ssl.ca.location"], "C:/certs/ca.pem");
                assert_eq!(secrets["sasl.password"], SECRET);
                self.smoke.checks += 1;
                self.save_server(&editor).unwrap();
                assert!(self.session.as_ref().is_none_or(|s| !s.running()));
                self.smoke.checks += 1;
                let id = self
                    .workspace
                    .servers
                    .iter()
                    .find(|s| s.name == "QA development")
                    .unwrap()
                    .id;
                self.workspace
                    .server_mut(id)
                    .unwrap()
                    .register_topic("manual.audit")
                    .unwrap();
                self.toggle_favorite(id, "orders.events");
                self.activate(Tab {
                    server: id,
                    topic: "manual.audit".into(),
                })
                .unwrap();
                self.activate(Tab {
                    server: id,
                    topic: "orders.events".into(),
                })
                .unwrap();
                assert!(
                    self.workspace
                        .server(id)
                        .unwrap()
                        .topics
                        .iter()
                        .any(|t| t.name == "orders.events" && t.favorite)
                );
                assert!(
                    self.workspace
                        .tabs
                        .iter()
                        .any(|t| t.server == id && t.topic == "manual.audit")
                );
                self.smoke.checks += 2;
                self.open_server(Some(id));
                self.server_editor.as_mut().unwrap().name = "DISCARDED EDIT".into();
                self.server_editor = None;
                assert_eq!(self.workspace.server(id).unwrap().name, "QA development");
                self.smoke.checks += 1;
                assert_eq!(self.credentials[&id]["sasl.password"], SECRET);
                assert!(!self.credentials.contains_key(&1));
                self.smoke.checks += 1;
                let original = self.workspace.settings.clone();
                assert_eq!(original.language, LanguagePreference::System);
                self.smoke.checks += 1;
                self.settings_editor = Some(SettingsEditor::new(&original));
                self.settings_editor.as_mut().unwrap().value.dark = !original.dark;
                self.settings_editor = None;
                assert_eq!(self.workspace.settings, original);
                self.smoke.checks += 1;
                let mut editor = SettingsEditor::new(&original);
                editor.value.dark = false;
                editor.value.language = LanguagePreference::Russian;
                editor.value.font_size = 13.;
                editor.value.max_records = 333;
                editor.value.seconds = 45;
                editor.value.recent_window = 75;
                editor.value.retained_records = 120;
                editor.value.retained_mib = 2;
                editor.value.utc = false;
                self.settings_editor = Some(editor);
                self.activate(Tab {
                    server: 1,
                    topic: "agreement.events".into(),
                })
                .unwrap();
                self.phase(
                    "settings",
                    "settings",
                    dialogs::settings_title(self.language()),
                );
                self.step(10);
            }
            10 if elapsed > Duration::from_millis(1200) => {
                let settings = self.settings_editor.take().unwrap().value;
                self.apply_settings(settings, ctx).unwrap();
                assert_eq!(self.view.max_records, 333);
                assert_eq!(self.view.seconds, 45);
                assert_eq!(self.language(), Language::Russian);
                self.smoke.checks += 1;
                self.smoke.checks += 1;
                let retained = self.view.buffer.records.len()
                    + self
                        .cached
                        .values()
                        .map(|v| v.buffer.records.len())
                        .sum::<usize>();
                assert!(retained <= 120);
                self.smoke.checks += 1;
                self.settings_editor = Some(SettingsEditor::new(&self.workspace.settings));
                self.phase(
                    "settings-ru",
                    "settings",
                    dialogs::settings_title(self.language()),
                );
                self.step(14);
            }
            14 if elapsed > Duration::from_millis(1200) => {
                self.settings_editor = None;
                self.phase("light", "main", "Kafka Reader");
                self.step(11);
            }
            11 if elapsed > Duration::from_millis(1200) => {
                let mut settings = self.workspace.settings.clone();
                settings.dark = true;
                settings.compact_rows = true;
                self.apply_settings(settings, ctx).unwrap();
                ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(egui::vec2(1050., 720.)));
                self.phase("compact", "main", "Kafka Reader");
                self.step(12);
            }
            12 if elapsed > Duration::from_millis(1200) => {
                self.persist();
                let text = fs::read_to_string(self.options.store.path()).unwrap();
                assert!(
                    !text.contains(SECRET)
                        && !text.contains("qa-reader")
                        && !text.contains("PRIVATE KEY")
                );
                self.smoke.checks += 1;
                assert!(!text.contains("agreementId"));
                self.smoke.checks += 1;
                assert!(
                    self.smoke.native_window_seen,
                    "Server/settings must be actual native viewports"
                );
                self.smoke.checks += 1;
                let loaded = self.options.store.load().unwrap();
                assert_eq!(loaded.settings, self.workspace.settings);
                self.smoke.checks += 1;
                assert!(loaded.servers.iter().all(|server| !server.offline));
                assert_eq!(loaded.servers.len(), 1);
                assert!(loaded.tabs.iter().all(|tab| tab.server != 1));
                assert!(!text.contains("QA sample") && !text.contains("agreement.events"));
                self.smoke.checks += 4;
                let report = serde_json::json!({"guiSmoke":"PASS","checks":self.smoke.checks,"retained":self.view.buffer.records.len(),"nativeChildViewport":true,"openTabs":self.workspace.tabs.len(),"appVersion":env!("CARGO_PKG_VERSION")});
                if let Some(folder) = &self.options.screenshot_dir {
                    atomic_write(
                        &folder.join("gui-smoke-report.json"),
                        report.to_string().as_bytes(),
                    )
                    .unwrap();
                }
                println!("{report}");
                self.smoke.done = true;
                self.step(13);
            }
            13 if elapsed > Duration::from_millis(200) => {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            _ => {}
        }
        if self.created.elapsed() > Duration::from_secs(45) {
            panic!(
                "GUI smoke did not finish within 45 seconds (stage {})",
                self.smoke.stage
            );
        }
    }
    pub(super) fn capture_dialog_if_needed(&mut self, ctx: &egui::Context, kind: &str) {
        if self.smoke.kind == kind
            && self.smoke.phase_started.elapsed() > Duration::from_millis(650)
            && !self.smoke.captured
        {
            self.announce_phase();
            self.capture_phase(ctx);
        }
    }
    fn capture_phase(&mut self, ctx: &egui::Context) {
        let Some(folder) = self
            .options
            .screenshot_dir
            .as_ref()
            .or(self.options.reopen.as_ref())
        else {
            return;
        };
        let file = folder.join(format!("rust-{}.png", self.smoke.phase));
        self.smoke.captured = true;
        save_screenshot(ctx, file, false);
    }
    pub(super) fn capture_main_if_needed(&mut self, ctx: &egui::Context) {
        if self.smoke.kind == "main"
            && self.smoke.phase_started.elapsed() > Duration::from_millis(650)
            && !self.smoke.captured
        {
            self.announce_phase();
            self.capture_phase(ctx);
        }
        if !self.options.smoke
            && self.options.reopen.is_none()
            && !self.screenshot_requested
            && self.created.elapsed() > Duration::from_millis(1600)
        {
            if let Some(path) = self.options.screenshot.clone() {
                self.screenshot_requested = true;
                save_screenshot(ctx, path, true);
            }
        }
    }
}
fn save_screenshot(ctx: &egui::Context, path: PathBuf, close: bool) {
    let context = ctx.clone();
    ctx.request_screenshot(move |image| {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).expect("Create screenshot directory");
            }
        }
        let pixels: Vec<u8> = image.pixels.iter().flat_map(|p| p.to_array()).collect();
        image::save_buffer(
            &path,
            &pixels,
            image.width() as u32,
            image.height() as u32,
            image::ColorType::Rgba8,
        )
        .expect("Save actual native screenshot");
        if close {
            context.send_viewport_cmd_to(egui::ViewportId::ROOT, egui::ViewportCommand::Close);
        }
        context.request_repaint();
    });
}
