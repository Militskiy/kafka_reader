use super::*;
use std::fs;
use std::io::Write;

pub(super) struct ServerEditor {
    original: Server,
    pub name: String,
    pub brokers: String,
    pub protocol: String,
    pub mechanism: String,
    pub username: String,
    pub password: String,
    pub ca: String,
    pub certificate: String,
    pub key: String,
    pub key_password: String,
    pub advanced: String,
    pub test_topic: String,
    pub file_path: String,
    pub tab: usize,
    pub message: String,
    test: Option<Session>,
    pub is_new: bool,
}
impl ServerEditor {
    fn new(server: Server, credentials: Properties, is_new: bool, topic: String) -> Self {
        let mut properties = server.properties.clone();
        properties.extend(credentials);
        let mut take = |key: &str| properties.remove(key).unwrap_or_default();
        let brokers = take("bootstrap.servers");
        let mut protocol = take("security.protocol");
        if protocol.is_empty() {
            protocol = "PLAINTEXT".into();
        }
        let mut mechanism = take("sasl.mechanism");
        if mechanism.is_empty() {
            mechanism = take("sasl.mechanisms");
        }
        if mechanism.is_empty() {
            mechanism = "PLAIN".into();
        }
        let username = take("sasl.username");
        let password = take("sasl.password");
        let ca = take("ssl.ca.location");
        let certificate = take("ssl.certificate.location");
        let key = take("ssl.key.location");
        let key_password = take("ssl.key.password");
        Self {
            name: server.name.clone(),
            original: server,
            brokers,
            protocol,
            mechanism,
            username,
            password,
            ca,
            certificate,
            key,
            key_password,
            advanced: properties
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("\n"),
            test_topic: topic,
            file_path: "server.properties".into(),
            tab: 0,
            message: String::new(),
            test: None,
            is_new,
        }
    }
    pub(super) fn build(&self) -> Result<(Server, Properties), String> {
        let mut p = parse_properties(&self.advanced)?;
        p.insert("bootstrap.servers".into(), self.brokers.trim().into());
        p.insert("security.protocol".into(), self.protocol.clone());
        p.insert("sasl.mechanism".into(), self.mechanism.clone());
        for (key, value) in [
            ("sasl.username", &self.username),
            ("sasl.password", &self.password),
            ("ssl.ca.location", &self.ca),
            ("ssl.certificate.location", &self.certificate),
            ("ssl.key.location", &self.key),
            ("ssl.key.password", &self.key_password),
        ] {
            if !value.is_empty() {
                p.insert(key.into(), value.clone());
            }
        }
        let secrets = p
            .iter()
            .filter(|(k, v)| session_only(k, v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        p.retain(|k, v| !k.starts_with("reader.") && !session_only(k, v));
        let mut server = self.original.clone();
        server.name = self.name.trim().into();
        server.properties = p;
        if server.name.is_empty() || server.name.len() > 100 {
            return Err("Use a server name containing 1–100 characters".into());
        }
        if !server.offline && self.brokers.trim().is_empty() {
            return Err("Enter at least one bootstrap broker as host:port".into());
        }
        if !self.test_topic.trim().is_empty() {
            server.register_topic(self.test_topic.trim())?;
        }
        Ok((server, secrets))
    }
    fn test(&mut self) {
        if self.test.as_ref().is_some_and(Session::running) {
            return;
        }
        let request = (|| {
            validate_topic(self.test_topic.trim())?;
            let (server, credentials) = self.build()?;
            let mut props = server.properties;
            props.extend(credentials);
            Ok(Request {
                props,
                topic: self.test_topic.trim().into(),
                partitions: vec![],
                mode: Mode::Earliest,
                value: 0,
                follow: false,
                max_records: 1,
                seconds: 5,
                demo: server.offline,
                inspect: true,
            })
        })();
        match request {
            Ok(request) => {
                self.test = Some(Session::start(request));
                self.message = "Checking this exact topic…".into();
            }
            Err(e) => self.message = e,
        }
    }
    fn drain_test(&mut self) {
        if let Some(test) = &self.test {
            while let Some(event) = test.try_next() {
                match event {
                    Event::Metadata(p) => {
                        self.message = format!("Topic access verified · {} partitions", p.len())
                    }
                    Event::State(state, detail)
                        if matches!(state.as_str(), "FAILED" | "STOPPED") =>
                    {
                        self.message = detail
                    }
                    _ => {}
                }
            }
        }
    }
    fn import(&mut self) {
        let result: Result<ServerEditor, String> = (|| {
            let path = PathBuf::from(&self.file_path);
            if fs::metadata(&path).map_err(|e| e.to_string())?.len() > 1024 * 1024 {
                return Err("Profile exceeds 1 MiB".into());
            }
            let mut props =
                parse_properties(&fs::read_to_string(path).map_err(|e| e.to_string())?)?;
            let topic = props
                .remove("reader.topic")
                .unwrap_or_else(|| self.test_topic.clone());
            let mut server = self.original.clone();
            server.name = self.name.clone();
            server.properties = props;
            let mut loaded = Self::new(server, Properties::new(), self.is_new, topic);
            loaded.file_path = self.file_path.clone();
            loaded.tab = 3;
            loaded.message =
                "Profile imported into this draft. Save applies it; Cancel discards it.".into();
            Ok(loaded)
        })();
        match result {
            Ok(loaded) => *self = loaded,
            Err(e) => self.message = format!("Import failed: {e}"),
        }
    }
    fn export(&mut self) {
        let result = (|| {
            let (server, _) = self.build()?;
            let mut properties = server.properties;
            properties.insert("reader.topic".into(), self.test_topic.clone());
            let path = PathBuf::from(&self.file_path);
            create_export(&path)?
                .write_all(profile_text(&properties).as_bytes())
                .map_err(|e| e.to_string())
        })();
        self.message = match result {
            Ok(()) => "Exported this profile without session credentials".into(),
            Err(e) => format!("Export failed: {e}"),
        };
    }
}
pub(super) struct SettingsEditor {
    pub value: Settings,
    pub error: String,
}
impl SettingsEditor {
    pub fn new(settings: &Settings) -> Self {
        Self {
            value: settings.clone(),
            error: String::new(),
        }
    }
}
pub(super) struct TopicEditor {
    pub server: u64,
    pub name: String,
    pub error: String,
}
pub(super) struct ExportEditor {
    pub path: String,
    pub records: Vec<Arc<Record>>,
}
#[derive(Clone, Copy, PartialEq)]
enum Action {
    None,
    Save,
    Cancel,
}

impl ReaderApp {
    pub(super) fn open_server(&mut self, id: Option<u64>) {
        if self.running() {
            self.notice = "Stop reading before changing server configuration".into();
            return;
        }
        let server = id
            .and_then(|id| self.workspace.server(id))
            .cloned()
            .unwrap_or_else(|| Server {
                id: self.workspace.next_id(),
                name: String::new(),
                offline: false,
                properties: Properties::new(),
                topics: vec![],
            });
        let credentials = self
            .credentials
            .get(&server.id)
            .cloned()
            .unwrap_or_default();
        let topic = if self.active().is_some_and(|t| t.server == server.id) {
            self.active().unwrap().topic.clone()
        } else {
            server
                .topics
                .first()
                .map(|t| t.name.clone())
                .unwrap_or_default()
        };
        self.server_editor = Some(ServerEditor::new(server, credentials, id.is_none(), topic));
    }
    pub(super) fn save_server(&mut self, editor: &ServerEditor) -> Result<(), String> {
        if self.running() {
            return Err("Stop reading before saving server changes".into());
        }
        let (mut server, credentials) = editor.build()?;
        let mut proposed = self.workspace.clone();
        if editor.is_new {
            proposed.servers.push(server.clone());
        } else {
            let old = proposed
                .server_mut(server.id)
                .ok_or("Server is unavailable")?;
            // The parent window can register or star topics while this draft is open.
            server.topics.clone_from(&old.topics);
            if !editor.test_topic.trim().is_empty() {
                server.register_topic(editor.test_topic.trim())?;
            }
            *old = server.clone();
        }
        proposed.selected_server = server.id;
        let initial_tab = if editor.is_new {
            server.topics.first().map(|topic| Tab {
                server: server.id,
                topic: topic.name.clone(),
            })
        } else {
            None
        };
        if let Some(tab) = &initial_tab {
            proposed.open_tab(tab.clone(), false)?;
        }
        proposed.validate()?;
        // Save the sanitized model before committing the draft into the live workspace.
        if self.persistence_error.is_some() {
            return Err("Repair or move the invalid workspace file before saving a server".into());
        }
        self.options.store.save(&proposed)?;
        let previous = self.active().cloned();
        self.workspace = proposed;
        self.credentials.insert(server.id, credentials);
        for (tab, view) in &mut self.cached {
            if tab.server == server.id {
                *view = TopicView::new(&self.workspace.settings);
            }
        }
        if let Some(tab) = initial_tab {
            self.switch_view(previous, &tab);
        } else if self.active().is_some_and(|t| t.server == server.id) {
            self.view = TopicView::new(&self.workspace.settings);
            self.session = None;
        }
        self.notice =
            "Server saved. Credentials stay in this session; no connection was started.".into();
        Ok(())
    }
    pub(super) fn native_dialogs(&mut self, ctx: &egui::Context) {
        if let Some(mut editor) = self.server_editor.take() {
            editor.drain_test();
            let title = if editor.is_new {
                "Add server · Kafka Reader"
            } else {
                "Edit server · Kafka Reader"
            };
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("server-editor"),
                egui::ViewportBuilder::default().with_title(title).with_inner_size([730., 650.]).with_min_inner_size([650., 580.]).with_position([160., 100.]), |ui, class| {
                    self.smoke.native_window_seen |= class == egui::ViewportClass::Immediate;
                    let mut action = if ui.input(|i| i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                    egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                        ui.label(RichText::new(if editor.is_new { "Add a server" } else { "Edit server" }).size(21.).strong());
                        ui.label(RichText::new("Keep connection details together. Read only when you choose a topic.").color(muted(self.dark())));
                        ui.add_space(16.);
                        ui.horizontal(|ui| { for (n,label) in ["Connection", "Authentication", "TLS", "Advanced"].iter().enumerate() { ui.selectable_value(&mut editor.tab,n,*label); } });
                        ui.separator(); ui.add_space(12.);
                        egui::Panel::bottom("server-editor-actions").exact_size(90.).frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, 8))).show(ui, |ui| {
                            ui.add(egui::Label::new(RichText::new(&editor.message).small()).wrap());
                            ui.add_space(8.);
                            ui.horizontal(|ui| {
                                if ui.add_enabled(!editor.test.as_ref().is_some_and(Session::running), egui::Button::new("Test exact topic")).clicked() { editor.test(); }
                                if editor.test.as_ref().is_some_and(Session::running) { ui.spinner(); }
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.add(egui::Button::new(RichText::new("Save server").strong()).fill(selection(self.dark()))).clicked() { action = Action::Save; }
                                    if ui.button("Cancel").clicked() { action = Action::Cancel; }
                                });
                            });
                        });
                        egui::ScrollArea::vertical().id_salt("server-editor-content").show(ui, |ui| {
                            match editor.tab {
                                0 => {
                                    field(ui, "Server name", &mut editor.name, "Development", false);
                                    field(ui, "Bootstrap servers", &mut editor.brokers, "broker-1:9092,broker-2:9092", false);
                                    ui.label("Security protocol");
                                    egui::ComboBox::from_id_salt("protocol").selected_text(&editor.protocol).width(260.).show_ui(ui, |ui| {
                                        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] { ui.selectable_value(&mut editor.protocol, protocol.into(), protocol); }
                                    }); ui.add_space(14.);
                                    field(ui, "Exact topic to test / register", &mut editor.test_topic, "orders.events", false);
                                    ui.label(RichText::new("Testing queries this exact topic asynchronously. Saving a server does not connect or read messages.").small().color(muted(self.dark())));
                                    ui.add_space(12.);
                                    ui.label(RichText::new("Add more topic names from the server tree after saving.").small().color(muted(self.dark())));
                                }
                                1 => {
                                    ui.label(RichText::new("Session credentials").strong());
                                    ui.label(RichText::new("Usernames, passwords and tokens stay in memory and are not saved with your workspace.").small().color(muted(self.dark())));
                                    ui.add_space(15.);
                                    ui.label("SASL mechanism");
                                    egui::ComboBox::from_id_salt("sasl-mechanism").selected_text(&editor.mechanism).width(260.).show_ui(ui, |ui| {
                                        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] { ui.selectable_value(&mut editor.mechanism, mechanism.into(), mechanism); }
                                    }); ui.add_space(13.);
                                    field(ui, "Username", &mut editor.username, "Kafka principal", false);
                                    field(ui, "Password", &mut editor.password, "Session only", true);
                                    if !editor.protocol.starts_with("SASL") { ui.label(RichText::new("Choose SASL_SSL or SASL_PLAINTEXT on Connection to use these credentials.").color(accent(self.dark()))); }
                                    ui.add_space(10.); ui.small("The native client uses sasl.username / sasl.password. Java JAAS is not used.");
                                }
                                2 => {
                                    ui.label(RichText::new("TLS certificates").strong()); ui.small("Certificate and hostname verification are enabled by default."); ui.add_space(12.);
                                    field(ui, "CA certificate · PEM path", &mut editor.ca, "C:/certs/ca.pem", false);
                                    field(ui, "Client certificate · optional PEM path", &mut editor.certificate, "C:/certs/client.pem", false);
                                    field(ui, "Client private key · optional PEM path", &mut editor.key, "C:/certs/client-key.pem", false);
                                    field(ui, "Private-key password · session only", &mut editor.key_password, "Only for an encrypted private key", true);
                                    ui.small("Paths can be saved. Inline private keys and key passwords are omitted from workspace/profile exports.");
                                }
                                _ => {
                                    ui.label("Additional librdkafka properties");
                                    ui.add(egui::TextEdit::multiline(&mut editor.advanced).code_editor().desired_width(f32::INFINITY).desired_rows(7).hint_text("isolation.level=read_committed"));
                                    ui.small("Literal UTF-8 key=value, one property per line. Read-only safety overrides remain enforced.");
                                    ui.add_space(14.); ui.separator(); ui.add_space(8.);
                                    ui.label(RichText::new("Import / export connection profile").strong());
                                    ui.add(egui::TextEdit::singleline(&mut editor.file_path).desired_width(f32::INFINITY).hint_text("server.properties"));
                                    ui.horizontal(|ui| { if ui.button("Import into draft").clicked() { editor.import(); } if ui.button("Export safe profile").clicked() { editor.export(); } });
                                    ui.small("Imports change this draft only. Export omits session credentials. Existing export files are not overwritten.");
                                }
                            }
                        });
                    });
                    self.capture_dialog_if_needed(ui.ctx(), "server");
                    action
                });
            match action {
                Action::Save => match self.save_server(&editor) {
                    Ok(()) => {}
                    Err(e) => {
                        editor.message = e;
                        self.server_editor = Some(editor);
                    }
                },
                Action::Cancel => {}
                Action::None => self.server_editor = Some(editor),
            }
        }
        if let Some(mut editor) = self.settings_editor.take() {
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("app-settings"),
                egui::ViewportBuilder::default().with_title("App settings · Kafka Reader").with_inner_size([670., 700.]).with_min_inner_size([610., 640.]).with_position([190., 90.]), |ui,class| {
                    self.smoke.native_window_seen |= class == egui::ViewportClass::Immediate;
                    let mut action = if ui.input(|i|i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                    egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                        ui.label(RichText::new("App settings").size(21.).strong());
                        ui.label(RichText::new("Appearance, reading defaults and local memory limits.").color(muted(self.dark()))); ui.add_space(16.);
                        egui::Panel::bottom("settings-actions").exact_size(68.).frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, 8))).show(ui, |ui| {
                            ui.label(RichText::new(&editor.error).small());
                            ui.horizontal(|ui| {
                                if self.running() { ui.small("Stop reading before applying changes."); }
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.add_enabled(!self.running(), egui::Button::new(RichText::new("Apply settings").strong()).fill(selection(self.dark()))).clicked() { action = Action::Save; }
                                    if ui.button("Cancel").clicked() { action = Action::Cancel; }
                                });
                            });
                        });
                        egui::ScrollArea::vertical().show(ui, |ui| {
                            section(ui, "Appearance", self.dark());
                            egui::Grid::new("settings-appearance").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label("Theme"); ui.horizontal(|ui| { ui.selectable_value(&mut editor.value.dark,true,"Dark"); ui.selectable_value(&mut editor.value.dark,false,"Light"); }); ui.end_row();
                                ui.label("Message rows"); ui.checkbox(&mut editor.value.compact_rows,"Compact"); ui.end_row();
                                ui.label("Font size"); ui.add(egui::DragValue::new(&mut editor.value.font_size).range(11.0..=20.0).speed(0.5).suffix(" pt")); ui.end_row();
                                ui.label("Display timestamps"); ui.horizontal(|ui| { ui.selectable_value(&mut editor.value.utc,true,"UTC"); ui.selectable_value(&mut editor.value.utc,false,"Local time"); }); ui.end_row();
                            });
                            ui.add_space(18.); section(ui, "Reading defaults", self.dark());
                            egui::Grid::new("settings-reading").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label("Maximum records per read"); ui.add(egui::DragValue::new(&mut editor.value.max_records).range(1..=1_000_000).speed(100.)); ui.end_row();
                                ui.label("Active reading time"); ui.add(egui::DragValue::new(&mut editor.value.seconds).range(1..=86_400).suffix(" seconds")); ui.end_row();
                                ui.label("Recent offset window"); ui.add(egui::DragValue::new(&mut editor.value.recent_window).range(1..=1_000_000).speed(10.)); ui.end_row();
                            });
                            ui.add_space(18.); section(ui, "Retained messages · all open tabs", self.dark());
                            egui::Grid::new("settings-retention").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label("Maximum retained records"); ui.add(egui::DragValue::new(&mut editor.value.retained_records).range(100..=50_000).speed(100.)); ui.end_row();
                                ui.label("Maximum raw payload bytes"); ui.add(egui::DragValue::new(&mut editor.value.retained_mib).range(1..=512).suffix(" MiB")); ui.end_row();
                            });
                            ui.add_space(8.); ui.small("Lower limits trim cached data immediately, starting with older inactive tabs. Message bodies are never saved in the workspace.");
                            ui.add_space(10.); ui.small("Profiles, favorites, tabs and these preferences are saved locally. Restoring them never connects to Kafka.");
                        });
                    });
                    self.capture_dialog_if_needed(ui.ctx(), "settings"); action
                });
            match action {
                Action::Save => match self.apply_settings(editor.value.clone(), ctx) {
                    Ok(()) => {}
                    Err(e) => {
                        editor.error = e;
                        self.settings_editor = Some(editor);
                    }
                },
                Action::Cancel => {}
                Action::None => self.settings_editor = Some(editor),
            }
        }
        if let Some(mut editor) = self.topic_editor.take() {
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("add-topic"), egui::ViewportBuilder::default().with_title("Add topic · Kafka Reader").with_inner_size([550.,280.]).with_min_inner_size([480.,260.]).with_position([220.,160.]), |ui,_| {
                let mut action = if ui.input(|i|i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                    ui.heading("Register a topic"); ui.label("Enter the exact name you have access to. No cluster-wide listing is requested."); ui.add_space(12.);
                    field(ui,"Topic name",&mut editor.name,"orders.events",false); ui.label(RichText::new(&editor.error).small());
                    ui.horizontal(|ui| { if ui.button("Add and open").clicked(){action=Action::Save;} if ui.button("Cancel").clicked(){action=Action::Cancel;} });
                }); action
            });
            if action == Action::Save {
                let result = (|| {
                    if self.running() {
                        return Err("Stop reading before adding a topic".into());
                    }
                    self.workspace
                        .server_mut(editor.server)
                        .ok_or("Server is unavailable")?
                        .register_topic(editor.name.trim())?;
                    self.activate(Tab {
                        server: editor.server,
                        topic: editor.name.trim().into(),
                    })
                })();
                if let Err(e) = result {
                    editor.error = e;
                    self.topic_editor = Some(editor);
                }
            } else if action == Action::None {
                self.topic_editor = Some(editor);
            }
        }
        if let Some(mut editor) = self.export_editor.take() {
            let action = ctx.show_viewport_immediate(
                egui::ViewportId::from_hash_of("export-records"),
                egui::ViewportBuilder::default()
                    .with_title("Export records · Kafka Reader")
                    .with_inner_size([560., 260.])
                    .with_min_inner_size([500., 240.])
                    .with_position([210., 160.]),
                |ui, _| {
                    let mut action = if ui.input(|i| i.viewport().close_requested()) {
                        Action::Cancel
                    } else {
                        Action::None
                    };
                    egui::CentralPanel::default()
                        .frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22))
                        .show(ui, |ui| {
                            ui.heading("Export visible messages");
                            ui.label("JSONL preserves exact bytes, nulls and ordered headers.");
                            ui.add_space(12.);
                            field(
                                ui,
                                "New output filename",
                                &mut editor.path,
                                "records.jsonl",
                                false,
                            );
                            ui.horizontal(|ui| {
                                if ui.button("Export JSONL").clicked() {
                                    action = Action::Save;
                                }
                                if ui.button("Cancel").clicked() {
                                    action = Action::Cancel;
                                }
                            });
                        });
                    action
                },
            );
            if action == Action::Save {
                self.export_to(editor);
            } else if action == Action::None {
                self.export_editor = Some(editor);
            }
        }
    }
}
fn field(ui: &mut egui::Ui, label: &str, value: &mut String, hint: &str, password: bool) {
    ui.label(label);
    ui.add(
        egui::TextEdit::singleline(value)
            .hint_text(hint)
            .password(password)
            .desired_width(f32::INFINITY),
    );
    ui.add_space(11.);
}
fn section(ui: &mut egui::Ui, title: &str, dark: bool) {
    ui.label(RichText::new(title).strong().color(accent(dark)));
    ui.add_space(8.);
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_app() -> ReaderApp {
        let cc = eframe::CreationContext::_new_kittest(egui::Context::default());
        ReaderApp::new(
            &cc,
            Options {
                light: false,
                compact: false,
                screenshot: None,
                screenshot_dir: None,
                smoke: false,
                reopen: None,
                close_after: None,
                store: Store::for_gui(None, true).unwrap(),
            },
        )
    }

    #[test]
    fn ninth_tab_failure_leaves_the_new_server_draft_uncommitted() {
        let mut app = test_app();
        let mut server = Server {
            id: 1,
            name: "Existing".into(),
            offline: false,
            properties: Properties::from([("bootstrap.servers".into(), "localhost:9092".into())]),
            topics: vec![],
        };
        for i in 0..MAX_TABS {
            server.register_topic(&format!("events-{i}")).unwrap();
        }
        app.workspace.servers.push(server);
        for i in 0..MAX_TABS {
            app.activate(Tab {
                server: 1,
                topic: format!("events-{i}"),
            })
            .unwrap();
        }
        let before = fs::read(app.options.store.path()).unwrap();
        app.open_server(None);
        let mut editor = app.server_editor.take().unwrap();
        editor.name = "New".into();
        editor.brokers = "localhost:9092".into();
        editor.test_topic = "new.events".into();
        assert!(
            app.save_server(&editor)
                .unwrap_err()
                .contains("Eight topic tabs")
        );
        assert_eq!(app.workspace.servers.len(), 1);
        assert_eq!(app.workspace.tabs.len(), MAX_TABS);
        assert!(app.credentials.is_empty());
        assert_eq!(fs::read(app.options.store.path()).unwrap(), before);
    }

    #[test]
    fn export_keeps_the_rows_visible_when_its_dialog_opened() {
        let mut app = test_app();
        app.view.buffer.add(Arc::new(demo_record(0, 7, "first", 0)));
        app.open_export("first");
        let mut editor = app.export_editor.take().unwrap();
        assert_eq!(editor.path, "first-records.jsonl");
        let path = app.options.store.directory.join("result.jsonl");
        editor.path = path.to_str().unwrap().into();
        app.view = TopicView::new(&app.workspace.settings);
        app.view
            .buffer
            .add(Arc::new(demo_record(0, 9, "second", 0)));
        app.export_to(editor);
        assert!(matches!(
            app.jobs_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Job::Exported(_)
        ));
        let bytes = fs::read(&path).unwrap();
        let row: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(row["topic"], "first");
        assert_eq!(row["offset"], 7);
        assert!(export(&path, &[]).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn profile_export_refuses_to_overwrite_an_existing_file() {
        let mut app = test_app();
        app.open_server(None);
        let mut editor = app.server_editor.take().unwrap();
        editor.name = "Profile".into();
        editor.brokers = "localhost:9092".into();
        let path = app.options.store.directory.join("export.properties");
        editor.file_path = path.to_str().unwrap().into();
        fs::write(&path, "original contents").unwrap();
        editor.export();
        assert!(editor.message.starts_with("Export failed"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "original contents");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn appearance_changes_preserve_read_controls_and_new_defaults_reach_untouched_tabs() {
        let mut app = test_app();
        app.view.max_records = 1;
        app.view.seconds = 6;
        app.view.start_value = "3".into();
        let idle = Tab {
            server: 1,
            topic: "idle".into(),
        };
        app.cached
            .insert(idle.clone(), TopicView::new(&app.workspace.settings));
        let mut settings = app.workspace.settings.clone();
        settings.dark = !settings.dark;
        let ctx = egui::Context::default();
        app.apply_settings(settings.clone(), &ctx).unwrap();
        assert_eq!(
            (
                app.view.max_records,
                app.view.seconds,
                app.view.start_value.as_str()
            ),
            (1, 6, "3")
        );
        settings.max_records = 500;
        settings.seconds = 10;
        settings.recent_window = 8;
        app.apply_settings(settings, &ctx).unwrap();
        let idle_view = &app.cached[&idle];
        assert_eq!(
            (
                idle_view.max_records,
                idle_view.seconds,
                idle_view.start_value.as_str()
            ),
            (500, 10, "8")
        );
        assert_eq!(
            (
                app.view.max_records,
                app.view.seconds,
                app.view.start_value.as_str()
            ),
            (1, 6, "3")
        );
    }
    #[test]
    fn typed_auth_tls_and_advanced_produce_native_settings() {
        let server = Server {
            id: 2,
            name: "Development".into(),
            offline: false,
            properties: Properties::new(),
            topics: vec![],
        };
        let mut editor = ServerEditor::new(server, Properties::new(), true, "events".into());
        editor.brokers = "localhost:9093".into();
        editor.protocol = "SASL_SSL".into();
        editor.mechanism = "SCRAM-SHA-512".into();
        editor.username = "reader".into();
        editor.password = "SENTINEL_PASSWORD".into();
        editor.ca = "ca.pem".into();
        editor.key = "private.pem".into();
        editor.key_password = "SENTINEL_KEY_PASSWORD".into();
        editor.advanced="isolation.level=read_committed\nsasl.password=overridden\nssl.key.pem=INLINE_PRIVATE_SENTINEL".into();
        let (server, secrets) = editor.build().unwrap();
        assert_eq!(server.properties["security.protocol"], "SASL_SSL");
        assert_eq!(server.properties["sasl.mechanism"], "SCRAM-SHA-512");
        assert_eq!(server.properties["ssl.ca.location"], "ca.pem");
        assert_eq!(server.properties["ssl.key.location"], "private.pem");
        assert_eq!(secrets["sasl.password"], "SENTINEL_PASSWORD");
        assert_eq!(secrets["ssl.key.password"], "SENTINEL_KEY_PASSWORD");
        assert_eq!(secrets["ssl.key.pem"], "INLINE_PRIVATE_SENTINEL");
        assert!(!server.properties.contains_key("sasl.username"));
        assert!(!serde_json::to_string(&server).unwrap().contains("SENTINEL"));
    }
    #[test]
    fn imported_profile_remains_a_draft_until_save() {
        let original = Server {
            id: 2,
            name: "Original".into(),
            offline: false,
            properties: Properties::new(),
            topics: vec![],
        };
        let mut editor = ServerEditor::new(original.clone(), Properties::new(), false, "".into());
        editor.name = "Unsaved change".into();
        editor.brokers = "example:9092".into();
        editor.password = "SECRET".into();
        let (changed, _) = editor.build().unwrap();
        assert_ne!(changed.name, original.name);
        assert_eq!(original.name, "Original");
        assert!(original.properties.is_empty());
    }
}
