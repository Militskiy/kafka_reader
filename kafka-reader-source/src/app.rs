mod dialogs;
mod presentation;
mod smoke;

use crossbeam_channel::{Receiver, Sender, unbounded};
use dialogs::{ExportEditor, ServerEditor, SettingsEditor, TopicEditor};
use eframe::egui::{self, Color32, RichText};
use kafka_reader_rust::{
    model::*,
    reader::{Event, Session},
    workspace::*,
};
use presentation::{MessageOrder, PayloadFormat, SortColumn, format_payload, record_time};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

pub struct Options {
    pub light: bool,
    pub compact: bool,
    pub screenshot: Option<PathBuf>,
    pub screenshot_dir: Option<PathBuf>,
    pub smoke: bool,
    pub reopen: Option<PathBuf>,
    pub close_after: Option<u64>,
    pub store: Store,
}

enum Job {
    Formatted(Tab, u64, String),
    Exported(String),
}

struct TopicView {
    buffer: Buffer,
    metadata: Vec<Partition>,
    selected: Option<Arc<Record>>,
    formatted: String,
    generation: u64,
    inspector_tab: usize,
    value_format: PayloadFormat,
    key_format: PayloadFormat,
    subview: usize,
    mode: Mode,
    start_value: String,
    partition_text: String,
    follow: bool,
    max_records: usize,
    seconds: u64,
    filter: String,
    key_filter: String,
    display_partition: Option<i32>,
    order: MessageOrder,
    fetched: usize,
    skipped: usize,
    state: String,
    detail: String,
}
impl TopicView {
    fn new(settings: &Settings) -> Self {
        let mut buffer = Buffer::default();
        buffer.set_limits(settings.retained_records, settings.retained_bytes());
        Self {
            buffer,
            metadata: vec![],
            selected: None,
            formatted: String::new(),
            generation: 0,
            inspector_tab: 0,
            value_format: PayloadFormat::Auto,
            key_format: PayloadFormat::Auto,
            subview: 0,
            mode: Mode::Recent,
            start_value: settings.recent_window.to_string(),
            partition_text: String::new(),
            follow: false,
            max_records: settings.max_records,
            seconds: settings.seconds,
            filter: String::new(),
            key_filter: String::new(),
            display_partition: None,
            order: MessageOrder::default(),
            fetched: 0,
            skipped: 0,
            state: "READY".into(),
            detail: "Choose Read to start this topic.".into(),
        }
    }
    fn matches(&self, record: &Record) -> bool {
        if self
            .display_partition
            .is_some_and(|p| record.partition != p)
        {
            return false;
        }
        if !self.key_filter.is_empty()
            && !preview(record.key.as_deref(), 256)
                .to_lowercase()
                .contains(&self.key_filter.to_lowercase())
        {
            return false;
        }
        self.filter.is_empty()
            || format!(
                "{} {} {} {} {}",
                record.topic,
                record.partition,
                record.offset,
                preview(record.key.as_deref(), 180),
                record.preview()
            )
            .to_lowercase()
            .contains(&self.filter.to_lowercase())
    }
    fn visible(&self) -> Vec<Arc<Record>> {
        let mut records: Vec<_> = self
            .buffer
            .records
            .iter()
            .filter(|r| self.matches(r))
            .cloned()
            .collect();
        records.sort_by(|a, b| self.order.compare(a, b));
        records
    }
}

pub struct ReaderApp {
    workspace: Workspace,
    persistence_error: Option<String>,
    credentials: BTreeMap<u64, Properties>,
    view: TopicView,
    cached: BTreeMap<Tab, TopicView>,
    cache_order: VecDeque<Tab>,
    session: Option<Session>,
    server_editor: Option<ServerEditor>,
    settings_editor: Option<SettingsEditor>,
    topic_editor: Option<TopicEditor>,
    export_editor: Option<ExportEditor>,
    topic_search: String,
    notice: String,
    focus_filter: bool,
    jobs_tx: Sender<Job>,
    jobs_rx: Receiver<Job>,
    exporting: bool,
    options: Options,
    created: Instant,
    smoke: smoke::Smoke,
    screenshot_requested: bool,
    next_preview_generation: u64,
}

impl ReaderApp {
    pub fn new(cc: &eframe::CreationContext<'_>, options: Options) -> Self {
        cc.egui_ctx.set_embed_viewports(false);
        let (mut workspace, persistence_error) = if options.smoke {
            // Smoke always starts fresh. Its first save also checks that the destination
            // was not populated between store preparation and GUI startup.
            (Workspace::qa_fixture(), None)
        } else {
            match options.store.load() {
                Ok(w) => (w, None),
                Err(e) => (Workspace::default(), Some(e)),
            }
        };
        if options.light {
            workspace.settings.dark = false;
        }
        if options.compact {
            workspace.settings.compact_rows = true;
        }
        theme(&cc.egui_ctx, &workspace.settings);
        let view = TopicView::new(&workspace.settings);
        let (jobs_tx, jobs_rx) = unbounded();
        let notice = persistence_error
            .as_ref()
            .map(|e| format!("Workspace was not loaded: {e}. The original file is unchanged."))
            .unwrap_or_default();
        let mut app = Self {
            workspace,
            persistence_error,
            credentials: BTreeMap::new(),
            view,
            cached: BTreeMap::new(),
            cache_order: VecDeque::new(),
            session: None,
            server_editor: None,
            settings_editor: None,
            topic_editor: None,
            export_editor: None,
            topic_search: String::new(),
            notice,
            focus_filter: false,
            jobs_tx,
            jobs_rx,
            exporting: false,
            options,
            created: Instant::now(),
            smoke: smoke::Smoke::default(),
            screenshot_requested: false,
            next_preview_generation: 0,
        };
        // Restoring a workspace or taking a screenshot never starts a reader.
        if app.options.smoke {
            let tab = Tab {
                server: 1,
                topic: "agreement.events".into(),
            };
            if app.workspace.server(1).is_some_and(|s| s.offline) {
                let _ = app.activate(tab);
                app.start(false);
            }
        }
        app
    }
    fn dark(&self) -> bool {
        self.workspace.settings.dark
    }
    fn active(&self) -> Option<&Tab> {
        self.workspace.active_tab.as_ref()
    }
    fn server(&self) -> Option<&Server> {
        self.active().and_then(|t| self.workspace.server(t.server))
    }
    fn offline(&self) -> bool {
        self.server().is_some_and(|s| s.offline)
    }
    fn running(&self) -> bool {
        self.session.as_ref().is_some_and(Session::active)
    }
    fn modal_open(&self) -> bool {
        self.server_editor.is_some()
            || self.settings_editor.is_some()
            || self.topic_editor.is_some()
            || self.export_editor.is_some()
    }
    fn persist(&mut self) {
        if self.persistence_error.is_some() {
            self.notice = "Workspace storage is unavailable. Session changes are kept in memory; repair or move workspace.json to restore saving.".into();
        } else if let Err(e) = self.options.store.save(&self.workspace) {
            self.notice = format!("Could not save workspace: {e}");
        }
    }
    fn activate(&mut self, tab: Tab) -> Result<(), String> {
        let previous = self.active().cloned();
        if self.running() && previous.as_ref() != Some(&tab) {
            return Err("Stop the active reader before switching topics or servers".into());
        }
        if previous.as_ref() == Some(&tab) {
            self.workspace.open_tab(tab, self.running())?;
            return Ok(());
        }
        self.drain_reader(usize::MAX);
        self.workspace.open_tab(tab.clone(), false)?;
        self.switch_view(previous, &tab);
        self.persist();
        Ok(())
    }
    fn switch_view(&mut self, previous: Option<Tab>, tab: &Tab) {
        let replacement = self
            .cached
            .remove(tab)
            .unwrap_or_else(|| TopicView::new(&self.workspace.settings));
        let prior = std::mem::replace(&mut self.view, replacement);
        if let Some(old) = previous {
            self.cached.insert(old.clone(), prior);
            self.cache_order.retain(|k| k != &old);
            self.cache_order.push_back(old);
        }
        self.cache_order.retain(|k| k != tab);
        self.session = None;
        self.notice.clear();
        self.enforce_budget();
    }
    fn close_tab(&mut self, tab: &Tab) -> Result<(), String> {
        let was_active = self.active() == Some(tab);
        self.workspace.close_tab(tab, self.running())?;
        self.cached.remove(tab);
        self.cache_order.retain(|t| t != tab);
        if was_active {
            self.session = None;
            self.view = self
                .active()
                .cloned()
                .and_then(|t| self.cached.remove(&t))
                .unwrap_or_else(|| TopicView::new(&self.workspace.settings));
        }
        self.persist();
        Ok(())
    }
    fn toggle_favorite(&mut self, server: u64, topic: &str) {
        if let Some(t) = self
            .workspace
            .server_mut(server)
            .and_then(|s| s.topics.iter_mut().find(|t| t.name == topic))
        {
            t.favorite = !t.favorite;
            self.persist();
        }
    }
    fn enforce_budget(&mut self) {
        let settings = &self.workspace.settings;
        self.view
            .buffer
            .set_limits(settings.retained_records, settings.retained_bytes());
        for v in self.cached.values_mut() {
            v.buffer
                .set_limits(settings.retained_records, settings.retained_bytes());
        }
        loop {
            let count = self.view.buffer.records.len()
                + self
                    .cached
                    .values()
                    .map(|v| v.buffer.records.len())
                    .sum::<usize>();
            let bytes = self.view.buffer.bytes
                + self.cached.values().map(|v| v.buffer.bytes).sum::<usize>();
            if count <= settings.retained_records && bytes <= settings.retained_bytes() {
                break;
            }
            let mut evicted = false;
            for tab in &self.cache_order {
                if let Some(v) = self.cached.get_mut(tab) {
                    if v.buffer.evict_oldest() {
                        evicted = true;
                        break;
                    }
                }
            }
            if !evicted {
                self.view.buffer.evict_oldest();
            }
        }
        for v in std::iter::once(&mut self.view).chain(self.cached.values_mut()) {
            if v.selected
                .as_ref()
                .is_some_and(|r| !v.buffer.records.iter().any(|v| Arc::ptr_eq(v, r)))
            {
                v.selected = None;
                v.formatted.clear();
                // Zero is never issued to a preview job; invalidate an evicted selection.
                v.generation = 0;
            }
        }
    }
    fn properties(&self) -> Result<Properties, String> {
        let server = self.server().ok_or("Select a topic first")?;
        Ok(session_properties(server, &self.credentials))
    }
    fn request(&self, inspect: bool) -> Result<Request, String> {
        let active = self.active().ok_or("Open a topic tab first")?;
        let partitions = if self.view.partition_text.trim().is_empty() {
            vec![]
        } else {
            self.view
                .partition_text
                .split(',')
                .map(|p| {
                    p.trim()
                        .parse::<i32>()
                        .map_err(|_| "Use partition numbers separated by commas".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if partitions.iter().any(|p| *p < 0) {
            return Err("Partition IDs cannot be negative".into());
        }
        let value = if matches!(self.view.mode, Mode::Earliest | Mode::Latest) {
            0
        } else if self.view.mode == Mode::Timestamp {
            self.view
                .start_value
                .trim()
                .parse::<i64>()
                .or_else(|_| {
                    chrono::DateTime::parse_from_rfc3339(self.view.start_value.trim())
                        .map(|t| t.timestamp_millis())
                })
                .map_err(|_| "Use epoch milliseconds or an ISO-8601 timestamp".to_string())?
        } else {
            self.view
                .start_value
                .trim()
                .parse()
                .map_err(|_| "Use a whole offset or recent window".to_string())?
        };
        if value < 0 || (self.view.mode == Mode::Recent && value == 0) {
            return Err("Use a nonnegative start or a positive recent window".into());
        }
        Ok(Request {
            props: self.properties()?,
            topic: active.topic.clone(),
            partitions,
            mode: self.view.mode,
            value,
            follow: self.view.follow,
            max_records: self.view.max_records,
            seconds: self.view.seconds,
            demo: self.offline(),
            inspect,
        })
    }
    fn start(&mut self, inspect: bool) {
        if self.running() {
            return;
        }
        match self.request(inspect) {
            Ok(request) => {
                if !inspect {
                    self.view.buffer.clear();
                    self.view.selected = None;
                    self.view.formatted.clear();
                    self.next_preview_generation += 1;
                    self.view.generation = self.next_preview_generation;
                    self.view.fetched = 0;
                    self.view.skipped = 0;
                }
                self.view.metadata.clear();
                self.view.state = "CONNECTING".into();
                self.view.detail = if request.demo {
                    "Opening the offline topic".into()
                } else {
                    "Connecting to the selected topic".into()
                };
                self.notice.clear();
                self.session = Some(Session::start(request));
            }
            Err(e) => {
                self.view.state = "CHECK SETTINGS".into();
                self.notice = e;
            }
        }
    }
    fn stop(&mut self) {
        if let Some(s) = &self.session {
            s.stop();
            self.view.state = "STOPPING".into();
        }
    }
    fn drain_reader(&mut self, limit: usize) {
        let mut events = Vec::new();
        if let Some(session) = &self.session {
            for _ in 0..limit {
                if let Some(event) = session.try_next() {
                    events.push(event);
                } else {
                    break;
                }
            }
        }
        for event in events {
            match event {
                Event::Record(r) => {
                    self.view.buffer.add(r.clone());
                    if self.view.selected.is_none() {
                        self.select(r);
                    }
                }
                Event::Metadata(p) => self.view.metadata = p,
                Event::State(state, detail) => {
                    self.view.state = state;
                    self.view.detail = detail;
                }
                Event::Progress(n) => self.view.fetched = n,
                Event::Skipped(count) => self.view.skipped += count,
            }
        }
        self.enforce_budget();
    }
    fn drain(&mut self) {
        self.drain_reader(512);
        while let Ok(job) = self.jobs_rx.try_recv() {
            match job {
                Job::Formatted(tab, generation, text) => {
                    let active = self.active().cloned();
                    deliver_preview(
                        active.as_ref(),
                        &mut self.view,
                        &mut self.cached,
                        &tab,
                        generation,
                        text,
                    );
                }
                Job::Exported(message) => {
                    self.exporting = false;
                    self.notice = message;
                }
            }
        }
    }
    fn select(&mut self, record: Arc<Record>) {
        let Some(key) = self.active().cloned() else {
            return;
        };
        self.view.selected = Some(record.clone());
        self.next_preview_generation += 1;
        self.view.generation = self.next_preview_generation;
        let generation = self.view.generation;
        let tab = self.view.inspector_tab;
        let format = if tab == 1 {
            self.view.key_format
        } else {
            self.view.value_format
        };
        self.view.formatted = "Preparing preview…".into();
        let tx = self.jobs_tx.clone();
        thread::spawn(move || {
            let output = match tab {
                0 => format_payload(record.value.as_deref(), format, false),
                1 => format_payload(record.key.as_deref(), format, true),
                2 => {
                    let mut result = String::new();
                    for (i, h) in record.headers.iter().take(500).enumerate() {
                        result.push_str(&format!(
                            "{:03}  {}\n     {}\n\n",
                            i + 1,
                            h.name,
                            preview(h.value.as_deref(), 400)
                        ));
                    }
                    if record.headers.len() > 500 {
                        result.push_str("[First 500 headers shown; export preserves all.]\n");
                    }
                    result
                }
                _ => format!(
                    "Topic           {}\nPartition       {}\nOffset          {}\nTimestamp       {}\nTimestamp type  {}\nKey             {}\nValue           {}\nHeaders         {}\nRaw bytes       {}\n",
                    record.topic,
                    record.partition,
                    record.offset,
                    record.timestamp,
                    record.timestamp_type,
                    size_desc(&record.key),
                    size_desc(&record.value),
                    record.headers.len(),
                    record.bytes()
                ),
            };
            let _ = tx.send(Job::Formatted(key, generation, output));
        });
    }
    fn open_export(&mut self, topic: &str) {
        let records = self.view.visible();
        if records.is_empty() {
            self.notice = "There are no visible retained records to export".into();
            return;
        }
        self.export_editor = Some(ExportEditor {
            path: format!("{topic}-records.jsonl"),
            records,
        });
    }
    fn export_to(&mut self, editor: ExportEditor) {
        if self.exporting {
            return;
        }
        let path = PathBuf::from(editor.path);
        let records = editor.records;
        self.exporting = true;
        let tx = self.jobs_tx.clone();
        thread::spawn(move || {
            let message = match export(&path, &records) {
                Ok(()) => format!("Exported {} records to {}", records.len(), path.display()),
                Err(e) => format!("Export failed: {e}"),
            };
            let _ = tx.send(Job::Exported(message));
        });
    }
    fn apply_settings(&mut self, settings: Settings, ctx: &egui::Context) -> Result<(), String> {
        if self.running() {
            return Err("Stop the active reader before applying app settings".into());
        }
        settings.validate()?;
        if self.persistence_error.is_some() {
            return Err("Repair or move the invalid workspace file before saving settings".into());
        }
        let mut proposed = self.workspace.clone();
        proposed.settings = settings.clone();
        self.options.store.save(&proposed)?;
        let previous = &self.workspace.settings;
        for view in std::iter::once(&mut self.view).chain(self.cached.values_mut()) {
            if view.max_records == previous.max_records {
                view.max_records = settings.max_records;
            }
            if view.seconds == previous.seconds {
                view.seconds = settings.seconds;
            }
            if view.mode == Mode::Recent && view.start_value == previous.recent_window.to_string() {
                view.start_value = settings.recent_window.to_string();
            }
        }
        self.workspace = proposed;
        self.enforce_budget();
        theme(ctx, &settings);
        self.notice = "App settings applied and saved".into();
        Ok(())
    }
    fn status(&self) -> &'static str {
        match self.view.state.as_str() {
            "CONNECTING" => {
                if self.offline() {
                    "Preparing demo"
                } else {
                    "Connecting"
                }
            }
            "READING" => {
                if self.offline() {
                    "Demo running"
                } else if self.view.follow {
                    "Following"
                } else {
                    "Reading snapshot"
                }
            }
            "PAUSED" => "Paused",
            "STOPPING" => "Stopping",
            "STOPPED" => "Stopped",
            "FAILED" => "Read failed",
            "COMPLETED" => {
                if self.offline() {
                    "Demo complete"
                } else {
                    "Completed"
                }
            }
            "CHECK SETTINGS" => "Check settings",
            _ => {
                if self.offline() {
                    "Offline demo"
                } else {
                    "Not connected"
                }
            }
        }
    }
    fn sidebar(&mut self, ui: &mut egui::Ui) {
        let running = self.running();
        ui.horizontal(|ui| {
            ui.label(RichText::new("Servers").size(17.).strong());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(
                        !running,
                        egui::Button::new(RichText::new("+ Add server").color(accent(self.dark())))
                            .frame(false),
                    )
                    .clicked()
                {
                    self.open_server(None);
                }
            });
        });
        ui.add_space(7.);
        ui.add(
            egui::TextEdit::singleline(&mut self.topic_search)
                .hint_text("Find a saved topic…")
                .desired_width(f32::INFINITY),
        );
        ui.add_space(10.);
        egui::Panel::bottom("sidebar-bottom")
            .exact_size(64.)
            .frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, 10)))
            .show(ui, |ui| {
                if ui
                    .add_sized(
                        [ui.available_width(), 28.],
                        egui::Button::new("App settings"),
                    )
                    .clicked()
                {
                    self.settings_editor = Some(SettingsEditor::new(&self.workspace.settings));
                }
                ui.label(
                    RichText::new("Local workspace  ·  Read only")
                        .small()
                        .color(muted(self.dark())),
                );
            });
        let servers = self.workspace.servers.clone();
        let query = self.topic_search.to_lowercase();
        egui::ScrollArea::vertical()
            .id_salt("server-tree")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let favorites: Vec<_> = servers
                    .iter()
                    .flat_map(|s| s.topics.iter().filter(|t| t.favorite).map(move |t| (s, t)))
                    .collect();
                if !favorites.is_empty() {
                    egui::CollapsingHeader::new(format!("Favorites ({})", favorites.len()))
                        .default_open(true)
                        .id_salt("favorites")
                        .show(ui, |ui| {
                            for (server, topic) in &favorites {
                                if query.is_empty()
                                    || topic.name.to_lowercase().contains(&query)
                                    || server.name.to_lowercase().contains(&query)
                                {
                                    self.topic_row(ui, server, topic, true);
                                }
                            }
                        });
                    ui.add_space(10.);
                }
                for server in &servers {
                    let matches: Vec<_> = server
                        .topics
                        .iter()
                        .filter(|t| {
                            query.is_empty()
                                || t.name.to_lowercase().contains(&query)
                                || server.name.to_lowercase().contains(&query)
                        })
                        .collect();
                    if !query.is_empty() && matches.is_empty() {
                        continue;
                    }
                    let title = if server.offline {
                        format!("{}  ·  demo", server.name)
                    } else {
                        server.name.clone()
                    };
                    egui::CollapsingHeader::new(RichText::new(title).strong())
                        .id_salt(("server", server.id))
                        .default_open(server.id == self.workspace.selected_server || server.offline)
                        .open(if query.is_empty() { None } else { Some(true) })
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new(format!("{} saved topics", server.topics.len()))
                                        .small()
                                        .color(muted(self.dark())),
                                );
                                if ui
                                    .add_enabled(
                                        !running && !server.offline,
                                        egui::Button::new("+ Topic").small().frame(false),
                                    )
                                    .clicked()
                                {
                                    self.topic_editor = Some(TopicEditor {
                                        server: server.id,
                                        name: String::new(),
                                        error: String::new(),
                                    });
                                }
                                if ui
                                    .add_enabled(
                                        !running && !server.offline,
                                        egui::Button::new("Edit").small().frame(false),
                                    )
                                    .clicked()
                                {
                                    self.open_server(Some(server.id));
                                }
                            });
                            for topic in matches {
                                self.topic_row(ui, server, topic, false);
                            }
                            if server.topics.is_empty() {
                                ui.small("Add an exact topic name to start.");
                            }
                        });
                    ui.add_space(7.);
                }
            });
    }
    fn topic_row(
        &mut self,
        ui: &mut egui::Ui,
        server: &Server,
        topic: &Topic,
        favorite_section: bool,
    ) {
        let tab = Tab {
            server: server.id,
            topic: topic.name.clone(),
        };
        let selected = self.active() == Some(&tab);
        let enabled = !self.running() || selected;
        let fill = if selected {
            selection(self.dark())
        } else {
            Color32::TRANSPARENT
        };
        egui::Frame::new()
            .fill(fill)
            .corner_radius(4)
            .inner_margin(egui::Margin::symmetric(5, 3))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    let label = if favorite_section && !server.offline {
                        format!("{}  /  {}", server.name, topic.name)
                    } else {
                        topic.name.clone()
                    };
                    let response = ui.add_enabled(
                        enabled,
                        egui::Button::new(
                            RichText::new(label).size(self.workspace.settings.font_size - 0.5),
                        )
                        .frame(false)
                        .truncate(),
                    );
                    if response.clicked() {
                        if let Err(e) = self.activate(tab) {
                            self.notice = e;
                        }
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if star(ui, topic.favorite, self.dark()).clicked() {
                            self.toggle_favorite(server.id, &topic.name);
                        }
                    });
                });
            });
    }
    fn tabs(&mut self, ui: &mut egui::Ui) {
        let tabs = self.workspace.tabs.clone();
        let mut activate = None;
        let mut close = None;
        egui::ScrollArea::horizontal()
            .id_salt("open-topic-tabs")
            .auto_shrink([false, true])
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 1.;
                    for tab in tabs {
                        let selected = self.active() == Some(&tab);
                        let fill = if selected {
                            panel(self.dark())
                        } else {
                            if self.dark() {
                                Color32::from_rgb(28, 28, 31)
                            } else {
                                Color32::from_rgb(229, 231, 235)
                            }
                        };
                        let frame = egui::Frame::new()
                            .fill(fill)
                            .inner_margin(egui::Margin::symmetric(10, 6))
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    let label = ui.add_enabled(
                                        !self.running() || selected,
                                        egui::Button::new(RichText::new(&tab.topic).strong())
                                            .frame(false),
                                    );
                                    if label.clicked() {
                                        activate = Some(tab.clone());
                                    }
                                    label.on_hover_text(
                                        self.workspace
                                            .server(tab.server)
                                            .map(|s| s.name.as_str())
                                            .unwrap_or("Server"),
                                    );
                                    if ui
                                        .add_enabled(
                                            !(selected && self.running()),
                                            egui::Button::new("×").small().frame(false),
                                        )
                                        .on_hover_text("Close topic tab")
                                        .clicked()
                                    {
                                        close = Some(tab.clone());
                                    }
                                });
                            });
                        if selected {
                            ui.painter().line_segment(
                                [
                                    frame.response.rect.left_bottom(),
                                    frame.response.rect.right_bottom(),
                                ],
                                egui::Stroke::new(2., accent(self.dark())),
                            );
                        }
                    }
                    if ui
                        .add_enabled(!self.running(), egui::Button::new("+").frame(false))
                        .on_hover_text(
                            if self
                                .workspace
                                .server(self.workspace.selected_server)
                                .is_some_and(|s| !s.offline)
                            {
                                "Add a topic to the selected server"
                            } else {
                                "Add a server"
                            },
                        )
                        .clicked()
                    {
                        let id = self.workspace.selected_server;
                        if self.workspace.server(id).is_some_and(|s| !s.offline) {
                            self.topic_editor = Some(TopicEditor {
                                server: id,
                                name: String::new(),
                                error: String::new(),
                            });
                        } else {
                            self.open_server(None);
                        }
                    }
                });
            });
        if let Some(tab) = close {
            if let Err(e) = self.close_tab(&tab) {
                self.notice = e;
            }
        }
        if let Some(tab) = activate {
            if let Err(e) = self.activate(tab) {
                self.notice = e;
            }
        }
    }
    fn controls(&mut self, ui: &mut egui::Ui) {
        let Some(tab) = self.active().cloned() else {
            return;
        };
        let server_name = self.server().map(|s| s.name.clone()).unwrap_or_default();
        let running = self.running();
        ui.horizontal(|ui| {
            ui.label(RichText::new(server_name).color(muted(self.dark())));
            ui.label(RichText::new("/").color(muted(self.dark())));
            ui.label(RichText::new(&tab.topic).strong().size(17.));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.add_enabled(running, egui::Button::new("Stop")).clicked() {
                    self.stop();
                }
                if running {
                    let paused = self.session.as_ref().is_some_and(Session::paused);
                    if ui.button(if paused { "Resume" } else { "Pause" }).clicked() {
                        if let Some(s) = &self.session {
                            s.toggle_pause();
                        }
                    }
                } else if ui
                    .add(
                        egui::Button::new(RichText::new("Read").strong().color(if self.dark() {
                            Color32::from_rgb(30, 24, 15)
                        } else {
                            Color32::WHITE
                        }))
                        .fill(accent(self.dark())),
                    )
                    .on_hover_text("Read this topic · Ctrl+Enter")
                    .clicked()
                {
                    self.start(false);
                }
                status_dot(
                    ui,
                    self.status(),
                    if self.running() && !self.offline() {
                        Color32::from_rgb(88, 183, 128)
                    } else {
                        muted(self.dark())
                    },
                );
            });
        });
        ui.add_space(4.);
        ui.horizontal(|ui| {
            for (n, label) in ["Messages", "Topic info", "Partitions"].iter().enumerate() {
                ui.selectable_value(&mut self.view.subview, n, *label);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(!running, egui::Button::new("Inspect topic").small())
                    .clicked()
                {
                    self.start(true);
                }
                let info = if self.view.metadata.is_empty() {
                    "Metadata not inspected".to_string()
                } else {
                    format!("{} inspected partitions", self.view.metadata.len())
                };
                ui.label(RichText::new(info).small().color(muted(self.dark())));
            });
        });
        ui.add_space(5.);
        ui.separator();
        if self.view.subview != 0 {
            return;
        }
        ui.add_space(5.);
        ui.horizontal(|ui| {
            let width = (ui.available_width() - 400.).max(150.);
            let search = ui.add(
                egui::TextEdit::singleline(&mut self.view.filter)
                    .id_salt("loaded-message-search")
                    .hint_text("Search loaded messages…")
                    .desired_width(width),
            );
            if self.focus_filter {
                search.request_focus();
                self.focus_filter = false;
            }
            let current = self
                .view
                .display_partition
                .map(|p| format!("Partition {p}"))
                .unwrap_or_else(|| "All partitions".into());
            egui::ComboBox::from_id_salt("display-partition")
                .selected_text(current)
                .width(120.)
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.view.display_partition, None, "All partitions");
                    for p in &self.view.metadata {
                        ui.selectable_value(
                            &mut self.view.display_partition,
                            Some(p.partition),
                            format!("Partition {}", p.partition),
                        );
                    }
                });
            ui.add(
                egui::TextEdit::singleline(&mut self.view.key_filter)
                    .hint_text("Filter by key…")
                    .desired_width(130.),
            );
            if ui
                .add_enabled(
                    !self.exporting
                        && self.export_editor.is_none()
                        && !self.view.buffer.records.is_empty(),
                    egui::Button::new("Export"),
                )
                .clicked()
            {
                self.open_export(&tab.topic);
            }
        });
        ui.add_space(3.);
        ui.add_enabled_ui(!running, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label(RichText::new("Start").small().color(muted(self.dark())));
                egui::ComboBox::from_id_salt("read-mode")
                    .selected_text(self.view.mode.label())
                    .width(120.)
                    .show_ui(ui, |ui| {
                        for mode in [
                            Mode::Recent,
                            Mode::Earliest,
                            Mode::Offset,
                            Mode::Timestamp,
                            Mode::Latest,
                        ] {
                            if ui
                                .selectable_value(&mut self.view.mode, mode, mode.label())
                                .changed()
                            {
                                self.view.start_value = if mode == Mode::Timestamp {
                                    chrono::Utc::now().to_rfc3339()
                                } else if mode == Mode::Recent {
                                    self.workspace.settings.recent_window.to_string()
                                } else {
                                    "0".into()
                                };
                            }
                        }
                    });
                if !matches!(self.view.mode, Mode::Earliest | Mode::Latest) {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.view.start_value).desired_width(
                            if self.view.mode == Mode::Timestamp {
                                185.
                            } else {
                                65.
                            },
                        ),
                    );
                }
                ui.label(
                    RichText::new("Read partitions")
                        .small()
                        .color(muted(self.dark())),
                );
                ui.add(
                    egui::TextEdit::singleline(&mut self.view.partition_text)
                        .hint_text("All / 0,1")
                        .desired_width(65.),
                );
                ui.label(RichText::new("Max").small().color(muted(self.dark())));
                ui.add(
                    egui::DragValue::new(&mut self.view.max_records)
                        .range(1..=1_000_000)
                        .speed(10.),
                );
                ui.add(
                    egui::DragValue::new(&mut self.view.seconds)
                        .range(1..=86_400)
                        .suffix(" s"),
                );
                ui.checkbox(&mut self.view.follow, "Follow");
            });
        });
        ui.add_space(5.);
    }
    fn messages(&mut self, ui: &mut egui::Ui) {
        if self.view.buffer.records.is_empty() {
            ui.vertical_centered(|ui| {
                ui.add_space((ui.available_height() * 0.22).max(15.));
                ui.label(RichText::new(if self.running() { "Waiting for messages" } else { "This topic is ready to explore" }).size(20.).strong());
                ui.add_space(6.);
                ui.label(if self.offline() { "Read a synthetic stream. No Kafka connection is made." } else { "Choose a start position, then Read. Your saved profile is not connected automatically." });
                ui.add_space(12.);
                if ui.add_enabled(!self.running(), egui::Button::new(if self.offline() { "Read offline demo" } else { "Read topic" })).clicked() { self.start(false); }
            });
            return;
        }
        let font = self.workspace.settings.font_size;
        let row_height = if self.workspace.settings.compact_rows {
            font + 12.
        } else {
            font + 24.
        };
        let partition_width = (font * 5.5 + 18.).max(90.);
        let offset_width = (font * 7. + 18.).max(110.);
        let time_width = (font * 9. + 18.).max(142.);
        let first_width = (ui.available_width()
            - partition_width
            - offset_width
            - time_width
            - ui.spacing().item_spacing.x * 3.
            - 26.)
            .max(140.);
        let dark = self.dark();
        egui::Frame::new()
            .inner_margin(egui::Margin::symmetric(5, 0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.allocate_ui_with_layout(
                        egui::vec2(first_width, 26.),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            ui.set_width(first_width);
                            ui.label(RichText::new("Event / key").small().strong());
                        },
                    );
                    for (column, label, width, hint) in [
                        (SortColumn::Partition, "Partition", partition_width,
                            "Sort by partition number. Offsets stay ascending within each partition."),
                        (SortColumn::Offset, "Offset", offset_width,
                            "Sort by numeric offset. Offsets belong to individual partitions; they are not a topic-wide sequence."),
                        (SortColumn::Time, if self.workspace.settings.utc { "Time · UTC" } else { "Time · local" }, time_width,
                            "Sort by the complete message timestamp. Messages without timestamps stay at the end."),
                    ] {
                        let active = self.view.order.column == column;
                        let caption = RichText::new(label)
                            .small()
                            .strong()
                            .color(if active { accent(dark) } else { muted(dark) });
                        let direction = if self.view.order.descending { "descending" } else { "ascending" };
                        let response = ui.add_sized([width, 26.], egui::Button::new(caption).frame(false))
                            .on_hover_text(if active {
                                format!("{hint}\nCurrent order: {direction}. Click to reverse.")
                            } else {
                                format!("{hint}\nClick to sort this column.")
                            });
                        if active {
                            let center = egui::pos2(response.rect.right() - 7., response.rect.center().y);
                            let sign = if self.view.order.descending { 1. } else { -1. };
                            ui.painter().add(egui::Shape::convex_polygon(
                                vec![
                                    egui::pos2(center.x - 3.5, center.y - 2.5 * sign),
                                    egui::pos2(center.x + 3.5, center.y - 2.5 * sign),
                                    egui::pos2(center.x, center.y + 2.5 * sign),
                                ],
                                accent(dark),
                                egui::Stroke::NONE,
                            ));
                        }
                        if response.clicked() {
                            self.view.order.select(column);
                        }
                    }
                });
            });
        ui.separator();
        let visible = self.view.visible();
        egui::ScrollArea::vertical()
            .id_salt(("messages", self.active().cloned()))
            .auto_shrink([false, false])
            .show_rows(ui, row_height, visible.len(), |ui, range| {
                for index in range {
                    let record = &visible[index];
                    let selected = self
                        .view
                        .selected
                        .as_ref()
                        .is_some_and(|s| Arc::ptr_eq(s, record));
                    let fill = if selected {
                        selection(dark)
                    } else if index % 2 == 0 {
                        if dark {
                            Color32::from_rgb(33, 33, 36)
                        } else {
                            Color32::from_rgb(247, 247, 249)
                        }
                    } else {
                        Color32::TRANSPARENT
                    };
                    let frame = egui::Frame::new()
                        .fill(fill)
                        .inner_margin(egui::Margin::symmetric(5, 4))
                        .show(ui, |ui| {
                            ui.set_min_height(row_height - 8.);
                            ui.horizontal(|ui| {
                                ui.allocate_ui_with_layout(
                                    egui::vec2(first_width, row_height - 8.),
                                    egui::Layout::top_down(egui::Align::Min),
                                    |ui| {
                                        // Compact, empty and tombstone rows must reserve the
                                        // same first-column width as rows with a value preview.
                                        ui.set_width(first_width);
                                        ui.horizontal(|ui| {
                                            ui.add(
                                                egui::Label::new(
                                                    RichText::new(event_label(record)).strong(),
                                                )
                                                .truncate(),
                                            );
                                            ui.add(
                                                egui::Label::new(
                                                    RichText::new(preview(
                                                        record.key.as_deref(),
                                                        30,
                                                    ))
                                                    .color(muted(dark)),
                                                )
                                                .truncate(),
                                            );
                                        });
                                        if !self.workspace.settings.compact_rows {
                                            ui.add(
                                                egui::Label::new(
                                                    RichText::new(record.preview())
                                                        .small()
                                                        .color(muted(dark)),
                                                )
                                                .truncate(),
                                            );
                                        }
                                    },
                                );
                                ui.add_sized(
                                    [partition_width, row_height - 8.],
                                    egui::Label::new(
                                        RichText::new(record.partition.to_string()).monospace(),
                                    )
                                    .truncate(),
                                );
                                ui.add_sized(
                                    [offset_width, row_height - 8.],
                                    egui::Label::new(
                                        RichText::new(record.offset.to_string()).monospace(),
                                    )
                                    .truncate(),
                                )
                                .on_hover_text(record.offset.to_string());
                                ui.add_sized(
                                    [time_width, row_height - 8.],
                                    egui::Label::new(
                                        RichText::new(
                                            record_time(record)
                                                .map(|time| {
                                                    timestamp(time, self.workspace.settings.utc)
                                                })
                                                .unwrap_or_else(|| "—".into()),
                                        )
                                        .monospace(),
                                    )
                                    .truncate(),
                                )
                                .on_hover_text(
                                    record_time(record)
                                        .and_then(chrono::DateTime::from_timestamp_millis)
                                        .map(|time| {
                                            if self.workspace.settings.utc {
                                                time.to_rfc3339_opts(
                                                    chrono::SecondsFormat::Millis,
                                                    true,
                                                )
                                            } else {
                                                time.with_timezone(&chrono::Local).to_rfc3339_opts(
                                                    chrono::SecondsFormat::Millis,
                                                    false,
                                                )
                                            }
                                        })
                                        .unwrap_or_else(|| "Timestamp unavailable".into()),
                                );
                            });
                        });
                    let response = ui.interact(
                        frame.response.rect,
                        ui.scope_id().with((record.partition, record.offset)),
                        egui::Sense::click(),
                    );
                    if selected {
                        ui.painter().line_segment(
                            [
                                frame.response.rect.left_top(),
                                frame.response.rect.left_bottom(),
                            ],
                            egui::Stroke::new(2., accent(dark)),
                        );
                    }
                    if response.clicked() {
                        self.select(record.clone());
                    }
                }
            });
    }
    fn inspector(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            for (n, label) in ["Value", "Key", "Headers", "Metadata"].iter().enumerate() {
                if ui
                    .selectable_value(&mut self.view.inspector_tab, n, *label)
                    .changed()
                {
                    if let Some(r) = self.view.selected.clone() {
                        self.select(r);
                    }
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(
                        self.view.selected.is_some(),
                        egui::Button::new("Copy record").small(),
                    )
                    .on_hover_text("Copy the complete record as JSON, including raw Base64 bytes.")
                    .clicked()
                {
                    if let Some(r) = &self.view.selected {
                        if r.bytes() > 4 * 1024 * 1024 {
                            self.notice =
                                "Clipboard limit is 4 MiB; export this record instead".into();
                        } else {
                            ui.ctx().copy_text(r.json().to_string());
                            self.notice = "Copied the full record as JSON".into();
                        }
                    }
                }
                if ui
                    .add_enabled(
                        self.view.selected.is_some() && self.view.formatted != "Preparing preview…",
                        egui::Button::new("Copy view").small(),
                    )
                    .on_hover_text("Copy the displayed preview in the selected format.")
                    .clicked()
                {
                    ui.ctx().copy_text(self.view.formatted.clone());
                    self.notice = "Copied the displayed preview".into();
                }
                if self.view.inspector_tab <= 1 {
                    let format = if self.view.inspector_tab == 1 {
                        &mut self.view.key_format
                    } else {
                        &mut self.view.value_format
                    };
                    let previous = *format;
                    egui::ComboBox::from_id_salt("payload-format")
                        .width(78.)
                        .selected_text(format.label())
                        .show_ui(ui, |ui| {
                            for choice in [
                                PayloadFormat::Auto,
                                PayloadFormat::Json,
                                PayloadFormat::Text,
                                PayloadFormat::Hex,
                            ] {
                                ui.selectable_value(format, choice, choice.label());
                            }
                        });
                    if previous != *format {
                        if let Some(record) = self.view.selected.clone() {
                            self.select(record);
                        }
                    }
                }
            });
        });
        ui.separator();
        if let Some(r) = &self.view.selected {
            ui.horizontal_wrapped(|ui| {
                ui.label(RichText::new(format!("Key  {}", preview(r.key.as_deref(), 40))).small());
                ui.label(RichText::new(format!("Partition  {}", r.partition)).small());
                ui.label(RichText::new(format!("Offset  {}", r.offset)).small());
                ui.label(
                    RichText::new(format!(
                        "{}  ·  {} headers",
                        human_bytes(r.bytes()),
                        r.headers.len()
                    ))
                    .small()
                    .color(muted(self.dark())),
                );
            });
        } else {
            ui.small("Select a message to inspect its bytes, key and ordered headers.");
        }
        ui.add_space(5.);
        egui::Frame::new()
            .fill(if self.dark() {
                Color32::from_rgb(24, 24, 27)
            } else {
                Color32::from_rgb(250, 250, 251)
            })
            .stroke(egui::Stroke::new(1., border(self.dark())))
            .corner_radius(4)
            .inner_margin(10)
            .show(ui, |ui| {
                ui.set_min_size(egui::vec2(
                    ui.available_width(),
                    ui.available_height().max(40.),
                ));
                egui::ScrollArea::both()
                    .id_salt("inspector-code")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let text = &self.view.formatted;
                        let format = if self.view.inspector_tab == 1 {
                            self.view.key_format
                        } else {
                            self.view.value_format
                        };
                        let json = self.view.inspector_tab <= 1
                            && matches!(format, PayloadFormat::Auto | PayloadFormat::Json)
                            && (text.trim_start().starts_with('{')
                                || text.trim_start().starts_with('['));
                        if json && text.len() < 64 * 1024 {
                            ui.horizontal_top(|ui| {
                                let numbers = (1..=text.lines().count())
                                    .map(|n| format!("{n:>3}"))
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                ui.add(egui::Label::new(
                                    RichText::new(numbers)
                                        .monospace()
                                        .size(self.workspace.settings.font_size - 0.5)
                                        .color(muted(self.dark())),
                                ));
                                ui.add_space(7.);
                                ui.add(
                                    egui::Label::new(highlight_json(
                                        text,
                                        self.dark(),
                                        self.workspace.settings.font_size - 0.5,
                                    ))
                                    .selectable(true)
                                    .wrap_mode(egui::TextWrapMode::Extend),
                                );
                            });
                        } else {
                            ui.add(
                                egui::Label::new(
                                    RichText::new(text)
                                        .monospace()
                                        .size(self.workspace.settings.font_size - 0.5),
                                )
                                .selectable(true),
                            );
                        }
                    });
            });
    }
    fn information(&mut self, ui: &mut egui::Ui) {
        if self.view.subview == 1 {
            let Some(active) = self.active() else {
                return;
            };
            ui.add_space(12.);
            ui.label(RichText::new("Topic information").size(18.).strong());
            ui.add_space(12.);
            egui::Grid::new("topic-information")
                .num_columns(2)
                .spacing([28., 14.])
                .show(ui, |ui| {
                    for (label, value) in [
                        ("Topic", active.topic.clone()),
                        (
                            "Server",
                            self.server().map(|s| s.name.clone()).unwrap_or_default(),
                        ),
                        (
                            "Source",
                            if self.offline() {
                                "Offline synthetic stream".into()
                            } else {
                                "Kafka · explicit topic access".into()
                            },
                        ),
                        ("Session", self.status().to_string()),
                        (
                            "Retained records",
                            self.view.buffer.records.len().to_string(),
                        ),
                        ("Retained raw data", human_bytes(self.view.buffer.bytes)),
                        (
                            "Isolation",
                            self.properties()
                                .ok()
                                .and_then(|p| p.get("isolation.level").cloned())
                                .unwrap_or_else(|| "read_committed".into()),
                        ),
                    ] {
                        ui.label(RichText::new(label).color(muted(self.dark())));
                        ui.label(value);
                        ui.end_row();
                    }
                });
            ui.add_space(20.);
            ui.label("Topic names are registered locally. Inspect topic queries only this exact topic and never lists the cluster.");
            ui.add_space(8.);
            ui.label("Finite reads capture exclusive partition ends. Read-committed ends respect the last stable offset, including open transactions.");
        } else {
            ui.add_space(12.);
            ui.label(
                RichText::new("Inspected partition bounds")
                    .size(18.)
                    .strong(),
            );
            ui.add_space(8.);
            ui.label("Ends are exclusive. An offset range is not a message count; compacted and transactional topics can contain gaps.");
            ui.add_space(16.);
            if self.view.metadata.is_empty() {
                ui.label("Choose Inspect topic to load partition metadata.");
            } else {
                egui::Grid::new("partition-bounds")
                    .striped(true)
                    .spacing([55., 14.])
                    .show(ui, |ui| {
                        for title in ["Partition", "Beginning", "Readable end", "Offset positions"]
                        {
                            ui.strong(title);
                        }
                        ui.end_row();
                        for p in &self.view.metadata {
                            ui.monospace(p.partition.to_string());
                            ui.monospace(p.beginning.to_string());
                            ui.monospace(p.end.to_string());
                            ui.monospace((p.end - p.beginning).to_string());
                            ui.end_row();
                        }
                    });
            }
        }
    }
    fn shortcuts(&mut self, ctx: &egui::Context) {
        if self.modal_open() {
            return;
        }
        if ctx.input_mut(|i| {
            i.consume_shortcut(&egui::KeyboardShortcut::new(
                egui::Modifiers::COMMAND,
                egui::Key::Enter,
            ))
        }) && !self.running()
        {
            self.start(false);
        }
        if ctx.input_mut(|i| {
            i.consume_shortcut(&egui::KeyboardShortcut::new(
                egui::Modifiers::COMMAND,
                egui::Key::F,
            ))
        }) {
            self.focus_filter = true;
            self.view.subview = 0;
        }
        if ctx.input_mut(|i| {
            i.consume_shortcut(&egui::KeyboardShortcut::new(
                egui::Modifiers::COMMAND,
                egui::Key::Comma,
            ))
        }) {
            self.settings_editor = Some(SettingsEditor::new(&self.workspace.settings));
        }
    }
}

impl eframe::App for ReaderApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain();
        let ctx = ui.ctx().clone();
        self.shortcuts(&ctx);
        self.automate(&ctx);
        egui::Panel::top("application-brand")
            .exact_size(36.)
            .frame(
                egui::Frame::new()
                    .fill(if self.dark() {
                        Color32::from_rgb(24, 24, 27)
                    } else {
                        Color32::from_rgb(244, 245, 247)
                    })
                    .inner_margin(egui::Margin::symmetric(14, 7)),
            )
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    let (rect, _) =
                        ui.allocate_exact_size(egui::vec2(22., 18.), egui::Sense::hover());
                    for n in 0..3 {
                        let y = rect.top() + 3. + 5. * n as f32;
                        ui.painter().line_segment(
                            [
                                egui::pos2(rect.left() + 2., y + 2.),
                                egui::pos2(rect.right() - 2., y),
                            ],
                            egui::Stroke::new(2., accent(self.dark())),
                        );
                    }
                    ui.label(RichText::new("Kafka Reader").strong());
                    ui.label(
                        RichText::new(env!("CARGO_PKG_VERSION"))
                            .small()
                            .color(muted(self.dark())),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(RichText::new("READ ONLY").small().color(muted(self.dark())));
                    });
                });
            });
        egui::Panel::bottom("status-bar")
            .exact_size(33.)
            .frame(
                egui::Frame::new()
                    .fill(panel(self.dark()))
                    .inner_margin(egui::Margin::symmetric(14, 6)),
            )
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    status_dot(
                        ui,
                        self.status(),
                        if self.running() && !self.offline() {
                            Color32::from_rgb(88, 183, 128)
                        } else {
                            muted(self.dark())
                        },
                    );
                    ui.separator();
                    let text = if !self.notice.is_empty() {
                        self.notice.clone()
                    } else {
                        format!(
                            "{} records loaded · {} fetched · {} evicted · {} skipped",
                            self.view.buffer.records.len(),
                            self.view.fetched,
                            self.view.buffer.evicted,
                            self.view.skipped
                        )
                    };
                    ui.add(egui::Label::new(RichText::new(text).small()).truncate())
                        .on_hover_text(if self.notice.is_empty() {
                            &self.view.detail
                        } else {
                            &self.notice
                        });
                });
            });
        egui::Panel::left("server-sidebar")
            .default_size(270.)
            .size_range(230.0..=380.0)
            .resizable(true)
            .frame(
                egui::Frame::new()
                    .fill(if self.dark() {
                        Color32::from_rgb(26, 26, 29)
                    } else {
                        Color32::from_rgb(242, 243, 246)
                    })
                    .inner_margin(12),
            )
            .show(ui, |ui| self.sidebar(ui));
        egui::CentralPanel::default()
            .frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(0))
            .show(ui, |ui| {
                egui::Panel::top("topic-tabs")
                    .exact_size(40.)
                    .frame(
                        egui::Frame::new()
                            .fill(if self.dark() {
                                Color32::from_rgb(25, 25, 28)
                            } else {
                                Color32::from_rgb(235, 237, 240)
                            })
                            .inner_margin(0),
                    )
                    .show(ui, |ui| self.tabs(ui));
                egui::Frame::new()
                    .inner_margin(egui::Margin::symmetric(16, 10))
                    .show(ui, |ui| {
                        if self.active().is_none() {
                            ui.vertical_centered(|ui| {
                                ui.add_space(100.);
                                ui.heading("Open a topic from your workspace");
                                ui.label(
                                    "Add a server and register an exact topic to start reading.",
                                );
                                ui.add_space(12.);
                                if ui
                                    .button(
                                        RichText::new("+ Add server").color(accent(self.dark())),
                                    )
                                    .clicked()
                                {
                                    self.open_server(None);
                                }
                            });
                        } else {
                            self.controls(ui);
                            if self.view.subview == 0 {
                                let available = ui.available_height();
                                egui::Panel::bottom("message-inspector")
                                    .default_size((available * 0.46).clamp(180., 400.))
                                    .size_range(130.0..=650.0)
                                    .resizable(true)
                                    .frame(
                                        egui::Frame::new()
                                            .inner_margin(egui::Margin::symmetric(0, 8)),
                                    )
                                    .show(ui, |ui| self.inspector(ui));
                                self.messages(ui);
                            } else {
                                egui::ScrollArea::vertical().show(ui, |ui| self.information(ui));
                            }
                        }
                    });
            });
        self.native_dialogs(&ctx);
        self.capture_main_if_needed(&ctx);
        if self
            .options
            .close_after
            .is_some_and(|s| self.created.elapsed() >= Duration::from_secs(s))
        {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        ctx.request_repaint_after(Duration::from_millis(40));
    }
    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if let Some(s) = &self.session {
            s.stop();
        }
        self.credentials.clear();
    }
}

fn accent(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(234, 167, 76)
    } else {
        Color32::from_rgb(166, 92, 21)
    }
}
fn panel(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(30, 30, 33)
    } else {
        Color32::from_rgb(252, 252, 253)
    }
}
fn muted(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(158, 158, 168)
    } else {
        Color32::from_rgb(104, 108, 119)
    }
}
fn border(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(61, 61, 66)
    } else {
        Color32::from_rgb(214, 217, 222)
    }
}
fn selection(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(69, 55, 41)
    } else {
        Color32::from_rgb(250, 235, 213)
    }
}
fn status_dot(ui: &mut egui::Ui, label: &str, color: Color32) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(9., 15.), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 3.5, color);
    ui.label(RichText::new(label).small());
}
fn star(ui: &mut egui::Ui, selected: bool, dark: bool) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(19., 19.), egui::Sense::click());
    let points: Vec<_> = (0..=10)
        .map(|i| {
            let angle = i as f32 * std::f32::consts::PI / 5. - std::f32::consts::FRAC_PI_2;
            rect.center()
                + egui::vec2(angle.cos(), angle.sin()) * if i % 2 == 0 { 6.5 } else { 2.8 }
        })
        .collect();
    ui.painter().add(egui::Shape::line(
        points,
        egui::Stroke::new(
            if selected { 1.8 } else { 1.1 },
            if selected { accent(dark) } else { muted(dark) },
        ),
    ));
    response.on_hover_text(if selected {
        "Remove favorite"
    } else {
        "Add favorite"
    })
}
fn human_bytes(n: usize) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1024 * 1024 {
        format!("{:.1} KiB", n as f64 / 1024.)
    } else {
        format!("{:.1} MiB", n as f64 / (1024. * 1024.))
    }
}
fn size_desc(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "null".into(),
        Some(v) if v.is_empty() => "empty (0 bytes)".into(),
        Some(v) => format!("{} bytes", v.len()),
    }
}
fn timestamp(ms: i64, utc: bool) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|v| {
            if utc {
                v.format("%H:%M:%S%.3f").to_string()
            } else {
                v.with_timezone(&chrono::Local)
                    .format("%H:%M:%S%.3f")
                    .to_string()
            }
        })
        .unwrap_or_else(|| "—".into())
}
fn event_label(record: &Record) -> String {
    let Some(value) = &record.value else {
        return "Tombstone".into();
    };
    if value.is_empty() {
        return "Empty value".into();
    }
    if value.len() <= 4096 {
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) {
            for key in ["eventType", "event", "type", "status"] {
                if let Some(s) = json.get(key).and_then(|v| v.as_str()) {
                    return s.chars().take(64).collect();
                }
            }
            return "JSON message".into();
        }
    }
    if std::str::from_utf8(value).is_ok() {
        "Text message".into()
    } else {
        "Binary message".into()
    }
}
fn highlight_json(text: &str, dark: bool, size: f32) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    let bytes = text.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = start + 1;
        let mut color = if dark {
            Color32::from_rgb(210, 212, 218)
        } else {
            Color32::from_rgb(50, 53, 62)
        };
        if bytes[start] == b'"' {
            let mut escaped = false;
            while end < bytes.len() {
                let c = bytes[end];
                end += 1;
                if c == b'"' && !escaped {
                    break;
                }
                if c == b'\\' {
                    escaped = !escaped;
                } else {
                    escaped = false;
                }
            }
            let key = bytes[end..]
                .iter()
                .copied()
                .find(|c| !c.is_ascii_whitespace())
                == Some(b':');
            color = if key {
                if dark {
                    Color32::from_rgb(139, 184, 216)
                } else {
                    Color32::from_rgb(45, 103, 148)
                }
            } else if dark {
                Color32::from_rgb(170, 196, 139)
            } else {
                Color32::from_rgb(76, 117, 36)
            };
        } else if bytes[start].is_ascii_digit() || bytes[start] == b'-' {
            while end < bytes.len()
                && (bytes[end].is_ascii_digit() || b".eE+-".contains(&bytes[end]))
            {
                end += 1;
            }
            color = accent(dark);
        } else {
            while end < bytes.len() && !bytes[end].is_ascii_digit() && bytes[end] != b'"' {
                end += 1;
            }
        }
        while !text.is_char_boundary(end) {
            end += 1;
        }
        job.append(
            &text[start..end],
            0.,
            egui::TextFormat {
                font_id: egui::FontId::monospace(size),
                color,
                ..Default::default()
            },
        );
        start = end;
    }
    job
}
fn theme(ctx: &egui::Context, settings: &Settings) {
    let dark = settings.dark;
    let mut visuals = if dark {
        egui::Visuals::dark()
    } else {
        egui::Visuals::light()
    };
    visuals.panel_fill = panel(dark);
    visuals.window_fill = panel(dark);
    visuals.extreme_bg_color = if dark {
        Color32::from_rgb(23, 23, 26)
    } else {
        Color32::WHITE
    };
    visuals.faint_bg_color = if dark {
        Color32::from_rgb(35, 35, 39)
    } else {
        Color32::from_rgb(244, 245, 247)
    };
    visuals.selection.bg_fill = selection(dark);
    visuals.selection.stroke = egui::Stroke::new(1., accent(dark));
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1., border(dark));
    visuals.widgets.inactive.bg_fill = if dark {
        Color32::from_rgb(43, 43, 48)
    } else {
        Color32::from_rgb(243, 244, 247)
    };
    visuals.widgets.inactive.weak_bg_fill = visuals.widgets.inactive.bg_fill;
    ctx.set_visuals(visuals);
    ctx.all_styles_mut(|style| {
        style.spacing.item_spacing = egui::vec2(8., if settings.compact_rows { 4. } else { 6. });
        style.spacing.button_padding = egui::vec2(10., 5.);
        style.text_styles.insert(
            egui::TextStyle::Body,
            egui::FontId::proportional(settings.font_size),
        );
        style.text_styles.insert(
            egui::TextStyle::Button,
            egui::FontId::proportional(settings.font_size),
        );
        style.text_styles.insert(
            egui::TextStyle::Small,
            egui::FontId::proportional((settings.font_size - 2.).max(10.)),
        );
        style.text_styles.insert(
            egui::TextStyle::Monospace,
            egui::FontId::monospace(settings.font_size - 0.5),
        );
    });
}

fn session_properties(server: &Server, credentials: &BTreeMap<u64, Properties>) -> Properties {
    let mut properties = server.properties.clone();
    if let Some(secret) = credentials.get(&server.id) {
        properties.extend(secret.clone());
    }
    properties
}
fn deliver_preview(
    active: Option<&Tab>,
    current: &mut TopicView,
    cached: &mut BTreeMap<Tab, TopicView>,
    destination: &Tab,
    generation: u64,
    text: String,
) {
    let target = if active == Some(destination) {
        Some(current)
    } else {
        cached.get_mut(destination)
    };
    if let Some(target) = target {
        if generation == target.generation {
            target.formatted = text;
        }
    }
}
#[cfg(test)]
mod view_tests {
    use super::*;
    #[test]
    fn sorting_filtered_rows_preserves_retention_order_and_selection() {
        let mut view = TopicView::new(&Settings::default());
        let first = Arc::new(demo_record(1, 10, "orders", 0));
        let second = Arc::new(demo_record(0, 2, "orders", 0));
        let third = Arc::new(demo_record(0, 20, "orders", 0));
        for record in [&first, &second, &third] {
            view.buffer.add(record.clone());
        }
        view.selected = Some(second.clone());
        view.display_partition = Some(0);
        view.order.select(SortColumn::Offset);
        let visible = view.visible();
        assert_eq!(
            visible.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [2, 20]
        );
        assert!(Arc::ptr_eq(&visible[0], &second));
        assert!(Arc::ptr_eq(view.selected.as_ref().unwrap(), &second));
        assert!(Arc::ptr_eq(view.buffer.records.front().unwrap(), &first));
        assert_eq!(view.buffer.records.len(), 3);
    }
    #[test]
    fn late_preview_is_scoped_to_topic_and_generation() {
        let active = Tab {
            server: 1,
            topic: "agreement.events".into(),
        };
        let background = Tab {
            server: 2,
            topic: "agreement.events".into(),
        };
        let mut current = TopicView::new(&Settings::default());
        current.generation = 8;
        current.formatted = "current".into();
        let mut prior = TopicView::new(&Settings::default());
        prior.generation = 4;
        let mut cached = BTreeMap::from([(background.clone(), prior)]);
        deliver_preview(
            Some(&active),
            &mut current,
            &mut cached,
            &background,
            4,
            "background result".into(),
        );
        assert_eq!(current.formatted, "current");
        assert_eq!(cached[&background].formatted, "background result");
        deliver_preview(
            Some(&active),
            &mut current,
            &mut cached,
            &active,
            7,
            "stale result".into(),
        );
        assert_eq!(current.formatted, "current");
        cached.remove(&background);
        deliver_preview(
            Some(&active),
            &mut current,
            &mut cached,
            &background,
            4,
            "closed tab".into(),
        );
        assert!(cached.is_empty());
        assert_eq!(current.formatted, "current");
        deliver_preview(
            Some(&active),
            &mut current,
            &mut cached,
            &active,
            8,
            "fresh result".into(),
        );
        assert_eq!(current.formatted, "fresh result");
    }
    #[test]
    fn credentials_never_cross_server_ids() {
        let first = Server {
            id: 2,
            name: "First".into(),
            offline: false,
            properties: Properties::new(),
            topics: vec![],
        };
        let second = Server {
            id: 3,
            name: "Second".into(),
            offline: false,
            properties: Properties::new(),
            topics: vec![],
        };
        let credentials = BTreeMap::from([(
            2,
            Properties::from([("sasl.password".into(), "only-first".into())]),
        )]);
        assert_eq!(
            session_properties(&first, &credentials)["sasl.password"],
            "only-first"
        );
        assert!(!session_properties(&second, &credentials).contains_key("sasl.password"));
    }
}
