use super::*;
use kafka_reader_rust::language::{Language, LanguagePreference};
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
    pub oauth_endpoint: String,
    pub oauth_client: String,
    pub refresh_token: String,
    pub max_token_seconds: String,
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
    oauth_session: OAuthSession,
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
        let alternate_mechanism = take("sasl.mechanisms");
        if mechanism.is_empty() {
            mechanism = alternate_mechanism;
        }
        if mechanism.is_empty() {
            mechanism = "PLAIN".into();
        }
        let username = take("sasl.username");
        let password = take("sasl.password");
        let oauth_endpoint = take("oauth.token.endpoint.uri");
        let refresh_token = take("oauth.refresh.token");
        let ca = take("ssl.ca.location");
        let certificate = take("ssl.certificate.location");
        let key = take("ssl.key.location");
        let key_password = take("ssl.key.password");
        let oauth_client = properties
            .remove("oauth.client.id")
            .unwrap_or_else(|| "kafka-cli".into());
        let max_token_seconds = properties
            .remove("oauth.max.token.expiry.seconds")
            .unwrap_or_else(|| "300".into());
        Self {
            name: server.name.clone(),
            original: server,
            brokers,
            protocol: protocol.to_ascii_uppercase(),
            mechanism: mechanism.to_ascii_uppercase(),
            username,
            password,
            oauth_endpoint,
            oauth_client,
            refresh_token,
            max_token_seconds,
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
            oauth_session: OAuthSession::default(),
            is_new,
        }
    }
    pub(super) fn build(&self, lang: Language) -> Result<(Server, Properties), String> {
        let mut p = parse_properties(&self.advanced)?;
        p.insert("bootstrap.servers".into(), self.brokers.trim().into());
        p.insert("security.protocol".into(), self.protocol.clone());
        p.remove("sasl.mechanisms");
        p.insert("sasl.mechanism".into(), self.mechanism.clone());
        let login = [
            ("sasl.username", &self.username),
            ("sasl.password", &self.password),
        ];
        let oauth = [
            ("oauth.token.endpoint.uri", &self.oauth_endpoint),
            ("oauth.client.id", &self.oauth_client),
            ("oauth.refresh.token", &self.refresh_token),
            ("oauth.max.token.expiry.seconds", &self.max_token_seconds),
        ];
        let (active, inactive): (&[_], &[_]) = if self.mechanism == "OAUTHBEARER" {
            if self.protocol != "SASL_SSL" {
                return Err(lang
                    .text(
                        "Choose SASL_SSL on Connection to use a refresh token",
                        "Для токена обновления выберите SASL_SSL на вкладке «Подключение»",
                    )
                    .into());
            }
            (&oauth, &login)
        } else {
            (&login, &oauth)
        };
        for (key, _) in inactive {
            p.remove(*key);
        }
        for (key, value) in active.iter().copied().chain([
            ("ssl.ca.location", &self.ca),
            ("ssl.certificate.location", &self.certificate),
            ("ssl.key.location", &self.key),
            ("ssl.key.password", &self.key_password),
        ]) {
            if !value.is_empty() {
                p.insert(key.into(), value.clone());
            }
        }
        if self.mechanism == "OAUTHBEARER" {
            let prop = |key: &str| p.get(key).map(String::as_str).unwrap_or_default();
            kafka_reader_rust::oauth::validate_endpoint(prop("oauth.token.endpoint.uri"))
                .map_err(|e| lang.message(&e))?;
            let client = prop("oauth.client.id");
            if client.trim().is_empty() || client.chars().any(char::is_control) {
                return Err(lang
                    .text(
                        "Enter an OAuth client ID",
                        "Введите идентификатор клиента OAuth",
                    )
                    .into());
            }
            if prop("oauth.max.token.expiry.seconds")
                .parse::<u64>()
                .unwrap_or_default()
                == 0
            {
                return Err(lang
                    .text(
                        "Enter a positive token lifetime in seconds",
                        "Введите положительный срок действия токена в секундах",
                    )
                    .into());
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
            return Err(lang
                .text(
                    "Use a server name containing 1–100 characters",
                    "Введите имя сервера длиной от 1 до 100 символов",
                )
                .into());
        }
        if !server.offline && self.brokers.trim().is_empty() {
            return Err(lang
                .text(
                    "Enter at least one bootstrap broker as host:port",
                    "Укажите хотя бы один начальный брокер в формате хост:порт",
                )
                .into());
        }
        if !self.test_topic.trim().is_empty() {
            server.register_topic(self.test_topic.trim())?;
        }
        Ok((server, secrets))
    }
    fn bound_oauth(&self, props: &Properties, saved: Option<&OAuthSession>) -> OAuthSession {
        let mut session = if self.oauth_session.matches(props) {
            self.oauth_session.clone()
        } else {
            saved.cloned().unwrap_or_default()
        };
        session.bind(props);
        session
    }
    fn test(&mut self, lang: Language, saved: Option<&OAuthSession>) {
        if self.test.as_ref().is_some_and(Session::running) {
            return;
        }
        let request: Result<Request, String> = (|| {
            validate_topic(self.test_topic.trim())?;
            let (server, credentials) = self.build(lang)?;
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
                self.oauth_session = self.bound_oauth(&request.props, saved);
                self.test = Some(Session::start_with_oauth(request, &mut self.oauth_session));
                self.message = lang
                    .text("Checking this exact topic…", "Проверяем доступ к топику…")
                    .into();
            }
            Err(e) => self.message = lang.message(&e),
        }
    }
    fn drain_test(&mut self, lang: Language) {
        if let Some(test) = &self.test {
            while let Some(event) = test.try_next() {
                match event {
                    Event::Metadata(p) => {
                        self.message = match lang {
                            Language::English => {
                                format!("Topic access verified · {} partitions", p.len())
                            }
                            Language::Russian => {
                                format!("Доступ к топику подтверждён · разделов: {}", p.len())
                            }
                        }
                    }
                    Event::State(state, detail)
                        if matches!(state.as_str(), "FAILED" | "STOPPED") =>
                    {
                        self.message = lang.message(&detail)
                    }
                    _ => {}
                }
            }
        }
    }
    fn import(&mut self, lang: Language) {
        let result: Result<ServerEditor, String> = (|| {
            let path = PathBuf::from(&self.file_path);
            if fs::metadata(&path).map_err(|e| e.to_string())?.len() > 1024 * 1024 {
                return Err(lang
                    .text("Profile exceeds 1 MiB", "Размер профиля превышает 1 МиБ")
                    .into());
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
            loaded.oauth_session = self.oauth_session.clone();
            loaded.file_path = self.file_path.clone();
            loaded.tab = 3;
            loaded.message = lang
                .text(
                    "Profile imported into this draft. Save applies it; Cancel discards it.",
                    "Профиль загружен в черновик. Сохраните его, чтобы применить изменения.",
                )
                .into();
            Ok(loaded)
        })();
        match result {
            Ok(loaded) => *self = loaded,
            Err(e) => {
                self.message = format!(
                    "{}: {}",
                    lang.text("Import failed", "Ошибка импорта"),
                    lang.message(&e)
                )
            }
        }
    }
    fn export(&mut self, lang: Language) {
        let result = (|| {
            let (server, _) = self.build(lang)?;
            let mut properties = server.properties;
            properties.insert("reader.topic".into(), self.test_topic.clone());
            let path = PathBuf::from(&self.file_path);
            create_export(&path)?
                .write_all(profile_text(&properties).as_bytes())
                .map_err(|e| e.to_string())
        })();
        self.message = match result {
            Ok(()) => lang
                .text(
                    "Exported this profile without session credentials",
                    "Профиль экспортирован без учётных данных сеанса",
                )
                .into(),
            Err(e) => format!(
                "{}: {}",
                lang.text("Export failed", "Ошибка экспорта"),
                lang.message(&e)
            ),
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

pub(super) fn server_title(is_new: bool, lang: Language) -> &'static str {
    if is_new {
        lang.text(
            "Add server · Kafka Reader",
            "Добавить сервер · Kafka Reader",
        )
    } else {
        lang.text(
            "Edit server · Kafka Reader",
            "Изменить сервер · Kafka Reader",
        )
    }
}

pub(super) fn settings_title(lang: Language) -> &'static str {
    lang.text(
        "App settings · Kafka Reader",
        "Настройки приложения · Kafka Reader",
    )
}

impl ReaderApp {
    pub(super) fn open_server(&mut self, id: Option<u64>) {
        if self.running() {
            self.notice = self
                .language()
                .text(
                    "Stop reading before changing server configuration",
                    "Остановите чтение перед изменением настроек сервера",
                )
                .into();
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
        let oauth_session = self
            .oauth_sessions
            .get(&server.id)
            .cloned()
            .unwrap_or_default();
        let mut editor = ServerEditor::new(server, credentials, id.is_none(), topic);
        editor.oauth_session = oauth_session;
        self.server_editor = Some(editor);
    }
    pub(super) fn save_server(&mut self, editor: &ServerEditor) -> Result<(), String> {
        let lang = self.language();
        if self.running() {
            return Err(lang
                .text(
                    "Stop reading before saving server changes",
                    "Остановите чтение перед сохранением настроек сервера",
                )
                .into());
        }
        let (mut server, credentials) = editor.build(lang)?;
        let mut proposed = self.workspace.clone();
        if editor.is_new {
            proposed.servers.push(server.clone());
        } else {
            let old = proposed
                .server_mut(server.id)
                .ok_or(lang.text("Server is unavailable", "Сервер недоступен"))?;
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
            return Err(lang.text("Repair or move the invalid workspace file before saving a server", "Чтобы сохранить сервер, исправьте или переместите повреждённый файл рабочего пространства").into());
        }
        self.options.store.save(&proposed)?;
        let previous = self.active().cloned();
        self.workspace = proposed;
        self.credentials.insert(server.id, credentials);
        if editor.mechanism == "OAUTHBEARER" {
            let props = session_properties(&server, &self.credentials);
            let session = editor.bound_oauth(&props, self.oauth_sessions.get(&server.id));
            self.oauth_sessions.insert(server.id, session);
        } else {
            self.oauth_sessions.remove(&server.id);
        }
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
        self.notice = lang.text(
            "Server saved. Credentials stay in this session; no connection was started.",
            "Сервер сохранён. Учётные данные доступны только в этом сеансе. Подключение начнётся по вашей команде.",
        ).into();
        Ok(())
    }
    pub(super) fn native_dialogs(&mut self, ctx: &egui::Context) {
        let lang = self.language();
        if let Some(mut editor) = self.server_editor.take() {
            editor.drain_test(lang);
            let title = server_title(editor.is_new, lang);
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("server-editor"),
                egui::ViewportBuilder::default().with_title(title).with_inner_size([730., 650.]).with_min_inner_size([650., 580.]).with_position([160., 100.]), |ui, class| {
                    self.smoke.native_window_seen |= class == egui::ViewportClass::Immediate;
                    let mut action = if ui.input(|i| i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                    egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                        ui.label(RichText::new(if editor.is_new { lang.text("Add a server", "Добавить сервер") } else { lang.text("Edit server", "Изменить сервер") }).size(21.).strong());
                        ui.label(RichText::new(lang.text("Keep connection details together. Read only when you choose a topic.", "Настройте подключение. Для чтения выберите топик.")).color(muted(self.dark())));
                        ui.add_space(16.);
                        ui.horizontal(|ui| { for (n,label) in [lang.text("Connection", "Подключение"), lang.text("Authentication", "Аутентификация"), "TLS", lang.text("Advanced", "Дополнительно")].iter().enumerate() { ui.selectable_value(&mut editor.tab,n,*label); } });
                        ui.separator(); ui.add_space(12.);
                        egui::Panel::bottom("server-editor-actions").exact_size(90.).frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, 8))).show(ui, |ui| {
                            ui.add(egui::Label::new(RichText::new(&editor.message).small()).wrap());
                            ui.add_space(8.);
                            ui.horizontal(|ui| {
                                if ui.add_enabled(!editor.test.as_ref().is_some_and(Session::running), egui::Button::new(lang.text("Test exact topic", "Проверить топик"))).clicked() { editor.test(lang, self.oauth_sessions.get(&editor.original.id)); }
                                if editor.test.as_ref().is_some_and(Session::running) { ui.spinner(); }
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.add(egui::Button::new(RichText::new(lang.text("Save server", "Сохранить сервер")).strong()).fill(selection(self.dark()))).clicked() { action = Action::Save; }
                                    if ui.button(lang.text("Cancel", "Отмена")).clicked() { action = Action::Cancel; }
                                });
                            });
                        });
                        egui::ScrollArea::vertical().id_salt("server-editor-content").show(ui, |ui| {
                            match editor.tab {
                                0 => {
                                    field(ui, lang, lang.text("Server name", "Имя сервера"), &mut editor.name, lang.text("Development", "Среда разработки"), false);
                                    field(ui, lang, lang.text("Bootstrap servers", "Начальные брокеры"), &mut editor.brokers, "broker-1:9092,broker-2:9092", false);
                                    ui.label(lang.text("Security protocol", "Протокол безопасности"));
                                    egui::ComboBox::from_id_salt("protocol").selected_text(&editor.protocol).width(260.).show_ui(ui, |ui| {
                                        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] { ui.selectable_value(&mut editor.protocol, protocol.into(), protocol); }
                                    }); ui.add_space(14.);
                                    field(ui, lang, lang.text("Exact topic to test / register", "Точное имя топика для проверки или добавления"), &mut editor.test_topic, "orders.events", false);
                                    ui.label(RichText::new(lang.text("Testing queries this exact topic asynchronously. Saving a server does not connect or read messages.", "Проверка обращается только к указанному топику в фоновом режиме. При сохранении сервера подключение не открывается.")).small().color(muted(self.dark())));
                                    ui.add_space(12.);
                                    ui.label(RichText::new(lang.text("Add more topic names from the server tree after saving.", "После сохранения можно добавить ещё топики в дереве серверов.")).small().color(muted(self.dark())));
                                }
                                1 => {
                                    ui.label(RichText::new(lang.text("Session credentials", "Учётные данные сеанса")).strong());
                                    ui.label(RichText::new(lang.text("Usernames, passwords and tokens stay in memory and are not saved with your workspace.", "Имена пользователей, пароли и токены хранятся только в памяти до конца сеанса.")).small().color(muted(self.dark())));
                                    ui.add_space(15.);
                                    ui.label(lang.text("SASL mechanism", "Механизм SASL"));
                                    let refresh_label = lang.text("OAUTHBEARER · Refresh token", "OAUTHBEARER · Токен обновления");
                                    let selected = if editor.mechanism == "OAUTHBEARER" { refresh_label } else { &editor.mechanism };
                                    egui::ComboBox::from_id_salt("sasl-mechanism").selected_text(selected).width(290.).show_ui(ui, |ui| {
                                        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] { ui.selectable_value(&mut editor.mechanism, mechanism.into(), mechanism); }
                                        if ui.selectable_value(&mut editor.mechanism, "OAUTHBEARER".into(), refresh_label).changed() {
                                            editor.protocol = "SASL_SSL".into();
                                        }
                                    }); ui.add_space(13.);
                                    if editor.mechanism == "OAUTHBEARER" {
                                        field(ui, lang, lang.text("Token endpoint URL", "URL для получения токена"), &mut editor.oauth_endpoint, "https://sso.example.org/realms/example/protocol/openid-connect/token", false);
                                        field(ui, lang, lang.text("OAuth client ID", "Идентификатор клиента OAuth"), &mut editor.oauth_client, "kafka-cli", false);
                                        field(ui, lang, lang.text("Refresh token", "Токен обновления"), &mut editor.refresh_token, lang.text("Session only", "Только для этого сеанса"), true);
                                        field(ui, lang, lang.text("Maximum access token lifetime (seconds)", "Предельный срок действия токена доступа (секунды)"), &mut editor.max_token_seconds, "300", false);
                                        if editor.protocol != "SASL_SSL" {
                                            ui.colored_label(accent(self.dark()), lang.text("Choose SASL_SSL on Connection to use a refresh token", "Для токена обновления выберите SASL_SSL на вкладке «Подключение»"));
                                        }
                                    } else {
                                        field(ui, lang, lang.text("Username", "Имя пользователя"), &mut editor.username, lang.text("Kafka principal", "Пользователь Kafka"), false);
                                        field(ui, lang, lang.text("Password", "Пароль"), &mut editor.password, lang.text("Session only", "Только для этого сеанса"), true);
                                        if !editor.protocol.starts_with("SASL") { ui.label(RichText::new(lang.text("Choose SASL_SSL or SASL_PLAINTEXT on Connection to use these credentials.", "Чтобы использовать эти данные, выберите SASL_SSL или SASL_PLAINTEXT на вкладке «Подключение».")).color(accent(self.dark()))); }
                                    }
                                }
                                2 => {
                                    ui.label(RichText::new(lang.text("TLS certificates", "Сертификаты TLS")).strong()); ui.small(lang.text("Certificate and hostname verification are enabled by default.", "Проверка сертификата и имени хоста включена по умолчанию.")); ui.add_space(12.);
                                    field(ui, lang, lang.text("CA certificate · PEM path", "Сертификат центра сертификации · путь к файлу PEM"), &mut editor.ca, "C:/certs/ca.pem", false);
                                    field(ui, lang, lang.text("Client certificate · optional PEM path", "Сертификат клиента · путь к файлу PEM (необязательно)"), &mut editor.certificate, "C:/certs/client.pem", false);
                                    field(ui, lang, lang.text("Client private key · optional PEM path", "Закрытый ключ клиента · путь к файлу PEM (необязательно)"), &mut editor.key, "C:/certs/client-key.pem", false);
                                    field(ui, lang, lang.text("Private-key password · session only", "Пароль закрытого ключа · только для этого сеанса"), &mut editor.key_password, lang.text("Only for an encrypted private key", "Для зашифрованного закрытого ключа"), true);
                                    ui.small(lang.text("Paths can be saved. Inline private keys and key passwords are omitted from workspace/profile exports.", "Пути к файлам сохраняются. Содержимое закрытых ключей и пароли остаются только в памяти."));
                                }
                                _ => {
                                    paste_edit(ui, lang, lang.text("Additional librdkafka properties", "Дополнительные параметры librdkafka"), egui::TextEdit::multiline(&mut editor.advanced).code_editor().desired_rows(7).hint_text("isolation.level=read_committed"), clipboard_text);
                                    ui.small(lang.text("Literal UTF-8 key=value, one property per line. Read-only safety overrides remain enforced.", "Формат UTF-8: ключ=значение, по одному параметру в строке. Приложение сохраняет режим только для чтения."));
                                    ui.add_space(14.); ui.separator(); ui.add_space(8.);
                                    field(ui, lang, lang.text("Import / export connection profile", "Импорт и экспорт профиля подключения"), &mut editor.file_path, "server.properties", false);
                                    ui.horizontal(|ui| { if ui.button(lang.text("Import into draft", "Импорт в черновик")).clicked() { editor.import(lang); } if ui.button(lang.text("Export safe profile", "Экспорт без учётных данных")).clicked() { editor.export(lang); } });
                                    ui.small(lang.text("Imports change this draft only. Export omits session credentials. Existing export files are not overwritten.", "Импорт изменяет только черновик. Для экспорта выберите новый файл. Учётные данные сеанса в него не попадут."));
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
                        editor.message = lang.message(&e);
                        self.server_editor = Some(editor);
                    }
                },
                Action::Cancel => {}
                Action::None => self.server_editor = Some(editor),
            }
        }
        if let Some(mut editor) = self.settings_editor.take() {
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("app-settings"),
                egui::ViewportBuilder::default().with_title(settings_title(lang)).with_inner_size([670., 700.]).with_min_inner_size([610., 640.]).with_position([190., 90.]), |ui,class| {
                    self.smoke.native_window_seen |= class == egui::ViewportClass::Immediate;
                    let mut action = if ui.input(|i|i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                    egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                        ui.label(RichText::new(lang.text("App settings", "Настройки приложения")).size(21.).strong());
                        ui.label(RichText::new(lang.text("Appearance, reading defaults and local memory limits.", "Оформление, параметры чтения по умолчанию и ограничения памяти.")).color(muted(self.dark()))); ui.add_space(16.);
                        egui::Panel::bottom("settings-actions").exact_size(68.).frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, 8))).show(ui, |ui| {
                            ui.label(RichText::new(&editor.error).small());
                            ui.horizontal(|ui| {
                                if self.running() { ui.small(lang.text("Stop reading before applying changes.", "Остановите чтение, чтобы применить изменения.")); }
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.add_enabled(!self.running(), egui::Button::new(RichText::new(lang.text("Apply settings", "Применить")).strong()).fill(selection(self.dark()))).clicked() { action = Action::Save; }
                                    if ui.button(lang.text("Cancel", "Отмена")).clicked() { action = Action::Cancel; }
                                });
                            });
                        });
                        egui::ScrollArea::vertical().show(ui, |ui| {
                            section(ui, lang.text("Appearance", "Оформление"), self.dark());
                            egui::Grid::new("settings-appearance").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label(lang.text("Language", "Язык"));
                                ui.horizontal(|ui| {
                                    ui.selectable_value(&mut editor.value.language, LanguagePreference::System, lang.text("System default", "Как в системе"));
                                    ui.selectable_value(&mut editor.value.language, LanguagePreference::English, "English");
                                    ui.selectable_value(&mut editor.value.language, LanguagePreference::Russian, "Русский");
                                });
                                ui.end_row();
                                ui.label(lang.text("Theme", "Тема")); ui.horizontal(|ui| { ui.selectable_value(&mut editor.value.dark,true,lang.text("Dark", "Тёмная")); ui.selectable_value(&mut editor.value.dark,false,lang.text("Light", "Светлая")); }); ui.end_row();
                                ui.label(lang.text("Message rows", "Строки сообщений")); ui.checkbox(&mut editor.value.compact_rows,lang.text("Compact", "Компактные")); ui.end_row();
                                ui.label(lang.text("Font size", "Размер шрифта")); ui.add(egui::DragValue::new(&mut editor.value.font_size).range(11.0..=20.0).speed(0.5).suffix(lang.text(" pt", " пт"))); ui.end_row();
                                ui.label(lang.text("Display timestamps", "Время сообщений")); ui.horizontal(|ui| { ui.selectable_value(&mut editor.value.utc,true,"UTC"); ui.selectable_value(&mut editor.value.utc,false,lang.text("Local time", "Местное время")); }); ui.end_row();
                            });
                            ui.add_space(18.); section(ui, lang.text("Reading defaults", "Параметры чтения по умолчанию"), self.dark());
                            egui::Grid::new("settings-reading").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label(lang.text("Maximum records per read", "Максимум сообщений за одно чтение")); ui.add(egui::DragValue::new(&mut editor.value.max_records).range(1..=1_000_000).speed(100.)); ui.end_row();
                                ui.label(lang.text("Active reading time", "Время активного чтения")); ui.add(egui::DragValue::new(&mut editor.value.seconds).range(1..=86_400).suffix(lang.text(" seconds", " с"))); ui.end_row();
                                ui.label(lang.text("Recent offset window", "Диапазон последних смещений")); ui.add(egui::DragValue::new(&mut editor.value.recent_window).range(1..=1_000_000).speed(10.)); ui.end_row();
                            });
                            ui.add_space(18.); section(ui, lang.text("Retained messages · all open tabs", "Хранение сообщений на всех открытых вкладках"), self.dark());
                            egui::Grid::new("settings-retention").num_columns(2).spacing([28., 12.]).show(ui, |ui| {
                                ui.label(lang.text("Maximum retained records", "Максимум сообщений в памяти")); ui.add(egui::DragValue::new(&mut editor.value.retained_records).range(100..=50_000).speed(100.)); ui.end_row();
                                ui.label(lang.text("Maximum raw payload bytes", "Предельный объём исходных данных")); ui.add(egui::DragValue::new(&mut editor.value.retained_mib).range(1..=512).suffix(lang.text(" MiB", " МиБ"))); ui.end_row();
                            });
                            ui.add_space(8.); ui.small(lang.text("Lower limits trim cached data immediately, starting with older inactive tabs. Message bodies are never saved in the workspace.", "При снижении лимитов данные сразу удаляются из памяти, начиная с давно неактивных вкладок. Содержимое сообщений не сохраняется в рабочем пространстве."));
                            ui.add_space(10.); ui.small(lang.text("Profiles, favorites, tabs and these preferences are saved locally. Restoring them never connects to Kafka.", "Профили, избранное, вкладки и настройки сохраняются на этом компьютере. При их восстановлении подключение к Kafka не открывается."));
                        });
                    });
                    self.capture_dialog_if_needed(ui.ctx(), "settings"); action
                });
            match action {
                Action::Save => match self.apply_settings(editor.value.clone(), ctx) {
                    Ok(()) => {}
                    Err(e) => {
                        editor.error = lang.message(&e);
                        self.settings_editor = Some(editor);
                    }
                },
                Action::Cancel => {}
                Action::None => self.settings_editor = Some(editor),
            }
        }
        if let Some(mut editor) = self.topic_editor.take() {
            let action = ctx.show_viewport_immediate(egui::ViewportId::from_hash_of("add-topic"), egui::ViewportBuilder::default().with_title(lang.text("Add topic · Kafka Reader", "Добавить топик · Kafka Reader")).with_inner_size([550.,280.]).with_min_inner_size([480.,260.]).with_position([220.,160.]), |ui,_| {
                let mut action = if ui.input(|i|i.viewport().close_requested()) { Action::Cancel } else { Action::None };
                egui::CentralPanel::default().frame(egui::Frame::new().fill(panel(self.dark())).inner_margin(22)).show(ui, |ui| {
                    ui.heading(lang.text("Register a topic", "Добавить топик")); ui.label(lang.text("Enter the exact name you have access to. No cluster-wide listing is requested.", "Введите точное имя доступного вам топика. Приложение не запрашивает список топиков кластера.")); ui.add_space(12.);
                    field(ui,lang,lang.text("Topic name", "Имя топика"),&mut editor.name,"orders.events",false); ui.label(RichText::new(&editor.error).small());
                    ui.horizontal(|ui| { if ui.button(lang.text("Add and open", "Добавить и открыть")).clicked(){action=Action::Save;} if ui.button(lang.text("Cancel", "Отмена")).clicked(){action=Action::Cancel;} });
                }); action
            });
            if action == Action::Save {
                let result = (|| {
                    if self.running() {
                        return Err(lang
                            .text(
                                "Stop reading before adding a topic",
                                "Остановите чтение перед добавлением топика",
                            )
                            .into());
                    }
                    self.workspace
                        .server_mut(editor.server)
                        .ok_or(lang.text("Server is unavailable", "Сервер недоступен"))?
                        .register_topic(editor.name.trim())?;
                    self.activate(Tab {
                        server: editor.server,
                        topic: editor.name.trim().into(),
                    })
                })();
                if let Err(e) = result {
                    editor.error = lang.message(&e);
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
                    .with_title(lang.text("Export records · Kafka Reader", "Экспорт сообщений · Kafka Reader"))
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
                            ui.heading(lang.text("Export visible messages", "Экспорт видимых сообщений"));
                            ui.label(lang.text("JSONL preserves exact bytes, nulls and ordered headers.", "В JSONL сохраняются исходные байты, значения null и порядок заголовков."));
                            ui.add_space(12.);
                            field(
                                ui,
                                lang,
                                lang.text("New output filename", "Имя нового файла"),
                                &mut editor.path,
                                "records.jsonl",
                                false,
                            );
                            ui.horizontal(|ui| {
                                if ui.button(lang.text("Export JSONL", "Экспорт в JSONL")).clicked() {
                                    action = Action::Save;
                                }
                                if ui.button(lang.text("Cancel", "Отмена")).clicked() {
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
fn field(
    ui: &mut egui::Ui,
    lang: Language,
    label: &str,
    value: &mut String,
    hint: &str,
    password: bool,
) {
    paste_edit(
        ui,
        lang,
        label,
        egui::TextEdit::singleline(value)
            .hint_text(hint)
            .password(password),
        clipboard_text,
    );
    ui.add_space(11.);
}

fn clipboard_text() -> Option<String> {
    arboard::Clipboard::new().ok()?.get_text().ok()
}

fn paste_edit(
    ui: &mut egui::Ui,
    lang: Language,
    label: &str,
    edit: egui::TextEdit<'_>,
    read_clipboard: impl FnOnce() -> Option<String>,
) -> egui::Response {
    let field_id = ui.make_persistent_id(("text-field", label));
    let error_id = field_id.with("paste-error");
    let (label_id, button) = ui
        .horizontal(|ui| {
            let label_id = ui.label(label).id;
            let help = format!("{} · {label}", lang.text("Paste", "Вставить"));
            let button = ui
                .add(egui::Button::new("").small().min_size(egui::vec2(26., 16.)))
                .on_hover_text(&help);
            let center = button.rect.center();
            let ink = ui.visuals().text_color();
            let board =
                egui::Rect::from_center_size(center + egui::vec2(0., 1.), egui::vec2(10., 11.));
            ui.painter()
                .rect_stroke(board, 1., (1., ink), egui::StrokeKind::Inside);
            let clip =
                egui::Rect::from_center_size(center + egui::vec2(0., -4.5), egui::vec2(6., 3.));
            ui.painter().rect_filled(clip, 1., ink);
            ui.painter()
                .hline(center.x - 2.5..=center.x + 2.5, center.y + 1., (1., ink));
            button.widget_info(|| {
                egui::WidgetInfo::labeled(egui::Role::Button, button.enabled(), &help)
            });
            (label_id, button)
        })
        .inner;
    let focus_policy = (button.is_pointer_button_down_on() || button.clicked()).then(|| {
        ui.memory_mut(|mem| mem.request_focus(field_id));
        // The adjacent icon acts as part of the field. Otherwise TextEdit
        // surrenders focus to this same click before handling the paste event.
        ui.ctx().options_mut(|options| {
            std::mem::replace(
                &mut options.input_options.surrender_focus_on,
                egui::SurrenderFocusOn::Never,
            )
        })
    });
    // Immediate child windows in eframe/glow 0.37 do not drain RequestPaste.
    // Deliver the click to this editor using the same paste event as Ctrl+V.
    let previous_events = if button.clicked() {
        let text = read_clipboard().filter(|s| !s.is_empty());
        ui.data_mut(|data| data.insert_temp(error_id, text.is_none()));
        text.map(|s| {
            ui.input_mut(|input| {
                std::mem::replace(
                    &mut input.events,
                    vec![egui::Event::Paste(s.replace("\r\n", "\n"))],
                )
            })
        })
    } else {
        None
    };
    let response = ui
        .add(edit.id(field_id).desired_width(f32::INFINITY))
        .labelled_by(label_id);
    if let Some(policy) = focus_policy {
        ui.ctx()
            .options_mut(|options| options.input_options.surrender_focus_on = policy);
    }
    if let Some(events) = previous_events {
        ui.input_mut(|input| input.events = events);
    }
    if response.changed() {
        ui.data_mut(|data| data.remove::<bool>(error_id));
    } else if ui.data(|data| data.get_temp::<bool>(error_id).unwrap_or(false)) {
        ui.small(lang.text(
            "Clipboard text is unavailable",
            "Текст из буфера обмена недоступен",
        ));
    }
    button
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
            Job::Exported(ref output, 1, Ok(())) if output == &path
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
        editor.export(Language::English);
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
    fn language_draft_applies_only_on_save_and_restores_the_preference() {
        let mut app = test_app();
        let ctx = egui::Context::default();
        let mut draft = SettingsEditor::new(&app.workspace.settings);
        draft.value.language = LanguagePreference::Russian;
        assert_eq!(app.workspace.settings.language, LanguagePreference::System);
        app.apply_settings(draft.value, &ctx).unwrap();
        assert_eq!(app.language(), Language::Russian);
        let reopened = app.options.store.load().unwrap();
        assert_eq!(reopened.settings.language, LanguagePreference::Russian);

        let mut cancelled = SettingsEditor::new(&app.workspace.settings);
        cancelled.value.language = LanguagePreference::English;
        drop(cancelled);
        assert_eq!(app.language(), Language::Russian);
        let mut system = SettingsEditor::new(&app.workspace.settings);
        system.value.language = LanguagePreference::System;
        app.apply_settings(system.value, &ctx).unwrap();
        assert_eq!(
            app.options.store.load().unwrap().settings.language,
            LanguagePreference::System
        );
    }

    #[test]
    fn changing_language_refreshes_active_and_cached_metadata_previews() {
        let mut app = test_app();
        let ctx = egui::Context::default();
        let mut settings = app.workspace.settings.clone();
        settings.language = LanguagePreference::English;
        app.apply_settings(settings.clone(), &ctx).unwrap();
        app.open_server(None);
        let mut editor = app.server_editor.take().unwrap();
        editor.name = "Preview test".into();
        editor.brokers = "localhost:9092".into();
        editor.test_topic = "first".into();
        app.save_server(&editor).unwrap();
        let first = app.active().unwrap().clone();
        let select_metadata = |app: &mut ReaderApp| {
            let record = Arc::new(demo_record(0, 7, &app.active().unwrap().topic, 0));
            app.view.buffer.add(record.clone());
            app.view.inspector_tab = 3;
            app.select(record.clone());
            record
        };
        let first_record = select_metadata(&mut app);
        app.workspace
            .server_mut(first.server)
            .unwrap()
            .register_topic("second")
            .unwrap();
        app.activate(Tab {
            server: first.server,
            topic: "second".into(),
        })
        .unwrap();
        let second_record = select_metadata(&mut app);

        // Both English jobs may still be in flight when the Russian previews are queued.
        settings.language = LanguagePreference::Russian;
        app.apply_settings(settings, &ctx).unwrap();
        // Deliver the older results last, including the one for the inactive tab.
        let mut jobs = Vec::new();
        for _ in 0..4 {
            jobs.push(app.jobs_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        jobs.sort_by_key(|job| match job {
            Job::Formatted(_, generation, _) => std::cmp::Reverse(*generation),
            Job::Exported(..) => panic!("Unexpected export"),
        });
        for job in jobs {
            app.jobs_tx.send(job).unwrap();
        }
        app.drain();
        for (view, record) in [
            (&app.view, &second_record),
            (&app.cached[&first], &first_record),
        ] {
            assert!(!view.pending_preview);
            assert!(view.formatted.contains("Смещение"));
            assert!(Arc::ptr_eq(view.selected.as_ref().unwrap(), record));
        }
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
        let (server, secrets) = editor.build(Language::English).unwrap();
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
    fn paste_button_replaces_a_unicode_selection_in_its_masked_field() {
        use egui::text::{CCursor, CCursorRange};
        let ctx = egui::Context::default();
        let mut password = "до☃после".to_string();
        let mut untouched = "other".to_string();
        let mut reads = 0;
        let mut frame = |events| {
            let mut target = None;
            let _ = ctx.run_ui(
                egui::RawInput {
                    events,
                    ..Default::default()
                },
                |ui| {
                    egui::CentralPanel::default().show(ui, |ui| {
                        let button = paste_edit(
                            ui,
                            Language::Russian,
                            "Пароль",
                            egui::TextEdit::singleline(&mut password).password(true),
                            || {
                                reads += 1;
                                Some("🦀ключ".into())
                            },
                        );
                        target = Some((
                            ui.make_persistent_id(("text-field", "Пароль")),
                            button.rect.center(),
                        ));
                        paste_edit(
                            ui,
                            Language::Russian,
                            "Другой",
                            egui::TextEdit::singleline(&mut untouched),
                            || panic!("unselected field read the clipboard"),
                        );
                    });
                },
            );
            target.unwrap()
        };
        let (field, pos) = frame(vec![]);
        ctx.memory_mut(|mem| mem.request_focus(field));
        let mut state = egui::TextEdit::load_state(&ctx, field).unwrap();
        state
            .cursor
            .set_char_range(Some(CCursorRange::two(CCursor::new(2), CCursor::new(3))));
        egui::TextEdit::store_state(&ctx, field, state);
        let mouse = |pressed| egui::Event::PointerButton {
            pos,
            button: egui::PointerButton::Primary,
            pressed,
            modifiers: egui::Modifiers::NONE,
        };
        frame(vec![egui::Event::PointerMoved(pos), mouse(true)]);
        frame(vec![mouse(false)]);
        assert_eq!(reads, 1);
        assert_eq!(password, "до🦀ключпосле");
        assert_eq!(untouched, "other");
        assert!(ctx.memory(|mem| mem.has_focus(field)));
    }

    #[test]
    fn refresh_token_profile_roundtrips_with_only_its_own_credentials() {
        let mut app = test_app();
        app.open_server(None);
        let mut editor = app.server_editor.take().unwrap();
        editor.name = "OAuth".into();
        editor.brokers = "kafka.example.org:9093".into();
        editor.protocol = "SASL_SSL".into();
        editor.mechanism = "OAUTHBEARER".into();
        editor.oauth_endpoint = "https://sso.example.org/token".into();
        editor.refresh_token = "REFRESH_SENTINEL".into();
        editor.username = "LOGIN_SENTINEL".into();
        editor.password = "PASSWORD_SENTINEL".into();
        editor.advanced = "sasl.username=inactive\nsasl.password=inactive".into();
        app.save_server(&editor).unwrap();
        let server = app.workspace.servers.last().unwrap();
        let id = server.id;
        let props = session_properties(server, &app.credentials);
        assert!(app.oauth_sessions[&id].matches(&props));
        assert_eq!(
            server.properties["oauth.token.endpoint.uri"],
            editor.oauth_endpoint
        );
        assert_eq!(server.properties["oauth.client.id"], "kafka-cli");
        assert_eq!(server.properties["oauth.max.token.expiry.seconds"], "300");
        assert_eq!(
            app.credentials[&id],
            Properties::from([("oauth.refresh.token".into(), "REFRESH_SENTINEL".into())])
        );
        let on_disk = fs::read_to_string(app.options.store.path()).unwrap();
        assert!(!on_disk.contains("SENTINEL"));
        app.open_server(Some(id));
        let mut reopened = app.server_editor.take().unwrap();
        assert!(reopened.oauth_session.matches(&props));
        reopened.refresh_token = "different-account".into();
        app.save_server(&reopened).unwrap();
        let changed_props = session_properties(app.workspace.server(id).unwrap(), &app.credentials);
        assert!(app.oauth_sessions[&id].matches(&changed_props));
        assert!(!app.oauth_sessions[&id].matches(&props));
        app.open_server(Some(id));
        let mut reopened = app.server_editor.take().unwrap();
        assert_eq!(reopened.refresh_token, "different-account");
        assert!(reopened.advanced.is_empty());
        reopened.refresh_token.clear();
        assert!(
            reopened.build(Language::English).is_ok(),
            "saving a profile allows entering the token later"
        );
        reopened.refresh_token = "INACTIVE_TOKEN".into();
        reopened.mechanism = "SCRAM-SHA-256".into();
        reopened.username = "reader".into();
        reopened.password = "password".into();
        reopened.advanced = "oauth.refresh.token=inactive".into();
        let (scram, secrets) = reopened.build(Language::English).unwrap();
        assert_eq!(secrets["sasl.username"], "reader");
        assert_eq!(secrets["sasl.password"], "password");
        assert!(
            !scram
                .properties
                .keys()
                .chain(secrets.keys())
                .any(|key| key.starts_with("oauth."))
        );
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
        let (changed, _) = editor.build(Language::English).unwrap();
        assert_ne!(changed.name, original.name);
        assert_eq!(original.name, "Original");
        assert!(original.properties.is_empty());
    }
}
