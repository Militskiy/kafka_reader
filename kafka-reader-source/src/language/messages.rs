use super::Language;

impl Language {
    /// Render shared reader/workspace diagnostics at the GUI boundary.
    /// Keep native error details, profile names, and values verbatim.
    pub fn message(self, message: &str) -> String {
        if self == Self::English {
            return message.into();
        }
        let translated = match message {
            "Font size must be between 11 and 20 points" => {
                "Размер шрифта должен быть от 11 до 20 пунктов"
            }
            "Use 1–1,000,000 records/window positions and 1–86,400 seconds" => {
                "Укажите от 1 до 1 000 000 сообщений или позиций в окне и от 1 до 86 400 секунд"
            }
            "Retention must be 100–50,000 records and 1–512 MiB across all topic tabs" => {
                "Лимит для всех вкладок — от 100 до 50 000 сообщений и от 1 до 512 МиБ"
            }
            "A server can register up to 500 exact topic names" => {
                "Для одного сервера можно добавить до 500 топиков"
            }
            "Stop the active reader before switching topics or servers" => {
                "Остановите чтение перед сменой топика или сервера"
            }
            "Register the exact topic name on this server first" => {
                "Сначала добавьте топик на этом сервере, указав его точное имя"
            }
            "Eight topic tabs are open. Close one before opening another" => {
                "Открыто восемь вкладок. Закройте одну, чтобы открыть другую"
            }
            "Stop the active reader before closing its topic tab" => {
                "Остановите чтение, прежде чем закрывать вкладку топика"
            }
            "This tab is already closed" => "Эта вкладка уже закрыта",
            "Unsupported workspace version" => "Версия рабочего пространства не поддерживается",
            "Workspace can have up to 50 server profiles" => {
                "В рабочем пространстве можно сохранить до 50 профилей серверов"
            }
            "Server names must be unique and contain 1–100 characters; IDs must be unique" => {
                "Имена серверов должны быть уникальными и содержать от 1 до 100 символов. Идентификаторы также должны быть уникальными"
            }
            "Too many registered topics" => "Добавлено слишком много топиков",
            "Duplicate registered topic" => "Топик уже добавлен",
            "Selected server is absent" => "Выбранный сервер отсутствует",
            "Too many open tabs" => "Открыто слишком много вкладок",
            "An open tab refers to an absent or duplicate topic" => {
                "В открытой вкладке указан отсутствующий или дублирующийся топик"
            }
            "Active tab is absent" => "Активная вкладка отсутствует",
            "Use an exact Kafka topic name: 1–249 letters, digits, dots, underscores or hyphens" => {
                "Укажите точное имя топика Kafka длиной от 1 до 249 символов. Допустимы латинские буквы, цифры, точки, подчёркивания и дефисы"
            }
            "Workspace exceeds 1 MiB" => "Размер файла рабочего пространства превышает 1 МиБ",
            "The workspace changed on disk. Reopen it before saving this session." => {
                "Рабочее пространство изменено на диске. Откройте его заново перед сохранением"
            }
            "Choose a new output filename; the file already exists" => {
                "Файл уже существует. Выберите другое имя"
            }
            "Properties file exceeds 1 MiB" => "Размер файла параметров превышает 1 МиБ",
            "Invalid offset bounds or negative starting value" => {
                "Недопустимые границы смещений или отрицательное начальное значение"
            }
            "Recent window must be positive" => "Размер окна должен быть больше нуля",
            "Inspecting only the supplied topic…" => "Проверяется указанный топик…",
            "Topic inspection exceeded 20 seconds. Check broker addresses, network and security settings." => {
                "Проверка топика заняла больше 20 секунд. Проверьте адреса брокеров, сеть и настройки безопасности"
            }
            "This build lacks the Kafka Metadata v4 safety guard; use the Linux or Windows GNU build" => {
                "В этой сборке отсутствует защита Kafka Metadata v4. Используйте сборку для Linux или Windows GNU"
            }
            "Enter the exact topic name" => "Введите точное имя топика",
            "Enter bootstrap.servers as host:port" => {
                "Укажите bootstrap.servers в формате хост:порт"
            }
            "Choose username/password or OAuth refresh token in Authentication. Java sasl.jaas.config is not supported." => {
                "На вкладке «Аутентификация» выберите вход по имени и паролю или по токену обновления OAuth. Параметр Java sasl.jaas.config не поддерживается"
            }
            "Supported SASL mechanisms are PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER" => {
                "Поддерживаются механизмы SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 и OAUTHBEARER"
            }
            "Refresh-token authentication requires SASL_SSL" => {
                "Для входа по токену обновления выберите SASL_SSL"
            }
            "Refresh-token authentication requires Kafka TLS certificate and hostname verification" => {
                "Для входа по токену обновления включите проверку сертификата TLS и имени узла Kafka"
            }
            "Enter a refresh token for this session"
            | "Enter an OAuth refresh token for this session" => {
                "Введите токен обновления OAuth для этого сеанса"
            }
            "Enter an HTTPS OAuth token endpoint without embedded credentials or a fragment" => {
                "Введите HTTPS-адрес для получения токена OAuth без учётных данных и фрагмента после символа #"
            }
            "Enter an OAuth client ID" => "Введите идентификатор клиента OAuth",
            "OAuth maximum token lifetime must be a positive number of seconds"
            | "OAuth maximum token lifetime must be positive" => {
                "Максимальный срок действия токена OAuth должен быть положительным целым числом секунд"
            }
            "Kafka client did not request an initial OAuth access token" => {
                "Клиент Kafka не запросил начальный токен доступа OAuth"
            }
            "Could not start Kafka token refresh worker" => {
                "Не удалось запустить обновление токена Kafka"
            }
            "Kafka token refresh worker stopped unexpectedly" => {
                "Обновление токена Kafka неожиданно остановилось"
            }
            "Kafka broker rejected OAuth authentication. Check the listener and token audience." => {
                "Брокер Kafka отклонил аутентификацию OAuth. Проверьте адрес и порт брокера и назначение токена (audience)"
            }
            "OAuth refresh is not configured" => "Обновление токена OAuth не настроено",
            "The remembered profile has no refresh token" => {
                "В сохранённых учётных данных нет токена обновления"
            }
            "Could not lock the remembered refresh token" => {
                "Не удалось получить исключительный доступ к сохранённому токену обновления"
            }
            "Another session is refreshing this saved token. Retry the connection." => {
                "Другой сеанс обновляет этот сохранённый токен. Повторите подключение."
            }
            "Token refresh is in progress. Try saving again when it finishes." => {
                "Токен обновляется. Повторите сохранение после завершения обновления."
            }
            "Choose SASL authentication to remember a credential" => {
                "Чтобы запомнить учётные данные, выберите аутентификацию SASL"
            }
            "Enter the Kafka username to remember the password" => {
                "Чтобы запомнить пароль, введите имя пользователя Kafka"
            }
            "Enter the password to remember" => "Введите пароль, который нужно запомнить",
            "Enter the refresh token to remember" => {
                "Введите токен обновления, который нужно запомнить"
            }
            "Remembering a refresh token requires SASL_SSL, a client ID and a saved HTTPS endpoint without a query" => {
                "Для запоминания токена нужны SASL_SSL, идентификатор клиента и сохранённый HTTPS-адрес без параметров запроса"
            }
            "Unsupported mechanism for a remembered credential" => {
                "Для этого механизма нельзя запомнить учётные данные"
            }
            "The remembered credential changed. Reopen the server before using it." => {
                "Сохранённые учётные данные изменились. Откройте сервер заново."
            }
            "Could not unlock the remembered credential for this Windows account and connection." => {
                "Не удалось расшифровать учётные данные для этой учётной записи Windows и этого подключения."
            }
            "Could not create remembered credential" => "Не удалось сохранить учётные данные",
            "Could not encode remembered credential" => {
                "Не удалось подготовить учётные данные к сохранению"
            }
            "Could not lock remembered credentials" => {
                "Не удалось получить исключительный доступ к сохранённым учётным данным"
            }
            "Could not remove the previous remembered credential" => {
                "Не удалось удалить ранее сохранённые учётные данные"
            }
            "Credential data exceeds the DPAPI limit" | "Credential is too large to remember" => {
                "Размер учётных данных превышает допустимый предел"
            }
            "Enter the credential again or forget it after changing the connection" => {
                "После изменения подключения введите учётные данные ещё раз или удалите сохранённые данные"
            }
            "Remembered credentials require Windows DPAPI" => {
                "Для запоминания учётных данных требуется Windows DPAPI"
            }
            "Windows could not allocate a credential reference" => {
                "Windows не удалось создать идентификатор сохранённых учётных данных"
            }
            "Windows could not protect the credential for this account" => {
                "Windows не удалось зашифровать учётные данные для этой учётной записи"
            }
            "Invalid remembered credential reference" => {
                "Некорректная ссылка на сохранённые учётные данные"
            }
            "OAuth token endpoint timed out" => "Сервер авторизации OAuth не ответил вовремя",
            "OAuth token endpoint TLS verification or handshake failed" => {
                "Не удалось установить защищённое TLS-соединение с сервером авторизации OAuth"
            }
            "Could not connect to the OAuth token endpoint" => {
                "Не удалось подключиться к серверу авторизации OAuth"
            }
            "Could not read OAuth token response within the size or time limit" => {
                "Ответ сервера авторизации OAuth слишком велик или не получен вовремя"
            }
            "Invalid OAuth token response" => "Некорректный ответ сервера авторизации OAuth",
            "Could not read the OAuth CA certificate file" => {
                "Не удалось прочитать файл сертификатов центра сертификации для OAuth"
            }
            "OAuth CA certificate file exceeds 8 MiB" => {
                "Файл сертификатов для OAuth превышает 8 МиБ"
            }
            "Invalid OAuth CA certificate file" => "Некорректный файл сертификатов для OAuth",
            "OAuth CA file contains no PEM certificates" => {
                "В файле центра сертификации для OAuth нет сертификатов PEM"
            }
            "System clock is before the Unix epoch" => {
                "Системные часы показывают дату до 1 января 1970 года"
            }
            "System clock is out of range" => "Системное время вне допустимого диапазона",
            "OAuth token response did not contain a usable bearer access token" => {
                "В ответе сервера авторизации OAuth отсутствует пригодный для входа токен доступа Bearer"
            }
            "OAuth token lifetime is out of range" => {
                "Срок действия токена OAuth вне допустимого диапазона"
            }
            "OAuth token endpoint returned an expired access token" => {
                "Сервер авторизации OAuth вернул просроченный токен доступа"
            }
            "OAuth token endpoint returned an invalid replacement refresh token" => {
                "Сервер авторизации OAuth вернул недействительный токен обновления взамен предыдущего"
            }
            "Kafka Reader requires Metadata v4 or newer (Kafka 0.11+) to prevent topic auto-creation" => {
                "Для защиты от автоматического создания топиков требуется Metadata v4 или новее (Kafka 0.11+)"
            }
            "Topic is absent from the metadata response" => {
                "Топик отсутствует в полученных метаданных"
            }
            "A selected partition does not exist in this topic" => {
                "В этом топике нет выбранного раздела"
            }
            "Broker returned inconsistent partition bounds" => {
                "Брокер вернул противоречивые границы раздела"
            }
            "Topic access verified" => "Доступ к топику подтверждён",
            "Following direct partition assignments" => {
                "Ожидание новых сообщений в выбранных разделах"
            }
            "Reading to the captured exclusive ends" => {
                "Чтение до зафиксированных конечных смещений"
            }
            "Paused; retained messages remain available" => {
                "Чтение приостановлено. Загруженные сообщения доступны"
            }
            "Resumed" => "Чтение возобновлено",
            "Stopped" => "Чтение остановлено",
            "Stopped. No offsets were committed." => {
                "Чтение остановлено. Смещения не зафиксированы"
            }
            "Snapshot made no progress for 30 seconds; connection or retention may have changed" => {
                "Чтение не продвигалось 30 секунд. Проверьте соединение и срок хранения сообщений"
            }
            "Topic name contains NUL" => "Имя топика содержит нулевой байт",
            "Unable to allocate native read queue" => "Не удалось создать очередь чтения",
            "Unable to allocate topic configuration" => "Не удалось создать конфигурацию топика",
            "Direct reads require an absolute, nonnegative offset" => {
                "Для прямого чтения требуется абсолютное неотрицательное смещение"
            }
            "Cannot decode a native Kafka header" => {
                "Не удалось декодировать заголовок сообщения Kafka"
            }
            "Offline demo has partitions 0, 1 and 2" => {
                "В тестовом топике доступны разделы 0, 1 и 2"
            }
            "Offline synthetic demo" => "Локальные тестовые данные",
            "Offline demo · synthetic messages · no broker connection" => {
                "Локальные тестовые сообщения без подключения к брокеру"
            }
            "Offline demo paused" => "Чтение тестовых сообщений приостановлено",
            "Offline demo resumed" => "Чтение тестовых сообщений возобновлено",
            _ => return diagnostic_with_values(message),
        };
        translated.into()
    }
}

fn diagnostic_with_values(message: &str) -> String {
    let reason = message.trim_start_matches("Could not save the rotated refresh token: ");
    if reason.len() < message.len() {
        return format!(
            "Не удалось сохранить новый токен обновления: {}",
            Language::Russian.message(reason)
        );
    }
    for (prefix, translated) in [
        (
            "OAuth token endpoint returned HTTP ",
            "Сервер авторизации OAuth вернул код HTTP ",
        ),
        (
            "Cannot load workspace: ",
            "Не удалось загрузить рабочее пространство: ",
        ),
        (
            "Could not lock the workspace for saving: ",
            "Не удалось заблокировать рабочее пространство для сохранения: ",
        ),
        (
            "Invalid Kafka configuration: ",
            "Ошибка конфигурации Kafka: ",
        ),
        (
            "Topic metadata failed: ",
            "Не удалось получить метаданные топика: ",
        ),
        ("Topic access failed: ", "Ошибка доступа к топику: "),
        (
            "Offset inspection failed for partition ",
            "Не удалось получить смещения раздела ",
        ),
        (
            "Timestamp lookup failed for partition ",
            "Ошибка поиска по времени в разделе ",
        ),
        ("Timestamp lookup failed: ", "Ошибка поиска по времени: "),
        (
            "Empty property name at line ",
            "Пустое имя параметра в строке ",
        ),
    ] {
        if let Some(detail) = message.strip_prefix(prefix) {
            return format!("{translated}{detail}");
        }
    }
    if let Some(name) = message.strip_suffix(" needs bootstrap servers") {
        return format!("Укажите адреса брокеров для сервера «{name}»");
    }
    if let Some(line) = message
        .strip_prefix("Properties line ")
        .and_then(|s| s.strip_suffix(" must be key=value"))
    {
        return format!("В строке {line} ожидается параметр в формате ключ=значение");
    }
    if let Some((offset, bounds)) = message
        .strip_prefix("Offset ")
        .and_then(|s| s.split_once(" is outside "))
    {
        return format!("Смещение {offset} выходит за границы {bounds}");
    }
    for (prefix, suffix, translated) in [
        (
            "Record limit reached · ",
            " fetched",
            "Достигнут лимит сообщений. Получено",
        ),
        (
            "Time limit reached · ",
            " fetched",
            "Истекло время чтения. Получено",
        ),
        (
            "Snapshot complete · ",
            " fetched",
            "Чтение снимка завершено. Получено",
        ),
        (
            "Demo record limit · ",
            " synthetic records",
            "Достигнут лимит тестовых сообщений. Получено",
        ),
        (
            "Demo complete · ",
            " synthetic records",
            "Чтение тестовых сообщений завершено. Получено",
        ),
    ] {
        if let Some(count) = message
            .strip_prefix(prefix)
            .and_then(|s| s.strip_suffix(suffix))
        {
            return format!("{translated}: {count}");
        }
    }
    message.into()
}

#[cfg(test)]
mod tests {
    use super::Language;

    #[test]
    fn localized_reader_diagnostics_preserve_native_details_and_identifiers() {
        let error = "Topic access failed: TopicAuthorizationFailed";
        assert_eq!(Language::English.message(error), error);
        assert_eq!(
            Language::Russian.message(error),
            "Ошибка доступа к топику: TopicAuthorizationFailed"
        );
        assert_eq!(
            Language::Russian.message("orders-01 needs bootstrap servers"),
            "Укажите адреса брокеров для сервера «orders-01»"
        );
        assert_eq!(
            Language::Russian.message("Record limit reached · 21 fetched"),
            "Достигнут лимит сообщений. Получено: 21"
        );
        assert_eq!(
            Language::Russian.message("OAuth token endpoint returned HTTP 401"),
            "Сервер авторизации OAuth вернул код HTTP 401"
        );
        assert_eq!(
            Language::Russian.message(
                "Could not save the rotated refresh token: Could not lock remembered credentials"
            ),
            "Не удалось сохранить новый токен обновления: Не удалось получить исключительный доступ к сохранённым учётным данным"
        );
        assert_eq!(
            Language::Russian.message("Native text 123"),
            "Native text 123"
        );
    }
}
