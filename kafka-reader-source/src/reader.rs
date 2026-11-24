use crate::direct_consumer::{DirectConsumer, Poll, metadata_guard_available};
use crate::model::*;
pub use crate::oauth_context::OAuthSession;
use crate::oauth_context::ReaderContext;
use crossbeam_channel::{Receiver, SendTimeoutError, Sender, TryRecvError, bounded};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub enum Event {
    Metadata(Vec<Partition>),
    Record(Arc<Record>),
    State(String, String),
    Progress(usize),
    Skipped(usize),
}

#[derive(Default)]
pub struct Control {
    pub stop: AtomicBool,
    pub pause: AtomicBool,
    pub queued_bytes: AtomicUsize,
    skipped: AtomicUsize,
}
pub struct Session {
    rx: Receiver<Event>,
    terminal: Receiver<Event>,
    worker: thread::JoinHandle<()>,
    pub control: Arc<Control>,
}
impl Session {
    pub fn start(request: Request) -> Self {
        Self::start_with_oauth(request, &mut OAuthSession::default())
    }
    pub fn start_with_oauth(request: Request, credentials: &mut OAuthSession) -> Self {
        credentials.bind(&request.props);
        Self::start_with_capacity(request, 256, credentials.clone())
    }
    fn start_with_capacity(request: Request, capacity: usize, credentials: OAuthSession) -> Self {
        let (tx, rx) = bounded(capacity);
        // The outcome must survive backpressure on the record queue.
        let (finished_tx, terminal) = bounded(1);
        let control = Arc::new(Control::default());
        let ctl = control.clone();
        let worker = thread::Builder::new()
            .name("kafka-rust-reader".into())
            .spawn(move || {
                let result = if request.demo {
                    demo(&request, &tx, &ctl)
                } else {
                    real(&request, &tx, &ctl, credentials)
                };
                let state = if ctl.stop.load(Ordering::Acquire) {
                    (
                        "STOPPED".into(),
                        "Stopped. No offsets were committed.".into(),
                    )
                } else {
                    match result {
                        Ok(detail) => ("COMPLETED".into(), detail),
                        Err(e) => ("FAILED".into(), redact(&e, &request.props)),
                    }
                };
                let _ = finished_tx.send(Event::State(state.0, state.1));
            })
            .expect("Cannot create Kafka worker");
        Self {
            rx,
            terminal,
            worker,
            control,
        }
    }
    pub fn try_next(&self) -> Option<Event> {
        match self.rx.try_recv() {
            Ok(event) => {
                if let Event::Record(r) = &event {
                    self.control
                        .queued_bytes
                        .fetch_sub(r.bytes(), Ordering::AcqRel);
                }
                Some(event)
            }
            // Disconnection guarantees that all data and progress events have
            // been drained before callers see the terminal state.
            Err(status) => {
                let count = self.control.skipped.swap(0, Ordering::AcqRel);
                if count > 0 {
                    Some(Event::Skipped(count))
                } else if status == TryRecvError::Disconnected {
                    self.terminal.try_recv().ok()
                } else {
                    None
                }
            }
        }
    }
    pub fn stop(&self) {
        self.control.stop.store(true, Ordering::Release);
    }
    pub fn paused(&self) -> bool {
        self.control.pause.load(Ordering::Acquire)
    }
    pub fn toggle_pause(&self) {
        self.control.pause.fetch_xor(true, Ordering::AcqRel);
    }
    pub fn running(&self) -> bool {
        !self.worker.is_finished()
    }
    pub fn active(&self) -> bool {
        self.running()
            || !self.rx.is_empty()
            || !self.terminal.is_empty()
            || self.control.skipped.load(Ordering::Acquire) != 0
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.stop();
    }
}

fn emit(tx: &Sender<Event>, ctl: &Control, mut event: Event) -> bool {
    let bytes = if let Event::Record(r) = &event {
        r.bytes()
    } else {
        0
    };
    if bytes > PENDING_BYTES {
        // Counting outside the queue also preserves omissions when Stop arrives
        // while the UI or a CLI export is still draining a full queue.
        ctl.skipped.fetch_add(1, Ordering::Release);
        return true;
    }
    if bytes > 0 {
        loop {
            if ctl.stop.load(Ordering::Acquire) {
                return false;
            }
            let used = ctl.queued_bytes.load(Ordering::Acquire);
            if used + bytes <= PENDING_BYTES
                && ctl
                    .queued_bytes
                    .compare_exchange(used, used + bytes, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    loop {
        match tx.send_timeout(event, Duration::from_millis(50)) {
            Ok(()) => return true,
            Err(SendTimeoutError::Disconnected(_)) => {
                if bytes > 0 {
                    ctl.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                }
                return false;
            }
            Err(SendTimeoutError::Timeout(e)) => {
                event = e;
                if ctl.stop.load(Ordering::Acquire) {
                    if bytes > 0 {
                        ctl.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                    }
                    return false;
                }
            }
        }
    }
}
fn state(tx: &Sender<Event>, ctl: &Control, name: &str, detail: &str) {
    emit(tx, ctl, Event::State(name.into(), detail.into()));
}

pub fn safe_properties(source: &Properties) -> Properties {
    let mut p = Properties::new();
    for (k, v) in source {
        if !k.starts_with("reader.")
            && !k.starts_with("oauth.")
            && !matches!(
                k.as_str(),
                "group.id"
                    | "group.instance.id"
                    | "interceptor.classes"
                    | "plugin.library.paths"
                    | "sasl.kerberos.kinit.cmd"
                    | "metric.reporters"
                    | "key.deserializer"
                    | "value.deserializer"
                    | "sasl.jaas.config"
            )
        {
            p.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in [
        ("enable.auto.commit", "false".into()),
        ("auto.commit.enable", "false".into()),
        ("offset.store.method", "broker".into()),
        ("enable.auto.offset.store", "false".into()),
        ("allow.auto.create.topics", "false".into()),
        // An unverified version must never imply support for Metadata v4.
        ("api.version.request", "true".into()),
        ("broker.version.fallback", "0.10.0".into()),
        ("enable.partition.eof", "true".into()),
        ("auto.offset.reset", "error".into()),
        ("socket.timeout.ms", "5000".into()),
        ("socket.connection.setup.timeout.ms", "5000".into()),
        ("queued.max.messages.kbytes", "8192".into()),
        ("queued.min.messages", "100".into()),
        ("fetch.max.bytes", "8388608".into()),
        ("fetch.message.max.bytes", "2097152".into()),
        ("enable.metrics.push", "false".into()),
    ] {
        p.insert(k.into(), v);
    }
    p.entry("isolation.level".into())
        .or_insert_with(|| "read_committed".into());
    p.entry("client.id".into())
        .or_insert_with(|| "kafka-reader-rust".into());
    p.entry("enable.ssl.certificate.verification".into())
        .or_insert_with(|| "true".into());
    p.entry("ssl.endpoint.identification.algorithm".into())
        .or_insert_with(|| "https".into());
    if p.get("security.protocol")
        .is_some_and(|v| v.to_ascii_uppercase().starts_with("SASL"))
    {
        let mechanism = p
            .remove("sasl.mechanism")
            .or_else(|| p.get("sasl.mechanisms").cloned())
            .unwrap_or_else(|| "PLAIN".into());
        if mechanism.eq_ignore_ascii_case("OAUTHBEARER") {
            // The application supplies access tokens via the native callback.
            p.retain(|k, _| {
                !k.starts_with("sasl.oauthbearer.")
                    && !matches!(
                        k.as_str(),
                        "enable.sasl.oauthbearer.unsecure.jwt"
                            | "enable_sasl_queue"
                            | "sasl.username"
                            | "sasl.password"
                    )
            });
        }
        p.insert("sasl.mechanisms".into(), mechanism.to_ascii_uppercase());
    }
    p
}

fn timeout(deadline: Instant) -> Result<Duration, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err("Topic inspection exceeded 20 seconds. Check broker addresses, network and security settings.".into())
    } else {
        Ok(remaining.min(Duration::from_secs(5)))
    }
}

fn real(
    req: &Request,
    tx: &Sender<Event>,
    ctl: &Control,
    credentials: OAuthSession,
) -> Result<String, String> {
    if !metadata_guard_available() {
        return Err("This build lacks the Kafka Metadata v4 safety guard; use the Linux or Windows GNU build".into());
    }
    if req.topic.trim().is_empty() {
        return Err("Enter the exact topic name".into());
    }
    if req
        .props
        .get("bootstrap.servers")
        .is_none_or(|v| v.trim().is_empty())
    {
        return Err("Enter bootstrap.servers as host:port".into());
    }
    if req.props.contains_key("sasl.jaas.config") {
        return Err("Choose an authentication method in the server editor or use native profile properties. Java sasl.jaas.config is not supported.".into());
    }
    let p = safe_properties(&req.props);
    if p.get("security.protocol")
        .is_some_and(|v| v.to_ascii_uppercase().starts_with("SASL"))
        && !matches!(
            p.get("sasl.mechanisms").map(String::as_str),
            Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "OAUTHBEARER")
        )
    {
        return Err(
            "Supported SASL mechanisms are PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER"
                .into(),
        );
    }
    state(tx, ctl, "CONNECTING", "Inspecting only the supplied topic…");
    let mut config = ClientConfig::new();
    for (k, v) in &p {
        config.set(k, v);
    }
    let context = ReaderContext::new(credentials, &req.props)?;
    let consumer: BaseConsumer<ReaderContext> = config
        .create_with_context(context)
        .map_err(|e| format!("Invalid Kafka configuration: {e}"))?;
    ReaderContext::with_token_refresh(&consumer, || read_topic(req, tx, ctl, &consumer))
}

fn read_topic(
    req: &Request,
    tx: &Sender<Event>,
    ctl: &Control,
    consumer: &BaseConsumer<ReaderContext>,
) -> Result<String, String> {
    if ctl.stop.load(Ordering::Acquire) {
        return Ok("Stopped".into());
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    let metadata = consumer
        .fetch_metadata(Some(&req.topic), timeout(deadline)?)
        .map_err(|e| match e {
            KafkaError::MetadataFetch(RDKafkaErrorCode::UnsupportedFeature) =>
                "Kafka Reader requires Metadata v4 or newer (Kafka 0.11+) to prevent topic auto-creation".to_string(),
            other => format!("Topic metadata failed: {other}"),
        })?;
    if ctl.stop.load(Ordering::Acquire) {
        return Ok("Stopped".into());
    }
    let topic = metadata
        .topics()
        .iter()
        .find(|t| t.name() == req.topic)
        .ok_or("Topic is absent from the metadata response")?;
    if let Some(error) = topic.error() {
        return Err(format!("Topic access failed: {error:?}"));
    }
    let available: BTreeSet<i32> = topic.partitions().iter().map(|p| p.id()).collect();
    let selected: Vec<i32> = if req.partitions.is_empty() {
        available.iter().copied().collect()
    } else {
        req.partitions
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    if selected.is_empty() || selected.iter().any(|p| !available.contains(p)) {
        return Err("A selected partition does not exist in this topic".into());
    }
    let mut bounds = Vec::new();
    for partition in selected {
        if ctl.stop.load(Ordering::Acquire) {
            return Ok("Stopped".into());
        }
        // Modern librdkafka ListOffsets honors isolation.level: read_committed => LSO.
        let (begin, end) = consumer
            .fetch_watermarks(&req.topic, partition, timeout(deadline)?)
            .map_err(|e| format!("Offset inspection failed for partition {partition}: {e}"))?;
        if begin < 0 || end < begin {
            return Err("Broker returned inconsistent partition bounds".into());
        }
        bounds.push(Partition {
            partition,
            beginning: begin,
            end,
        });
    }
    emit(tx, ctl, Event::Metadata(bounds.clone()));
    if req.inspect {
        return Ok("Topic access verified".into());
    }
    let mut time_offsets = BTreeMap::new();
    if req.mode == Mode::Timestamp {
        let mut times = TopicPartitionList::new();
        for p in &bounds {
            times
                .add_partition_offset(&req.topic, p.partition, Offset::Offset(req.value))
                .map_err(|e| e.to_string())?;
        }
        let found = consumer
            .offsets_for_times(times, timeout(deadline)?)
            .map_err(|e| format!("Timestamp lookup failed: {e}"))?;
        time_offsets = timestamp_offsets(&found)?;
    }
    let mut assignment = TopicPartitionList::new();
    let mut positions = BTreeMap::new();
    let mut ends = BTreeMap::new();
    let mut finished = BTreeSet::new();
    for p in &bounds {
        let start = start_offset(
            req.mode,
            req.value,
            p.beginning,
            p.end,
            time_offsets.get(&p.partition).copied(),
        )?;
        assignment
            .add_partition_offset(&req.topic, p.partition, Offset::Offset(start))
            .map_err(|e| e.to_string())?;
        positions.insert(p.partition, start);
        ends.insert(p.partition, p.end);
        if !req.follow && start >= p.end {
            finished.insert(p.partition);
        }
    }
    // A high-level assignment with group.id triggers coordinator requests. The
    // low-level partition queue is deliberately group-free and uses absolute starts.
    let direct = DirectConsumer::new(consumer, &req.topic, &positions)?;
    state(
        tx,
        ctl,
        "READING",
        if req.follow {
            "Following direct partition assignments"
        } else {
            "Reading to the captured exclusive ends"
        },
    );
    let mut fetched = 0usize;
    let mut active = Duration::ZERO;
    let mut previous = Instant::now();
    let mut last_progress = Instant::now();
    let mut was_paused = false;
    let mut pause_finished = TopicPartitionList::new();
    for p in &finished {
        pause_finished.add_partition(&req.topic, *p);
    }
    if pause_finished.count() > 0 {
        consumer.pause(&pause_finished).map_err(|e| e.to_string())?;
    }
    loop {
        if ctl.stop.load(Ordering::Acquire) {
            break;
        }
        if consumer.context().uses_oauth() {
            consumer.context().check_refresh()?;
        } else {
            // The direct partition queue owns data; this serves main-queue events.
            consumer.poll(Duration::ZERO);
        }
        if !req.follow && finished.len() == bounds.len() {
            break;
        }
        if req.max_records > 0 && fetched >= req.max_records {
            return Ok(format!("Record limit reached · {fetched} fetched"));
        }
        let now = Instant::now();
        let delta = now - previous;
        previous = now;
        if ctl.pause.load(Ordering::Acquire) {
            if !was_paused {
                consumer.pause(&assignment).map_err(|e| e.to_string())?;
                state(
                    tx,
                    ctl,
                    "PAUSED",
                    "Paused; retained messages remain available",
                );
                was_paused = true;
            }
            last_progress += delta;
            thread::sleep(Duration::from_millis(40));
            continue;
        }
        if was_paused {
            let mut resume = TopicPartitionList::new();
            for p in ends.keys().filter(|p| !finished.contains(p)) {
                resume.add_partition(&req.topic, *p);
            }
            consumer.resume(&resume).map_err(|e| e.to_string())?;
            state(tx, ctl, "READING", "Resumed");
            was_paused = false;
        }
        active += delta;
        if req.seconds > 0 && active >= Duration::from_secs(req.seconds) {
            return Ok(format!("Time limit reached · {fetched} fetched"));
        }
        match direct.poll()? {
            Poll::Record(record) => {
                let p = record.partition;
                let offset = record.offset;
                positions.insert(p, offset + 1);
                last_progress = Instant::now();
                if req.follow || offset < *ends.get(&p).unwrap_or(&0) {
                    fetched += 1;
                    if !emit(tx, ctl, Event::Record(Arc::new(record))) {
                        break;
                    }
                    emit(tx, ctl, Event::Progress(fetched));
                }
            }
            Poll::Eof(p, offset) => {
                positions.insert(p, offset);
                last_progress = Instant::now();
            }
            Poll::Empty => {}
        }
        if !req.follow {
            let mut newly = TopicPartitionList::new();
            for (&p, &end) in &ends {
                if positions.get(&p).copied().unwrap_or(0) >= end && finished.insert(p) {
                    newly.add_partition(&req.topic, p);
                }
            }
            if newly.count() > 0 {
                consumer.pause(&newly).map_err(|e| e.to_string())?;
            }
            if last_progress.elapsed() > Duration::from_secs(30) {
                return Err("Snapshot made no progress for 30 seconds; connection or retention may have changed".into());
            }
        }
    }
    Ok(format!("Snapshot complete · {fetched} fetched"))
}

fn timestamp_offsets(found: &TopicPartitionList) -> Result<BTreeMap<i32, i64>, String> {
    let mut offsets = BTreeMap::new();
    for e in found.elements() {
        e.error().map_err(|err| {
            format!(
                "Timestamp lookup failed for partition {}: {err}",
                e.partition()
            )
        })?;
        if let Offset::Offset(o) = e.offset() {
            if o >= 0 {
                offsets.insert(e.partition(), o);
            }
        }
    }
    Ok(offsets)
}

fn demo(req: &Request, tx: &Sender<Event>, ctl: &Control) -> Result<String, String> {
    let selected = if req.partitions.is_empty() {
        vec![0, 1, 2]
    } else {
        req.partitions.clone()
    };
    if selected.iter().any(|p| !(0..3).contains(p)) {
        return Err("Offline demo has partitions 0, 1 and 2".into());
    }
    let bounds: Vec<_> = selected
        .iter()
        .map(|p| Partition {
            partition: *p,
            beginning: 0,
            end: 300,
        })
        .collect();
    emit(tx, ctl, Event::Metadata(bounds));
    if req.inspect {
        return Ok("Offline synthetic demo".into());
    }
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
        - 300000;
    let timestamp_offset = if req.mode == Mode::Timestamp {
        Some((req.value.saturating_sub(epoch).saturating_add(999) / 1000).clamp(0, 300))
    } else {
        None
    };
    let start = start_offset(req.mode, req.value, 0, 300, timestamp_offset)?;
    let mut positions: BTreeMap<i32, i64> = selected.into_iter().map(|p| (p, start)).collect();
    let mut count = 0;
    let mut active = Duration::ZERO;
    let mut previous = Instant::now();
    let mut was_paused = false;
    state(
        tx,
        ctl,
        "READING",
        "Offline demo · synthetic messages · no broker connection",
    );
    loop {
        if ctl.stop.load(Ordering::Acquire) {
            break;
        }
        if !req.follow && positions.values().all(|p| *p >= 300) {
            break;
        }
        let now = Instant::now();
        let delta = now - previous;
        previous = now;
        if ctl.pause.load(Ordering::Acquire) {
            if !was_paused {
                state(tx, ctl, "PAUSED", "Offline demo paused");
                was_paused = true;
            }
            thread::sleep(Duration::from_millis(40));
            continue;
        }
        if was_paused {
            state(tx, ctl, "READING", "Offline demo resumed");
            was_paused = false;
        }
        active += delta;
        if req.seconds > 0 && active >= Duration::from_secs(req.seconds) {
            break;
        }
        for _ in 0..3 {
            for (p, offset) in positions.iter_mut() {
                if !req.follow && *offset >= 300 {
                    continue;
                }
                if req.max_records > 0 && count >= req.max_records {
                    return Ok(format!("Demo record limit · {count} synthetic records"));
                }
                let record = demo_record(*p, *offset, &req.topic, epoch);
                *offset += 1;
                count += 1;
                if !emit(tx, ctl, Event::Record(Arc::new(record))) {
                    return Ok("Stopped".into());
                }
            }
        }
        emit(tx, ctl, Event::Progress(count));
        thread::sleep(Duration::from_millis(60));
    }
    Ok(format!("Demo complete · {count} synthetic records"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> Request {
        Request {
            props: Properties::new(),
            topic: "test.events".into(),
            partitions: vec![0],
            mode: Mode::Earliest,
            value: 0,
            follow: false,
            max_records: 1,
            seconds: 5,
            demo: true,
            inspect: false,
        }
    }

    #[test]
    fn a_full_queue_preserves_failure_after_the_last_data_event() {
        // The dummy source emits its bounds, filling the single slot, and
        // then fails validation of the starting offset.
        let session = Session::start_with_capacity(
            Request {
                mode: Mode::Offset,
                value: 999,
                ..request()
            },
            1,
            OAuthSession::default(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while session.running() {
            assert!(
                Instant::now() < deadline,
                "Worker blocked on terminal delivery"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert!(session.active());
        assert!(matches!(session.try_next(), Some(Event::Metadata(_))));
        assert!(session.active());
        assert!(matches!(session.try_next(), Some(Event::State(state, _)) if state == "FAILED"));
        assert!(!session.active());
    }

    #[test]
    fn oversized_records_are_counted_even_if_the_queue_is_full() {
        let (tx, _rx) = bounded(1);
        tx.try_send(Event::Progress(1)).unwrap();
        let ctl = Control::default();
        let mut oversized = demo_record(0, 0, "test.events", 0);
        oversized.value = Some(vec![0; PENDING_BYTES + 1]);
        let record = Arc::new(oversized);
        for _ in 0..3 {
            assert!(emit(&tx, &ctl, Event::Record(record.clone())));
        }
        assert_eq!(ctl.skipped.load(Ordering::Acquire), 3);
        assert_eq!(ctl.queued_bytes.load(Ordering::Acquire), 0);
    }

    #[cfg(guarded_kafka_metadata)]
    #[test]
    fn legacy_metadata_broker_cannot_create_the_requested_topic() {
        use rdkafka::{mocking::MockCluster, types::RDKafkaApiKey};
        let cluster = MockCluster::new(1).unwrap();
        cluster
            .apiversion(RDKafkaApiKey::Metadata, Some(0), Some(3))
            .unwrap();
        let req = Request {
            demo: false,
            inspect: true,
            props: Properties::from([("bootstrap.servers".into(), cluster.bootstrap_servers())]),
            ..request()
        };
        let (tx, _rx) = bounded(4);
        let error = real(&req, &tx, &Control::default(), OAuthSession::default()).unwrap_err();
        assert!(error.contains("Metadata v4"), "{error}");
        // The mock auto-creates a named topic for Metadata v0-v3. Creation
        // succeeds here only if our inspection never reached it by name.
        cluster.create_topic(&req.topic, 1, 1).unwrap();
    }

    #[test]
    fn timestamp_lookup_errors_are_distinct_from_missing_timestamps() {
        let mut found = TopicPartitionList::new();
        found
            .add_partition_offset("test.events", 0, Offset::Invalid)
            .unwrap();
        found
            .add_partition_offset("test.events", 1, Offset::Offset(42))
            .unwrap();
        assert_eq!(
            timestamp_offsets(&found).unwrap(),
            BTreeMap::from([(1, 42)])
        );
        // SAFETY: our owned list has two initialized entries. This reproduces
        // librdkafka's result with an errored partition and its input offset.
        unsafe {
            (*(*found.ptr()).elems.add(1)).err =
                rdkafka::bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
        }
        assert!(
            timestamp_offsets(&found)
                .unwrap_err()
                .contains("partition 1")
        );
    }

    #[test]
    fn applied_sasl_settings_use_the_validated_mechanism() {
        let mut props = parse_properties("security.protocol=SASL_SSL").unwrap();
        assert_eq!(safe_properties(&props)["sasl.mechanisms"], "PLAIN");
        props.insert("sasl.mechanisms".into(), "scram-sha-256".into());
        assert_eq!(safe_properties(&props)["sasl.mechanisms"], "SCRAM-SHA-256");
        props.insert("sasl.mechanism".into(), "scram-sha-512".into());
        let applied = safe_properties(&props);
        assert_eq!(applied["sasl.mechanisms"], "SCRAM-SHA-512");
        assert!(!applied.contains_key("sasl.mechanism"));
    }

    #[test]
    fn refresh_credentials_never_enter_native_kafka_properties() {
        let props = parse_properties("security.protocol=SASL_SSL\nsasl.mechanism=oauthbearer\noauth.token.endpoint.uri=https://id.example/token\noauth.refresh.token=example\noauth.client.id=test\noauth.max.token.expiry.seconds=300\nsasl.username=old\nsasl.password=old\nsasl.oauthbearer.method=oidc\nenable.sasl.oauthbearer.unsecure.jwt=true").unwrap();
        let safe = safe_properties(&props);
        assert_eq!(safe["sasl.mechanisms"], "OAUTHBEARER");
        assert!(
            !safe
                .keys()
                .any(|k| k.starts_with("oauth.") || k.starts_with("sasl.oauthbearer."))
        );
        for k in [
            "sasl.username",
            "sasl.password",
            "enable.sasl.oauthbearer.unsecure.jwt",
            "group.id",
        ] {
            assert!(!safe.contains_key(k), "{k}");
        }
    }

    #[test]
    fn readonly_cannot_be_overridden() {
        let p=parse_properties("group.id=production\nenable.auto.commit=true\nenable.auto.offset.store=true\nallow.auto.create.topics=true\nplugin.library.paths=evil.so").unwrap();
        let s = safe_properties(&p);
        assert!(!s.contains_key("group.id"));
        for k in [
            "enable.auto.commit",
            "enable.auto.offset.store",
            "allow.auto.create.topics",
        ] {
            assert_eq!(s[k], "false");
        }
        assert!(!s.contains_key("plugin.library.paths"));
    }
    #[test]
    fn offline_read_honors_limit() {
        let req = Request {
            max_records: 7,
            ..request()
        };
        let s = Session::start(req);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut count = 0;
        while s.active() {
            if let Some(Event::Record(_)) = s.try_next() {
                count += 1;
            } else {
                thread::sleep(Duration::from_millis(5));
            }
            assert!(Instant::now() < deadline);
        }
        assert_eq!(count, 7);
    }
    #[test]
    fn stop_unblocks_a_full_channel() {
        let req = Request {
            props: Properties::new(),
            topic: "demo".into(),
            partitions: vec![],
            mode: Mode::Latest,
            value: 0,
            follow: true,
            max_records: 0,
            seconds: 30,
            demo: true,
            inspect: false,
        };
        let s = Session::start(req);
        thread::sleep(Duration::from_millis(100));
        s.stop();
        let until = Instant::now() + Duration::from_secs(2);
        while s.running() {
            thread::sleep(Duration::from_millis(10));
            assert!(Instant::now() < until);
        }
    }
}
