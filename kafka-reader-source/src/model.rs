use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

pub const RETAIN_COUNT: usize = 10_000;
pub const RETAIN_BYTES: usize = 64 * 1024 * 1024;
pub const PENDING_BYTES: usize = 16 * 1024 * 1024;
pub const PREVIEW_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub timestamp_type: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

impl Record {
    pub fn bytes(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len)
            + self.value.as_ref().map_or(0, Vec::len)
            + self
                .headers
                .iter()
                .map(|h| h.name.len() + h.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
    }
    pub fn json(&self) -> Value {
        fn b64(v: &Option<Vec<u8>>) -> Option<String> {
            v.as_ref().map(|v| STANDARD.encode(v))
        }
        fn utf8(v: &Option<Vec<u8>>) -> Option<&str> {
            v.as_ref().and_then(|v| std::str::from_utf8(v).ok())
        }
        let headers: Vec<_> = self
            .headers
            .iter()
            .map(|h| {
                json!({
                    "name": h.name,
                    "valueBase64": b64(&h.value),
                    "valueUtf8": utf8(&h.value)
                })
            })
            .collect();
        json!({
            "format": "kafka-reader-jsonl-v1",
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "timestampType": self.timestamp_type,
            "keyBase64": b64(&self.key),
            "keyUtf8": utf8(&self.key),
            "valueBase64": b64(&self.value),
            "valueUtf8": utf8(&self.value),
            "headers": headers
        })
    }
    pub fn preview(&self) -> String {
        preview(self.value.as_deref(), 180)
    }
}

pub fn preview(bytes: Option<&[u8]>, chars: usize) -> String {
    let Some(b) = bytes else {
        return "∅ null / tombstone".into();
    };
    if b.is_empty() {
        return "(empty value)".into();
    }
    let limit = b.len().min(chars.saturating_mul(4).saturating_add(4));
    let mut prefix = &b[..limit];
    let mut decoded = std::str::from_utf8(prefix);
    if limit < b.len() {
        for _ in 0..3 {
            if decoded.is_ok() || prefix.is_empty() {
                break;
            }
            prefix = &prefix[..prefix.len() - 1];
            decoded = std::str::from_utf8(prefix);
        }
    }
    let Ok(s) = decoded else {
        return format!("[binary · {} bytes]", b.len());
    };
    if s.chars()
        .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
    {
        return format!("[binary · {} bytes]", b.len());
    }
    let mut result: String = s
        .chars()
        .take(chars)
        .map(|c| if c.is_whitespace() { ' ' } else { c })
        .collect();
    if s.chars().count() > chars || prefix.len() < b.len() {
        result.push('…');
    }
    result
}

pub fn text(bytes: Option<&[u8]>) -> String {
    let Some(b) = bytes else {
        return "<null — distinct from an empty byte array>".into();
    };
    if b.is_empty() {
        return "<empty — 0 bytes>".into();
    }
    let len = b.len().min(PREVIEW_BYTES);
    let mut prefix = &b[..len];
    let mut s = std::str::from_utf8(prefix);
    if len < b.len() {
        for _ in 0..3 {
            if s.is_ok() || prefix.is_empty() {
                break;
            }
            prefix = &prefix[..prefix.len() - 1];
            s = std::str::from_utf8(prefix);
        }
    }
    match s {
        Ok(s) => format!(
            "{}{}",
            s,
            if len < b.len() {
                "\n\n[Preview limited to 256 KiB. JSONL export preserves every byte.]"
            } else {
                ""
            }
        ),
        Err(_) => format!(
            "<binary / invalid UTF-8 · {} bytes>\nUse Hex or export raw Base64.",
            b.len()
        ),
    }
}

pub fn hex(bytes: Option<&[u8]>) -> String {
    let Some(b) = bytes else {
        return "<null>".into();
    };
    if b.is_empty() {
        return "<empty — 0 bytes>".into();
    }
    let mut result = String::new();
    use std::fmt::Write as _;
    for (line, chunk) in b[..b.len().min(8192)].chunks(16).enumerate() {
        let _ = write!(result, "{:08X}  ", line * 16);
        for n in 0..16 {
            if let Some(v) = chunk.get(n) {
                let _ = write!(result, "{v:02X} ");
            } else {
                result.push_str("   ");
            }
        }
        result.push_str(" | ");
        for v in chunk {
            result.push(if (32..127).contains(v) {
                *v as char
            } else {
                '.'
            });
        }
        result.push('\n');
    }
    if b.len() > 8192 {
        result.push_str("\n[Hex preview limited to 8 KiB; export preserves full bytes.]\n");
    }
    result
}

pub fn pretty_json(bytes: Option<&[u8]>) -> String {
    let Some(b) = bytes else {
        return "// null value — Kafka tombstone".into();
    };
    if b.is_empty() {
        return "// Empty payload: 0 bytes".into();
    }
    if b.len() > PREVIEW_BYTES {
        return "// JSON preview exceeds 256 KiB; export the complete record.".into();
    }
    match serde_json::from_slice::<Value>(b).and_then(|v| serde_json::to_string_pretty(&v)) {
        Ok(s) if s.len() <= 512 * 1024 => s,
        Ok(_) => "// Formatted JSON exceeds 512 KiB; use Text or export.".into(),
        Err(_) => format!(
            "// Not a valid JSON document.\n// Use Text or Hex.\n\n{}",
            preview(Some(b), 1500)
        ),
    }
}

pub fn export(path: &Path, records: &[Arc<Record>]) -> Result<(), String> {
    let mut out = BufWriter::new(File::create(path).map_err(|e| e.to_string())?);
    for r in records {
        serde_json::to_writer(&mut out, &r.json()).map_err(|e| e.to_string())?;
        out.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    out.flush().map_err(|e| e.to_string())
}

pub struct Buffer {
    pub records: VecDeque<Arc<Record>>,
    pub bytes: usize,
    pub evicted: usize,
    max_records: usize,
    max_bytes: usize,
}
impl Default for Buffer {
    fn default() -> Self {
        Self {
            records: VecDeque::new(),
            bytes: 0,
            evicted: 0,
            max_records: RETAIN_COUNT,
            max_bytes: RETAIN_BYTES,
        }
    }
}
impl Buffer {
    pub fn set_limits(&mut self, records: usize, bytes: usize) {
        self.max_records = records.max(1);
        self.max_bytes = bytes.max(1);
        self.enforce_limits();
    }
    pub fn evict_oldest(&mut self) -> bool {
        if let Some(old) = self.records.pop_front() {
            self.bytes -= old.bytes();
            self.evicted += 1;
            true
        } else {
            false
        }
    }
    fn enforce_limits(&mut self) {
        while self.records.len() > self.max_records || self.bytes > self.max_bytes {
            self.evict_oldest();
        }
    }
    pub fn add(&mut self, record: Arc<Record>) {
        self.bytes += record.bytes();
        self.records.push_back(record);
        self.enforce_limits();
    }
    pub fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
        self.evicted = 0;
    }
}

pub type Properties = BTreeMap<String, String>;
pub fn parse_properties(text: &str) -> Result<Properties, String> {
    if text.len() > 1024 * 1024 {
        return Err("Properties file exceeds 1 MiB".into());
    }
    let mut props = Properties::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("Properties line {} must be key=value", n + 1));
        };
        if key.trim().is_empty() {
            return Err(format!("Empty property name at line {}", n + 1));
        }
        props.insert(key.trim().into(), value.trim().into());
    }
    Ok(props)
}
pub fn secret_key(key: &str) -> bool {
    let k = key.to_lowercase();
    ["password", "passwd", "secret", "token", "jaas", "private"]
        .iter()
        .any(|x| k.contains(x))
        || k == "ssl.key.pem"
        || k == "ssl.keystore.key"
}
pub fn profile_text(props: &Properties) -> String {
    let mut s =
        "# Kafka Reader profile. Credentials are omitted. Values are literal UTF-8.\n".to_string();
    for (k, v) in props {
        if !secret_key(k) && !v.contains("PRIVATE KEY") {
            s.push_str(k);
            s.push('=');
            s.push_str(&v.replace(['\n', '\r'], ""));
            s.push('\n');
        }
    }
    s
}
pub fn redact(error: &str, props: &Properties) -> String {
    let mut s = error.to_string();
    for (k, v) in props {
        if secret_key(k) && !v.is_empty() {
            s = s.replace(v, "[redacted]");
        }
    }
    s
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Earliest,
    Recent,
    Offset,
    Timestamp,
    Latest,
}
impl Mode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "earliest" => Ok(Self::Earliest),
            "recent" => Ok(Self::Recent),
            "offset" => Ok(Self::Offset),
            "timestamp" => Ok(Self::Timestamp),
            "latest" => Ok(Self::Latest),
            _ => Err("Mode must be earliest, recent, offset, timestamp or latest".into()),
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Earliest => "Beginning",
            Self::Recent => "Recent window",
            Self::Offset => "From offset",
            Self::Timestamp => "From timestamp",
            Self::Latest => "New messages",
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Partition {
    pub partition: i32,
    pub beginning: i64,
    pub end: i64,
}
#[derive(Clone)]
pub struct Request {
    pub props: Properties,
    pub topic: String,
    pub partitions: Vec<i32>,
    pub mode: Mode,
    pub value: i64,
    pub follow: bool,
    pub max_records: usize,
    pub seconds: u64,
    pub demo: bool,
    pub inspect: bool,
}
pub fn start_offset(
    mode: Mode,
    value: i64,
    begin: i64,
    end: i64,
    at_time: Option<i64>,
) -> Result<i64, String> {
    if begin < 0 || end < begin || value < 0 {
        return Err("Invalid offset bounds or negative starting value".into());
    }
    match mode {
        Mode::Earliest => Ok(begin),
        Mode::Latest => Ok(end),
        Mode::Recent => {
            if value == 0 {
                Err("Recent window must be positive".into())
            } else {
                Ok(end.saturating_sub(value).max(begin))
            }
        }
        Mode::Offset => {
            if value < begin || value > end {
                Err(format!("Offset {value} is outside [{begin}, {end}]"))
            } else {
                Ok(value)
            }
        }
        Mode::Timestamp => Ok(at_time.unwrap_or(end).clamp(begin, end)),
    }
}

pub fn demo_record(partition: i32, offset: i64, topic: &str, epoch: i64) -> Record {
    let id = offset * 3 + i64::from(partition);
    let status = ["CREATED", "VERIFIED", "SIGNED", "ACTIVE", "RENEWED"][(id % 5) as usize];
    let key = if id % 53 == 0 {
        None
    } else {
        Some(format!("AGR-{:06}", 100000 + id).into_bytes())
    };
    let value = if id % 47 == 0 {
        None
    } else if id % 43 == 0 {
        Some(vec![])
    } else if id % 37 == 0 {
        Some(vec![0, 1, 255, 128])
    } else if id % 41 == 0 {
        Some(b"{\"malformed\":}".to_vec())
    } else {
        let region = ["Amsterdam", "Москва", "Tokyo", "Montréal", "São Paulo"][(id % 5) as usize];
        let event = json!({
            "eventId": format!("demo-{partition}-{offset}"),
            "agreementId": format!("AGR-{:06}", 100000 + id),
            "eventType": format!("agreement.{}", status.to_lowercase()),
            "status": status,
            "customer": {"id": id % 17, "region": region},
            "terms": {"currency": "EUR", "amount": 100 + id % 900, "durationMonths": 12},
            "tags": ["synthetic", "rust-native"],
            "note": "Offline sample (synthetic data)"
        });
        Some(serde_json::to_vec(&event).unwrap())
    };
    Record {
        topic: topic.into(),
        partition,
        offset,
        timestamp: epoch + offset * 1000,
        timestamp_type: "CREATE_TIME".into(),
        key,
        value,
        headers: vec![
            Header {
                name: "source".into(),
                value: Some(b"offline-demo".to_vec()),
            },
            Header {
                name: "tag".into(),
                value: Some(b"synthetic".to_vec()),
            },
            Header {
                name: "tag".into(),
                value: Some(b"ordered-duplicate".to_vec()),
            },
            Header {
                name: "nullable".into(),
                value: None,
            },
            Header {
                name: "empty".into(),
                value: Some(vec![]),
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn offsets_reject_outside_bounds() {
        assert!(start_offset(Mode::Offset, 5, 10, 20, None).is_err());
        assert_eq!(start_offset(Mode::Offset, 20, 10, 20, None).unwrap(), 20);
    }
    #[test]
    fn recent_clamps_and_timestamp_never_rewinds() {
        assert_eq!(start_offset(Mode::Recent, 100, 10, 20, None).unwrap(), 10);
        assert_eq!(
            start_offset(Mode::Timestamp, 100, 10, 20, None).unwrap(),
            20
        );
        assert!(start_offset(Mode::Recent, 0, 0, 20, None).is_err());
    }
    #[test]
    fn json_preserves_null_empty_binary_headers() {
        let r = Record {
            topic: "t".into(),
            partition: 0,
            offset: 1,
            timestamp: 2,
            timestamp_type: "CREATE_TIME".into(),
            key: None,
            value: Some(vec![255, 0]),
            headers: vec![
                Header {
                    name: "x".into(),
                    value: None,
                },
                Header {
                    name: "x".into(),
                    value: Some(vec![]),
                },
            ],
        };
        let j = r.json();
        assert!(j["keyBase64"].is_null());
        assert_eq!(j["valueBase64"], "/wA=");
        assert!(j["valueUtf8"].is_null());
        assert!(j["headers"][0]["valueBase64"].is_null());
        assert_eq!(j["headers"][1]["valueBase64"], "");
    }
    #[test]
    fn profiles_remove_credentials() {
        let p=parse_properties("bootstrap.servers=x:1\nsasl.password=secret\nsasl.jaas.config=foo\nssl.key.pem=private\nreader.topic=t").unwrap();
        let s = profile_text(&p);
        assert!(!s.contains("=secret"));
        assert!(!s.contains("jaas.config"));
        assert!(!s.contains("ssl.key.pem"));
        assert!(s.contains("reader.topic=t"));
    }
    #[test]
    fn unicode_preview_and_binary() {
        assert_eq!(preview(Some("Москва ✓".as_bytes()), 100), "Москва ✓");
        assert!(preview(Some(&[255, 0]), 100).contains("binary"));
        assert_ne!(text(None), text(Some(&[])));
    }
    #[test]
    fn retained_count_bound() {
        let mut b = Buffer::default();
        for i in 0..RETAIN_COUNT + 3 {
            b.add(Arc::new(demo_record(0, i as i64, "t", 0)));
        }
        assert_eq!(b.records.len(), RETAIN_COUNT);
        assert_eq!(b.evicted, 3);
    }
    #[test]
    fn malformed_json_does_not_panic() {
        assert!(pretty_json(Some(b"{bad}")).contains("Not a valid"));
    }
    #[test]
    fn configured_retention_is_enforced_immediately() {
        let mut buffer = Buffer::default();
        for i in 0..40 {
            buffer.add(Arc::new(demo_record(0, i, "test", 0)));
        }
        buffer.set_limits(7, RETAIN_BYTES);
        assert_eq!(buffer.records.len(), 7);
        assert_eq!(buffer.records.front().unwrap().offset, 33);
        buffer.set_limits(7, 1);
        assert_eq!(buffer.records.len(), 0);
        assert_eq!(buffer.bytes, 0);
    }
}
