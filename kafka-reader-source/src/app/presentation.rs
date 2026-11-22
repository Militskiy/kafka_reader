use kafka_reader_rust::model::{PREVIEW_BYTES, Record, hex, pretty_json, text};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SortColumn {
    Time,
    Partition,
    Offset,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MessageOrder {
    pub column: SortColumn,
    pub descending: bool,
}

impl Default for MessageOrder {
    fn default() -> Self {
        Self {
            column: SortColumn::Time,
            descending: true,
        }
    }
}

impl MessageOrder {
    pub fn select(&mut self, column: SortColumn) {
        if self.column == column {
            self.descending = !self.descending;
        } else {
            self.column = column;
            self.descending = column == SortColumn::Time;
        }
    }

    pub fn compare(&self, a: &Record, b: &Record) -> Ordering {
        let primary = match self.column {
            SortColumn::Time => match (record_time(a), record_time(b)) {
                (Some(a), Some(b)) => a.cmp(&b),
                // Missing timestamps stay below known times in either direction.
                (Some(_), None) => return Ordering::Less,
                (None, Some(_)) => return Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            SortColumn::Partition => a.partition.cmp(&b.partition),
            SortColumn::Offset => a.offset.cmp(&b.offset),
        };
        let primary = if self.descending {
            primary.reverse()
        } else {
            primary
        };
        // Offsets identify an order within a partition. Ties never depend on
        // arrival timing, rendered time strings, or the previous sort order.
        primary
            .then_with(|| a.partition.cmp(&b.partition))
            .then_with(|| a.offset.cmp(&b.offset))
            .then_with(|| a.topic.cmp(&b.topic))
    }
}

pub(super) fn record_time(record: &Record) -> Option<i64> {
    (record.timestamp >= 0 && record.timestamp_type != "NO_TIMESTAMP_TYPE")
        .then_some(record.timestamp)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum PayloadFormat {
    #[default]
    Auto,
    Json,
    Text,
    Hex,
}

impl PayloadFormat {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Json => "JSON",
            Self::Text => "Text",
            Self::Hex => "Hex",
        }
    }
}

pub(super) fn format_payload(bytes: Option<&[u8]>, format: PayloadFormat, key: bool) -> String {
    let Some(bytes) = bytes else {
        return if key {
            "<null key — distinct from an empty byte array>".into()
        } else {
            "<null value — Kafka tombstone>".into()
        };
    };
    if bytes.is_empty() {
        return "<empty — 0 bytes>".into();
    }
    match format {
        PayloadFormat::Hex => hex(Some(bytes)),
        PayloadFormat::Text => text(Some(bytes)),
        PayloadFormat::Json => pretty_json(Some(bytes)),
        PayloadFormat::Auto => {
            // Decode/format only the bounded preview. Large records remain
            // exportable byte-for-byte without expensive full JSON parsing.
            if bytes.len() <= PREVIEW_BYTES
                && serde_json::from_slice::<serde_json::Value>(bytes).is_ok()
            {
                return pretty_json(Some(bytes));
            }
            let mut end = bytes.len().min(PREVIEW_BYTES);
            let mut decoded = std::str::from_utf8(&bytes[..end]);
            if end < bytes.len() {
                for _ in 0..3 {
                    if decoded.is_ok() {
                        break;
                    }
                    end -= 1;
                    decoded = std::str::from_utf8(&bytes[..end]);
                }
            }
            if decoded.is_ok_and(|value| {
                !value
                    .chars()
                    .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
            }) {
                text(Some(bytes))
            } else {
                format!(
                    "<binary data · {} bytes>\nChoose Hex in the format menu to inspect the bytes.\nJSONL export preserves the complete record as Base64.",
                    bytes.len()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(partition: i32, offset: i64, timestamp: i64) -> Record {
        Record {
            topic: "orders".into(),
            partition,
            offset,
            timestamp,
            timestamp_type: if timestamp < 0 {
                "NO_TIMESTAMP_TYPE"
            } else {
                "CREATE_TIME"
            }
            .into(),
            key: None,
            value: None,
            headers: vec![],
        }
    }

    #[test]
    fn time_order_uses_full_timestamp_and_keeps_missing_last() {
        let mut records = vec![
            record(0, 1, 86_399_999),
            record(0, 2, 86_400_001),
            record(0, 3, -1),
        ];
        let mut order = MessageOrder::default();
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [2, 1, 3]
        );
        order.select(SortColumn::Time);
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn offsets_are_numeric_with_partition_ties_and_no_overflow() {
        let mut records = vec![
            record(2, 10, 1),
            record(1, 10, 1),
            record(0, 2, 1),
            record(0, i64::MAX, 1),
        ];
        let mut order = MessageOrder::default();
        order.select(SortColumn::Offset);
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            [(0, 2), (1, 10), (2, 10), (0, i64::MAX)]
        );
        order.select(SortColumn::Offset);
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            [(0, i64::MAX), (1, 10), (2, 10), (0, 2)]
        );
    }

    #[test]
    fn partition_order_keeps_offsets_ascending_within_each_partition() {
        let mut records = vec![record(10, 2, 0), record(2, 10, 0), record(2, 2, 0)];
        let mut order = MessageOrder::default();
        order.select(SortColumn::Partition);
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            [(2, 2), (2, 10), (10, 2)]
        );
        order.select(SortColumn::Partition);
        records.sort_by(|a, b| order.compare(a, b));
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            [(10, 2), (2, 2), (2, 10)]
        );
    }

    #[test]
    fn equal_times_have_a_repeatable_partition_and_offset_order() {
        let mut records = vec![record(2, 2, 7), record(0, 20, 7), record(0, 3, 7)];
        records.sort_by(|a, b| MessageOrder::default().compare(a, b));
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            [(0, 3), (0, 20), (2, 2)]
        );
    }

    #[test]
    fn auto_format_shows_json_or_text_without_a_second_hex_copy() {
        let json = format_payload(
            Some(br#"{"event":"created","n":2}"#),
            PayloadFormat::Auto,
            false,
        );
        assert!(json.contains("\n  \"event\": \"created\""));
        assert_eq!(
            format_payload(Some(b"order-42"), PayloadFormat::Auto, true),
            "order-42"
        );
        assert!(!json.contains("00000000"));
    }

    #[test]
    fn binary_requires_explicit_hex_and_null_key_is_not_a_tombstone() {
        for bytes in [b"\xFF\x00".as_slice(), b"\x00\x01".as_slice()] {
            let automatic = format_payload(Some(bytes), PayloadFormat::Auto, false);
            assert!(automatic.contains("Choose Hex"));
            assert!(!automatic.contains("00000000"));
            assert!(format_payload(Some(bytes), PayloadFormat::Hex, false).contains("00000000"));
        }
        for format in [
            PayloadFormat::Auto,
            PayloadFormat::Json,
            PayloadFormat::Text,
            PayloadFormat::Hex,
        ] {
            let key = format_payload(None, format, true);
            assert!(key.contains("null key") && !key.contains("tombstone"));
            assert_ne!(key, format_payload(Some(&[]), format, true));
            assert!(format_payload(None, format, false).contains("tombstone"));
        }
    }
}
