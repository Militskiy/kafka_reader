//! Narrow, ownership-checked adapter for librdkafka's group-free partition API.
//!
//! The high-level consumer starts coordinator discovery when group.id is present,
//! even with manual assignment. This adapter deliberately omits group.id and uses
//! consume_start_queue at absolute offsets. Native allocations stay on one worker;
//! each message is copied before destruction and handles are released in order.
use crate::model::{Header, Record};
use rdkafka::{
    bindings as ffi,
    consumer::{BaseConsumer, Consumer},
};
use std::{
    collections::BTreeMap,
    ffi::{CStr, CString, c_void},
    ptr,
    time::Duration,
};

pub enum Poll {
    Record(Record),
    Eof(i32, i64),
    Empty,
}

pub struct DirectConsumer<'a> {
    owner: &'a BaseConsumer,
    topic: String,
    native_topic: *mut ffi::rd_kafka_topic_t,
    queue: *mut ffi::rd_kafka_queue_t,
    started: Vec<i32>,
}

impl<'a> DirectConsumer<'a> {
    pub fn new(
        owner: &'a BaseConsumer,
        topic: &str,
        positions: &BTreeMap<i32, i64>,
    ) -> Result<Self, String> {
        let name = CString::new(topic).map_err(|_| "Topic name contains NUL")?;
        // SAFETY: owner remains alive for 'a; no native pointer escapes this adapter.
        unsafe {
            let native = owner.client().native_ptr();
            let queue = ffi::rd_kafka_queue_new(native);
            if queue.is_null() {
                return Err("Unable to allocate native read queue".into());
            }
            let conf = ffi::rd_kafka_default_topic_conf_dup(native);
            if conf.is_null() {
                ffi::rd_kafka_queue_destroy(queue);
                return Err("Unable to allocate topic configuration".into());
            }
            let mut error = [0i8; 512];
            for (k, v) in [
                (c"auto.commit.enable", c"false"),
                (c"offset.store.method", c"broker"),
            ] {
                let result = ffi::rd_kafka_topic_conf_set(
                    conf,
                    k.as_ptr(),
                    v.as_ptr(),
                    error.as_mut_ptr(),
                    error.len(),
                );
                if result != ffi::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK {
                    let message = CStr::from_ptr(error.as_ptr())
                        .to_string_lossy()
                        .into_owned();
                    ffi::rd_kafka_topic_conf_destroy(conf);
                    ffi::rd_kafka_queue_destroy(queue);
                    return Err(message);
                }
            }
            // topic_new takes ownership of conf, including on failure.
            let native_topic = ffi::rd_kafka_topic_new(native, name.as_ptr(), conf);
            if native_topic.is_null() {
                ffi::rd_kafka_queue_destroy(queue);
                return Err(last_error());
            }
            let mut reader = Self {
                owner,
                topic: topic.into(),
                native_topic,
                queue,
                started: vec![],
            };
            for (&partition, &offset) in positions {
                if offset < 0 {
                    return Err("Direct reads require an absolute, nonnegative offset".into());
                }
                if ffi::rd_kafka_consume_start_queue(native_topic, partition, offset, queue) != 0 {
                    return Err(last_error());
                }
                reader.started.push(partition);
            }
            Ok(reader)
        }
    }

    pub fn poll(&self) -> Result<Poll, String> {
        // Service informational client callbacks. The separate read queue owns data.
        let _ = self.owner.poll(Duration::ZERO);
        // SAFETY: queue belongs to this adapter and only this worker polls it.
        unsafe {
            let pointer = ffi::rd_kafka_consume_queue(self.queue, 100);
            if pointer.is_null() {
                return Ok(Poll::Empty);
            }
            let guard = NativeMessage(pointer);
            let message = &*guard.0;
            if message.err == ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF {
                return Ok(Poll::Eof(message.partition, message.offset));
            }
            if message.err != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(CStr::from_ptr(ffi::rd_kafka_message_errstr(pointer))
                    .to_string_lossy()
                    .into_owned());
            }
            let mut kind = ffi::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
            let timestamp = ffi::rd_kafka_message_timestamp(pointer, &mut kind);
            let timestamp_type = match kind {
                ffi::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => "CREATE_TIME",
                ffi::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                    "LOG_APPEND_TIME"
                }
                _ => "NO_TIMESTAMP_TYPE",
            };
            let mut headers = Vec::new();
            let mut raw_headers = ptr::null_mut();
            if ffi::rd_kafka_message_headers(pointer, &mut raw_headers)
                == ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !raw_headers.is_null()
            {
                for n in 0..ffi::rd_kafka_header_cnt(raw_headers) {
                    let mut name = ptr::null();
                    let mut value = ptr::null();
                    let mut length = 0usize;
                    let result = ffi::rd_kafka_header_get_all(
                        raw_headers,
                        n,
                        &mut name,
                        &mut value,
                        &mut length,
                    );
                    if result != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                        || name.is_null()
                    {
                        return Err("Cannot decode a native Kafka header".into());
                    }
                    headers.push(Header {
                        name: CStr::from_ptr(name).to_string_lossy().into_owned(),
                        value: copy_bytes(value, length),
                    });
                }
            }
            let record = Record {
                topic: self.topic.clone(),
                partition: message.partition,
                offset: message.offset,
                timestamp,
                timestamp_type: timestamp_type.into(),
                key: copy_bytes(message.key, message.key_len),
                value: copy_bytes(message.payload, message.len),
                headers,
            };
            Ok(Poll::Record(record))
        }
    }
}

// No Send/Sync implementation: raw handles remain on the worker that created them.
impl Drop for DirectConsumer<'_> {
    fn drop(&mut self) {
        unsafe {
            for partition in &self.started {
                ffi::rd_kafka_consume_stop(self.native_topic, *partition);
            }
            ffi::rd_kafka_topic_destroy(self.native_topic);
            ffi::rd_kafka_queue_destroy(self.queue);
        }
    }
}
struct NativeMessage(*mut ffi::rd_kafka_message_t);
impl Drop for NativeMessage {
    fn drop(&mut self) {
        unsafe {
            ffi::rd_kafka_message_destroy(self.0);
        }
    }
}

/// Pointer and length are borrowed from a live native message/header, copied once.
unsafe fn copy_bytes(pointer: *const c_void, length: usize) -> Option<Vec<u8>> {
    if pointer.is_null() {
        None
    } else if length == 0 {
        Some(Vec::new())
    } else {
        Some(unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), length) }.to_vec())
    }
}
unsafe fn last_error() -> String {
    unsafe {
        CStr::from_ptr(ffi::rd_kafka_err2str(ffi::rd_kafka_last_error()))
            .to_string_lossy()
            .into_owned()
    }
}
