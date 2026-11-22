#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]
mod app;
use kafka_reader_rust::{
    model::*,
    reader::{Event, Session, safe_properties},
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    #[cfg(windows)]
    if args.iter().any(|a| {
        matches!(
            a.as_str(),
            "--help" | "--version" | "--self-test" | "--read-config"
        )
    }) {
        attach_parent_console();
    }
    let mut values = BTreeMap::new();
    let mut flags = std::collections::BTreeSet::new();
    let mut i = 0;
    while i < args.len() {
        let name = &args[i];
        if [
            "--follow",
            "--self-test",
            "--inspect",
            "--help",
            "--version",
            "--gui-smoke",
            "--light",
            "--compact",
        ]
        .contains(&name.as_str())
        {
            flags.insert(name.clone());
            i += 1;
        } else if [
            "--read-config",
            "--topic",
            "--mode",
            "--value",
            "--partitions",
            "--max",
            "--seconds",
            "--export",
            "--screenshot",
            "--exit-after",
            "--data-dir",
            "--screenshot-dir",
            "--gui-check-reopen",
        ]
        .contains(&name.as_str())
        {
            let v = args
                .get(i + 1)
                .ok_or_else(|| format!("Missing value for {name}"))?;
            values.insert(name.clone(), v.clone());
            i += 2;
        } else {
            return Err(format!("Unknown argument {name}. Use --help."));
        }
    }
    if flags.contains("--help") {
        println!(
            "Kafka Reader {}\nNative desktop: kafka-reader [--light] [--data-dir DIR]\nHeadless read: --read-config FILE --topic TOPIC --mode earliest|recent|offset|timestamp|latest --value N --partitions CSV --max N --seconds N [--follow] --export FILE\n--inspect with --read-config performs metadata inspection only.\n--self-test runs offline safety/codec checks.\n--gui-smoke --screenshot-dir DIR --data-dir DIR exercises the actual native workspace and windows with an in-memory QA fixture.\n--gui-check-reopen OUTPUT_DIR --data-dir DIR verifies restored workspace without connecting.\nProperties use librdkafka key=value names; no Java JAAS. Values are literal UTF-8.",
            env!("CARGO_PKG_VERSION")
        );
        return Ok(());
    }
    if flags.contains("--version") {
        println!(
            "Kafka Reader {} · native binary · librdkafka {}",
            env!("CARGO_PKG_VERSION"),
            rdkafka::util::get_rdkafka_version().1
        );
        return Ok(());
    }
    if flags.contains("--self-test") {
        return self_test();
    }
    if let Some(path) = values.get("--read-config") {
        let meta = std::fs::metadata(path).map_err(|e| e.to_string())?;
        if meta.len() > 1024 * 1024 {
            return Err("Properties file exceeds 1 MiB".into());
        }
        let props = parse_properties(&std::fs::read_to_string(path).map_err(|e| e.to_string())?)?;
        let topic = values
            .get("--topic")
            .cloned()
            .or_else(|| props.get("reader.topic").cloned())
            .unwrap_or_default();
        let mode = Mode::parse(values.get("--mode").map(String::as_str).unwrap_or("recent"))?;
        let value = number(&values, "--value", 100i64)?;
        let max_records = number(&values, "--max", 2000usize)?;
        let seconds = number(&values, "--seconds", 60u64)?;
        let partitions = values
            .get("--partitions")
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|v| {
                        v.trim()
                            .parse::<i32>()
                            .map_err(|_| "Invalid partition list".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        let req = Request {
            props,
            topic,
            partitions,
            mode,
            value,
            follow: flags.contains("--follow"),
            max_records,
            seconds,
            demo: false,
            inspect: flags.contains("--inspect"),
        };
        return headless(req, values.get("--export").map(PathBuf::from));
    }
    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size(if flags.contains("--compact") {
                [1050., 720.]
            } else {
                [1400., 900.]
            })
            .with_min_inner_size([1050., 720.]),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };
    let close_after = values
        .get("--exit-after")
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| "Invalid --exit-after".to_string())
        })
        .transpose()?;
    let options = app::Options {
        smoke: flags.contains("--gui-smoke"),
        light: flags.contains("--light"),
        compact: flags.contains("--compact"),
        screenshot: values.get("--screenshot").map(PathBuf::from),
        screenshot_dir: values.get("--screenshot-dir").map(PathBuf::from),
        reopen: values.get("--gui-check-reopen").map(PathBuf::from),
        close_after,
        data_dir: values.get("--data-dir").map(PathBuf::from),
    };
    eframe::run_native(
        "Kafka Reader",
        native_options,
        Box::new(move |cc| Ok(Box::new(app::ReaderApp::new(cc, options)))),
    )
    .map_err(|e| e.to_string())
}

/// The release desktop binary has no console window. CLI invocations can reuse
/// the caller's console while keeping redirected output pipes/files intact.
#[cfg(windows)]
fn attach_parent_console() {
    use std::ffi::c_void;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn AttachConsole(process: u32) -> i32;
        fn GetStdHandle(kind: u32) -> *mut c_void;
        fn SetStdHandle(kind: u32, handle: *mut c_void) -> i32;
        fn GetFileType(handle: *mut c_void) -> u32;
    }
    // SAFETY: these Win32 functions accept borrowed process standard handles;
    // no handles are owned or closed here. INVALID_HANDLE_VALUE is excluded.
    unsafe {
        let mut redirected = Vec::new();
        for kind in [(-11i32) as u32, (-12i32) as u32] {
            let handle = GetStdHandle(kind);
            if !handle.is_null() && handle as isize != -1 {
                if matches!(GetFileType(handle), 1 | 3) {
                    redirected.push((kind, handle));
                }
            }
        }
        AttachConsole(u32::MAX);
        for (kind, handle) in redirected {
            SetStdHandle(kind, handle);
        }
    }
}
fn number<T: std::str::FromStr>(
    values: &BTreeMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, String> {
    values
        .get(key)
        .map(|v| v.parse().map_err(|_| format!("Invalid {key}")))
        .unwrap_or(Ok(default))
}
fn headless(request: Request, path: Option<PathBuf>) -> Result<(), String> {
    let started = Instant::now();
    let mut output = path
        .map(|p| File::create(p).map(BufWriter::new))
        .transpose()
        .map_err(|e| e.to_string())?;
    let s = Session::start(request);
    let mut count = 0;
    let mut skipped = 0;
    let mut state = "CONNECTING".to_string();
    let mut detail = String::new();
    let mut metadata = Vec::new();
    while s.running() || !s.rx.is_empty() {
        if let Some(event) = s.try_next() {
            match event {
                Event::Metadata(m) => metadata = m,
                Event::Record(r) => {
                    count += 1;
                    if let Some(out) = &mut output {
                        serde_json::to_writer(&mut *out, &r.json()).map_err(|e| e.to_string())?;
                        out.write_all(b"\n").map_err(|e| e.to_string())?;
                    }
                }
                Event::Skipped => skipped += 1,
                Event::State(name, text) => {
                    eprintln!("{name}: {text}");
                    state = name;
                    detail = text;
                }
                Event::Progress(_) => {}
            }
        } else {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    if let Some(out) = &mut output {
        out.flush().map_err(|e| e.to_string())?;
    }
    println!(
        "{}",
        serde_json::json!({
            "state": state,
            "count": count,
            "skipped": skipped,
            "partitions": metadata,
            "detail": detail,
            "elapsedMs": started.elapsed().as_millis(),
            "client": "rust-rdkafka",
            "rdkafkaVersion": rdkafka::util::get_rdkafka_version().1
        })
    );
    if state == "FAILED" {
        Err(detail)
    } else {
        Ok(())
    }
}
fn self_test() -> Result<(), String> {
    let p = parse_properties(
        "group.id=production\nenable.auto.commit=true\nenable.auto.offset.store=true\nallow.auto.create.topics=true\nsasl.password=private-fixture\nreader.topic=events",
    )?;
    let s = safe_properties(&p);
    assert!(!s.contains_key("group.id"));
    for k in [
        "enable.auto.commit",
        "enable.auto.offset.store",
        "allow.auto.create.topics",
    ] {
        assert_eq!(s[k], "false");
    }
    assert!(!profile_text(&p).contains("private-fixture"));
    assert_eq!(start_offset(Mode::Timestamp, 99, 0, 20, None)?, 20);
    let r = demo_record(0, 37, "demo", 0);
    let j = r.json();
    assert!(j["headers"].is_array());
    assert!(j["headers"][3]["valueBase64"].is_null());
    assert_eq!(j["headers"][4]["valueBase64"], "");
    assert!(pretty_json(Some(b"{bad}")).contains("Not a valid"));
    println!(
        "{}",
        serde_json::json!({"selfTest":"PASS","checks":9,"librdkafka":rdkafka::util::get_rdkafka_version().1})
    );
    Ok(())
}
