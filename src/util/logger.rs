use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{OnceLock, mpsc},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
static LOGGER: OnceLock<mpsc::Sender<LogMessage>> = OnceLock::new();

struct LogMessage {
    timestamp_ms: u128,
    sender: Option<i64>,
    message: String,
}

/// Initialize logger (call once)
pub fn startup() {
    let (tx, rx) = mpsc::channel::<LogMessage>();

    LOGGER.set(tx).expect("Logger already initialized");

    thread::spawn(move || {
        // Prepare log directory
        let log_dir = Path::new("logs");
        fs::create_dir_all(log_dir).expect("Failed to create log directory");

        let start_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let path = log_dir.join(format!("log_{}.txt", start_ts));

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("Failed to open log file");

        // Dedicated logging loop
        for msg in rx {
            let timestamp_box = fixed_box(&msg.timestamp_ms.to_string(), 13);

            let sender_box = match msg.sender {
                Some(id) => fixed_box(&format!("{}", id), 19),
                None => fixed_box("", 19),
            };

            let line = format!("{} {} {}", timestamp_box, sender_box, msg.message);

            println!("{}", line);
            let _ = writeln!(file, "{}", line);
        }
    });
}
fn fixed_box(content: &str, width: usize) -> String {
    let s = content.chars().take(width).collect::<String>();
    let len = s.chars().count();
    if len < width {
        let mut a = " ".repeat(width - len);
        a.push_str(&s);
        format!("[{}]", a)
    } else {
        s
    }
}
/// Internal function (sync + async safe)
pub fn log_internal(sender: Option<i64>, message: String) {
    if let Some(tx) = LOGGER.get() {
        let _ = tx.send(LogMessage {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            sender,
            message,
        });
    } else {
        println!("{}", message);
    }
}

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::util::logger::log_internal(None, format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_from {
    ($sender:expr, $($arg:tt)*) => {
        $crate::util::logger::log_internal(Some($sender), format!($($arg)*))
    };
}
#[macro_export]
macro_rules! log_in {
    ($($arg:tt)*) => {
        $crate::util::logger::log_internal(
            None,
            format!("> {}", format!($($arg)*))
        )
    };
}

#[macro_export]
macro_rules! log_out {
    ($($arg:tt)*) => {
        $crate::util::logger::log_internal(
            None,
            format!("< {}", format!($($arg)*))
        )
    };
}

#[macro_export]
macro_rules! log_in_from {
    ($sender:expr, $($arg:tt)*) => {
        $crate::util::logger::log_internal(
            Some($sender),
            format!("> {}", format!($($arg)*))
        )
    };
}

#[macro_export]
macro_rules! log_out_from {
    ($sender:expr, $($arg:tt)*) => {
        $crate::util::logger::log_internal(
            Some($sender),
            format!("< {}", format!($($arg)*))
        )
    };
}
