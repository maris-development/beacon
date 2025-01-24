use std::{
    env,
    io::Write,
    sync::{Arc, Mutex},
};

use console::{style, Color};
pub use log;
use rolling_file::{RollingConditionBasic, RollingFileAppender};

#[derive(Debug, Clone)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

// //Macro for creating a progress bar
#[macro_export]
macro_rules! progress_bar {
    ($counter:literal) => {
        beacon_logger::LOGGER.spawn_progress_bar($counter)
    };
    ($counter: expr) => {
        beacon_logger::LOGGER.spawn_progress_bar($counter)
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        beacon_logger::log::error!($($arg)*);
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        beacon_logger::log::warn!($($arg)*);
    };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        beacon_logger::log::debug!($($arg)*);
    };
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        beacon_logger::log::trace!($($arg)*);
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        beacon_logger::log::info!($($arg)*);
    };
}

lazy_static::lazy_static! {
    pub static ref LOGGER: Logger = Logger::new();
}

pub fn init() -> Result<(), log::SetLoggerError> {
    log::set_logger(&*LOGGER).map(|()| {
        let log_level = match beacon_config::CONFIG.log_level.as_str() {
            "ERROR" => log::LevelFilter::Error,
            "WARN" => log::LevelFilter::Warn,
            "INFO" => log::LevelFilter::Info,
            "DEBUG" => log::LevelFilter::Debug,
            "TRACE" => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info,
        };

        log::set_max_level(log_level)
    })
}

pub struct Logger {
    fs_writer: Arc<Mutex<RollingFileAppender<RollingConditionBasic>>>,
}

impl Logger {
    const LOG_FILE_PATH: &'static str = "./logs/beacon.log";

    pub(crate) fn new() -> Self {
        //Create directory if it doesn't exist
        if let Err(err) = std::fs::create_dir_all("./logs") {
            panic!("Failed to create log directory: {}", err);
        }

        let fs_writer = RollingFileAppender::new(
            Self::LOG_FILE_PATH,
            RollingConditionBasic::new().max_size(100 * 1024 * 1024),
            0,
        )
        .unwrap();

        Self {
            fs_writer: Arc::new(Mutex::new(fs_writer)),
        }
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let (level, color) = match record.level() {
            log::Level::Error => ("ERROR", Color::Red),
            log::Level::Warn => ("WARN", Color::Yellow),
            log::Level::Info => ("INFO", Color::Cyan),
            log::Level::Debug => ("DEBUG", Color::Magenta),
            log::Level::Trace => ("TRACE", Color::White),
        };

        let msg = format!(
            "{} - [{}] - [{}] - {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            style(level).fg(color),
            record.module_path().unwrap_or("<unknown>"),
            record.args()
        );

        println!("{}", msg);

        let mut fs_writer = self.fs_writer.lock().unwrap();
        fs_writer
            .write_all(msg.as_bytes())
            .expect("Failed to write to log file");

        //Write a newline to the log file
        fs_writer
            .write_all("\n".as_bytes())
            .expect("Failed to write to log file");

        fs_writer.flush().expect("Failed to flush log file");
    }

    fn flush(&self) {}
}
