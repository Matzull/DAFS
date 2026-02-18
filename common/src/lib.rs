struct CompactTimestamp;

impl tracing_subscriber::fmt::time::FormatTime for CompactTimestamp {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        use chrono::{Datelike, Timelike};

        let now = chrono::Local::now();
        let year = now.year().rem_euclid(100) as u32;
        let frac4 = now.timestamp_subsec_micros() / 100;

        write!(
            w,
            "{:02}/{:02}/{:02}-{:02}:{:02}:{:02}.{:04}",
            year,
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            frac4
        )
    }
}

// Re-export the generated code
pub mod dfs {
    // The build script generates a file named "dfs.rs" (based on package name in proto)
    // inside the OUT_DIR. We include it here.
    tonic::include_proto!("dfs");
}

pub fn init_tracing(component: &str, default_level: &str) {
    let configured_level = std::env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_string());
    let base_filter = tracing_subscriber::EnvFilter::new("info");
    let directive = format!("{component}={configured_level}").parse();

    let filter = match directive {
        Ok(directive) => base_filter.add_directive(directive),
        Err(_) => base_filter,
    };

    if let Err(err) = tracing_subscriber::fmt()
        .compact()
        .with_timer(CompactTimestamp)
        .with_target(false)
        .with_env_filter(filter)
        .try_init()
    {
        eprintln!("Tracing init skipped: {err}");
    }
}

// Helper to easily print Audit Events (Optional but useful for debugging)
impl std::fmt::Display for dfs::AuditRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] User:{} Action:{} Res:{} Status:{}",
            self.timestamp, self.user_id, self.action, self.resource, self.status
        )
    }
}

// #[cfg(test)]
// mod tests;
