// Re-export the generated code
pub mod dfs {
    // The build script generates a file named "dfs.rs" (based on package name in proto)
    // inside the OUT_DIR. We include it here.
    tonic::include_proto!("dfs");
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
