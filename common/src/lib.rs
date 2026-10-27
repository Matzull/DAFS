struct CompactTimestamp;

impl tracing_subscriber::fmt::time::FormatTime for CompactTimestamp {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
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
// Generated code is not ours to lint
#[allow(clippy::all)]
pub mod dfs {
    // The build script generates a file named "dfs.rs" (based on package name in proto)
    // inside the OUT_DIR. We include it here.
    tonic::include_proto!("dfs");
}

#[allow(clippy::all)]
pub mod raft {
    tonic::include_proto!("raft");
}

pub const AUTHORIZATION_HEADER: &str = "authorization";
pub const ROLE_USER: &str = "user";
pub const ROLE_MASTER: &str = "master";
pub const ROLE_DATANODE: &str = "datanode";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthClaims {
    pub sub: String,
    pub role: String,
    pub exp: usize,
    pub iat: usize,
}

pub fn create_access_token(
    secret: &str,
    subject: &str,
    role: &str,
    expires_in_seconds: i64,
) -> Result<String, jsonwebtoken::errors::Error> {
    use chrono::Utc;
    use jsonwebtoken::{encode, EncodingKey, Header};

    let issued_at = Utc::now().timestamp();
    let expiration = (issued_at + expires_in_seconds.max(1)) as usize;
    let claims = AuthClaims {
        sub: subject.to_string(),
        role: role.to_string(),
        exp: expiration,
        iat: issued_at as usize,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

pub fn decode_access_token(
    token: &str,
    secret: &str,
) -> Result<AuthClaims, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{decode, DecodingKey, Validation};

    let data = decode::<AuthClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )?;

    Ok(data.claims)
}

#[allow(clippy::result_large_err)]
pub fn extract_bearer_token(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<String, tonic::Status> {
    let auth_header = metadata
        .get(AUTHORIZATION_HEADER)
        .ok_or_else(|| tonic::Status::unauthenticated("Missing authorization header"))?
        .to_str()
        .map_err(|_| tonic::Status::unauthenticated("Invalid authorization header"))?;

    let Some(token) = auth_header.strip_prefix("Bearer ") else {
        return Err(tonic::Status::unauthenticated(
            "Authorization header must be Bearer token",
        ));
    };

    if token.trim().is_empty() {
        return Err(tonic::Status::unauthenticated("Empty bearer token"));
    }

    Ok(token.to_string())
}

#[allow(clippy::result_large_err)]
pub fn authorize_metadata(
    metadata: &tonic::metadata::MetadataMap,
    secret: &str,
) -> Result<AuthClaims, tonic::Status> {
    let token = extract_bearer_token(metadata)?;
    decode_access_token(&token, secret)
        .map_err(|_| tonic::Status::unauthenticated("Invalid or expired access token"))
}

#[allow(clippy::result_large_err)]
pub fn require_role(claims: &AuthClaims, expected_role: &str) -> Result<(), tonic::Status> {
    if claims.role != expected_role {
        return Err(tonic::Status::permission_denied("Insufficient role"));
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
pub fn attach_bearer_token<T>(
    request: &mut tonic::Request<T>,
    token: &str,
) -> Result<(), tonic::Status> {
    let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|_| tonic::Status::internal("Invalid token metadata"))?;
    request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
    Ok(())
}

pub fn init_tracing(
    component: &str,
    default_level: &str,
) -> tracing_appender::non_blocking::WorkerGuard {
    let configured_level = std::env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_string());

    let filter = tracing_subscriber::EnvFilter::new(configured_level.clone())
        .add_directive(
            format!("{component}={configured_level}")
                .parse()
                .unwrap_or("info".parse().unwrap()),
        )
        .add_directive("openraft=off".parse().expect("valid tracing directive"));

    // 'never' es perfecto si vas a usar logrotate con copytruncate
    let file_appender = tracing_appender::rolling::never("./logs", format!("{}.log", component));
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .compact()
        .with_timer(CompactTimestamp)
        .with_target(false)
        .with_env_filter(filter)
        .with_ansi(false) // Recomendado: evita caracteres de colores en el archivo
        .with_writer(non_blocking_file) // <--- ESTO ES CLAVE
        .init();

    guard // Devolvemos el guard
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

#[cfg(test)]
mod tests;
