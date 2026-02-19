use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use rusqlite::{params, Connection};

#[derive(Clone, Debug)]
pub struct AuthManager {
    db_path: String,
    jwt_secret: String,
    jwt_exp_seconds: i64,
    service_token_exp_seconds: i64,
}

impl AuthManager {
    pub fn new(
        db_path: String,
        jwt_secret: String,
        jwt_exp_seconds: i64,
        service_token_exp_seconds: i64,
    ) -> Result<Self, String> {
        let manager = Self {
            db_path,
            jwt_secret,
            jwt_exp_seconds,
            service_token_exp_seconds,
        };
        manager.initialize_schema()?;
        Ok(manager)
    }

    fn initialize_schema(&self) -> Result<(), String> {
        let conn = Connection::open(&self.db_path)
            .map_err(|err| format!("Could not open users database: {err}"))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|err| format!("Could not initialize users table: {err}"))?;

        Ok(())
    }

    pub fn register_user(&self, username: &str, password: &str) -> Result<(), String> {
        if username.trim().is_empty() || password.trim().is_empty() {
            return Err("Username and password are required".to_string());
        }

        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|err| format!("Could not hash password: {err}"))?
            .to_string();

        let conn = Connection::open(&self.db_path)
            .map_err(|err| format!("Could not open users database: {err}"))?;

        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO users(username, password_hash, created_at) VALUES (?1, ?2, ?3)",
            params![username, password_hash, now],
        )
        .map_err(|err| {
            if let rusqlite::Error::SqliteFailure(inner, _) = &err {
                if inner.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE {
                    return "Username already exists".to_string();
                }
            }
            format!("Could not register user: {err}")
        })?;

        Ok(())
    }

    pub fn login_user(&self, username: &str, password: &str) -> Result<(String, i64), String> {
        if username.trim().is_empty() || password.trim().is_empty() {
            return Err("Username and password are required".to_string());
        }

        let conn = Connection::open(&self.db_path)
            .map_err(|err| format!("Could not open users database: {err}"))?;

        let password_hash: String = conn
            .query_row(
                "SELECT password_hash FROM users WHERE username = ?1",
                params![username],
                |row| row.get(0),
            )
            .map_err(|_| "Invalid username or password".to_string())?;

        let parsed_hash = PasswordHash::new(&password_hash)
            .map_err(|_| "Stored password hash is invalid".to_string())?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| "Invalid username or password".to_string())?;

        let token = common::create_access_token(
            &self.jwt_secret,
            username,
            common::ROLE_USER,
            self.jwt_exp_seconds,
        )
        .map_err(|err| format!("Could not issue access token: {err}"))?;

        let expires_at = chrono::Utc::now().timestamp() + self.jwt_exp_seconds.max(1);

        Ok((token, expires_at))
    }

    pub fn create_master_service_token(&self) -> Result<String, String> {
        common::create_access_token(
            &self.jwt_secret,
            "master-service",
            common::ROLE_MASTER,
            self.service_token_exp_seconds,
        )
        .map_err(|err| format!("Could not create master service token: {err}"))
    }

    pub fn jwt_secret(&self) -> &str {
        &self.jwt_secret
    }

    pub fn create_datanode_service_token(&self, node_subject: &str) -> Result<String, String> {
        common::create_access_token(
            &self.jwt_secret,
            node_subject,
            common::ROLE_DATANODE,
            self.service_token_exp_seconds,
        )
        .map_err(|err| format!("Could not create datanode service token: {err}"))
    }
}
