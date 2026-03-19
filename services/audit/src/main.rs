use common::dfs::audit_service_server::{AuditService, AuditServiceServer};
use common::dfs::{AuditRequest, AuditResponse};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

#[derive(Serialize)]
struct AuditLogDocument {
    timestamp: i64,
    user_id: String,
    action: String,
    resource: Option<String>,
    component: String,
    status: String,
    success: bool,
    details: Value,
}

#[derive(Debug, Default)]
pub struct MyAuditService {
    http_client: reqwest::Client,
    elastic_url: String,
}

fn parse_details(details: &str) -> Value {
    let trimmed = details.trim();
    if trimmed.is_empty() {
        return json!({});
    }

    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => value,
        Err(_) => json!({ "message": trimmed }),
    }
}

fn infer_success(status: &str) -> bool {
    matches!(
        status.trim().to_uppercase().as_str(),
        "SUCCESS" | "OK" | "ACK"
    )
}

#[tonic::async_trait]
impl AuditService for MyAuditService {
    async fn log_operation(
        &self,
        request: Request<AuditRequest>,
    ) -> Result<Response<AuditResponse>, Status> {
        let req = request.into_inner();
        info!(
            component = req.component,
            user_id = req.user_id,
            action = req.action,
            "Audit log received"
        );

        let normalized_status = if req.status.trim().is_empty() {
            "UNKNOWN".to_string()
        } else {
            req.status.clone()
        };

        let resource = if req.resource.trim().is_empty() {
            None
        } else {
            Some(req.resource.clone())
        };

        let doc = AuditLogDocument {
            timestamp: req.timestamp,
            user_id: req.user_id,
            action: req.action,
            resource,
            component: req.component,
            success: infer_success(&normalized_status),
            status: normalized_status,
            details: parse_details(&req.details),
        };

        let response = self
            .http_client
            .post(&self.elastic_url)
            .json(&doc)
            .send()
            .await;

        match response {
            Ok(res) => {
                if res.status().is_success() {
                    info!(status = ?res.status(), "Audit log indexed in Elasticsearch");
                } else {
                    warn!(status = ?res.status(), "Elasticsearch rejected audit log");
                }
            }
            Err(e) => error!(error = %e, "Failed to send audit log to Elasticsearch"),
        }

        Ok(Response::new(AuditResponse { ack: true }))
    }
}

fn env_bool(key: &str, default_value: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes" | "y"),
        Err(_) => default_value,
    }
}

fn build_http_client() -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let allow_invalid_certs = env_bool("ELASTIC_ALLOW_INVALID_CERTS", false);
    let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(allow_invalid_certs);

    let elastic_user = env::var("ELASTIC_USER").ok();
    let elastic_password = env::var("ELASTIC_PASSWORD").ok();
    if let (Some(user), Some(password)) = (elastic_user, elastic_password) {
        if !user.is_empty() {
            builder = builder.default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                let value = format!(
                    "Basic {}",
                    {
                        use base64::Engine;
                        use base64::engine::general_purpose::STANDARD;
                        STANDARD.encode(format!("{user}:{password}"))
                    }
                );
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    reqwest::header::HeaderValue::from_str(&value)?,
                );
                headers
            });
        }
    }

    Ok(builder.build()?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = common::init_tracing("audit", "info");

    let listen_addr = env::var("AUDIT_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50053".to_string());
    let elastic_url = env::var("ELASTIC_URL")
        .unwrap_or_else(|_| "http://localhost:9200/audit-logs/_doc".to_string());

    let addr = listen_addr.parse()?;
    let service = MyAuditService {
        http_client: build_http_client()?,
        elastic_url: elastic_url.clone(),
    };

    info!(%addr, "Audit Service listening");
    info!(%elastic_url, "Elasticsearch target configured");

    Server::builder()
        .add_service(AuditServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
