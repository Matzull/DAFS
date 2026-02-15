use common::dfs::audit_service_server::{AuditService, AuditServiceServer};
use common::dfs::{AuditRequest, AuditResponse};
use serde::Serialize;
use serde_json::{json, Value};
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

        let es_url = "http://localhost:9200/audit-logs/_doc";
        let response = self.http_client.post(es_url).json(&doc).send().await;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:50053".parse()?;
    let service = MyAuditService::default();

    info!(%addr, "Audit Service listening");
    info!("Elasticsearch target: http://localhost:9200/audit-logs/_doc");

    Server::builder()
        .add_service(AuditServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
