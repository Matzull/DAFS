use tonic::{transport::Server, Request, Response, Status};
use common::dfs::audit_service_server::{AuditService, AuditServiceServer};
use common::dfs::{AuditRequest, AuditResponse};
use serde::Serialize;

// 1. Definimos una estructura "Espejo" para Elasticsearch.
// Necesitamos que derive 'Serialize' para convertirse en JSON.
// Los structs generados por gRPC no tienen 'Serialize' por defecto.
#[derive(Serialize)]
struct AuditLogDocument {
    timestamp: i64,
    user_id: String,
    action: String,
    resource: String,
    component: String,
    status: String,
    details: String,
}

// 2. Definimos nuestro servicio
#[derive(Debug, Default)]
pub struct MyAuditService {
    // Podríamos guardar aquí el cliente HTTP para reutilizarlo
    http_client: reqwest::Client,
}

#[tonic::async_trait]
impl AuditService for MyAuditService {
    async fn log_operation(
        &self,
        request: Request<AuditRequest>, // <-- Lo que llega por gRPC
    ) -> Result<Response<AuditResponse>, Status> {
        
        let req = request.into_inner();
        println!("📝 Recibido log: [{}] {} -> {}", req.component, req.user_id, req.action);

        // 3. Mapeo: Protobuf -> Struct Rust -> JSON
        let doc = AuditLogDocument {
            timestamp: req.timestamp,
            user_id: req.user_id,
            action: req.action,
            resource: req.resource,
            component: req.component,
            status: req.status,
            details: req.details,
        };

        // 4. Enviamos a Elasticsearch (Asíncrono)
        // URL: localhost:9200 / índice "audit-logs" / tipo "_doc"
        let es_url = "http://localhost:9200/audit-logs/_doc";
        
        // Hacemos la petición HTTP POST
        // Nota: En un sistema real, esto debería ir a una cola (Kafka) para no bloquear.
        // Para el TFM, el HTTP directo demuestra integración.
        let response = self.http_client.post(es_url)
            .json(&doc)
            .send()
            .await;

        match response {
            Ok(res) => {
                if res.status().is_success() {
                   // Todo OK
                } else {
                    eprintln!("⚠️ Error Elasticsearch: {:?}", res.status());
                }
            },
            Err(e) => eprintln!("❌ Error conexión Elastic: {}", e),
        }

        // 5. Respondemos al cliente gRPC "Todo OK" (ACK)
        Ok(Response::new(AuditResponse { ack: true }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Inicializar logs de consola
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:50053".parse()?; // Puerto para Auditoría
    let service = MyAuditService::default();

    println!("🛡️  Audit Service escuchando en {}", addr);
    println!("📊 Conectando con Elasticsearch en localhost:9200...");

    Server::builder()
        .add_service(AuditServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}