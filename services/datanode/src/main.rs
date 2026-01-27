use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::fs::{File, create_dir_all};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use std::path::PathBuf;
use tonic::{Request, Response, Streaming, Status};
use common::dfs::{ReadBlockRequest, ReadBlockResponse, WriteBlockRequest, WriteBlockResponse};
use common::dfs::{write_block_request::RequestData};
use common::dfs::data_node_service_server::{DataNodeService, DataNodeServiceServer};

// use tokio::fs::{}

const DATA_FOLDER: &'static str = "./data/";

#[derive(Default)]
pub struct Datanode{}

#[tonic::async_trait]
impl DataNodeService for Datanode {
    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;

    async fn write_block(&self, request: Request<Streaming<WriteBlockRequest>>) -> Result<Response<WriteBlockResponse>, Status> {
        let mut stream = request.into_inner();
        let mut file: Option<File> = None;
        let mut bytes_written: i64 = 0;
        while let Some(msg) = stream.message().await? {
            match msg.request_data {
                Some(RequestData::Metadata(metadata)) => {
                    let mut file_path_pathbuf = PathBuf::from(DATA_FOLDER);
                    file_path_pathbuf.push(metadata.block_id);
                    file = Some(File::create_new(file_path_pathbuf).await.map_err(|err| Status::internal(err.to_string()))?);
                },
                Some(RequestData::ChunkData(chunkdata)) => {
                    if let Some(ref mut f) = file {
                        f.write_all(&chunkdata).await.map_err(|err| Status::internal(err.to_string()))?;
                        bytes_written += chunkdata.len() as i64;
                    }
                    else {
                        return Err(Status::failed_precondition("Metadata missing"));
                    }
                },
                None => {
                    return Err(Status::invalid_argument("The message is empty"));
                }

            }
        }
        if let Some(f) = file {
            f.sync_all().await.map_err(|err| Status::internal(err.to_string()))?;
        }
        Ok(Response::new(WriteBlockResponse { success: true, message: "Everything went well".to_string(), bytes_written: bytes_written }))
    }

    async fn read_block(&self, request: Request<ReadBlockRequest>) -> Result<Response<Self::ReadBlockStream>, Status> {
        let mut file_path_pathbuf = PathBuf::from(DATA_FOLDER);
        file_path_pathbuf.push(request.into_inner().block_id);
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let file_res = File::open(file_path_pathbuf).await;
            let mut buffer = [0u8; 64 * 1024];
            let mut file = match file_res {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(Status::not_found(e.to_string()))).await;
                    return;
                }
            };
            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let response = ReadBlockResponse {
                            chunk_data: buffer[..n].to_vec(),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            return; 
                        }
                        continue;
                    },
                    Err(err) => {
                        let _ = tx.send(Err(Status::internal(err.to_string()))).await;
                        break;
                    }
                }
            }
        });
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream))
    }
}


#[tokio::main]
async fn main() {
    create_dir_all(DATA_FOLDER).await.unwrap();

}
