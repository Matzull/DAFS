use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Arc;

use common::raft::raft_service_client::RaftServiceClient;
use common::raft::{
    AppendEntriesRequest as WireAppendEntriesRequest,
    InstallSnapshotRequest as WireInstallSnapshotRequest, VoteRequest as WireVoteRequest,
};
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::storage::Adaptor;
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::models::MasterState;
use crate::raft::state_machine::MasterStateMachine;
use crate::raft::types::{NodeId, TypeConfig};
use crate::InternalSettings;

pub type MasterRaft = openraft::Raft<TypeConfig>;

#[derive(Clone)]
pub struct MasterRaftContext {
    pub raft: MasterRaft,
    pub node_id: NodeId,
    pub nodes: Arc<RwLock<HashMap<NodeId, BasicNode>>>,
}

impl MasterRaftContext {
    pub async fn leader_hint(&self) -> Option<(NodeId, String)> {
        let leader_id = self.raft.current_leader().await?;
        let nodes = self.nodes.read().await;
        let leader_addr = nodes.get(&leader_id)?.addr.clone();
        Some((leader_id, leader_addr))
    }

    pub async fn is_local_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    pub async fn bootstrap(
        settings: &InternalSettings,
        initial_state: Arc<MasterState>,
    ) -> Result<Self, Box<dyn Error>> {
        let config = openraft::Config {
            cluster_name: settings.raft_cluster_name.clone(),
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);

        let store = MasterStateMachine::new_with_state(initial_state);

        // Auto-detect if cluster was already initialized in persistent storage
        // If there are prior logs or membership, don't bootstrap even if flag is set
        let was_previously_initialized = store.has_prior_initialization().await;

        let (log_store, state_machine_store) = Adaptor::new(store);

        let network = GrpcRaftNetworkFactory;
        let raft = openraft::Raft::new(
            settings.raft_node_id,
            config,
            network,
            log_store,
            state_machine_store,
        )
        .await?;

        let mut nodes = parse_member_nodes(&settings.raft_initial_members)?;
        // The bind address ([::] / 0.0.0.0) is only valid for listening; peers and
        // datanodes must reach this node through a routable address, so advertise
        // the configured one when provided and only fall back to listen_address.
        let self_addr = if settings.advertise_address.is_empty() {
            normalize_http_addr(&settings.listen_address)
        } else {
            normalize_http_addr(&settings.advertise_address)
        };
        if let Some(existing) = nodes.get(&settings.raft_node_id) {
            if existing.addr != self_addr {
                warn!(
                    node_id = settings.raft_node_id,
                    configured = %existing.addr,
                    effective = %self_addr,
                    "raft_initial_members contains local node id with a different address; overriding with local listen address"
                );
            }
        }
        nodes.insert(settings.raft_node_id, BasicNode::new(self_addr));

        // Only bootstrap if:
        // 1. bootstrap flag is set AND
        // 2. cluster is not yet initialized (checked via is_initialized())
        // 3. AND no prior persistent logs/membership found
        if settings.raft_bootstrap && !was_previously_initialized {
            let initialized = raft.is_initialized().await?;
            if !initialized {
                // Bootstrap ONLY with the current node to avoid blocking on unreachable peers
                let mut bootstrap_members = BTreeMap::new();
                bootstrap_members.insert(
                    settings.raft_node_id,
                    nodes.get(&settings.raft_node_id).unwrap().clone(),
                );

                info!(
                    node_id = settings.raft_node_id,
                    bootstrap_flag = settings.raft_bootstrap,
                    previous_init_detected = false,
                    "Bootstrapping Raft cluster with single node. \
                     Other known members will need to join via change_membership"
                );
                let _ = raft.initialize(bootstrap_members).await;
            } else {
                info!(
                    node_id = settings.raft_node_id,
                    "Bootstrap flag set but cluster already initialized in memory, skipping"
                );
            }
        } else if settings.raft_bootstrap && was_previously_initialized {
            warn!(
                node_id = settings.raft_node_id,
                bootstrap_flag = settings.raft_bootstrap,
                "Bootstrap flag set but detected prior initialization in persistent storage; \
                 skipping bootstrap to protect existing cluster"
            );
        }

        Ok(Self {
            raft,
            node_id: settings.raft_node_id,
            nodes: Arc::new(RwLock::new(nodes)),
        })
    }
}

fn parse_member_nodes(items: &[String]) -> Result<HashMap<NodeId, BasicNode>, Box<dyn Error>> {
    let mut nodes = HashMap::new();

    for item in items {
        // Support both repeated CLI flags:
        // --raft-initial-members 1=addr --raft-initial-members 2=addr
        // and a single quoted argument with spaces:
        // --raft-initial-members '1=addr 2=addr'
        for token in item.split_whitespace() {
            let (id_part, addr_part) = token.split_once('=').ok_or_else(|| {
                format!("Invalid raft member format '{token}', expected '<id>=<addr>'")
            })?;

            let node_id: NodeId = id_part.parse()?;
            nodes.insert(node_id, BasicNode::new(normalize_http_addr(addr_part)));
        }
    }
    debug!(?nodes, "Parsed initial raft members");

    Ok(nodes)
}

// fn warn_membership_fault_tolerance(voter_count: usize) {
//     if voter_count <= 1 {
//         return;
//     }

//     if voter_count == 2 {
//         warn!(
//             voter_count,
//             "Raft cluster configured with 2 voters: if one node disconnects, there is no quorum for writes or leader election"
//         );
//         return;
//     }

//     if voter_count % 2 == 0 {
//         warn!(
//             voter_count,
//             "Raft cluster configured with an even number of voters; use an odd number (3,5,...) for better fault tolerance"
//         );
//     }
// }

fn normalize_http_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[derive(Clone, Default)]
pub struct GrpcRaftNetworkFactory;

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    target: NodeId,
    target_node: BasicNode,
}

impl RaftNetworkFactory<TypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork {
            target,
            target_node: node.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let mut client = connect_client(&self.target_node.addr)
            .await
            .map_err(RPCError::Unreachable)?;

        let payload = encode_wire(&rpc).map_err(RPCError::Network)?;
        let response = client
            .append_entries(tonic::Request::new(WireAppendEntriesRequest { payload }))
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

        let remote_result: Result<
            openraft::raft::AppendEntriesResponse<NodeId>,
            RaftError<NodeId>,
        > = decode_wire(&response.into_inner().payload).map_err(RPCError::Network)?;

        remote_result.map_err(|err| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.target_node.clone(),
                err,
            ))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut client = connect_client(&self.target_node.addr)
            .await
            .map_err(RPCError::Unreachable)?;

        let payload = encode_wire(&rpc).map_err(RPCError::Network)?;
        let response = client
            .install_snapshot(tonic::Request::new(WireInstallSnapshotRequest { payload }))
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

        let remote_result: Result<
            openraft::raft::InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = decode_wire(&response.into_inner().payload).map_err(RPCError::Network)?;

        remote_result.map_err(|err| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.target_node.clone(),
                err,
            ))
        })
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        let mut client = connect_client(&self.target_node.addr)
            .await
            .map_err(RPCError::Unreachable)?;

        let payload = encode_wire(&rpc).map_err(RPCError::Network)?;
        let response = client
            .vote(tonic::Request::new(WireVoteRequest { payload }))
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;

        let remote_result: Result<openraft::raft::VoteResponse<NodeId>, RaftError<NodeId>> =
            decode_wire(&response.into_inner().payload).map_err(RPCError::Network)?;

        remote_result.map_err(|err| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.target_node.clone(),
                err,
            ))
        })
    }
}

async fn connect_client(
    addr: &str,
) -> Result<RaftServiceClient<tonic::transport::Channel>, Unreachable> {
    RaftServiceClient::connect(addr.to_string())
        .await
        .map_err(|err| Unreachable::new(&err))
}

fn encode_wire<T: Serialize>(value: &T) -> Result<Vec<u8>, NetworkError> {
    serde_json::to_vec(value).map_err(|err| NetworkError::new(&err))
}

fn decode_wire<T: DeserializeOwned>(payload: &[u8]) -> Result<T, NetworkError> {
    serde_json::from_slice(payload).map_err(|err| NetworkError::new(&err))
}
