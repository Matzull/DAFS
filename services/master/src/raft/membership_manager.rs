use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::time::interval;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use common::raft::raft_service_client::RaftServiceClient;
use common::raft::GetLeaderRequest;
use openraft::BasicNode;

use super::runtime::MasterRaftContext;
use super::types::NodeId;

/// Periodically monitors node health and updates Raft membership:
/// - Leader: Removes unresponsive nodes from voters, re-adds responsive known nodes
/// - Follower: Attempts to register self as voter if not already one
pub struct MembershipManager {
    raft_ctx: Arc<MasterRaftContext>,
    node_health: Arc<RwLock<HashMap<NodeId, bool>>>,
    node_last_down_log: Arc<RwLock<HashMap<NodeId, Instant>>>,
    last_quorum_warning: Arc<RwLock<Option<Instant>>>,
    last_local_role: Arc<RwLock<Option<LocalRaftRole>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalRaftRole {
    Leader,
    Voter,
    Learner,
}

impl LocalRaftRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Voter => "voter",
            Self::Learner => "learner",
        }
    }
}

impl MembershipManager {
    pub fn new(raft_ctx: Arc<MasterRaftContext>, _settings: Arc<crate::InternalSettings>) -> Self {
        Self {
            raft_ctx,
            node_health: Arc::new(RwLock::new(HashMap::new())),
            node_last_down_log: Arc::new(RwLock::new(HashMap::new())),
            last_quorum_warning: Arc::new(RwLock::new(None)),
            last_local_role: Arc::new(RwLock::new(None)),
        }
    }

    /// Spawn a background task that periodically updates cluster membership
    pub fn spawn(self) {
        tokio::spawn(async move {
            // Give initial time for cluster to stabilize
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Reconcile quickly after failures to reduce transient replication errors.
            let mut ticker = interval(Duration::from_secs(2));

            loop {
                ticker.tick().await;

                self.log_local_role_transition().await;

                // Only leader manages membership
                if self.raft_ctx.is_local_leader().await {
                    if let Err(e) = self.update_membership_as_leader().await {
                        error!("Failed to update membership: {}", e);
                    }
                } else {
                    // Non-leaders do not mutate membership directly.
                    // The leader will add responsive known nodes as learners/voters.
                    debug!(node_id = self.raft_ctx.node_id, "Follower waiting for leader membership updates");
                    
                    // Check if cluster has lost quorum
                    self.check_quorum_status().await;
                }
            }
        });
    }

    async fn log_local_role_transition(&self) {
        let metrics = self.raft_ctx.raft.metrics().borrow().clone();
        let local_role = if metrics.current_leader == Some(self.raft_ctx.node_id) {
            LocalRaftRole::Leader
        } else {
            let voters: BTreeSet<NodeId> = metrics.membership_config.voter_ids().collect();
            if voters.contains(&self.raft_ctx.node_id) {
                LocalRaftRole::Voter
            } else {
                LocalRaftRole::Learner
            }
        };

        let mut last_role = self.last_local_role.write().await;
        match *last_role {
            None => {
                info!(
                    node_id = self.raft_ctx.node_id,
                    role = local_role.as_str(),
                    current_leader = ?metrics.current_leader,
                    "Raft local role initialized"
                );
                *last_role = Some(local_role);
            }
            Some(previous_role) if previous_role != local_role => {
                info!(
                    node_id = self.raft_ctx.node_id,
                    previous_role = previous_role.as_str(),
                    role = local_role.as_str(),
                    current_leader = ?metrics.current_leader,
                    "Raft local role changed"
                );
                *last_role = Some(local_role);
            }
            _ => {}
        }
    }

    /// Leader: detect dead/alive nodes and update membership accordingly
    async fn update_membership_as_leader(&self) -> Result<(), Box<dyn std::error::Error>> {
        const MIN_AUTOMATIC_VOTERS: usize = 3;

        let nodes = self.raft_ctx.nodes.read().await;
        let current_nodes: Vec<NodeId> = nodes.keys().copied().collect();
        drop(nodes);

        // Health check all known nodes
        let mut responsive_nodes = Vec::new();

        for node_id in &current_nodes {
            let nodes = self.raft_ctx.nodes.read().await;
            let node = match nodes.get(node_id) {
                Some(n) => n.clone(),
                None => continue,
            };
            drop(nodes);

            if self.is_node_responsive(&node.addr, *node_id).await {
                responsive_nodes.push(*node_id);
                self.update_health_state(*node_id, true).await;
            } else {
                self.update_health_state(*node_id, false).await;
            }
        }

        let current_voters = self.current_voters();
        let current_quorum = (current_voters.len() / 2) + 1;
        let responsive_current_voters = responsive_nodes
            .iter()
            .filter(|id| current_voters.contains(id))
            .count();

        // Any membership change must still be committed under the current voter set.
        // If old-config quorum is already lost, changing membership is impossible.
        if responsive_current_voters < current_quorum {
            self.maybe_warn_quorum_lost(current_voters.len(), responsive_current_voters)
                .await;
            return Ok(());
        }

        if responsive_nodes.is_empty() {
            warn!("No responsive nodes detected, skipping membership update");
            return Ok(());
        }

        let desired_voters: BTreeSet<NodeId> = responsive_nodes.iter().copied().collect();

        // Avoid shrinking a healthy 3+ voter cluster to 2 voters automatically.
        // A 2-voter config is fragile: if one voter dies, a restarted non-voter node
        // cannot help recover quorum until the missing voter returns.
        if current_voters.len() >= MIN_AUTOMATIC_VOTERS && desired_voters.len() < MIN_AUTOMATIC_VOTERS {
            debug!(
                current_voters = ?current_voters,
                desired_voters = ?desired_voters,
                "Skipping automatic membership shrink below 3 voters"
            );
            return Ok(());
        }

        if desired_voters == current_voters {
            debug!(voters = ?desired_voters, "Membership already converged, skipping update");
            return Ok(());
        }

        // Ensure every responsive target exists as learner before voting membership update.
        for node_id in &responsive_nodes {
            if *node_id == self.raft_ctx.node_id {
                continue;
            }

            // If already a voter, no need to add as learner.
            if current_voters.contains(node_id) {
                continue;
            }

            let nodes = self.raft_ctx.nodes.read().await;
            let node = match nodes.get(node_id) {
                Some(n) => n.clone(),
                None => continue,
            };
            drop(nodes);

            self.ensure_learner(*node_id, node).await;
        }

        responsive_nodes.sort_unstable();

        // Attempt membership change with responsive nodes only
        match self
            .raft_ctx
            .raft
            .change_membership(responsive_nodes.clone(), false)
            .await
        {
            Ok(_) => {
                info!(voters = ?responsive_nodes, "Cluster membership updated");
            }
            Err(e) => {
                debug!("Could not update membership: {}", e);
            }
        }

        Ok(())
    }

    async fn ensure_learner(&self, node_id: NodeId, node: BasicNode) {
        match self.raft_ctx.raft.add_learner(node_id, node, true).await {
            Ok(_) => {
                debug!(node_id, "Learner is present");
            }
            Err(e) => {
                debug!(node_id, error = ?e, "Could not ensure learner (may already be in-progress)");
            }
        }
    }

    fn current_voters(&self) -> BTreeSet<NodeId> {
        self.raft_ctx
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect()
    }

    async fn update_health_state(&self, node_id: NodeId, is_responsive: bool) {
        if node_id == self.raft_ctx.node_id {
            return;
        }

        let mut health = self.node_health.write().await;
        let previous = health.insert(node_id, is_responsive);

        match (previous, is_responsive) {
            (Some(false), true) => {
                self.node_last_down_log.write().await.remove(&node_id);
                info!(node_id, "Node recovered and is responsive again");
            }
            (Some(true), false) | (None, false) => {
                self.maybe_warn_unresponsive(node_id).await;
            }
            (Some(false), false) => {
                self.maybe_warn_unresponsive(node_id).await;
            }
            _ => {}
        }
    }

    async fn maybe_warn_unresponsive(&self, node_id: NodeId) {
        const DOWN_WARN_COOLDOWN: Duration = Duration::from_secs(30);

        let now = Instant::now();
        let mut last_down_log = self.node_last_down_log.write().await;
        let should_log = last_down_log
            .get(&node_id)
            .map(|last| now.duration_since(*last) >= DOWN_WARN_COOLDOWN)
            .unwrap_or(true);

        if should_log {
            warn!(node_id, "Node is not responsive");
            last_down_log.insert(node_id, now);
        }
    }

    /// Check if the cluster has lost quorum and warn operator
    async fn check_quorum_status(&self) {
        let metrics = self.raft_ctx.raft.metrics().borrow().clone();
        
        // If we have a leader, quorum is fine
        if metrics.current_leader.is_some() {
            // Reset warning timestamp
            *self.last_quorum_warning.write().await = None;
            return;
        }
        
        // Count how many voters we have and how many are alive
        let voter_ids: BTreeSet<NodeId> = metrics.membership_config.voter_ids().collect();
        let voter_count = voter_ids.len();
        
        if voter_count == 0 {
            return; // Not yet initialized
        }
        
        let health = self.node_health.read().await;
        let responsive_voters: usize = voter_ids
            .iter()
            .filter(|&&id| {
                id == self.raft_ctx.node_id || health.get(&id).copied().unwrap_or(false)
            })
            .count();
        
        let quorum_size = (voter_count / 2) + 1;
        
        // If we don't have enough responsive nodes for quorum, warn
        if responsive_voters < quorum_size {
            self.maybe_warn_quorum_lost(voter_count, responsive_voters)
                .await;
        }
    }

    async fn maybe_warn_quorum_lost(&self, voter_count: usize, responsive_voters: usize) {
        const QUORUM_WARNING_INTERVAL: Duration = Duration::from_secs(30);

        let quorum_size = (voter_count / 2) + 1;
        let now = Instant::now();
        let mut last_warning = self.last_quorum_warning.write().await;

        let should_warn = last_warning
            .map(|last| now.duration_since(last) >= QUORUM_WARNING_INTERVAL)
            .unwrap_or(true);

        if should_warn {
            error!(
                node_id = self.raft_ctx.node_id,
                voter_count,
                responsive_voters,
                quorum_needed = quorum_size,
                "QUORUM LOST: Cluster cannot elect a leader. \
                 Responsive voters ({}) < quorum ({}). \
                 Manual intervention required: restart failed nodes or use forced reconfiguration.",
                responsive_voters,
                quorum_size
            );
            *last_warning = Some(now);
        }
    }

    /// Check if a node is responsive via gRPC
    async fn is_node_responsive(&self, addr: &str, node_id: NodeId) -> bool {
        // Skip self-check (we're obviously online)
        if node_id == self.raft_ctx.node_id {
            return true;
        }

        match RaftServiceClient::connect(addr.to_string()).await {
            Ok(mut client) => {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    client.get_leader(tonic::Request::new(GetLeaderRequest {})),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        debug!(node_id, "Node is responsive");
                        true
                    }
                    Ok(Err(e)) => {
                        debug!(node_id, ?e, "Node RPC error");
                        false
                    }
                    Err(_) => {
                        debug!(node_id, "Node timeout");
                        false
                    }
                }
            }
            Err(e) => {
                debug!(node_id, ?e, "Cannot connect to node");
                false
            }
        }
    }
}

