use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

use blake2::{Blake2s, Digest};
use log::{info, trace};
use parity_scale_codec::{Decode, Encode};

use crate::error::Error;
use crate::message::MessageType::PrePrepare;
use crate::message::{ExtrinsicProof, ExtrinsicProofs, MessageType, NewView, NormalMessage, ViewChange};
use crate::replica::checkpoint::CheckPoint;
use crate::replica::storage::Storage;
use crate::replica::{SequenceNum, ViewNum, ViewSeqRange, FAULTY_REPLICA_MAX};

/// Inner state of a replica
pub struct State<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub(crate) id: I,
	pub replicas: Vec<I>,
	pub seq_num: SequenceNum,
	pub(crate) view_num: ViewNum,
	pub storage: Storage<I, H>,
	pub stage: Stage,
	// TODO do cleanup, or find another way to check if an extrinsic finalized
	pub finalized: BTreeSet<H>,
	// extrinsic that the replica currently working on
	pub ongoing_extrinsic: Option<H>,
}

impl<I, H> State<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn new(id: I, seq_num: u32) -> State<I, H> {
		State {
			id: id.clone(),
			seq_num,
			replicas: vec![id],
			view_num: 0,
			storage: Storage::new(),
			stage: Stage::Idle,
			finalized: BTreeSet::new(),
			ongoing_extrinsic: None,
		}
	}

	pub fn add_replica(&mut self, replica_id: I) {
		self.replicas.push(replica_id);
		self.replicas.sort();

		trace!(target: "PBFT", "connected peers: {:?}, not enough replicas, running idle", self.replicas);

		if self.stage == Stage::Idle && self.is_replicas_enough() {
			self.stage = Stage::Inited;
			info!(target: "PBFT", "connected to enough replicas, start processing");
		}
	}

	fn is_replicas_enough(&self) -> bool {
		self.replicas.len() > self.min_replicas()
	}

	// minimum replicas required
	fn min_replicas(&self) -> usize {
		2 * FAULTY_REPLICA_MAX + 1
	}

	pub fn is_primary(&self) -> bool {
		self.id == self.primary_id(self.view_num)
	}

	/// check if this replica is the primary for the next view
	pub fn is_next_primary(&self) -> bool {
		self.id == self.primary_id(self.view_num + 1)
	}

	/// Get the ID of the primary replica of current view
	pub fn primary_id(&self, view: ViewNum) -> I {
		let index = view as usize % self.replicas.len();
		self.replicas[index].clone()
	}

	/// Check if the replica is `prepared` for a request:
	/// - has a pre-prepare message with according view and seq num
	/// - has at least2f prepares messages from different backups
	pub fn prepare_status(&self, msg: &NormalMessage<I, H>) -> Result<usize, Error> {
		if self.is_pre_prepared(msg) && self.storage.seq_states.is_enough(msg.key(), MessageType::Prepare) {
			let prepares_num = self.storage.seq_states.get_prepares(msg.key()).unwrap().len();
			Ok(prepares_num)
		} else {
			Err(Error::NotPrePared)
		}
	}

	/// Check if have received PrePrepare message
	pub fn is_pre_prepared(&self, msg: &NormalMessage<I, H>) -> bool {
		if let Some(pre_prepare) = self.storage.seq_states.get_pre_prepare(msg.key()) {
			if pre_prepare.digest == msg.digest {
				return true;
			}
		}

		false
	}

	/// check if the replica is ready to move into `committed-local` stage:
	/// - the replica was in  `prepared` state
	/// - has accepted at least 2f+1 commits (possibly including its own) from different replicas
	pub fn is_committed_local(&self, msg: &NormalMessage<I, H>) -> bool {
		self.prepare_status(msg).is_ok() && self.storage.seq_states.is_enough(msg.key(), MessageType::Commit)
	}

	pub fn create_view_change_message(&self) -> ViewChange<I, H> {
		let (seq, digest, checkpoint_proofs) = {
			if let Some(checkpoint) = &self.storage.last_stable_checkpoint {
				(checkpoint.seq, Some(checkpoint.digest), checkpoint.proofs.clone())
			} else {
				(0, None, Vec::new())
			}
		};

		let proofs = self
			.storage
			.extrinsics_after(seq)
			.iter()
			.map(|e| {
				let pre_prepare = self.storage.seq_states.get_pre_prepare(e.key()).unwrap();
				let prepares = self.storage.seq_states.get_prepares(e.key()).unwrap();
				// seq, view and digest already contained in pre_prepare
				ExtrinsicProof { pre_prepare, prepares }
			})
			.collect();

		ViewChange {
			replica_id: self.id.clone(),
			next_view: self.view_num + 1,
			stable_checkpoint_seq: seq,
			stable_checkpoint_digest: digest,
			checkpoint_proofs,
			extrinsic_proofs: ExtrinsicProofs::new(proofs),
		}
	}

	pub fn create_new_view_message(&mut self) -> (NewView<I, H>, Option<CheckPoint<I, H>>) {
		let ((min_seq, _), pre_prepares) = self.create_new_view_pre_prepares();

		// view num in view change message is of next view
		let view = self.view_num + 1;

		// If min_seq is greater than the sequence number of this replica's last stable checkpoint,
		// insert the proof for min_seq's according checkpoint in the log.
		// Because in view changing stage, a replica only stores ViewChange messages received from
		// other replicas, however, ViewChange message created by itself will be discarded after
		// multicasted to other replicas. It means among those received ViewChange messages,
		// there's the possibility that some stable checkpoints are created later than this
		// replica's last stable checkpoint.
		let stable_checkpoint = self.storage.view_changes.get_checkpoint(view, min_seq);

		// TODO do garbage collection

		(
			NewView {
				view,
				view_changes: self.storage.view_changes.get(view).unwrap().clone(),
				pre_prepares,
			},
			stable_checkpoint,
		)
	}

	/// Generate PrePrepare messages to be included in a NewView message
	pub fn create_new_view_pre_prepares(&self) -> (ViewSeqRange, VecDeque<NormalMessage<I, H>>) {
		// the view num of view-change messages are the num of next view, thus plus 1 on current view num
		let view = self.view_num + 1;

		let (min_seq, max_seq) = self.storage.view_changes.seq_range(view);
		let old_pre_prepares = self.storage.view_changes.get_pre_prepares_in_range(view, min_seq, max_seq);
		let mut pre_prepares = VecDeque::new();
		// construct new PrePrepare messages
		for (seq, pre_prepare) in old_pre_prepares.iter() {
			// TODO if no previous PrePrepare message found for this sequence number, assign the digest of
			// a special null request.
			if pre_prepare.is_none() {
				continue;
			}

			let digest = pre_prepare.as_ref().unwrap().digest;

			let pre_prepare =
				NormalMessage { replica_id: self.id.clone(), msg_type: PrePrepare, view, digest, seq: *seq };
			pre_prepares.push_back(pre_prepare);
		}

		pre_prepares.make_contiguous().sort();
		((min_seq, max_seq), pre_prepares)
	}

	pub fn create_checkpoint(&mut self) -> CheckPoint<I, H> {
		self.storage.checkpoint_cache.sort();
		trace!(target: "PBFT", "create checkpoint at #{} with {:?}", self.seq_num, self.storage.checkpoint_cache);

		let output = Blake2s::digest(&self.storage.checkpoint_cache.encode());
		let digest = H::decode(&mut output.as_slice()).unwrap();

		let checkpoint = CheckPoint::new(self.seq_num, digest);
		self.storage.checkpoint_cache.clear();
		self.storage.add_checkpoint(checkpoint.clone());

		checkpoint
	}

	pub fn is_valid_view_change(&self, message: &ViewChange<I, H>) -> bool {
		let ViewChange { next_view, checkpoint_proofs, extrinsic_proofs, .. } = message;

		// received checkpoint seq is 0 and this replica have no stable checkpoint yet, this situation
		// usually happens just after the system launched
		if *next_view == self.view_num + 1 {
			return if message.stable_checkpoint_seq == 0 && self.storage.last_stable_checkpoint.is_none() {
				true
			} else {
				self.storage.is_valid_checkpoint_proofs(&checkpoint_proofs)
					&& self.is_valid_extrinsic_proofs(extrinsic_proofs)
			};
		}

		false
	}

	/// 1. when backup replica validating a NewView message, it should still in old view, thus replica's
	/// view should be 1 less than NewView message's
	/// 2. the backup replicas generate a set of PrePrepare message based on local's ViewChange messages.
	/// If it equals to the PrePrepare messages in the NewView message, then it's safe to same the primary
	/// of next view have correct ViewChange messages and generated correct PrePrepare messages for
	///the next view
	pub fn is_valid_new_view(&self, _message: &NewView<I, H>) -> bool {
		//let (_, _pre_prepares) = state.create_new_view_pre_prepares();
		// if pre_prepares != self.pre_prepares {
		//     return false;
		// }

		true
	}

	pub fn is_valid_extrinsic_proofs(&self, proofs: &ExtrinsicProofs<I, H>) -> bool {
		let is_valid_proof = |proof: &ExtrinsicProof<I, H>| {
			// verify if current replica has a pre-prepare message with the same sequence and digest in
			// this ExtrinsicProof. Because SeqState contains a digest fields that has the same value as
			// the pre-prepare it includes, so it only requires to find a matching SeqState
			if self.storage.seq_states.any(proof.pre_prepare.key(), proof.pre_prepare.digest) {
				// if replica's pre-prepare message is consistent with the pre_prepare message in extrinsic proof,
				// then verify its consistency with all the prepares in extrinsic proof
				if proof.prepares.iter().any(|p| p.digest != proof.pre_prepare.digest) {
					return false;
				}
			}

			true
		};

		if proofs.proofs.iter().any(|p| !is_valid_proof(p)) {
			false
		} else {
			true
		}
	}

	pub fn is_accepting_message(&self) -> bool {
		self.stage != Stage::ViewChange
	}

	/// In normal mode, replica only accepts extrinsic when:
	/// - it just started
	/// - have precious extrinsic finalized
	/// replica will have extrinsic cached if is in other stages
	pub fn is_accepting_extrinsic(&self) -> bool {
		self.stage == Stage::Inited || self.stage == Stage::Finalized
	}

	pub fn new_view_finalized(&self) -> bool {
		let new_view = self.storage.last_new_view.as_ref();
		if new_view.is_some() {
			new_view.unwrap().pre_prepares.is_empty()
		} else {
			true
		}
	}

	pub fn pop_new_view(&mut self) -> Option<NormalMessage<I, H>> {
		if self.storage.last_new_view.is_some() {
			let new_view = self.storage.last_new_view.as_mut().unwrap();
			return new_view.pre_prepares.pop_front();
		}

		None
	}

	pub fn remove_new_view(&mut self, digest: H) -> bool {
		if self.storage.last_new_view.is_some() {
			let new_view = self.storage.last_new_view.as_mut().unwrap();
			if let Ok(index) = new_view.pre_prepares.binary_search_by_key(&digest, |p| p.digest) {
				new_view.pre_prepares.remove(index);
				return true;
			}
		}
		false
	}
}

/// In `Normal` stage, a replica can receive pre-prepare/prepare/commit messages, when view change happens,
/// changes to `ViewChange` stage.
#[derive(Debug, Copy, Clone, PartialEq)]
/// Stages of a replica
pub enum Stage {
	// not connected to enough replicas
	Idle,
	/// A replica is in one of these status:
	/// - just started and connected to enough replicas
	/// - finished view change process
	Inited,
	PrePrepare,
	Prepare,
	Commit,
	/// Finalized a extrinsic, ready to process next one
	Finalized,
	// processing pre-prepares in a new view message
	Catchup,
	/// This staage means a replica are in one of these status:
	/// - have retrieved a extrinsic, ready to process consensus messages
	/// - have finished a view change process
	WaitingConsensus,
	ViewChange,
}

#[cfg(test)]
mod tests {
	use sp_utils::mpsc;
	use crate::message::{MessageType, NormalMessage};
	use crate::replica::state::State;
	use crate::replica::{PeerId, Replica};
	use crate::test_utility::{mock_replica, Digest, Envi_mocked, TaskHandle, Comm};

	fn new_replica() -> (Replica<Envi_mocked, TaskHandle>, Comm) {
		let last_finalized = 4 as u32;
		let (tx, rv) = mpsc::tracing_unbounded("finalzied");
		let (replica, comms) = mock_replica(last_finalized, PeerId::new(String::from("0")), tx.clone());

		(replica, comm)
	}

	#[tokio::test]
	async fn create_view_change() {
		let (mut replica_1, mut comms_1) = new_replica();
		let (mut replica_2, mut comms_2) = new_replica();
		tokio::spawn(comms_1);
		tokio::spawn(comms_2);

		let extrinsic = Digest::from("abcdef");
		let pre_prepare = replica_1.process_new_block(extrinsic).unwrap();
		assert_eq!(pre_prepare.view, 0);

		replica_2.process_pre_prepare(pre_prepare);
		let view_change = replica_2.state.write().create_view_change_message();
		assert_eq!(view_change.next_view, replica_2.state.read().view_num + 1);
		assert_eq!(view_change.stable_checkpoint_seq, 0);
		assert_eq!(view_change.stable_checkpoint_digest, None);
		let p = view_change.extrinsic_proofs;
		assert_eq!(p.proofs.len(), 1);
		assert_eq!(p.proofs[0].prepares.len(), 1);
		assert_eq!(p.max_seq(), replica_2.state.read().seq_num);
	}

	#[tokio::test]
	async fn extrinsic_proofs_should_empty_if_no_pre_prepare() {

		let (mut replica_1, mut comms_1) = new_replica();
		let (mut replica_2, mut comms_2) = new_replica();
		tokio::spawn(comms_1);
		tokio::spawn(comms_2);

		let extrinsic = Digest::from("abcdef");
		let pre_prepare = replica_1.process_new_block(extrinsic).unwrap();
		let prepare = {
			let State { id, seq_num, view_num, .. } = &*replica_2.state.read();
			NormalMessage {
				replica_id: id.clone(),
				seq: *seq_num,
				view: *view_num,
				msg_type: MessageType::Prepare,
				digest: pre_prepare.digest,
			}
		};

		// if didn't received pre-prepare but have prepare messages, the max_seq should be 0
		replica_2.process_prepare(prepare);
		let view_change = replica_2.state.write().create_view_change_message();
		let p = view_change.extrinsic_proofs;
		assert_eq!(p.max_seq(), 0);
	}
}
