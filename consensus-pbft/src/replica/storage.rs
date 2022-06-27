use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use parity_scale_codec::{Decode, Encode};

use crate::message::{
	CheckPointMessage, Extrinsic, MessageKey, MessageType, NewView, NormalMessage, ViewChange,
};
use crate::replica::checkpoint::CheckPoint;
use crate::replica::{SequenceNum, ViewNum, ViewSeqRange, FAULTY_REPLICA_MAX};

/// Storage for PBFT messages and extrinsic messages
pub struct Storage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	last_checkpoint_seq: SequenceNum,
	pub checkpoint_cache: Vec<H>, // temporal storage for a checkpoint
	pub extrinsic: HashMap<H, Extrinsic<H>>,
	// TODO maybe another data structure
	// the key for this HashMap storing checkpoings should be a pair of seq and digest because:
	// - for the same seq, a replica may generate a checkpoint later than checkpoints received from other replicas,
	//   in this case, those checkpoints will be kept
	// - for the same seq, if the replica generate a checkpoint with different digest than other replicas, the replica
	//   have to store multiple checkpoints for this same seq, thus each checkpoint should be identified by the combination
	//   of (seq, digest). Also each checkpoint carries its own proofs, whenever a checkpoint becomes stable, other checkpoints
	//   in the same seq will be discarded
	checkpoints: HashMap<(SequenceNum, H), CheckPoint<I, H>>,
	pub last_stable_checkpoint: Option<CheckPoint<I, H>>,
	pub view_changes: ViewChanges<I, H>,
	pub seq_states: SeqStates<I, H>,
	// last new view message
	pub last_new_view: Option<NewView<I, H>>,
}

impl<I, H> Storage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn new() -> Storage<I, H> {
		Storage {
			last_checkpoint_seq: 0,
			checkpoint_cache: Vec::new(),
			extrinsic: HashMap::new(),
			checkpoints: HashMap::new(),
			last_stable_checkpoint: None,
			view_changes: ViewChanges { views: HashMap::new() },
			seq_states: SeqStates(HashMap::new()),
			last_new_view: None,
		}
	}

	pub fn get_checkpoint(&mut self, seq: SequenceNum, digest: H) -> Option<&mut CheckPoint<I, H>> {
		self.checkpoints.get_mut(&(seq, digest))
	}

	pub fn add_checkpoint(&mut self, checkpoint: CheckPoint<I, H>) {
		// if a checkpoint with the same seq, digest already exists, it means this replica have received
		// checkpoint from other replicas. Do not replace existing checkpoint because it may carries
		// checkpoint proofs
		if self.get_checkpoint(checkpoint.seq, checkpoint.digest).is_none() {
			self.checkpoints.insert(checkpoint.key(), checkpoint);
		}
	}

	pub fn get_extrinsic_message(&self, digest: &H) -> Option<&Extrinsic<H>> {
		self.extrinsic.get(digest)
	}

	pub fn add_extrinsic_message(&mut self, digest: H, message: Extrinsic<H>) {
		self.extrinsic.insert(digest, message);
	}

	pub fn extrinsics_after(&self, seq: SequenceNum) -> Vec<&Extrinsic<H>> {
		self.extrinsic.values().filter(|e| e.seq >= seq).collect()
	}

	pub fn is_valid_checkpoint_proofs(&self, _proofs: &Vec<CheckPointMessage<I, H>>) -> bool {
		if let Some(_checkpoint) = &self.last_stable_checkpoint {
			//return checkpoint.proofs == *proofs;
			return true;
		}

		false
	}
}

/// Storage for ViewChange messages. Only stores ViewChange messages received from other replicas
pub struct ViewChanges<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	// key is the next_view
	views: HashMap<ViewNum, Vec<ViewChange<I, H>>>,
}

impl<I, H> ViewChanges<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn is_enough(&self, view: ViewNum) -> bool {
		if let Some(changes) = self.views.get(&view) {
			return changes.len() >= 2 * FAULTY_REPLICA_MAX;
		}

		false
	}

	pub fn insert(&mut self, view_change: ViewChange<I, H>) {
		if let Some(view_changes) = self.views.get_mut(&view_change.next_view) {
			view_changes.push(view_change);
		} else {
			self.views.insert(view_change.next_view, vec![view_change]);
		}
	}

	pub fn get(&self, view: ViewNum) -> Option<&Vec<ViewChange<I, H>>> {
		self.views.get(&view)
	}

	/// find the sequence range required to create a NewView message:
	/// - the smallest stable checkpoint sequence, i.e. iterate over all the ViewChanges for tha view,
	///   find the smallest `last_check_point` value
	/// - the highest sequence, i.e. iterate over all the PrePrepare message contains in the ViewChanges,
	///   find the largest `seq` value
	pub fn seq_range(&self, view: ViewNum) -> ViewSeqRange {
		let view_changes = self.views.get(&view).unwrap();
		let mut min_s = view_changes.first().unwrap().stable_checkpoint_seq;
		let mut max_s = view_changes.first().unwrap().extrinsic_proofs.max_seq();

		self.views.get(&view).unwrap().iter().for_each(|view_change| {
			if view_change.stable_checkpoint_seq < min_s {
				min_s = view_change.stable_checkpoint_seq
			}

			let seq = view_change.extrinsic_proofs.max_seq();
			if seq > max_s {
				max_s = seq
			}
		});

		(min_s, max_s)
	}

	/// Among all the VIEW-CHANGE messages received at this view, for each sequence inside the range `min_seq..max_seq`,
	/// find according PrePrepare message with the highest view number. The reason to get "highest view number" is
	/// there may be multiple successful view change happened since last stable checkpoint to now. Since request
	/// processed in this period haven't "package" into a stable checkpoint, they need to be processed again
	/// if view change occurs.
	pub fn get_pre_prepares_in_range(
		&self, view: ViewNum, min_seq: SequenceNum, max_seq: SequenceNum,
	) -> HashMap<SequenceNum, Option<NormalMessage<I, H>>> {
		let mut pre_prepares: HashMap<SequenceNum, Option<NormalMessage<I, H>>> = HashMap::new();

		for i in min_seq..max_seq + 1 {
			pre_prepares.insert(i, None);
		}

		self.views.get(&view).unwrap().iter().for_each(|view_change| {
			view_change.all_pre_prepares().iter().for_each(|p| {
				// sequences in the range `(min_seq, max_seq)` should be a superset of all the sequence numbers
				// contains in all view_change's pre_prepares, so `unwrap` should be appropriate here
				if let Some(pre_prepare) = pre_prepares.get(&p.seq).unwrap() {
					if pre_prepare.view < p.view {
						pre_prepares.insert(p.seq, Some(p.clone()));
					}
				} else {
					pre_prepares.insert(p.seq, Some(p.clone()));
				}
			})
		});

		pre_prepares
	}

	/// Find an stable checkpoint in a ViewChange by its according stable checkpoint sequence
	pub fn get_checkpoint(&self, view: ViewNum, seq: SequenceNum) -> Option<CheckPoint<I, H>> {
		let view_change = self
			.get(view)
			.unwrap()
			.iter()
			.find(|view_change| view_change.stable_checkpoint_seq == seq)
			.unwrap();

		if view_change.stable_checkpoint_digest.is_some() {
			let mut checkpoint = CheckPoint::new(seq, view_change.stable_checkpoint_digest.unwrap());
			checkpoint.proofs = view_change.checkpoint_proofs.clone();
			checkpoint.is_stable = true;

			return Some(checkpoint);
		}

		None
	}
}

/// This struct represent the state of each sequence.
pub struct SeqState<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	seq: SequenceNum,
	view: ViewNum,
	// digest for extrinsic in the sequence
	digest: H,
	pub pre_prepare: Option<NormalMessage<I, H>>,
	pub(crate) prepares: BTreeSet<NormalMessage<I, H>>,
	pub(crate) commits: HashSet<NormalMessage<I, H>>,
}

impl<I, H> SeqState<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn new(message: &NormalMessage<I, H>) -> SeqState<I, H> {
		SeqState {
			view: message.view,
			seq: message.seq,
			digest: message.digest,
			pre_prepare: None,
			prepares: BTreeSet::new(),
			commits: HashSet::new(),
		}
	}

	pub fn enough_prepares(&self) -> bool {
		self.prepares.len() >= 2 * FAULTY_REPLICA_MAX
	}

	pub fn enough_commits(&self) -> bool {
		self.commits.len() >= 2 * FAULTY_REPLICA_MAX
	}
}

/// Because of PBFT's view change mechanism, a extrinsic with the same sequence could be re-processed
/// in subsequent views, thus the storage should use `(view, seq)` as the identifier for a extrinsic
/// in a specific view
pub struct SeqStates<I, H>(HashMap<MessageKey, SeqState<I, H>>)
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd;

impl<I, H> SeqStates<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	/// Given the uncertainty of message transmission in the network, the pre-prepare/prepare/commit messages
	/// for the same request may arrive at different order, i.e. a prepare message may arrive earlier than
	/// its pre-prepare message. Thus this struct is created whatever type of message arrived firstly and fill
	/// in according information.
	pub(crate) fn insert(&mut self, message: NormalMessage<I, H>) {
		let seq_state = if let Some(state) = self.0.get_mut(&message.key()) {
			state
		} else {
			let key = message.key();
			let state = SeqState::new(&message);
			self.0.insert(key.clone(), state);
			self.0.get_mut(&key).unwrap()
		};

		match message.msg_type {
			MessageType::PrePrepare => {
				seq_state.pre_prepare = Some(message);
			},
			MessageType::Prepare => {
				seq_state.prepares.insert(message.clone());
			},
			MessageType::Commit => {
				seq_state.commits.insert(message.clone());
			},
			_ => {},
		}
	}

	pub fn get_pre_prepare(&self, key: MessageKey) -> Option<NormalMessage<I, H>> {
		if let Some(state) = self.0.get(&key) {
			return state.pre_prepare.clone();
		}

		None
	}

	pub fn get_prepares(&self, key: MessageKey) -> Option<BTreeSet<NormalMessage<I, H>>> {
		if let Some(state) = self.0.get(&key) {
			return Some(state.prepares.clone());
		}

		None
	}

	/// For each sequence in a view(defined by MessageKey(view, seq), check if have enough Prepare/Commit
	/// messages to move on to next stage.
	pub(crate) fn is_enough(&self, key: MessageKey, msg_type: MessageType) -> bool {
		match msg_type {
			MessageType::Prepare => self.0.get(&key).unwrap().enough_prepares(),
			MessageType::Commit => self.0.get(&key).unwrap().enough_commits(),
			MessageType::PrePrepare => true,
			_ => true,
		}
	}

	pub fn any(&self, key: MessageKey, digest: H) -> bool {
		if let Some(state) = self.0.get(&key) {
			return state.digest == digest;
		}

		false
	}
}
