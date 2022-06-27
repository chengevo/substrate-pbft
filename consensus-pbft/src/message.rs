use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;

use parity_scale_codec::{Decode, Encode};

use crate::replica::{SequenceNum, ViewNum};

pub type NormalMessageIdentifier = (MessageType, ViewNum, SequenceNum);

pub type MessageKey = (ViewNum, SequenceNum);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Encode, Decode, Ord, PartialOrd)]
pub enum MessageType {
	PrePrepare,
	Prepare,
	Commit,
	NewView,
	ViewChange,
}

#[derive(Debug, Encode, Decode)]
pub enum PbftMessage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	Normal(NormalMessage<I, H>),
	CheckPoint(CheckPointMessage<I, H>),
	ViewChange(ViewChange<I, H>),
	NewView(NewView<I, H>),
}

/// Generic type for PRE-PREPARE, PREPARE and COMMIT messages.
// TODO The `digst` field should wraps in the `Option<T>` enum is to handle a situation in view change stage.
#[derive(Eq, Hash, PartialEq, Copy, Clone, Debug, Encode, Decode, PartialOrd, Ord)]
pub struct NormalMessage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub replica_id: I,
	pub msg_type: MessageType,
	pub seq: SequenceNum,
	pub view: ViewNum,
	pub digest: H,
}

impl<I, H> NormalMessage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn key(&self) -> MessageKey {
		(self.view, self.seq)
	}
}

/// checkpoint message
#[derive(Clone, Debug, Encode, Decode)]
pub struct CheckPointMessage<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub replica_id: I,
	// sequence of last request upon generating this checkpoint message
	pub seq: SequenceNum,
	// digest of this checkpoint
	pub digest: H,
}

#[derive(Debug, Clone, Encode, Decode)]
/// Extrinsic proof are a pre-prepare and its according prepare messages to prove the correctness of a
/// request coming into the PBFT system.
pub struct ExtrinsicProof<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub pre_prepare: NormalMessage<I, H>,
	pub prepares: BTreeSet<NormalMessage<I, H>>,
}

#[derive(Clone, Debug, Encode, Decode)]
/// A set of information that proves a replica prepared to process a request. It is generated when view chagne
/// happens.  A proof is a pre-prepare message and its according prepares messages.
/// If when view change happens, a replica received some prepare messages but not pre-prepare message, the generated
/// extrinsic proofs will be empty, because for a replica, an extrinsic was "seen" only and if only its pre-prepared
/// was received
pub struct ExtrinsicProofs<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub proofs: Vec<ExtrinsicProof<I, H>>,
}

impl<I, H> ExtrinsicProofs<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub fn new(proofs: Vec<ExtrinsicProof<I, H>>) -> ExtrinsicProofs<I, H> {
		ExtrinsicProofs { proofs }
	}

	/// Find the largest sequence among all the ExtrinsicProof. It's used when creating a NewView message.
	/// If proofs is empty, return 0.
	pub fn max_seq(&self) -> SequenceNum {
		if self.proofs.len() == 0 {
			return 0;
		}
		self.proofs
			.iter()
			.max_by(|a, b| a.pre_prepare.seq.cmp(&b.pre_prepare.seq))
			.unwrap()
			.pre_prepare
			.seq
	}

	pub fn as_vec(&self) -> &Vec<ExtrinsicProof<I, H>> {
		&self.proofs
	}
}

#[derive(Clone, Encode, Decode)]
/// If there's no stable checkpoint generated when view change happens(it happens when the system just
/// started up), set `stable_checkpoint_seq` to 0 and `stable_checkpoint_digest` to None.
pub struct ViewChange<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub replica_id: I,
	pub next_view: ViewNum,
	// sequence of last stable checkpoint
	pub stable_checkpoint_seq: SequenceNum,
	pub stable_checkpoint_digest: Option<H>,
	pub checkpoint_proofs: Vec<CheckPointMessage<I, H>>,
	pub extrinsic_proofs: ExtrinsicProofs<I, H>,
}

impl<I, H> ViewChange<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	/// Get all PrePrepares message contained in the ExtrinsicProofs
	pub fn all_pre_prepares(&self) -> Vec<NormalMessage<I, H>> {
		let mut v = Vec::new();
		self.extrinsic_proofs
			.as_vec()
			.iter()
			.for_each(|e| v.push(e.pre_prepare.clone()));
		v
	}
}

impl<I, H> Debug for ViewChange<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(
			f,
			"replica_id: {:?}, next view: {:?}, stable checkpoint seq: {}",
			self.replica_id, self.next_view, self.stable_checkpoint_seq
		)
	}
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct NewView<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	// the new view number
	pub(crate) view: ViewNum,
	// valid ViewChange messages received by the primary plus the ViewChange message the primary sent
	pub(crate) view_changes: Vec<ViewChange<I, H>>,
	//  a set of pre-prepare messages
	pub(crate) pre_prepares: VecDeque<NormalMessage<I, H>>,
}

// impl<I, H> Debug for NewView<I, H>
// 	where
// 		I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
// 		H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,{
// 	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
// 		write!(f, "next view: {:?}", self.view)
// 	}
// }

#[derive(Clone, Encode, Decode)]
pub struct Extrinsic<H> {
	pub seq: SequenceNum,
	pub view: ViewNum,
	pub digest: H,
}

impl<H> Extrinsic<H> {
	pub fn key(&self) -> MessageKey {
		(self.view, self.seq)
	}
}
