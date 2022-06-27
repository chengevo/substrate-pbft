use std::fmt::Debug;
use std::hash::Hash;

use parity_scale_codec::{Decode, Encode};

use crate::message::CheckPointMessage;
use crate::replica::{SequenceNum, FAULTY_REPLICA_MAX};

// Maintains the state and proofs of a checkpoiont
#[derive(Clone)]
pub struct CheckPoint<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub(crate) digest: H, // genearted from a checkpoint's state
	pub(crate) seq: SequenceNum,
	pub proofs: Vec<CheckPointMessage<I, H>>,
	pub state: Vec<H>,
	pub(crate) is_stable: bool,
}

impl<I, H> CheckPoint<I, H>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
{
	pub(crate) fn new(seq: SequenceNum, digest: H) -> CheckPoint<I, H> {
		CheckPoint { digest, seq, proofs: Vec::new(), state: Vec::new(), is_stable: false }
	}

	pub(crate) fn key(&self) -> (SequenceNum, H) {
		(self.seq, self.digest)
	}

	pub(crate) fn add_proof(&mut self, msg: CheckPointMessage<I, H>) {
		self.proofs.push(msg);
		if self.proofs.len() >= (1 + 2 * FAULTY_REPLICA_MAX) {
			self.is_stable = true;
		}
		// TODO discard outdated pre-prepares, i.e. garbage collection
	}
}
