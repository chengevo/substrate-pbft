use std::fmt::{Debug, Display};
use std::future::Future;
use std::hash::Hash;

use parity_scale_codec::{Codec, Decode, Encode};
use sc_service::SpawnTaskHandle;

pub trait Environment: Send {
	type ReplicaId: Ord + Clone + Eq + Debug + Hash + PartialEq + Send + Sync + Encode + Decode;
	type H: Hash
		+ Eq
		+ PartialEq
		+ Debug
		+ Ord
		+ Copy
		+ Display
		+ Codec
		+ Send
		+ Sync
		+ Encode
		+ Decode
		+ Display
		+ 'static;
	type Error: std::error::Error;

	fn head_block_num(&self) -> u32;

	fn init_replica_state(&self) -> (Self::ReplicaId, u32);

	// with the given digest, check if the extrinsic(i.e. a block) received/exists?
	fn extrinsic_exists(&self, digest: Self::H) -> bool;

	fn new_replica_id(s: String) -> Self::ReplicaId;

	fn finalize_block(&self, hash: Self::H, number: u32) -> Result<(), Self::Error>;

	fn block_status(&self, digest: Self::H) -> sp_consensus::BlockStatus;
}

pub trait TaskRunner: Send + Unpin + Clone {
	fn execute(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
}

impl TaskRunner for SpawnTaskHandle {
	fn execute(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static) {
		self.spawn_blocking(name, task)
	}
}
