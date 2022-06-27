//! A replica represents a node in a PBFT network
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, thread};

use futures::prelude::*;
use futures::{Future, Stream};
use log::{debug, error, info, trace, warn};
use parity_scale_codec::{Decode, Encode};
use parking_lot::RwLock;
use rand::Rng;
use sp_runtime::traits::Header as HeaderT;
use sp_utils::mpsc::TracingUnboundedSender;

use crate::environment::{Environment, TaskRunner};
use crate::error::Error;
use crate::error::Error::InvalidMessage;
use crate::message::{
	CheckPointMessage, Extrinsic, MessageType, NewView, NormalMessage, PbftMessage, ViewChange,
};
use crate::replica::checkpoint::CheckPoint;
use crate::replica::state::{Stage, State};
use crate::replica::timer::TimerSwitch;

mod checkpoint;
mod config;
pub mod state;
mod storage;
pub mod timer;

pub type SequenceNum = u32;
pub type ViewNum = u32;

// max amount of faulty replicas in the network
pub static FAULTY_REPLICA_MAX: usize = 1;
// for every INTERVAL number of seqs to make a checkpoint
pub static CHECKPOINT_INTERVAL: u32 = 10;

/// Sequence number range in a view.
pub type ViewSeqRange = (SequenceNum, SequenceNum);

pub struct Replica<E: Environment, T: TaskRunner> {
	pub env: E,
	pub state: Arc<RwLock<State<E::ReplicaId, E::H>>>,
	pub extrinsic_in_channel: Pin<Box<dyn Stream<Item = E::H> + Send>>,
	// read consensus messages from the network
	pub consensus_in: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
	pub consensus_out: Arc<RwLock<TracingUnboundedSender<Vec<u8>>>>,
	pub task_handle: T,
	pub commit_timers: HashMap<SequenceNum, TimerSwitch<E::ReplicaId, E::H, T>>,
	/// Temporal storage for extrinsic, every extrinsic will come to cache before being processed for two puropose:
	/// - to maintain the consistency of the system, the seq number will only be advanced after the primary finalized
	///   a extrinsic, during the period of the pre-prepare being broadcasted and finalized, the primary won't be
	///   able to assign a seq number for incoming extrinsic. For that reason, extrinsic during that period must be
	///   cached until last extrinsic being finalized
	/// - after a view change, the new primary will only re-process extrinsics between the sequence of minimum
	///   last stable checkpoint to the point view change happened. Any newly received extrinsic have to be cached,
	///   otherwise the consensus system don't even know they exist.
	pub extrinsic_cache: VecDeque<E::H>,
}

impl<E: Environment, T: TaskRunner> Replica<E, T> {
	pub fn new(
		env: E, task_handle: T, extrinsic_in: Pin<Box<dyn Stream<Item = E::H> + Send>>,
		consensus_in: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
		consensus_out: TracingUnboundedSender<Vec<u8>>,
	) -> Replica<E, T> {
		let (peer_id, seq_num) = env.init_replica_state();

		Replica {
			env,
			extrinsic_in_channel: extrinsic_in,
			state: Arc::new(RwLock::new(State::new(peer_id, seq_num))),
			consensus_in,
			consensus_out: Arc::new(RwLock::new(consensus_out)),
			task_handle,
			commit_timers: HashMap::new(),
			extrinsic_cache: VecDeque::new(),
		}
	}

	pub fn process_pre_prepare(
		&mut self, pre_prepare: NormalMessage<E::ReplicaId, E::H>,
	) -> Result<(), Error> {
		let NormalMessage { seq, view, digest, .. } = pre_prepare;
		{
			let mut state = self.state.write();
			state.storage.add_extrinsic_message(digest, Extrinsic { seq, view, digest });
			state.storage.seq_states.insert(pre_prepare.clone());

			// If the replica is in Catchup stage meaning it's still processing pre-prepares from last new-view message,
			// and this pre-prepare message is not coming from the primary but retrieved from last new-view message.
			if state.stage != Stage::Catchup {
				state.stage == Stage::PrePrepare;
			}
		}

		self.try_prepare(&pre_prepare);

		Ok(())
	}

	fn verify_message(&mut self, view: ViewNum, seq: SequenceNum, digest: E::H) -> Result<(), Error> {
		// check if replica has the same view as the message
		let state = self.state.read();

		if state.view_num != view {
			let err_msg = format!("replica view: {}, message view: {}", state.view_num, view);
			return Err(InvalidMessage(err_msg));
		}

		// check if has accepted a pre-prepare message for view and sequence number containing a different digest
		if let Some(msg) = state.storage.seq_states.get_pre_prepare((view, seq)) {
			if msg.digest != digest {
				let err = format!(
					"digest not match for view {}, seq {}, existing: {}, received: {}",
					view, seq, msg.digest, digest
				);
				return Err(InvalidMessage(err));
			}
		}

		Ok(())
	}

	pub fn process_prepare(&mut self, message: NormalMessage<E::ReplicaId, E::H>) -> Result<(), Error> {
		let NormalMessage { seq, digest, .. } = message;

		self.state.write().storage.seq_states.insert(message.clone());
		let status = self.state.read().prepare_status(&message);

		// every time when receiving a prepare message, check if the replica meet the condition for prepared
		if status.is_ok() && status.unwrap() == 2 * FAULTY_REPLICA_MAX {
			// only send commit message when received prepares equal to the minimum amount required
			// otherwise the replica will broadcast multiple commit message
			let commit = PbftMessage::Normal(NormalMessage {
				replica_id: self.state.read().id.clone(),
				msg_type: MessageType::Commit,
				seq,
				view: self.state.read().view_num,
				digest,
			});

			if self.state.read().stage != Stage::Catchup {
				self.state.write().stage == Stage::Prepare;
			}

			self.save_and_broadcast(commit);
		}

		Ok(())
	}

	pub fn process_commit(&mut self, commit: NormalMessage<E::ReplicaId, E::H>) -> Result<(), Error> {
		let NormalMessage { seq, digest, .. } = commit;
		// for consistency, accepts every commit message even if this extrinsic may already finalized
		self.state.write().storage.seq_states.insert(commit.clone());

		if self.try_skip_finalized(&commit) {
			return Ok(());
		}

		self.state.write().stage = Stage::Commit;

		if self.state.read().is_committed_local(&commit) {
			self.accept_extrinsic(digest, seq);
			self.stop_commit_timer(seq);

			self.state.write().stage = Stage::Finalized;
			self.state.write().seq_num += 1;
			self.state.write().finalized.insert(digest);
			self.state.write().ongoing_extrinsic = None;

			// a replica may receive a pre-prepare before the extrinsic
			match self.extrinsic_cache.binary_search(&digest) {
				Ok(index) => {
					self.extrinsic_cache.remove(index);
					trace!(target: "PBFT", "extrinsic {} found and removed from cache", digest);
				},
				Err(_) => warn!(target: "PBFT", "extrinsic {} not found in cache", digest),
			}

			if let Some(extrinsic) = self.extrinsic_cache.pop_front() {
				trace!(target: "PBFT", "extrinsic cache not empty, move on to ({})", extrinsic);
				if let Some(pre_prepare) = self.process_new_block(extrinsic) {
					self.save_and_broadcast(PbftMessage::Normal(pre_prepare));
				}
			}
		}

		Ok(())
	}

	fn try_skip_finalized(&mut self, commit: &NormalMessage<E::ReplicaId, E::H>) -> bool {
		if self.state.read().finalized.contains(&commit.digest) {
			trace!(target: "PBFT", "extrinsic {} already finalized", commit.digest);

			if self.state.read().stage == Stage::Catchup {
				if self.state.read().is_primary() {
					if self.state.write().remove_new_view(commit.digest) {
						trace!(target: "PBFT", "pre-prepare {} from last new view committed and removed", commit.digest);
					} else {
						self.state.write().stage = Stage::Inited;
						self.state.write().storage.last_new_view = None;
						debug!(target: "PBFT", "primary moved to {:?} stage", Stage::Inited);
					}
				} else {
					self.try_catchup();
				}
			}

			return true;
		}

		false
	}

	// When finalizing a request, clear timers for current and previous seqs
	fn stop_commit_timer(&mut self, seq: SequenceNum) {
		// if timer is not found, it means this extrinsic have already been finalized
		// TODO may be a more elegant approach to determine if an extrinsic was finalized?
		if let Some(mut timer) = self.commit_timers.remove(&seq) {
			let _ = &timer.drop();
			debug!(target: "PBFT", "reached committed-local, timer for #{} stopped", seq);
		}
	}

	pub fn process_checkpoint(&mut self, msg: CheckPointMessage<E::ReplicaId, E::H>) -> Result<(), Error> {
		let checkpoint = {
			if let Some(checkpoint) = self.state.write().storage.get_checkpoint(msg.seq, msg.digest) {
				checkpoint.add_proof(msg.clone());
				checkpoint.clone()

			//warn!(target: "PBFT", "failed to add checkpoint proof, proof exists, replica_id: ({:?}), #{}, ({})", msg.replica_id, msg.seq, &msg.digest);
			} else {
				// for a checkpoint identified by a pair (seq, digest) not found in this replica, this may because the replica
				// haven't generate a checkpoint for this seq yet or because they have different digest. Either way the received
				// checkpoint will be kept
				let mut checkpoint = CheckPoint::new(msg.seq, msg.digest);
				checkpoint.add_proof(msg.clone());
				checkpoint
			}
		};

		if checkpoint.is_stable {
			self.state.write().storage.last_stable_checkpoint = Some(checkpoint);
			debug!(target: "PBFT", "checkpoint #{}, ({:?}) became stable", msg.seq, msg.digest);
		} else {
			self.state.write().storage.add_checkpoint(checkpoint);
		}

		Ok(())
	}

	pub fn process_view_change(&mut self, msg: ViewChange<E::ReplicaId, E::H>) -> Result<(), Error> {
		let create_new_view = {
			// deliberately holds a lock here, to make sure that view_num remains the same when accepting
			// the view change message and calling is_next_primary().
			let mut state = self.state.write();
			if state.is_valid_view_change(&msg) {
				state.storage.view_changes.insert(msg.clone());
			} else {
				return Err(InvalidMessage(format!("Received invalid ViewChange message: {:?}", msg)));
			}

			state.is_next_primary() && state.storage.view_changes.is_enough(msg.next_view)
		};

		// only the primary for next view has the authority to create a new view message
		if create_new_view {
			self.state.write().stage = Stage::ViewChange;
			self.commit_timers.clear();

			debug!(target: "PBFT", "primary for next view ({}), received enough view change messages, creating new-view message", self.state.read().view_num+1);

			let (new_view, stable_checkpoint) = self.state.write().create_new_view_message();

			for p in new_view.pre_prepares.iter() {
				// these pre-prepare message no need to broadcast to other replicas because they
				// contained in the new-view message
				self.state.write().storage.seq_states.insert(p.clone());
			}

			let seq = {
				let state = self.state.read();
				let checkpoint = state.storage.last_stable_checkpoint.as_ref();
				if checkpoint.is_some() {
					Some(checkpoint.unwrap().seq)
				} else {
					// if no last_stable_checkpoint found, use -1 to compare with stable checkpoint
					// found in view change message
					None
				}
			};

			if seq.is_some()
				&& stable_checkpoint.is_some()
				&& seq.unwrap() < stable_checkpoint.as_ref().unwrap().seq
			{
				self.state.write().storage.last_stable_checkpoint = stable_checkpoint;
			}

			self.state.write().view_num += 1;
			self.recover_extrinsic();
			self.save_and_broadcast(PbftMessage::NewView(new_view.clone()));

			if self.state.read().new_view_finalized() {
				self.state.write().stage = Stage::Inited;
			} else {
				self.state.write().stage = Stage::Catchup;
			}

			info!(target: "PBFT",
				"promoted to primary replica for next view: (#{}) at ({:?}) stage",
				self.state.read().view_num, self.state.read().stage);
		}

		Ok(())
	}

	pub fn broadcast_checkpoint(&self) {
		let message = {
			let checkpoint = self.state.write().create_checkpoint();
			CheckPointMessage {
				replica_id: self.state.read().id.clone(),
				seq: checkpoint.seq,
				digest: checkpoint.digest,
			}
		};

		self.save_and_broadcast(PbftMessage::CheckPoint(message));
	}

	/// if there's an extrinsic working in progress when view change happens, put it back to the cache
	fn recover_extrinsic(&mut self) {
		if self.state.read().ongoing_extrinsic.is_some() {
			let extrinsic = self.state.read().ongoing_extrinsic.clone().unwrap();
			self.state.write().ongoing_extrinsic = None;
			self.extrinsic_cache.push_front(extrinsic);
		}
	}

	fn process_new_view(&mut self, new_view: NewView<E::ReplicaId, E::H>) -> Result<(), Error> {
		// TODO verify message in pre_prepares
		self.commit_timers.clear();
		self.recover_extrinsic();

		let view = self.state.write().view_num + 1;
		self.state.write().view_num = view;
		self.state.write().storage.last_new_view = Some(new_view);

		if !self.state.read().new_view_finalized() {
			self.state.write().stage = Stage::Catchup;
		} else {
			self.state.write().stage = Stage::Inited;
		}

		info!(target: "PBFT", "new view message processed, current view: ({}), at ({:?}) stage",
			self.state.read().view_num, self.state.read().stage);

		Ok(())
	}

	/// for a verified PrePrepare message:
	/// - add to log
	/// - create a new Prepare message and multicast it
	fn try_prepare(&mut self, pre_prepare: &NormalMessage<E::ReplicaId, E::H>) {
		// Because of the latency of message transmission in network, the primary may received extrinsic
		// earlier than other backups received the PRE-PREPARE message it broadcasts. Only broadcast
		// PREPARE message when extrinsic was in chain. Because verified PRE-PREPARE messages are stored
		// in backups, everytime a extrinsic comes in, will try to retrieve the unhandled PRE-PREPARE message
		// for that extrinsic and move on to next stage.

		if self.env.extrinsic_exists(pre_prepare.digest) {
			let prepare = {
				let state = self.state.read();
				NormalMessage {
					replica_id: state.id.clone(),
					msg_type: MessageType::Prepare,
					seq: pre_prepare.seq,
					view: state.view_num,
					digest: pre_prepare.digest,
				}
			};

			self.save_and_broadcast(PbftMessage::Normal(prepare));
		}
	}

	/// PBFT message send by current replica won't received by itself, thus save a copy before broadcasting
	fn save_and_broadcast(&self, message: PbftMessage<E::ReplicaId, E::H>) {
		self.consensus_out
			.read()
			.unbounded_send(message.encode())
			.expect(&format!("Failed to send PBFT message: {:?}", message));

		trace!(target: "PBFT", "sent: {:?}", &message);

		// TODO handle all message types
		// 1. view change message is handled in TimerSwitch
		// 2. it seems no need to store new-view message
		match message {
			PbftMessage::Normal(m) => self.state.write().storage.seq_states.insert(m),
			PbftMessage::CheckPoint(m) => {
				let mut state = self.state.write();
				let checkpoint = state.storage.get_checkpoint(m.seq, m.digest).unwrap();
				checkpoint.add_proof(m);
			},
			PbftMessage::NewView(m) => {
				self.state.write().storage.last_new_view = Some(m);
			},

			_ => (),
		}
	}

	fn accept_extrinsic(&self, digest: E::H, seq: SequenceNum) {
		// TODO at the stage, instruct outside service to execute the operation in the request/block
		if let Ok(_) = self.env.finalize_block(digest, seq as u32) {
			info!(target: "PBFT", "extrinsic finalized, seq: {}, digest: {}", seq, digest);
		}
	}

	/// Called when received new block notification from the upstream BlockImport(e.g. AuraBlockImport)
	/// - for the primary replica, save and broadcast a PRE-PREPARE message to other backups. The primary replica
	///   will not start a timer: for whatever reason backups not receiving the PRE-PREPARE message, either because
	///   of a malicious or network issue, once the commit timer in a backup expires will trigger a view change
	/// - for a backup, it will try to locate a unhandled PRE-PREPARE message for this block, and starts a
	///   commit timer(if no timer is running).
	pub fn receive_new_block(&mut self, cx: &mut Context) -> Result<(), Error> {
		while let Poll::Ready(Some(item)) = Stream::poll_next(Pin::new(&mut self.extrinsic_in_channel), cx) {
			trace!(target: "PBFT", "received new extrinsic: {}, saved to cache", item);
			self.extrinsic_cache.push_back(item);

			if self.state.read().is_accepting_extrinsic() {
				let extrinsic = self.extrinsic_cache.pop_front().unwrap();
				trace!(target: "PBFT", "retrieved extrinsic ({}) from cache, replica seq: ({})", extrinsic, self.state.read().seq_num);

				if let Some(pre_prepare) = self.process_new_block(extrinsic) {
					self.save_and_broadcast(PbftMessage::Normal(pre_prepare));
				}
			} else if self.state.read().stage == Stage::Catchup && !self.state.read().is_primary() {
				trace!(target: "PBFT", "replica in {:?} stage, extrinsic ({}) cached but not processed", self.state.read().stage, item);
				self.try_catchup();
			}
		}

		Ok(())
	}

	fn try_catchup(&mut self) {
		let pre_prepare = self.state.write().pop_new_view();

		if pre_prepare.is_some() {
			let p = pre_prepare.unwrap();
			trace!(target: "PBFT", "processing pre-prepare from last new view: ({:?}) e", &p);
			self.process_pre_prepare(p);
		} else {
			self.state.write().stage = Stage::Inited;
			self.state.write().storage.last_new_view = None;
			debug!(target: "PBFT", "replica moved to {:?} stage", Stage::Inited);
		}
	}

	fn process_new_block(&mut self, item: E::H) -> Option<NormalMessage<E::ReplicaId, E::H>> {
		self.state.write().stage = Stage::WaitingConsensus;
		self.state.write().ongoing_extrinsic = Some(item);
		self.state.write().storage.checkpoint_cache.push(item);

		let seq_num = self.state.read().seq_num;
		if seq_num >= CHECKPOINT_INTERVAL && seq_num % CHECKPOINT_INTERVAL == 0 {
			self.broadcast_checkpoint();
		}

		// The original thought was only backups need to have commit timers. However, a primary may become
		// a backup, so all types of replica need to start commit timers.
		//
		// For backups, commit timer should start not matter retrieved a PRE-PREPARE  or not. Missing
		// or belated PRE-PREPARE could result from a malicious primary or network issue. Either way once the commit
		// timer expires, should start a view change.
		let mut timer_switch = TimerSwitch::new(
			self.state.read().seq_num,
			self.state.clone(),
			self.consensus_out.clone(),
			Duration::from_secs(10),
			self.task_handle.clone(),
		);
		timer_switch.start();
		self.commit_timers.insert(timer_switch.seq, timer_switch);

		if self.state.read().is_primary() {
			let pre_prepare = {
				let State { id, view_num, seq_num, .. } = &*self.state.read();
				NormalMessage {
					replica_id: id.clone(),
					msg_type: MessageType::PrePrepare,
					seq: *seq_num,
					view: *view_num,
					digest: item,
				}
			};

			self.state.write().stage = Stage::PrePrepare;
			Some(pre_prepare)
		} else {
			if self.state.read().finalized.contains(&item) {
				return None;
			}
			// An workaround for immutable/mutable reference conflict:
			// `state.storage.get_extrinsic_message(&item)` holds an immutable reference,
			// however, self.try_prepare() requires an mutable reference. Thus put the former inside a
			// separate scope
			let mut pre_prepare = None;
			{
				let state = self.state.read();
				if let Some(extrinsic) = state.storage.get_extrinsic_message(&item) {
					let Extrinsic { seq, view, .. } = extrinsic;
					let p = state.storage.seq_states.get_pre_prepare((*view, *seq)).unwrap();
					pre_prepare = Some(p);
				}
			}

			if pre_prepare.is_some() {
				self.try_prepare(&pre_prepare.unwrap());
			}

			None
		}
	}

	pub fn receive_pbft_messages(&mut self, cx: &mut Context) -> Result<(), Error> {
		while let Poll::Ready(Some(message)) = Stream::poll_next(Pin::new(&mut self.consensus_in), cx) {
			match <PbftMessage<E::ReplicaId, E::H>>::decode(&mut &message[..]) {
				Ok(msg) => {
					trace!(target: "PBFT", "received: {:?}", &msg);
					self.dispatch_pbft_message(msg);
				},

				Err(e) => error!(target: "PBFT", "Cound not decode PBFT message, {}", e),
			}
		}

		Ok(())
	}

	fn dispatch_pbft_message(&mut self, msg: PbftMessage<E::ReplicaId, E::H>) -> Result<(), Error> {
		match msg {
			PbftMessage::Normal(m) => {
				if !self.state.read().is_accepting_message() {
					trace!(target: "PBFT", "received PBFT normal message, but not in state to process, discarded");
					return Ok(());
				}

				let NormalMessage { seq, view, digest, .. } = m;
				if let Err(e) = self.verify_message(view, seq, digest) {
					error!(target: "PBFT", "failed to verify message: {:?}, error: {:?}", m, e);
					return Err(e);
				}

				match m.msg_type {
					// TODO seems appropriate to use a macro
					MessageType::PrePrepare => self.process_pre_prepare(m),
					MessageType::Prepare => self.process_prepare(m),
					MessageType::Commit => self.process_commit(m),
					_ => Ok(()),
				}
			},
			PbftMessage::CheckPoint(m) => self.process_checkpoint(m),
			PbftMessage::ViewChange(m) => self.process_view_change(m),
			PbftMessage::NewView(m) => self.process_new_view(m),

			_ => Ok(()),
		}
	}

	pub fn update_timers(&mut self, cx: &mut Context<'_>) {
		self.commit_timers.iter_mut().for_each(|(_i, t)| t.update_state(cx));
	}
}

// TODO each task should be a seperate future
impl<E: Environment, T: TaskRunner> Future for Replica<E, T> {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		self.receive_new_block(cx);
		self.receive_pbft_messages(cx);
		self.update_timers(cx);

		Poll::Pending
	}
}

impl<E: Environment, T: TaskRunner> Unpin for Replica<E, T> {}

#[derive(Ord, PartialOrd, Clone, Eq, Hash, PartialEq, Encode, Decode)]
pub struct PeerId(String);

impl PeerId {
	pub fn new(s: String) -> Self {
		PeerId(s)
	}
}

impl Into<String> for PeerId {
	fn into(self) -> String {
		self.0
	}
}
unsafe impl Send for PeerId {}
unsafe impl Sync for PeerId {}

impl Debug for PeerId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let len = self.0.len();
		if len > 8 {
			let s = &self.0;
			write!(f, "{}…{}", &s[0..4], &s[len - 4..len])
		} else {
			write!(f, "{}", self.0)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use crate::test_utility::{Digest, ReplicaSet, Worker};
	use futures::executor::LocalPool;
	use futures::task::SpawnExt;
	use futures::StreamExt;
	use std::future;
	use std::sync::Mutex;
	use sp_utils::mpsc;

	#[test]
	fn display_peer_id() {
		let id0 = PeerId(String::from("abcdefghijklmnopq"));
		assert_eq!(format!("{id0:?}"), "abcd...nopq");

		let id1 = PeerId(String::from("0"));
		assert_eq!(format!("{id1:?}"), "0");
	}

	#[test]
	fn should_send_extrinsics() {
		let (tx, rv) = mpsc::tracing_unbounded("finalzied");
		let replica_set = Arc::new(Mutex::new(ReplicaSet::new(tx.clone())));
		let worker = Worker(replica_set.clone());

		let extrinsics = vec!["a", "b", "c"];
		let last_finalized = replica_set.lock().unwrap().last_finalized + extrinsics.len() as u32 - 1;
		replica_set.lock().unwrap().send_extrinsics_unordered(extrinsics.clone());

		let mut ex = Vec::new();
		for e in extrinsics.iter() {
			ex.push(Digest::from(*e));
		}

		let mut pool = LocalPool::new();
		pool.spawner().spawn(worker);
		pool.run_until(
			rv.take_while(|&(_, n)| future::ready(n < last_finalized))
				.for_each(|_| future::ready(())),
		);

		for (_, replica) in replica_set.lock().unwrap().replicas.iter() {
			let state = replica.state.read();
			assert_eq!(state.seq_num, last_finalized + 1);
			assert_eq!(state.stage, Stage::Finalized);
		}
	}
}
