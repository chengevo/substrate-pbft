use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use parity_scale_codec::{Decode, Encode};
use sp_utils::mpsc;
use sp_utils::mpsc::{TracingUnboundedReceiver, TracingUnboundedSender};

use crate::environment::{Environment, TaskRunner};
use crate::replica::state::Stage;
use crate::replica::{PeerId, Replica};

#[derive(Hash, Eq, PartialEq, Debug, Ord, Copy, Clone, Encode, Decode, PartialOrd)]
pub struct Digest([u8; 1]);
unsafe impl Send for Digest {}
unsafe impl Sync for Digest {}
impl From<&str> for Digest {
	fn from(s: &str) -> Self {
		Digest(s.as_bytes()[..].try_into().unwrap())
	}
}

impl Display for Digest {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", String::from_utf8_lossy(&self.0))
	}
}

pub struct Envi_mocked {
	pub head_block_num: u32,
	pub peer_id: PeerId,
	// channel to send digest when finalized
	pub finalized_sender: TracingUnboundedSender<(Digest, u32)>,
}

impl Envi_mocked {
	fn new(
		head_block_num: u32, finalized_sender: TracingUnboundedSender<(Digest, u32)>, peer_id: PeerId,
	) -> Envi_mocked {
		Envi_mocked { head_block_num, peer_id, finalized_sender }
	}
}
#[derive(Clone)]
pub struct TaskHandle();

impl TaskRunner for TaskHandle {
	fn execute(&self, _name: &'static str, _task: impl Future<Output = ()> + Send + 'static) {
		// let mut pool = LocalPool::new();
		// pool.spawner().spawn(task);

		//tokio::spawn(task);
	}
}

impl Environment for Envi_mocked {
	type ReplicaId = PeerId;
	type H = Digest;
	type Error = sp_blockchain::Error;

	fn head_block_num(&self) -> u32 {
		self.head_block_num
	}

	fn init_replica_state(&self) -> (Self::ReplicaId, u32) {
		(self.peer_id.clone(), self.head_block_num())
	}

	fn extrinsic_exists(&self, _digest: Self::H) -> bool {
		true
	}

	fn new_replica_id(_s: String) -> Self::ReplicaId {
		todo!()
	}

	fn finalize_block(&self, hash: Self::H, number: u32) -> Result<(), Self::Error> {
		self.finalized_sender
			.unbounded_send((hash, number))
			.expect("failed to send finalization");
		Ok(())
	}

	fn block_status(&self, _digest: Self::H) -> sp_consensus::BlockStatus {
		todo!()
	}
}

pub fn mock_task_handle() -> TaskHandle {
	TaskHandle()
}

pub fn mock_replica(
	head_block_num: u32, peer_id: PeerId, finalized_sender: TracingUnboundedSender<(Digest, u32)>,
) -> (Replica<Envi_mocked, TaskHandle>, Comm) {
	let (extrinsic_sender, extrinsic_receiver) = mpsc::tracing_unbounded("pbft_block");
	let (in_sender, in_receiver) = mpsc::tracing_unbounded("consensus_message_in");
	let (out_sender, out_receiver) = mpsc::tracing_unbounded("consensus_message_out");

	let envi = Envi_mocked::new(head_block_num, finalized_sender, peer_id);
	let task_handle = mock_task_handle();

	(
		Replica::new(envi, task_handle, Box::pin(extrinsic_receiver), Box::pin(in_receiver), out_sender),
		Comm { extrinsic_sender, consensus_sender: in_sender, consensus_receiver: Box::pin(out_receiver) },
	)
}

/// A set of communication chennel to send to and receive  message from replica
pub struct Comm {
	extrinsic_sender: TracingUnboundedSender<Digest>,
	pub consensus_sender: TracingUnboundedSender<Vec<u8>>,
	pub consensus_receiver: Pin<Box<TracingUnboundedReceiver<Vec<u8>>>>,
}

impl Future for Comm {
	type Output = Vec<u8>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		while let Poll::Ready(Some(message)) = Stream::poll_next(Pin::new(&mut self.consensus_receiver), cx) {
			return Poll::Ready(message);
		}

		Poll::Pending
	}
}

pub struct Worker(Arc<Mutex<ReplicaSet>>);

impl Future for Worker {
	type Output = ();

	/// Poll consensus_receiver in each Comm to read messages from replicas, and send each message to other oreplicas
	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let mut replica_set = self.0.lock().unwrap();

		for (_, replica) in replica_set.replicas.iter_mut() {
			Pin::new(&mut *replica).poll(cx);
		}

		let mut messages = Vec::new();
		for (id, comm) in replica_set.comms.iter_mut() {
			while let Poll::Ready(msg) = Pin::new(&mut *comm).poll(cx) {
				messages.push((id.clone(), msg));
			}
		}

		replica_set.send_consensus(messages);

		Poll::Pending
	}
}

// A set contains 4 replicas
pub struct ReplicaSet {
	pub replicas: HashMap<String, Replica<Envi_mocked, TaskHandle>>,
	pub comms: HashMap<PeerId, Comm>,
	pub last_finalized: u32,
}

impl ReplicaSet {
	pub fn new(finalized_sender: TracingUnboundedSender<(Digest, u32)>) -> Self {
		let last_finalized = 4;
		let mut replicas = HashMap::new();
		let mut comms = HashMap::new();

		let replica_ids: Vec<PeerId> =
			vec!["1", "2", "3", "4"].iter().map(|i| PeerId::new(String::from(*i))).collect();

		for id in replica_ids.iter() {
			let (replica, comm) = mock_replica(last_finalized, id.clone(), finalized_sender.clone());
			replica.state.write().replicas = replica_ids.clone();
			replica.state.write().stage = Stage::Inited;
			replicas.insert(id.clone().into(), replica);
			comms.insert(id.clone(), comm);
		}

		ReplicaSet { replicas, comms, last_finalized }
	}

	// move to view change stage, return next primary replica
	pub fn view_change(&mut self) -> String {
		let mut view_change_message = vec![];
		let mut next_primary = None;

		self.replicas.values_mut().for_each(|replica| {
			if !replica.state.read().is_primary() {
				if replica.state.read().is_next_primary() {
					next_primary = Some(replica);
				} else {
					let m = replica.state.write().create_view_change_message();
					view_change_message.push(m);
				}
			}
		});

		let primary = next_primary.unwrap();
		let next_view = primary.state.read().view_num + 1;
		for m in view_change_message {
			primary.process_view_change(m);
		}

		assert!(primary.state.read().storage.view_changes.is_enough(next_view));
		return primary.state.read().id.clone().into();
	}

	/// Send a pbft message to replicas other than the sender
	/// - peer_id: sender of the message
	fn send_consensus(&mut self, messages: Vec<(PeerId, Vec<u8>)>) {
		for (peer_id, message) in messages {
			for (_, r) in self.replicas.iter() {
				let replica_id = r.state.read().id.clone();
				if r.state.read().id != peer_id {
					self.comms
						.get(&replica_id)
						.as_ref()
						.unwrap()
						.consensus_sender
						.unbounded_send(message.clone());
				}
			}
		}
	}

	/// Send extrinsics to replicas, the order of extrinsics received by replicas are different.
	pub fn send_extrinsics_unordered(&self, extrisics: Vec<&str>) {
		for (_, comm) in self.comms.iter() {
			//extrisics.shuffle(&mut thread_rng());
			for e in extrisics.iter() {
				comm.extrinsic_sender.unbounded_send(Digest::from(*e));
			}
		}
	}
}
