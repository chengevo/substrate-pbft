use std::borrow::Cow;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use futures::prelude::*;
use futures::task::{Context, Poll};
use futures::{Future, Stream, StreamExt};
use log::{trace, warn};
use sc_client_api::{
	AuxStore, Backend, BlockchainEvents, ExecutorProvider, Finalizer, LockImportRun, TransactionFor,
};
use sc_network::{Event, ExHashT, NetworkService, PeerId};
use sp_api::{BlockT, HeaderT, ProvideRuntimeApi};
use sp_blockchain::{BlockStatus, HeaderBackend, HeaderMetadata};
use sp_consensus::block_validation::Chain;
use sp_consensus::BlockImport;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::Block;
use sp_utils::mpsc::{tracing_unbounded, TracingUnboundedSender};

use consensus_pbft::environment::{Environment, TaskRunner};
use consensus_pbft::replica::PeerId as ReplicaPeerId;
use consensus_pbft::replica::Replica;
pub use import::PbftBlockImport;

mod import;

/// Name of the notifications protocol used by PBFT. Must be registered towards the networking
pub const PBFT_PROTOCOL_NAME: &'static str = "/consensus/pbft/1";

pub struct PbftWorker<E: Environment, T: TaskRunner, B: BlockT, H: ExHashT> {
	network_service: Arc<NetworkService<B, H>>,
	network_event_stream: Pin<Box<dyn Stream<Item = Event> + Send>>,
	peers: Vec<PeerId>,
	protocol: Cow<'static, str>,
	replica: Replica<E, T>,
	consensus_sender: TracingUnboundedSender<Vec<u8>>,
	consensus_receiver: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
}

impl<E: Environment, T: TaskRunner, B: BlockT, H: ExHashT> PbftWorker<E, T, B, H> {
	pub fn new(
		env: E, task_handle: T, service: Arc<NetworkService<B, H>>,
		extrinsic_in: Pin<Box<dyn Stream<Item = E::H> + Send>>,
	) -> PbftWorker<E, T, B, H> {
		let (in_sender, in_receiver) = tracing_unbounded("consensus_message_in");
		let (out_sender, out_receiver) = tracing_unbounded("consensus_message_out");

		let replica = Replica::new(env, task_handle, extrinsic_in, Box::pin(in_receiver), out_sender);

		PbftWorker {
			network_event_stream: Box::pin(service.event_stream("PBFT")),
			network_service: service,
			peers: Vec::new(),
			protocol: PBFT_PROTOCOL_NAME.into(),
			replica,
			consensus_sender: in_sender,
			consensus_receiver: Box::pin(out_receiver),
		}
	}

	fn add_replica(&mut self, peer_id: E::ReplicaId) {
		self.replica.state.write().add_replica(peer_id);
	}
}

// TODO why Unpin?
impl<E: Environment, T: TaskRunner, B: BlockT, H: ExHashT> Unpin for PbftWorker<E, T, B, H> {}

pub fn run_pbft_worker<E: Environment, T: TaskRunner, BE, C, B: BlockT, H: ExHashT>(
	env: E, task_handle: T, service: Arc<NetworkService<B, H>>,
	extrinsic_in: Pin<Box<dyn Stream<Item = E::H> + Send>>,
) -> sp_blockchain::Result<impl Future<Output = ()> + Unpin + Send>
where
	C: ClientForPbft<B, BE>,
	BE: Backend<B>,
	B: BlockT,
{
	let worker: PbftWorker<E, T, B, H> = PbftWorker::new(env, task_handle, service, Box::pin(extrinsic_in));
	Ok(worker)
}

pub fn block_import<Block: BlockT, Client, SC, BE>(
	client: Arc<Client>, select_chain: SC, block_sender: TracingUnboundedSender<Block::Hash>,
) -> PbftBlockImport<Block, BE, Client, SC> {
	PbftBlockImport { inner: client, select_chain, _phantom: PhantomData, block_sender }
}

pub fn pbft_peers_set_config() -> sc_network::config::NonDefaultSetConfig {
	sc_network::config::NonDefaultSetConfig {
		notifications_protocol: PBFT_PROTOCOL_NAME.into(),
		// Notifications reach ~256kiB in size at the time of writing on Kusama and Polkadot.
		max_notification_size: 1024 * 1024,
		set_config: sc_network::config::SetConfig {
			in_peers: 0,
			out_peers: 0,
			reserved_nodes: Vec::new(),
			non_reserved_mode: sc_network::config::NonReservedPeerMode::Deny,
		},
	}
}

impl<E: Environment, T: TaskRunner, B: Block, H: ExHashT> Future for PbftWorker<E, T, B, H> {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let this = &mut *self;
		loop {
			let message = String::from("hello world").as_bytes().to_vec();

			match this.network_event_stream.poll_next_unpin(cx) {
				Poll::Ready(Some(event)) => match event {
					Event::SyncConnected { remote } => {
						let addr = core::iter::once(parity_multiaddr::Protocol::P2p(remote.clone().into()))
							.collect::<parity_multiaddr::Multiaddr>();
						this.network_service.add_peers_to_reserved_set(
							this.protocol.clone(),
							core::iter::once(addr).collect(),
						);
						trace!(target: "PBFT",  "Connected to node: {}", remote);
					},
					Event::NotificationStreamOpened { remote, protocol, role: _ } => {
						trace!(target: "PBFT", "Stream opened, protocol: {:}", protocol);
						this.network_service.write_notification(
							remote.clone(),
							this.protocol.clone(),
							message.clone(),
						);
						this.peers.push(remote.clone());
						trace!(target: "PBFT", "Send a message to peer {}", remote);
						this.add_replica(E::new_replica_id(remote.to_string()));
					},
					Event::NotificationsReceived { remote: _, messages } => {
						messages.into_iter().for_each(|(protocol, data)| {
							if protocol == this.protocol {
								let message = data.to_vec();
								this.consensus_sender.unbounded_send(message.clone());
							}
						});
					},
					_ => warn!(target: "PBFT", "other events...."),
				},
				Poll::Ready(None) => {},
				Poll::Pending => {},
			}

			match Future::poll(Pin::new(&mut this.replica), cx) {
				Poll::Pending => {},
				Poll::Ready(()) => {},
			}

			match Stream::poll_next(Pin::new(&mut this.consensus_receiver), cx) {
				Poll::Pending => break,
				Poll::Ready(msg) => {
					let network_service = this.network_service.clone();
					let protocol = this.protocol.clone();
					let msg = msg.unwrap();
					this.peers.iter().for_each(|peer| {
						network_service.clone().write_notification(
							peer.clone(),
							protocol.clone(),
							msg.clone(),
						);
					});
				},
			}
		}

		Poll::Pending
	}
}

pub trait ClientForPbft<Block, BE>:
	LockImportRun<Block, BE>
	+ Finalizer<Block, BE>
	+ AuxStore
	+ HeaderMetadata<Block, Error = sp_blockchain::Error>
	+ HeaderBackend<Block>
	+ BlockchainEvents<Block>
	+ ProvideRuntimeApi<Block>
	+ ExecutorProvider<Block>
	+ BlockImport<Block, Transaction = TransactionFor<BE, Block>, Error = sp_consensus::Error>
	+ Chain<Block>
where
	BE: Backend<Block>,
	Block: BlockT,
{
}

impl<Block, BE, T> ClientForPbft<Block, BE> for T
where
	BE: Backend<Block>,
	Block: BlockT,
	T: LockImportRun<Block, BE>
		+ Finalizer<Block, BE>
		+ AuxStore
		+ HeaderMetadata<Block, Error = sp_blockchain::Error>
		+ HeaderBackend<Block>
		+ BlockchainEvents<Block>
		+ ProvideRuntimeApi<Block>
		+ ExecutorProvider<Block>
		+ BlockImport<Block, Transaction = TransactionFor<BE, Block>, Error = sp_consensus::Error>
		+ Chain<Block>,
{
}

pub struct Envi<Block, BE, C>
where
	C: ClientForPbft<Block, BE>,
	BE: Backend<Block>,
	Block: BlockT,
{
	pub head_block_num: u32,
	pub peer_id: ReplicaPeerId,
	pub client: Arc<C>,
	pub _phantom: PhantomData<Block>,
	pub _phantom_be: PhantomData<BE>,
}

impl<Block, BE, C> Envi<Block, BE, C>
where
	C: ClientForPbft<Block, BE>,
	BE: Backend<Block>,
	Block: BlockT,
{
	pub fn new(client: Arc<C>, peer_id: ReplicaPeerId, head_block_num: u32) -> Envi<Block, BE, C> {
		Envi { client, head_block_num, peer_id, _phantom: PhantomData, _phantom_be: PhantomData }
	}
}

// The implementation of Environment trait, i.e. the Envi struct defines the concrete type of `ReplicaId`,
// ExtrinsicIn and `H` used in Replica
impl<Block, BE, C> Environment for Envi<Block, BE, C>
where
	C: ClientForPbft<Block, BE>,
	BE: Backend<Block>,
	Block: BlockT,
{
	type ReplicaId = ReplicaPeerId;
	type H = <Block as BlockT>::Hash;
	type Error = sp_blockchain::Error;

	fn head_block_num(&self) -> u32 {
		self.head_block_num
	}

	fn init_replica_state(&self) -> (Self::ReplicaId, u32) {
		(self.peer_id.clone(), self.head_block_num() + 1)
	}

	fn extrinsic_exists(&self, digest: Self::H) -> bool {
		if let Ok(BlockStatus::InChain) = self.client.status(BlockId::Hash(digest)) {
			return true;
		}

		false
	}

	fn new_replica_id(s: String) -> Self::ReplicaId {
		ReplicaPeerId::new(s)
	}

	fn finalize_block(&self, hash: Self::H, number: u32) -> Result<(), Self::Error> {
		self.client.lock_import_and_run(|import_op| {
			self.client
				.apply_finality(import_op, BlockId::Hash(hash), None, true)
				.map_err(|e| {
					let msg = format!("Error applying finality to block {:?}", (hash, number));
					warn!(target: "PBFT", "{}, {}", msg, e);
					e
				})?;

			Ok(())
		})
	}

	fn block_status(&self, digest: Self::H) -> sp_consensus::BlockStatus {
		self.client.block_status(&BlockId::Hash(digest)).unwrap()
	}
}
