use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use sc_client_api::Backend;
use sp_api::{BlockT, HeaderT, TransactionFor};
use sp_blockchain::BlockStatus;
use sp_consensus::Error as ConsensusError;
use sp_consensus::{BlockCheckParams, BlockImport, BlockImportParams, ImportResult};
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block, Header};
use sp_utils::mpsc::TracingUnboundedSender;

use crate::ClientForPbft;

pub struct PbftBlockImport<Block: BlockT, BE, Client, SC> {
	pub(crate) inner: Arc<Client>,
	pub select_chain: SC,
	pub(crate) _phantom: PhantomData<BE>,
	pub block_sender: TracingUnboundedSender<Block::Hash>,
}

impl<Block: BlockT, BE, Client, SC: Clone> Clone for PbftBlockImport<Block, BE, Client, SC> {
	fn clone(&self) -> Self {
		PbftBlockImport {
			inner: self.inner.clone(),
			select_chain: self.select_chain.clone(),
			block_sender: self.block_sender.clone(),
			_phantom: PhantomData,
		}
	}
}

impl<Block: BlockT, BE, Client, SC> BlockImport<Block> for PbftBlockImport<Block, BE, Client, SC>
where
	BE: Backend<Block>,
	Client: ClientForPbft<Block, BE>,
	for<'a> &'a Client:
		BlockImport<Block, Error = ConsensusError, Transaction = TransactionFor<Client, Block>>,
{
	type Error = sp_consensus::Error;
	type Transaction = sp_api::TransactionFor<Client, Block>;

	fn check_block(&mut self, block: BlockCheckParams<Block>) -> Result<ImportResult, Self::Error> {
		self.inner.check_block(block)
	}

	fn import_block(
		&mut self, block: BlockImportParams<Block, Self::Transaction>, new_cache: HashMap<[u8; 4], Vec<u8>>,
	) -> Result<ImportResult, Self::Error> {
		let hash = block.post_hash();
		let _number = *block.header.number();

		match self.inner.status(BlockId::Hash(hash)) {
			Ok(BlockStatus::InChain) => return Ok(ImportResult::AlreadyInChain),
			Ok(BlockStatus::Unknown) => {},
			Err(e) => return Err(ConsensusError::ClientImport(e.to_string())),
		}

		let import_result = (&*self.inner).import_block(block, new_cache);

		let imported_aux = {
			match import_result {
				Ok(ImportResult::Imported(aux)) => aux,
				Ok(r) => {
					return Ok(r);
				},
				Err(e) => {
					return Err(ConsensusError::ClientImport(e.to_string()));
				},
			}
		};

		self.block_sender.unbounded_send(hash);

		Ok(ImportResult::Imported(imported_aux))
	}
}

fn verify_block_seal() -> bool {
	true
}
