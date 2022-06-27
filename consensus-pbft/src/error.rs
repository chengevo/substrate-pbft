#[derive(Debug)]
pub enum Error {
	InvalidView(String),
	InvalidMessage(String),
	FaultyPrimary(String),
	NoPrePrepare(String),
	NotPrePared,
	ViewChangeFailed(String),
	FinalizeBLockFailed(String),
}
