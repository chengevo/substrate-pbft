use std::time::Duration;

pub struct Config {
	/// The longest time a replica waits until it sends a View-Change message to other replicas,
	/// starting from the time it receives a block.
	commit_timeout: Duration,
}
