use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::mpsc;
use futures::Stream;
use futures_timer::Delay;
use log::{info, trace};
use parity_scale_codec::{Decode, Encode};
use parking_lot::RwLock;
use sp_utils::mpsc::TracingUnboundedSender;

use crate::environment::TaskRunner;
use crate::message::PbftMessage;
use crate::replica::state::{Stage, State};
use crate::replica::SequenceNum;

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum TimerState {
	Running,
	Stopped,
	Inited,
	Dropped,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TimerCommand {
	Drop,
	Execute,
}

pub struct Timer {
	state: TimerState,
	delay: Delay,
	duration: Duration,
	sender: mpsc::UnboundedSender<TimerCommand>,
	receiver: mpsc::UnboundedReceiver<TimerCommand>,
}

impl Timer {
	pub fn new(
		duration: Duration, sender: mpsc::UnboundedSender<TimerCommand>,
		receiver: mpsc::UnboundedReceiver<TimerCommand>,
	) -> Timer {
		Timer { duration, state: TimerState::Inited, delay: Delay::new(duration), sender, receiver }
	}

	pub fn start(&mut self) {
		self.state = TimerState::Running;
	}

	pub fn stop(&mut self) {
		self.state = TimerState::Stopped;
	}

	pub fn is_running(&self) -> bool {
		self.state == TimerState::Running
	}
}

impl Future for Timer {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		// when received command from timer switch
		while let Poll::Ready(Some(command)) = Stream::poll_next(Pin::new(&mut self.receiver), cx) {
			match command {
				TimerCommand::Drop => {
					self.state = TimerState::Dropped;
				},
				_ => unimplemented!(),
			}
		}

		// if not receiving any command, let the executor handle
		match Pin::new(&mut self.delay).poll(cx) {
			Poll::Ready(_) => {
				if self.state == TimerState::Running {
					self.state = TimerState::Stopped;

					let command = TimerCommand::Execute;
					self.sender.unbounded_send(command);
				}
				return Poll::Ready(());
			},

			Poll::Pending => {
				// if the Delay responses with Pending but its state is not Running, it means it's get
				// polled because of receiving command from the switch while the timer is running
				match self.state {
					TimerState::Inited => {
						// the first polled after the timer created
						self.state = TimerState::Running;
						return Poll::Pending;
					},
					TimerState::Running => {
						// the timer not fished yet, keep running
						return Poll::Pending;
					},
					TimerState::Dropped => {
						// the timer received command from the switch, it's state already being set
						// to dropped, thus stop the timer
						self.stop();
						// in this case, stop the timer immediately by responses Poll::Result to the executor
						return Poll::Ready(());
					},
					_ => Poll::Pending,
				}
			},
		}
	}
}

/// The gateway to control a timer
/// Because the inner Timer have to deploy to a async runtime(i.e. the runtime "owns" the timer), thus
/// the timer switch cannot have a timer field,
pub struct TimerSwitch<I, H, T: TaskRunner>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
	T: TaskRunner,
{
	pub seq: SequenceNum,
	pub state: TimerState,
	replica_state: Arc<RwLock<State<I, H>>>,
	view_change_sender: Arc<RwLock<TracingUnboundedSender<Vec<u8>>>>,
	task_handle: T,
	duration: Duration,
	sender: Option<mpsc::UnboundedSender<TimerCommand>>,
	receiver: Option<mpsc::UnboundedReceiver<TimerCommand>>,
}

impl<I, H, T: TaskRunner> TimerSwitch<I, H, T>
where
	I: Eq + Hash + PartialEq + Debug + Clone + Ord + Encode + Decode,
	H: Eq + Hash + PartialEq + Debug + Clone + Encode + Decode + Copy + Ord + PartialOrd,
	T: TaskRunner,
{
	pub fn new(
		seq: SequenceNum, replica_state: Arc<RwLock<State<I, H>>>,
		view_change_sender: Arc<RwLock<TracingUnboundedSender<Vec<u8>>>>, duration: Duration, task_handle: T,
	) -> TimerSwitch<I, H, T> {
		TimerSwitch {
			seq,
			state: TimerState::Inited,
			replica_state,
			task_handle,
			sender: None,
			receiver: None,
			duration,
			view_change_sender,
		}
	}

	pub fn is_running(&self) -> bool {
		self.state == TimerState::Running
	}

	pub fn update_state(&mut self, cx: &mut Context) {
		// Inited state means timer switch is just created, haven't depoy a timer for execution. Thus
		// don't update state at this point
		if self.state == TimerState::Inited || self.state == TimerState::Stopped {
			return;
		}

		// when received message from the timer
		let receiver = self.receiver.as_mut().unwrap();

		if let Poll::Ready(Some(TimerCommand::Execute)) = Stream::poll_next(Pin::new(receiver), cx) {
			// this timer is supposed to use for one time only, i.e. once the timer ended and send a message
			// back to the switch, the timer ends itself by responding Poll::Ready to the executor. Thus once
			// the switch received a message from the timer, it should respond Poll::Ready to the executor
			// to ends itself, too.
			self.state = TimerState::Stopped;
		}

		// self.stage != Stage::ViewChange to prevent multiple timers starting view change
		if self.state == TimerState::Stopped && self.replica_state.read().stage != Stage::ViewChange {
			self.start_view_change();
		}
	}

	/// Deploy the inner timer to the async runtime
	pub fn start(&mut self) {
		let (switch_tx, timer_rv) = mpsc::unbounded();
		let (timer_tx, switch_rv) = mpsc::unbounded();

		self.sender = Some(switch_tx);
		self.receiver = Some(switch_rv);
		self.state = TimerState::Running;

		let timer = Timer::new(self.duration, timer_tx, timer_rv);
		self.task_handle.execute("pbft-timer", timer);
		trace!(target: "PBFT", "timer #{} started running, duration: {:?} millis", self.seq, self.duration.as_millis());
	}

	pub fn drop(&mut self) {
		self.state = TimerState::Dropped;
		self.sender.as_ref().unwrap().unbounded_send(TimerCommand::Drop);
	}

	fn start_view_change(&mut self) {
		self.replica_state.write().stage = Stage::ViewChange;

		let view_change = self.replica_state.write().create_view_change_message();

		self.view_change_sender
			.read()
			.unbounded_send(PbftMessage::ViewChange(view_change.clone()).encode())
			.expect(&format!("Failed to send VIEW CHANGE: {:?}", view_change));

		info!(target: "PBFT", "timer created at seq #{} expired, sent view change ({:?})", self.seq, view_change);

		self.replica_state.write().storage.view_changes.insert(view_change);
	}
}
