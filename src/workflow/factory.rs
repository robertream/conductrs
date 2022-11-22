use std::collections::VecDeque;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};

use super::action::{ActionId, ActionType};
use super::context::WorkflowContext;
use super::events::{Event, EventQueue, EventRecord, EventTime};
use super::state::WorkflowState;
use super::{WorkflowError, WorkflowResult};

pub struct WorkflowFactory(BoxWorkflowFn);

impl WorkflowFactory {
    pub fn new<In, Out, Func>(func: Func) -> Self
    where
        In: Send + DeserializeOwned,
        Out: Serialize,
        Func: 'static + for<'b> WorkflowFn<'b, In, Out>,
    {
        let func = Arc::new(func);
        let waker = Arc::new(EmptyWaker);
        Self(Box::new(move |new| {
            let (state, started) = match new {
                NewWorkflow::Start(now, started) => {
                    Ok((WorkflowState::start(now, started.clone()), started))
                }
                NewWorkflow::Replay(mut replay) => match replay.pop_front() {
                    Some((0, 0, now, Event::Started(started))) => {
                        Ok((WorkflowState::replay(now, replay), started))
                    }
                    Some(actual) => Err(WorkflowError::StartFailed {
                        actual: Box::new(actual),
                    }),
                    None => Err(WorkflowError::Panic(None)),
                },
            }?;
            let func = func.clone();
            let waker = waker.clone();
            let mut context = WorkflowContext::new(state.clone());
            let future = Mutex::new(
                async move {
                    let input = serde_json::from_str::<In>(&started)?;
                    let result = func.call(&mut context, input).await;
                    let finished = result.map(|output| serde_json::to_string(&output).unwrap());
                    context.0.record_workflow_result(finished)
                }
                .boxed(),
            );
            let workflow = Workflow {
                waker,
                state,
                future,
            };
            workflow.resume()?;
            Ok(workflow)
        }))
    }

    pub fn start(&self, now: EventTime, start: String) -> WorkflowResult<(Workflow, EventQueue)> {
        let mut workflow = self.create(NewWorkflow::Start(now, start))?;
        let events = workflow.drain_pending_events();
        Ok((workflow, events))
    }

    pub fn replay(&self, replay: EventQueue) -> WorkflowResult<Workflow> {
        self.create(NewWorkflow::Replay(replay))
    }

    fn create(&self, workflow: NewWorkflow) -> WorkflowResult<Workflow> {
        (self.0)(workflow)
    }
}

pub struct Workflow {
    future: BoxWorkflowFuture,
    waker: Arc<EmptyWaker>,
    state: WorkflowState,
}

impl Workflow {
    pub fn apply(&mut self, events: VecDeque<EventRecord>) -> WorkflowResult<()> {
        self.state.apply(events);
        self.resume()
    }

    pub fn handle_action_response(
        &mut self,
        now: time::OffsetDateTime,
        response: (ActionType, ActionId, String),
    ) -> WorkflowResult<EventQueue> {
        self.state.handle_action_response(now, response)?;
        self.resume()?;
        Ok(self.state.drain_pending_events())
    }

    pub fn drain_pending_events(&mut self) -> EventQueue {
        self.state.drain_pending_events()
    }

    fn resume(&self) -> WorkflowResult<()> {
        if let Some(error) = self.state.pop_error() {
            return Err(error);
        }
        let waker = waker_ref(&self.waker);
        let mut future = self.future.lock().unwrap();
        let cx = &mut Context::from_waker(&*waker);
        let pin = Pin::new(future.deref_mut());
        match pin.poll(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Ok(()),
        }
    }
}

pub trait WorkflowFn<'a, In, Out>: Send + Sync {
    type Output: 'a + Future<Output = WorkflowResult<Out>> + Send + Sync;
    fn call(&self, context: &'a mut WorkflowContext, input: In) -> Self::Output;
}

impl<'a, In, Out, Fut, Func> WorkflowFn<'a, In, Out> for Func
where
    Fut: 'a + Send + Sync + Future<Output = WorkflowResult<Out>>,
    Func: Send + Sync + Fn(&'a mut WorkflowContext, In) -> Fut,
{
    type Output = Fut;

    fn call(&self, context: &'a mut WorkflowContext, input: In) -> Fut {
        self(context, input)
    }
}

type BoxWorkflowFuture = Mutex<BoxFuture<'static, WorkflowResult<()>>>;

struct EmptyWaker;
unsafe impl Send for EmptyWaker {}
impl ArcWake for EmptyWaker {
    fn wake_by_ref(_: &Arc<Self>) {}
}

type BoxWorkflowFn = Box<dyn 'static + Send + Fn(NewWorkflow) -> WorkflowResult<Workflow>>;
enum NewWorkflow {
    Start(time::OffsetDateTime, String),
    Replay(EventQueue),
}
