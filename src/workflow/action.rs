use std::any::type_name;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::state::WorkflowState;
use super::{WorkflowError, WorkflowResult};

pub struct ActionFuture<A>(ActionState<A>)
where
    for<'a> A: ActionRequest + 'a;

impl<A: ActionRequest> ActionFuture<A>
where
    for<'a> A: ActionRequest + 'a,
{
    pub fn new(state: WorkflowState, request: A) -> Self {
        Self(ActionState::Created { state, request })
    }
}

impl<A> Future for ActionFuture<A>
where
    for<'a> A: ActionRequest + Unpin + 'a,
{
    type Output = WorkflowResult<A::Response>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        use ActionState::*;
        match &mut self.0 {
            Created { state, request } => {
                let state = state.clone();
                let (result, action_id) = state.begin_action(request);
                self.0 = Started {
                    state,
                    action_id,
                    result,
                };
                Poll::Pending
            }
            Started {
                state,
                action_id,
                result,
                ..
            } => {
                let poll = result.poll();
                if poll.is_ready() {
                    state.end_action(*action_id);
                    self.0 = Completed;
                }
                poll
            }
            Completed => panic!("polled after ready"),
        }
    }
}

impl<A> Drop for ActionFuture<A>
where
    for<'a> A: ActionRequest + 'a,
{
    fn drop(&mut self) {
        use ActionState::*;
        match &mut self.0 {
            Created { .. } => self.0 = Completed,
            Started {
                state, action_id, ..
            } => {
                state.cancel_action(*action_id);
                self.0 = Completed;
            }
            Completed => {}
        }
    }
}

enum ActionState<A>
where
    for<'a> A: ActionRequest + 'a,
{
    Created {
        state: WorkflowState,
        request: A,
    },
    Started {
        state: WorkflowState,
        action_id: ActionId,
        result: ActionResult<A>,
    },
    Completed,
}

pub struct ActionResult<A>(Arc<Mutex<Poll<Option<WorkflowResult<A::Response>>>>>)
where
    for<'a> A: ActionRequest + 'a;

impl<A: ActionRequest> ActionResult<A>
where
    for<'a> A: ActionRequest + 'a,
{
    pub fn new() -> (Self, ActionResultHandler) {
        let result = Self(Arc::new(Mutex::new(Poll::Pending)));
        let handler = ActionResultHandler::new(&result);
        (result, handler)
    }

    pub fn set_error(&self, err: WorkflowError) {
        *self.0.lock().unwrap() = Poll::Ready(Some(Err(err)))
    }

    pub fn poll(&self) -> Poll<WorkflowResult<A::Response>> {
        match self.0.lock().unwrap().deref_mut() {
            Poll::Ready(result) => Poll::Ready(result.take().expect("polled after ready")),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct ActionResultHandler(
    Box<dyn Send + Sync + for<'a> Fn(WorkflowResult<&'a str>) -> WorkflowResult<()>>,
);

impl ActionResultHandler {
    fn new<A: ActionRequest>(result: &ActionResult<A>) -> ActionResultHandler
    where
        for<'a> A: ActionRequest + 'a,
    {
        let result = result.0.clone();
        Self(Box::new(move |r| {
            let mut result = result.lock().unwrap();
            if result.is_pending() {
                let rs = match r {
                    Ok(rr) => Ok(serde_json::from_str(rr)?),
                    Err(e) => Err(e),
                };
                *result = Poll::Ready(Some(rs));
            }
            Ok(())
        }))
    }

    pub fn invoke(&self, result: WorkflowResult<&str>) -> WorkflowResult<()> {
        (self.0)(result)
    }
}

pub trait ActionRequest: Serialize + DeserializeOwned {
    type Response: Serialize + DeserializeOwned + Send;
    fn type_name() -> ActionType {
        type_name::<Self>().into()
    }
}

pub type ActionType = std::borrow::Cow<'static, str>;
pub type ActionId = u32;
