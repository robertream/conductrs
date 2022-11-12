use std::any::type_name;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::actions::ActionRequestId;
use super::state::WorkflowState;
use super::{WorkflowError, WorkflowResult};

pub struct ActionFuture<A: ActionRequest> {
    result: ActionResult<A>,
    drop_handler: ActionDropHandler,
}

impl<A: ActionRequest> ActionFuture<A> {
    pub fn new(result: ActionResult<A>, drop_handler: ActionDropHandler) -> Self {
        Self {
            result,
            drop_handler,
        }
    }
}

impl<A: ActionRequest + Unpin> Future for ActionFuture<A> {
    type Output = WorkflowResult<A::Response>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().result.poll()
    }
}

impl<A: ActionRequest> Drop for ActionFuture<A> {
    fn drop(&mut self) {
        let was_pending = self.result.0.lock().unwrap().is_pending();
        self.drop_handler.invoke(was_pending);
    }
}

pub trait ActionRequest: Serialize + DeserializeOwned {
    type Response: Serialize + DeserializeOwned + Send;
    fn type_name() -> &'static str {
        type_name::<Self>()
    }
}

pub struct ActionResult<A: ActionRequest>(Arc<Mutex<Poll<Option<WorkflowResult<A::Response>>>>>);

impl<A: ActionRequest> ActionResult<A> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Poll::Pending)))
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
    pub fn new<A: ActionRequest>(result: &ActionResult<A>) -> ActionResultHandler
    where
        for<'a> <A as ActionRequest>::Response: 'a,
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

pub struct ActionDropHandler(WorkflowState, ActionRequestId);

impl ActionDropHandler {
    pub fn new(state: WorkflowState, action_id: u32) -> Self {
        Self(state, action_id)
    }
}

impl ActionDropHandler {
    fn invoke(&self, was_pending: bool) {
        self.0.record_action_drop(self.1, was_pending)
    }
}
