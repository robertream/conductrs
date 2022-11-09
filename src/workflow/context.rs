use std::any::type_name;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};

use super::state::{Event, WorkflowState};
use super::WorkflowResult;

pub struct WorkflowContext(pub(crate) Arc<Mutex<WorkflowState>>);

impl WorkflowContext {
    pub fn new(state: Arc<Mutex<WorkflowState>>) -> Self {
        Self(state)
    }

    pub fn eval<T: Serialize + DeserializeOwned>(
        &mut self,
        evaluate: impl FnOnce() -> T,
    ) -> WorkflowResult<T> {
        let evaluation = self
            .0
            .lock()
            .unwrap()
            .record_eval(|| serde_json::to_string(&evaluate()).unwrap());
        Ok(serde_json::from_str(&evaluation?)?)
    }

    pub fn import<Ctx, In, Out, Func>(
        &mut self,
        _: Func,
    ) -> impl Fn(In) -> ActionFuture<FunctionActionRequest<Func, In, Out>>
    where
        In: Serialize + DeserializeOwned,
        Out: Serialize + DeserializeOwned,
        Func: ActionFnImport<Ctx, In, Out>,
    {
        let state = self.0.clone();
        move |input| {
            let request = FunctionActionRequest(input, Default::default(), Default::default());
            ActionFuture::new(state.clone(), request)
        }
    }
}

pub trait ActionFnImport<Ctx, In, Out> {
    type Output: Future<Output = Out>;
}

impl<Ctx, In, Out, Fut, Func> ActionFnImport<Ctx, In, Out> for Func
where
    Fut: Future<Output = Out>,
    Func: Fn(Ctx, In) -> Fut,
{
    type Output = Fut;
}

pub trait ActionRequest: Serialize + DeserializeOwned {
    type Response: Serialize + DeserializeOwned;
    fn type_name() -> &'static str {
        type_name::<Self>()
    }
}

pub struct ActionFuture<A: ActionRequest> {
    action_id: u32,
    state: Arc<Mutex<WorkflowState>>,
    phantom: PhantomData<A>,
}

impl<A: ActionRequest> ActionFuture<A> {
    pub fn new(state: Arc<Mutex<WorkflowState>>, request: A) -> ActionFuture<A> {
        let request = serde_json::to_string(&request).unwrap();
        let action_id = state
            .lock()
            .unwrap()
            .record_action_request(A::type_name(), request)
            // TODO: maybe don't panic on failure?
            //  but do we want replay failure to show up in the signature of all async functions in a workflow?
            .expect("ActivityFuture");
        Self {
            action_id,
            state,
            phantom: Default::default(),
        }
    }
}

impl<A: ActionRequest + Unpin> Future for ActionFuture<A> {
    type Output = WorkflowResult<A::Response>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.lock().unwrap();
        let expected_type = A::type_name();
        let expected_id = this.action_id;
        match state.actions.pop_response(expected_type, expected_id)? {
            Some(output) => {
                let output = output.as_str();
                let output = serde_json::from_str::<A::Response>(output).unwrap();
                Poll::Ready(Ok(output))
            }
            _ => Poll::Pending,
        }
    }
}

impl<A: ActionRequest> Drop for ActionFuture<A> {
    // TODO: save any errors to self.state in order to return the error on the next Future::poll
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state
            .actions
            .drop_action(A::type_name(), self.action_id)
            .unwrap()
        {
            state
                .record_event(Event::ActionDropped((A::type_name(), self.action_id)))
                .unwrap();
        }
    }
}

pub struct FunctionActionRequest<
    Func,
    In: Serialize + DeserializeOwned,
    Out: Serialize + DeserializeOwned,
>(In, PhantomData<Func>, PhantomData<Out>);

impl<Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned> ActionRequest
    for FunctionActionRequest<Func, In, Out>
{
    type Response = Out;
    fn type_name() -> &'static str {
        type_name::<Func>()
    }
}

impl<Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned> Serialize
    for FunctionActionRequest<Func, In, Out>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned>
    Deserialize<'de> for FunctionActionRequest<Func, In, Out>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(
            In::deserialize(deserializer)?,
            Default::default(),
            Default::default(),
        ))
    }
}
