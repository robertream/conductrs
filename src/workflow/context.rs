use std::any::type_name;
use std::future::Future;
use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};

use super::action::{ActionFuture, ActionRequest, ActionType};
use super::state::WorkflowState;
use super::WorkflowResult;

pub struct WorkflowContext(pub(crate) WorkflowState);

impl WorkflowContext {
    pub fn new(state: WorkflowState) -> Self {
        Self(state)
    }

    pub fn eval<T: Serialize + DeserializeOwned>(
        &mut self,
        evaluate: impl FnOnce() -> T,
    ) -> WorkflowResult<T> {
        let evaluation = self
            .0
            .record_eval(|| serde_json::to_string(&evaluate()).unwrap());
        Ok(serde_json::from_str(&evaluation?)?)
    }

    pub fn import<Ctx, In, Out, Func>(
        &mut self,
        _: Func,
    ) -> impl Fn(In) -> ActionFuture<FunctionActionRequest<Func, In, Out>>
    where
        for<'a> In: Serialize + DeserializeOwned + 'a,
        for<'a> Out: Serialize + DeserializeOwned + Send + 'a,
        for<'a> Func: ActionFnImport<Ctx, In, Out> + 'a,
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

pub struct FunctionActionRequest<
    Func,
    In: Serialize + DeserializeOwned,
    Out: Serialize + DeserializeOwned + Send,
>(In, PhantomData<Out>, PhantomData<Func>);

impl<Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned + Send> ActionRequest
    for FunctionActionRequest<Func, In, Out>
{
    type Response = Out;
    fn type_name() -> ActionType {
        type_name::<Func>().into()
    }
}

impl<Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned + Send> Serialize
    for FunctionActionRequest<Func, In, Out>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, Func, In: Serialize + DeserializeOwned, Out: Serialize + DeserializeOwned + Send>
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
