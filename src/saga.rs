use std::any::type_name;
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use futures::FutureExt;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Error;
use time::OffsetDateTime;

pub trait Workflow: Send {
    type Input: Serialize + DeserializeOwned;
    type Output: Serialize + DeserializeOwned;

    fn start(
        self,
        state: Arc<Mutex<WorkflowState>>,
        input: Self::Input,
    ) -> BoxFuture<'static, WorkflowResult<()>>;
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
        let action_id = state
            .lock()
            .unwrap()
            .record_action_request(request)
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

pub type WorkflowResult<T> = Result<T, WorkflowError>;

#[derive(Debug, PartialEq, Clone)]
pub enum WorkflowError {
    Canceled,
    TimedOut,
    StartFailed { actual: Box<EventRecord> },
    Panic(Option<String>),
    EventConflict(EventId),
    UnhandledEvent(Box<EventRecord>),
    ReplayFailed { expected: String, actual: String },
    ParseError(String),
}

impl From<serde_json::Error> for WorkflowError {
    fn from(error: Error) -> Self {
        Self::ParseError(format!("{error:?}"))
    }
}

struct EmptyWaker;
impl ArcWake for EmptyWaker {
    fn wake_by_ref(_: &Arc<Self>) {}
}

type EventId = u32;
type Revision = u32;
type ActionType = &'static str;
type ActionRequestId = u32;
type EvaluationId = u32;

type SubscriptionType = &'static str;
type SubscriptionId = u32;
type ItemRequestId = u32;

#[derive(Debug, PartialEq, Clone)]
pub enum Event {
    Started(String),
    Evaluated((EvaluationId, String)),
    ActionRequested((ActionType, ActionRequestId, String)),
    ActionResponse((ActionType, ActionRequestId, String)),
    ActionDropped((ActionType, ActionRequestId)),
    StreamRequested((SubscriptionType, SubscriptionId, String)),
    StreamOpened((SubscriptionType, SubscriptionId, String)),
    ItemRequested((SubscriptionType, SubscriptionId, ItemRequestId, String)),
    ItemResponse((SubscriptionType, SubscriptionId, ItemRequestId, String)),
    StreamClosed((SubscriptionType, SubscriptionId)),
    Finished(Result<String, WorkflowError>),
}

type EventRecord = (EventId, Revision, time::OffsetDateTime, Event);

type EventQueue = VecDeque<EventRecord>;

pub struct WorkflowState {
    now: time::OffsetDateTime,
    last_event: u32,
    last_revision: u32,
    next_eval: u32,
    actions: Actions,
    replay: EventQueue,
    pending: EventQueue,
}

impl WorkflowState {
    pub fn start(now: time::OffsetDateTime, started: String) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            pending: [(0, 0, now, Event::Started(started))].into(),
            ..Self::new(now)
        }))
    }

    pub fn replay(now: time::OffsetDateTime, replay: VecDeque<EventRecord>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            replay,
            ..Self::new(now)
        }))
    }

    fn new(now: time::OffsetDateTime) -> Self {
        Self {
            now,
            last_event: 0,
            last_revision: 0,
            next_eval: 0,
            actions: Default::default(),
            replay: Default::default(),
            pending: Default::default(),
        }
    }

    pub fn handle_action_response<A: ActionRequest>(
        &mut self,
        now: time::OffsetDateTime,
        action_id: ActionRequestId,
        response: A::Response,
    ) -> WorkflowResult<()> {
        let response = (
            A::type_name(),
            action_id,
            serde_json::to_string(&response).unwrap(),
        );
        self.record_new_revision(now, Event::ActionResponse(response.clone()));
        self.actions.apply_response(response)?;
        Ok(())
    }

    fn record_eval<T: Serialize + DeserializeOwned>(
        &mut self,
        evaluate: impl FnOnce() -> T,
    ) -> WorkflowResult<T> {
        let eval_id = self.next_eval;
        self.last_event += 1;
        self.next_eval += 1;
        let actual = (self.last_event, self.last_revision, self.now, eval_id);
        match self.replay.pop_front() {
            Some((event_id, last_revision, now, Event::Evaluated((eval_id, evaluation))))
                if (event_id, last_revision, now, eval_id) == actual =>
            {
                Ok(serde_json::from_str(evaluation.as_str())?)
            }
            None => {
                let value = evaluate();
                let evaluation = serde_json::to_string(&value).unwrap();
                self.pending.push_back((
                    self.last_event,
                    self.last_revision,
                    self.now,
                    Event::Evaluated((eval_id, evaluation)),
                ));
                Ok(value)
            }
            Some(expected) => Err(WorkflowError::ReplayFailed {
                expected: format!("{expected:?}"),
                actual: format!("{actual:?}"),
            }),
        }
    }

    pub fn record_action_request<A: ActionRequest>(
        &mut self,
        request: A,
    ) -> Result<ActionRequestId, WorkflowError> {
        let request = serde_json::to_string(&request).unwrap();
        let action_id = self.actions.new_request(A::type_name());
        let event = Event::ActionRequested((A::type_name(), action_id, request));
        self.record_event(event)?;
        Ok(action_id)
    }

    pub fn record_workflow_result<Out: Serialize>(
        &mut self,
        result: WorkflowResult<Out>,
    ) -> WorkflowResult<()> {
        let finished = result.map(|output| serde_json::to_string(&output).unwrap());
        self.record_event(Event::Finished(finished))
    }

    pub fn drain_pending_events(&mut self) -> Vec<EventRecord> {
        self.pending.drain(..).collect()
    }

    fn record_new_revision(&mut self, now: OffsetDateTime, event: Event) {
        self.last_event += 1;
        self.last_revision += 1;
        self.now = now;
        self.pending
            .push_back((self.last_event, self.last_revision, self.now, event))
    }

    fn record_event(&mut self, event: Event) -> WorkflowResult<()> {
        self.last_event += 1;
        let actual = (self.last_event, self.last_revision, self.now, event);
        match self.replay.pop_front() {
            Some(expected) if actual == expected => self.replay_next_revisions(),
            Some(expected) => Err(WorkflowError::ReplayFailed {
                expected: format!("{expected:?}"),
                actual: format!("{actual:?}"),
            }),
            None => {
                self.pending.push_back(actual);
                Ok(())
            }
        }
    }

    fn replay_next_revisions(&mut self) -> WorkflowResult<()> {
        while let Some((_, revision, _, _)) = self.replay.front() {
            if *revision == self.last_revision {
                return Ok(());
            }

            let (event_id, revision, event_time, event) = self.replay.pop_front().unwrap();
            self.last_event = event_id;
            self.last_revision = revision;
            self.now = event_time;
            match event {
                Event::ActionResponse(response) => self.actions.apply_response(response)?,
                _ => return Err(WorkflowError::Panic(None)),
            }
        }
        Ok(())
    }
}

struct ActionRequestEntry {
    action_type: ActionType,
    response: Option<String>,
}

#[derive(Default)]
struct Actions {
    next: ActionRequestId,
    responses: HashMap<ActionRequestId, ActionRequestEntry>,
}

impl Actions {
    pub fn new_request(&mut self, request_type: &'static str) -> u32 {
        let action_id = self.next;
        let entry = ActionRequestEntry {
            action_type: request_type,
            response: None,
        };
        self.responses.insert(action_id, entry);
        self.next += 1;
        action_id
    }

    pub fn validate_response(
        &mut self,
        action_type: ActionType,
        action_id: ActionRequestId,
    ) -> Result<(), WorkflowError> {
        match self.responses.entry(action_id) {
            Entry::Occupied(entry)
                if entry.get().action_type == action_type && entry.get().response.is_none() =>
            {
                Ok(())
            }
            _ => {
                // TODO: handle duplicates and invalid requests
                //  use an appropriate error type too
                Err(WorkflowError::EventConflict(action_id))
            }
        }
    }

    pub fn apply_response(
        &mut self,
        response: (ActionType, ActionRequestId, String),
    ) -> WorkflowResult<()> {
        let (action_type, action_id, response) = response;
        self.validate_response(action_type, action_id)?;
        match self.responses.entry(action_id) {
            Entry::Occupied(mut entry)
                if entry.get().action_type == action_type && entry.get().response.is_none() =>
            {
                entry.get_mut().response = Some(response);
                Ok(())
            }
            _ => {
                // TODO: maybe return error for logging?
                Err(WorkflowError::Panic(None))
            }
        }
    }

    pub fn pop_response(
        &mut self,
        expected_type: ActionType,
        expected_id: ActionRequestId,
    ) -> Result<Option<String>, WorkflowError> {
        let ActionRequestEntry {
            action_type,
            response,
        } = self
            .responses
            .get(&expected_id)
            .ok_or(WorkflowError::Panic(None))?;
        if action_type != &expected_type {
            // TODO: is this how we should handle a mismatched action type?
            let _ = self.responses.remove(&expected_id);
            return Err(WorkflowError::Panic(None));
        }
        if response.is_none() {
            return Ok(None);
        }
        Ok(self
            .responses
            .remove(&expected_id)
            .and_then(|entry| entry.response))
    }

    pub fn drop_action(
        &mut self,
        expected_type: ActionType,
        expected_id: ActionRequestId,
    ) -> WorkflowResult<bool> {
        match self.responses.remove(&expected_id) {
            Some(ActionRequestEntry { action_type, .. }) => {
                if action_type != expected_type {
                    // TODO: is this how we should handle a mismatched action type?
                    return Err(WorkflowError::Panic(None));
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

// struct SubscriptionEntry {
//     r#type: SubscriptionType,
//     next: SubscriptionId,
//     requests: HashMap<ItemRequestId, Option<String>>,
// }
//
// #[derive(Default)]
// struct Subscriptions {
//     next: SubscriptionId,
//     items: HashMap<SubscriptionId, SubscriptionEntry>,
// }

pub struct WorkflowExecution {
    state: Arc<Mutex<WorkflowState>>,
    future: WorkflowFuture,
}

impl WorkflowExecution {
    pub fn start<In: Serialize, Out, W: IntoWorkflow<In, Out>>(
        workflow: W,
        now: time::OffsetDateTime,
        input: In,
    ) -> WorkflowResult<Self> {
        let workflow = IntoWorkflow::into_workflow(workflow);
        let started = serde_json::to_string(&input).unwrap();
        let state = WorkflowState::start(now, started);
        Self::new(workflow, state, input)
    }

    pub fn replay<In: DeserializeOwned, Out, W: IntoWorkflow<In, Out>>(
        workflow: W,
        mut replay: VecDeque<EventRecord>,
    ) -> WorkflowResult<Self> {
        let workflow = IntoWorkflow::into_workflow(workflow);
        match replay.pop_front() {
            Some((0, 0, now, Event::Started(started))) => {
                let input = serde_json::from_str(started.as_str())?;
                let state = WorkflowState::replay(now, replay);
                Self::new(workflow, state, input)
            }
            Some(actual) => Err(WorkflowError::StartFailed {
                actual: Box::new(actual),
            }),
            None => Err(WorkflowError::Panic(None)),
        }
    }

    pub fn handle_action_response<A: ActionRequest>(
        &mut self,
        now: time::OffsetDateTime,
        action_id: ActionRequestId,
        response: A::Response,
    ) -> WorkflowResult<()> {
        self.state
            .lock()
            .unwrap()
            .handle_action_response::<A>(now, action_id, response)?;
        self.future.resume_execution()
    }

    pub fn handle_function_action_response<Ctx, In, Out, Fut, Func>(
        &mut self,
        now: time::OffsetDateTime,
        action_id: ActionRequestId,
        _: Func,
        response: Out,
    ) -> WorkflowResult<()>
    where
        In: Serialize + DeserializeOwned,
        Out: Serialize + DeserializeOwned,
        Fut: Future<Output = Out> + Send,
        Func: Send + Sync + 'static + Fn(Ctx, In) -> Fut,
    {
        self.state
            .lock()
            .unwrap()
            .handle_action_response::<FunctionActionRequest<Func, In, Out>>(
                now, action_id, response,
            )?;
        self.future.resume_execution()
    }

    pub fn drain_pending_events(&mut self) -> Vec<EventRecord> {
        self.state.lock().unwrap().drain_pending_events()
    }

    fn new<W: Workflow>(
        workflow: W,
        state: Arc<Mutex<WorkflowState>>,
        input: W::Input,
    ) -> Result<WorkflowExecution, WorkflowError> {
        let mut future = WorkflowFuture::new::<W>(workflow, state.clone(), input)?;
        future.resume_execution()?;
        Ok(Self { state, future })
    }
}

pub struct WorkflowFuture {
    waker: Arc<EmptyWaker>,
    future: Mutex<BoxFuture<'static, Result<(), WorkflowError>>>,
}

impl WorkflowFuture {
    pub fn new<W: Workflow>(
        workflow: W,
        state: Arc<Mutex<WorkflowState>>,
        input: W::Input,
    ) -> WorkflowResult<Self> {
        Ok(Self {
            waker: Arc::new(EmptyWaker),
            future: Mutex::new(workflow.start(state.clone(), input)),
        })
    }

    pub fn resume_execution(&mut self) -> WorkflowResult<()> {
        let mut future = self.future.lock().unwrap();
        let waker = waker_ref(&self.waker);
        let cx = &mut Context::from_waker(&*waker);
        let pin = Pin::new(future.deref_mut());
        match pin.poll(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Ok(()),
        }
    }
}

pub struct WorkflowContext(Arc<Mutex<WorkflowState>>);

impl WorkflowContext {
    pub fn new(state: Arc<Mutex<WorkflowState>>) -> Self {
        Self(state)
    }

    pub fn wait(&mut self, duration: time::Duration) -> ActionFuture<TimerRequest> {
        self.send(TimerRequest(duration))
    }

    pub fn send<A: ActionRequest>(&mut self, request: A) -> ActionFuture<A> {
        ActionFuture::<A>::new(self.0.clone(), request)
    }

    pub fn eval<T: Serialize + DeserializeOwned>(
        &mut self,
        evaluate: impl FnOnce() -> T,
    ) -> WorkflowResult<T> {
        self.0.lock().unwrap().record_eval(evaluate)
    }

    pub fn import<Ctx, In, Out, Fut, Func>(
        &mut self,
        _: Func,
    ) -> impl Fn(In) -> ActionFuture<FunctionActionRequest<Func, In, Out>>
    where
        In: Serialize + DeserializeOwned,
        Out: Serialize + DeserializeOwned,
        Fut: Future<Output = Out> + Send,
        Func: Fn(Ctx, In) -> Fut,
    {
        let state = self.0.clone();
        move |input| {
            let request = FunctionActionRequest(input, Default::default(), Default::default());
            ActionFuture::new(state.clone(), request)
        }
    }
}

pub trait IntoWorkflow<In, Out>: Sized {
    type Workflow: Workflow<Input = In, Output = Out>;
    /// Turns this value into its corresponding [`Workflow`].
    fn into_workflow(this: Self) -> Self::Workflow;
}

impl<In, Out, Fut, Func> IntoWorkflow<In, Out> for Func
where
    In: Serialize + DeserializeOwned + Send + 'static,
    Out: Serialize + DeserializeOwned,
    Fut: Future<Output = WorkflowResult<Out>> + Send,
    Func: Send + Sync + 'static + Fn(WorkflowContext, In) -> Fut,
{
    type Workflow = FunctionWorkflow<In, Out, Func>;

    fn into_workflow(func: Self) -> Self::Workflow {
        FunctionWorkflow {
            func,
            marker: Default::default(),
        }
    }
}

pub struct FunctionWorkflow<In, Out, Func> {
    func: Func,
    // NOTE: PhantomData<fn()-> T> gives this safe Send/Sync impls
    marker: PhantomData<fn() -> (In, Out)>,
}

impl<In, Out, Fut, Func> Workflow for FunctionWorkflow<In, Out, Func>
where
    In: Serialize + DeserializeOwned + Send + 'static,
    Out: Serialize + DeserializeOwned,
    Fut: Future<Output = WorkflowResult<Out>> + Send,
    Func: Send + Sync + 'static + Fn(WorkflowContext, In) -> Fut,
{
    type Input = In;
    type Output = Out;

    fn start(
        self,
        state: Arc<Mutex<WorkflowState>>,
        input: Self::Input,
    ) -> BoxFuture<'static, WorkflowResult<()>> {
        async move {
            let context = WorkflowContext::new(state.clone());
            let result = (self.func)(context, input).await;
            state.lock().unwrap().record_workflow_result(result)
        }
        .boxed()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TimerRequest(time::Duration);
impl ActionRequest for TimerRequest {
    type Response = ();
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions_sorted::assert_eq;

    struct Time;
    impl Time {
        async fn sleep(&self, duration: time::Duration) -> () {
            tokio::time::sleep(duration.try_into().unwrap()).await
        }
    }

    struct HttpService(reqwest::Url);
    impl HttpService {
        async fn put(&self, (id, state): (u32, String)) -> String {
            let url = self
                .0
                .join(&id.to_string())
                .map_err(|e| e.to_string())
                .unwrap();
            match reqwest::Client::new()
                .put(url)
                .body(state.clone())
                .send()
                .await
                .map_err(|e| e.to_string())
            {
                Ok(_) => state,
                Err(e) => e,
            }
        }
    }

    pub async fn test_workflow(
        mut context: WorkflowContext,
        request: usize,
    ) -> WorkflowResult<usize> {
        let days = time::Duration::days;
        let sleep = context.import(Time::sleep);
        let http_put = context.import(HttpService::put);
        let eval1 = context.eval(|| 666)?;
        let wait1 = sleep(days(1));
        let http_response = http_put((42, format!("{request} {eval1}")));
        wait1.await?;
        let wait2 = sleep(days(2));
        wait2.await?;
        let eval2 = context.eval(|| 100usize)?;
        std::mem::drop(sleep(days(3)));
        http_response.await.map(|r| r.len() + eval2)
    }

    #[test]
    #[rustfmt::skip]
    fn start() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::start( test_workflow, now, 42).unwrap();
        assert_eq!(execution.drain_pending_events(), vec![
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested(("async_workflow::saga::tests::HttpService::put", 1, "[42,\"42 666\"]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_function_action_response(now, 0, Time::sleep, ()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (4, 1, now, Event::ActionResponse(("async_workflow::saga::tests::Time::sleep", 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 2, "[172800,0]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_function_action_response(now, 2, Time::sleep, ()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (6, 2, now, Event::ActionResponse(("async_workflow::saga::tests::Time::sleep", 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (8, 2, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 3, "[259200,0]".to_owned()))),
            (9, 2, now, Event::ActionDropped(("async_workflow::saga::tests::Time::sleep", 3))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_function_action_response(now, 1, HttpService::put, "24".to_owned()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (10, 3, now, Event::ActionResponse(("async_workflow::saga::tests::HttpService::put", 1, "\"24\"".to_owned()))),
            (11, 3, now, Event::Finished(Ok("102".to_owned()))),
        ]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::replay(test_workflow, [
                (0, 0, now, Event::Started("42".to_owned())),
                (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
                (2, 0, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 0, "[86400,0]".to_owned()))),
                (3, 0, now, Event::ActionRequested(("async_workflow::saga::tests::HttpService::put", 1, "[42,\"42 666\"]".to_owned()))),
                (4, 1, now, Event::ActionResponse(("async_workflow::saga::tests::Time::sleep", 0, "null".to_owned()))),
                (5, 1, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 2, "[172800,0]".to_owned()))),
            ].into()
        ).unwrap();

        assert_eq!(execution.drain_pending_events(), vec![]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay_all() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::replay(test_workflow, [
                (0, 0, now, Event::Started("42".to_owned())),
                (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
                (2, 0, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 0, "[86400,0]".to_owned()))),
                (3, 0, now, Event::ActionRequested(("async_workflow::saga::tests::HttpService::put", 1, "[42,\"42 666\"]".to_owned()))),
                (4, 1, now, Event::ActionResponse(("async_workflow::saga::tests::Time::sleep", 0, "null".to_owned()))),
                (5, 1, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 2, "[172800,0]".to_owned()))),
                (6, 2, now, Event::ActionResponse(("async_workflow::saga::tests::Time::sleep", 2, "null".to_owned()))),
                (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
                (8, 2, now, Event::ActionRequested(("async_workflow::saga::tests::Time::sleep", 3, "[259200,0]".to_owned()))),
                (9, 2, now, Event::ActionDropped(("async_workflow::saga::tests::Time::sleep", 3))),
                (10, 3, now, Event::ActionResponse(("async_workflow::saga::tests::HttpService::put", 1, "\"24\"".to_owned()))),
                (11, 3, now, Event::Finished(Ok("102".to_owned()))),
            ].into(),
        ).unwrap();

        assert_eq!(execution.drain_pending_events(), vec![]);
    }
