use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use time::OffsetDateTime;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use serde_json::Error;

pub trait WorkflowTrait {
    const NAME: &'static str;
    type Input: Serialize + DeserializeOwned;
    type Output: Serialize + DeserializeOwned;

    fn start(
        state: Arc<Mutex<WorkflowState>>,
        input: Self::Input,
    ) -> BoxFuture<'static, WorkflowResult<()>>;
}

pub trait ActionRequest : Serialize + DeserializeOwned {
    const TYPE_NAME: &'static str;
    type Response : Serialize + DeserializeOwned;
}

pub struct ActionFuture<A: ActionRequest> {
    action_id: u32,
    state: Arc<Mutex<WorkflowState>>,
    phantom: PhantomData<A>
}

impl<A: ActionRequest> ActionFuture< A> {
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
        let expected_type = A::TYPE_NAME;
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

pub type WorkflowResult<T> = Result<T, WorkflowError>;

#[derive(Debug, PartialEq, Clone)]
pub enum WorkflowError {
    Canceled,
    TimedOut,
    StartFailed {
        actual: Box<EventRecord>,
    },
    Panic(Option<String>),
    EventConflict(EventId),
    UnhandledEvent(Box<EventRecord>),
    ReplayFailed {
        expected: String,
        actual: String,
    },
    ParseError(String)
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
            .. Self::new(now)
        }))
    }

    pub fn replay(now: time::OffsetDateTime, replay: VecDeque<EventRecord>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            replay,
            .. Self::new(now)
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
            pending: Default::default()
        }
    }

    pub fn handle_action_response<A: ActionRequest>(
        &mut self,
        now: time::OffsetDateTime,
        action_id: ActionRequestId,
        response: A::Response,
    ) -> WorkflowResult<()> {
        let response = (A::TYPE_NAME, action_id, serde_json::to_string(&response).unwrap());
        self.record_new_revision(now, Event::ActionResponse(response.clone()));
        self.actions.apply_response(response)?;
        Ok(())
    }

    fn record_eval<T: Serialize + DeserializeOwned>(&mut self, evaluate: impl FnOnce() -> T) -> WorkflowResult<T> {
        let eval_id = self.next_eval;
        self.last_event += 1;
        self.next_eval += 1;
        let actual = (self.last_event, self.last_revision, self.now, eval_id);
        match self.replay.pop_front() {
            Some((event_id, last_revision, now, Event::Evaluated((eval_id, evaluation))))
                if (event_id, last_revision, now, eval_id) == actual => {
                    Ok(serde_json::from_str(evaluation.as_str())?)
            },
            None => {
                let value = evaluate();
                let evaluation = serde_json::to_string(&value).unwrap();
                self.pending.push_back((self.last_event, self.last_revision, self.now, Event::Evaluated((eval_id, evaluation))));
                Ok(value)
            },
            Some(expected) => Err(WorkflowError::ReplayFailed { expected: format!("{expected:?}"), actual: format!("{actual:?}") }),
        }
    }

    pub fn record_action_request<A: ActionRequest>(
        &mut self,
        request: A,
    ) -> Result<ActionRequestId, WorkflowError> {
        let request = serde_json::to_string(&request).unwrap();
        let action_id = self.actions.new_request(A::TYPE_NAME);
        let event = Event::ActionRequested((A::TYPE_NAME, action_id, request));
        self.record_event(event)?;
        Ok(action_id)

    }

    pub fn record_workflow_result(
        &mut self,
        result: WorkflowResult<String>,
    ) -> WorkflowResult<()> {
        self.record_event(Event::Finished(result))
    }

    pub fn drain_pending_events(&mut self) -> Vec<EventRecord> {
        self.pending.drain(..).collect()
    }

    fn record_new_revision(&mut self, now: OffsetDateTime, event: Event) {
        self.last_event += 1;
        self.last_revision += 1;
        self.now = now;
        self.pending.push_back((self.last_event, self.last_revision, self.now, event))
    }

    fn record_event(&mut self, event: Event) -> WorkflowResult<()> {
        self.last_event += 1;
        let actual = (self.last_event, self.last_revision, self.now, event);
        match self.replay.pop_front() {
            Some(expected) if actual == expected  => self.replay_next_revisions(),
            Some(expected) => Err(WorkflowError::ReplayFailed { expected: format!("{expected:?}"), actual: format!("{actual:?}") }),
            None => {
                self.pending.push_back(actual);
                Ok(())
            },
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
        self.next = action_id + 1;
        let entry = ActionRequestEntry { action_type: request_type, response: None };
        self.responses.insert(action_id, entry);
        action_id
    }

    pub fn validate_response(&mut self, action_type: ActionType, action_id: ActionRequestId) -> Result<(), WorkflowError> {
        match self.responses.entry(action_id) {
            Entry::Occupied(entry)
            if entry.get().action_type == action_type && entry.get().response.is_none() => {
                Ok(())
            },
            _ => {
                // TODO: handle duplicates and invalid requests
                //  use an appropriate error type too
                Err(WorkflowError::EventConflict(action_id))
            }
        }
    }

    pub fn apply_response(&mut self, response: (ActionType, ActionRequestId, String)) -> WorkflowResult<()> {
        let (action_type, action_id, response) = response;
        self.validate_response(action_type, action_id)?;
        match self.responses.entry(action_id) {
            Entry::Occupied(mut entry)
            if entry.get().action_type == action_type && entry.get().response.is_none() => {
                entry.get_mut().response = Some(response);
                Ok(())
            },
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
        let ActionRequestEntry{
            action_type, response
        } = self.responses
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
        Ok(self.responses
            .remove(&expected_id)
            .and_then(|entry| entry.response))
    }
}

pub struct WorkflowExecution {
    state: Arc<Mutex<WorkflowState>>,
    future: WorkflowFuture,
}

impl WorkflowExecution {
    pub fn start<W: WorkflowTrait>(now: time::OffsetDateTime, input: W::Input) -> WorkflowResult<Self> {
        let started = serde_json::to_string(&input).unwrap();
        let state = WorkflowState::start(now, started);
        Self::new::<W>(state, input)
    }

    pub fn replay<W: WorkflowTrait>(mut replay: VecDeque<EventRecord>) -> WorkflowResult<Self> {
        match replay.pop_front() {
            Some((0, 0, now, Event::Started(started))) => {
                let input = serde_json::from_str(started.as_str())?;
                let state = WorkflowState::replay(now, replay);
                Self::new::<W>(state, input)
            }
            Some(actual) => Err(WorkflowError::StartFailed { actual: Box::new(actual) }),
            None => Err(WorkflowError::Panic(None)),
        }
    }

    pub fn handle_action_response<A: ActionRequest>(
        &mut self,
        now: time::OffsetDateTime,
        action_id: ActionRequestId,
        response: A::Response,
    ) -> WorkflowResult<()> {
        self.state.lock().unwrap().handle_action_response::<A>(now, action_id, response)?;
        self.future.resume_execution()
    }

    pub fn drain_pending_events(&mut self) -> Vec<EventRecord> {
        self.state.lock().unwrap().drain_pending_events()
    }

    fn new<W: WorkflowTrait>(state: Arc<Mutex<WorkflowState>>, input: W::Input) -> Result<WorkflowExecution, WorkflowError> {
        let mut future = WorkflowFuture::new::<W>(state.clone(), input)?;
        future.resume_execution()?;
        Ok(Self {
            state,
            future,
        })
    }
}

pub struct WorkflowFuture {
    waker: Arc<EmptyWaker>,
    future: Mutex<BoxFuture<'static, Result<(), WorkflowError>>>,
}

impl WorkflowFuture {
    pub fn new<W:WorkflowTrait>(state: Arc<Mutex<WorkflowState>>, input: W::Input) -> WorkflowResult<Self> {
        let mut future = Self {
            waker: Arc::new(EmptyWaker),
            future: Mutex::new(W::start(state.clone(), input)),
        };
        future.resume_execution()?;
        Ok(future)
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

    pub fn wait(&mut self, duration: time::Duration) -> ActionFuture::<TimerRequest> {
        self.send( TimerRequest(duration))
    }

    pub fn send<A: ActionRequest>(&mut self, request: A) -> ActionFuture::<A> {
        ActionFuture::<A>::new(self.0.clone(), request)
    }

    pub fn eval<T: Serialize + DeserializeOwned>(&mut self, evaluate: impl FnOnce() -> T) -> WorkflowResult<T> {
        self.0.lock().unwrap().record_eval(evaluate)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TimerRequest(time::Duration);
impl ActionRequest for TimerRequest {
    const TYPE_NAME: &'static str = "timer";
    type Response = ();
}

#[cfg(test)]
mod tests {
    use pretty_assertions_sorted::assert_eq;
    use serde::{Serialize, Deserialize};
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct HttpRequest(String);

    impl ActionRequest for HttpRequest {
        const TYPE_NAME: &'static str = "http";
        type Response = String;
    }

    #[derive(Debug, PartialEq)]
    struct TestWorkflow;

    impl WorkflowTrait for TestWorkflow {
        const NAME: &'static str = "test-workflow";
        type Input = usize;
        type Output = usize;

        fn start(
            state: Arc<Mutex<WorkflowState>>,
            request: Self::Input,
        ) -> BoxFuture<'static, WorkflowResult<()>> {
            let future = async move {
                let mut context = WorkflowContext::new(state.clone());
                let result = Self::execute(&mut context, request).await.map(|x| serde_json::to_string(&x).unwrap());
                let result = context.0.lock().unwrap().record_workflow_result(result);
                result
            };
            future.boxed()
        }
    }

    impl TestWorkflow {
        pub async fn execute(
            context: &mut WorkflowContext,
            request: usize,
        ) -> WorkflowResult<usize> {
            let eval1 = context.eval(|| 666)?;
            let wait1 = context.wait(time::Duration::days(1));
            let http_response = context.send(HttpRequest(format!("{request} {eval1}")));
            let _ = wait1.await;
            let wait2 = context.wait(time::Duration::days(2));
            let _ = wait2.await;
            let eval2 = context.eval(|| 100usize)?;
            http_response.await.map(|x| x.len() + eval2)
        }
    }

    #[test]
    #[rustfmt::skip]
    fn start() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::start::<TestWorkflow>(now, 42).unwrap();
        assert_eq!(execution.drain_pending_events(), vec![
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested(("timer", 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested(("http", 1, "\"42 666\"".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_action_response::<TimerRequest>(now, 0, ()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (4, 1, now, Event::ActionResponse(("timer", 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested(("timer", 2, "[172800,0]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_action_response::<TimerRequest>(now, 2, ()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (6, 2, now, Event::ActionResponse(("timer", 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(execution.handle_action_response::<HttpRequest>(now, 1, "24".to_owned()), Ok(()));
        assert_eq!(execution.drain_pending_events(), vec![
            (8, 3, now, Event::ActionResponse(("http", 1, "\"24\"".to_owned()))),
            (9, 3, now, Event::Finished(Ok("102".to_owned()))),
        ]);
    }

    #[test]
    fn replay() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::replay::<TestWorkflow>([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested(("timer", 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested(("http", 1, "\"42 666\"".to_owned()))),
            (4, 1, now, Event::ActionResponse(("timer", 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested(("timer", 2, "[172800,0]".to_owned()))),
        ].into()).unwrap();

        assert_eq!(execution.drain_pending_events(), vec![]);
    }

    #[test]
    fn replay_all() {
        let now = time::OffsetDateTime::now_utc();
        let mut execution = WorkflowExecution::replay::<TestWorkflow>([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested(("timer", 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested(("http", 1, "\"42 666\"".to_owned()))),
            (4, 1, now, Event::ActionResponse(("timer", 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested(("timer", 2, "[172800,0]".to_owned()))),
            (6, 2, now, Event::ActionResponse(("timer", 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (8, 3, now, Event::ActionResponse(("http", 1, "\"24\"".to_owned()))),
            (9, 3, now, Event::Finished(Ok("102".to_owned()))),
        ].into()).unwrap();

        assert_eq!(execution.drain_pending_events(), vec![]);
    }
}