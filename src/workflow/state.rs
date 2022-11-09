use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

use super::actions::Actions;
use super::actions::{ActionRequestId, ActionType};
use super::{WorkflowError, WorkflowResult};

pub type EventId = u32;
pub type Revision = u32;
pub type EvaluationId = u32;

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

pub type EventRecord = (EventId, Revision, time::OffsetDateTime, Event);

type EventQueue = VecDeque<EventRecord>;

pub struct WorkflowState {
    now: time::OffsetDateTime,
    last_event: u32,
    last_revision: u32,
    next_eval: u32,
    replay: EventQueue,
    pending: EventQueue,
    pub actions: Actions,
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

    pub fn apply(&mut self, event: EventRecord) {
        self.replay.push_back(event)
    }

    pub fn record_eval(&mut self, evaluate: impl FnOnce() -> String) -> WorkflowResult<String> {
        let eval_id = self.next_eval;
        self.last_event += 1;
        self.next_eval += 1;
        let actual = (self.last_event, self.last_revision, self.now, eval_id);
        match self.replay.pop_front() {
            Some((event_id, last_revision, now, Event::Evaluated((eval_id, evaluation))))
                if (event_id, last_revision, now, eval_id) == actual =>
            {
                Ok(evaluation)
            }
            None => {
                let evaluation = evaluate();
                self.pending.push_back((
                    self.last_event,
                    self.last_revision,
                    self.now,
                    Event::Evaluated((eval_id, evaluation.clone())),
                ));
                Ok(evaluation)
            }
            Some(expected) => Err(WorkflowError::ReplayFailed {
                expected: format!("{expected:?}"),
                actual: format!("{actual:?}"),
            }),
        }
    }

    pub fn record_action_request(
        &mut self,
        action_type: ActionType,
        request: String,
    ) -> Result<ActionRequestId, WorkflowError> {
        let action_id = self.actions.new_request(action_type);
        let event = Event::ActionRequested((action_type, action_id, request));
        self.record_event(event)?;
        Ok(action_id)
    }

    pub fn record_workflow_result(
        &mut self,
        finished: WorkflowResult<String>,
    ) -> WorkflowResult<()> {
        self.record_event(Event::Finished(finished))
    }

    pub fn handle_action_response(
        &mut self,
        now: time::OffsetDateTime,
        action_type: &str,
        action_id: ActionRequestId,
        response: String,
    ) -> WorkflowResult<()> {
        let (action_type, action_id) = self.actions.validate_response(action_type, action_id)?;
        let response = (action_type, action_id, response);
        self.record_new_revision(now, Event::ActionResponse(response.clone()));
        self.actions.apply_response(response)?;
        Ok(())
    }

    pub fn drain_pending_events(&mut self) -> Vec<EventRecord> {
        self.pending.drain(..).collect()
    }

    pub(crate) fn record_event(&mut self, event: Event) -> WorkflowResult<()> {
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

    fn record_new_revision(&mut self, now: OffsetDateTime, event: Event) {
        self.last_event += 1;
        self.last_revision += 1;
        self.now = now;
        self.pending
            .push_back((self.last_event, self.last_revision, self.now, event))
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
}
