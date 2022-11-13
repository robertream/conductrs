use std::sync::{Arc, Mutex};

use super::action::{ActionId, ActionRequest, ActionResult, ActionType};
use super::actions::Actions;
use super::events::{Event, EventQueue, EventRecord, EventTime};
use super::{WorkflowError, WorkflowResult};

#[derive(Clone)]
pub struct WorkflowState(Arc<Mutex<WorkflowStateInternal>>);

impl WorkflowState {
    pub fn start(now: time::OffsetDateTime, started: String) -> Self {
        Self(Arc::new(Mutex::new(WorkflowStateInternal {
            pending: [(0, 0, now, Event::Started(started))].into(),
            ..WorkflowStateInternal::new(now)
        })))
    }

    pub fn replay(now: time::OffsetDateTime, replay: EventQueue) -> Self {
        Self(Arc::new(Mutex::new(WorkflowStateInternal {
            replay,
            ..WorkflowStateInternal::new(now)
        })))
    }

    pub fn apply(&self, event: EventRecord) {
        self.0.lock().unwrap().replay.push_back(event)
    }

    pub fn record_eval(&self, evaluate: impl FnOnce() -> String) -> WorkflowResult<String> {
        let mut this = self.0.lock().unwrap();
        let eval_id = this.next_eval;
        this.last_event += 1;
        this.next_eval += 1;
        let actual = (this.last_event, this.last_revision, this.now, eval_id);
        match this.replay.pop_front() {
            Some((event_id, last_revision, now, Event::Evaluated((eval_id, evaluation))))
                if (event_id, last_revision, now, eval_id) == actual =>
            {
                Ok(evaluation)
            }
            None => {
                let evaluation = evaluate();
                let event = (
                    this.last_event,
                    this.last_revision,
                    this.now,
                    Event::Evaluated((eval_id, evaluation.clone())),
                );
                this.pending.push_back(event);
                Ok(evaluation)
            }
            Some(expected) => Err(WorkflowError::ReplayFailed {
                expected: format!("{expected:?}"),
                actual: format!("{actual:?}"),
            }),
        }
    }

    pub fn begin_action<A>(&self, request: &A) -> (ActionResult<A>, ActionId)
    where
        for<'a> A: ActionRequest + 'a,
    {
        let mut this = self.0.lock().unwrap();
        let type_name = A::type_name();
        let (result, result_handler) = ActionResult::new();
        let action_id = this.actions.add_action(type_name.clone(), result_handler);
        this.try_this(|this| {
            let action = serde_json::to_string(&request)?;
            this.record_event(Event::ActionRequested((type_name, action_id, action)))
        });
        (result, action_id)
    }

    pub fn cancel_action(&self, action_id: ActionId) {
        self.0.lock().unwrap().try_this(|this| {
            let action_type = this.actions.remove_action(action_id)?;
            this.record_event(Event::ActionCanceled((action_type, action_id)))
        });
    }

    pub fn end_action(&self, action_id: ActionId) {
        self.0.lock().unwrap().try_this(|this| {
            let _ = this.actions.remove_action(action_id)?;
            Ok(())
        });
    }

    pub fn record_workflow_result(&self, finished: WorkflowResult<String>) -> WorkflowResult<()> {
        self.0
            .lock()
            .unwrap()
            .record_event(Event::Finished(finished))
    }

    pub fn handle_action_response(
        &self,
        now: time::OffsetDateTime,
        action_type: ActionType,
        action_id: ActionId,
        response: String,
    ) -> WorkflowResult<()> {
        let mut this = self.0.lock().unwrap();
        let response = (action_type, action_id, response);
        this.actions.apply_response(&response)?;
        this.record_new_revision(now, Event::ActionResponse(response));
        Ok(())
    }

    pub fn drain_pending_events(&self) -> EventQueue {
        self.0.lock().unwrap().pending.drain(..).collect()
    }

    pub fn pop_error(&self) -> Option<WorkflowError> {
        self.0.lock().unwrap().error.take()
    }
}

struct WorkflowStateInternal {
    now: time::OffsetDateTime,
    last_event: u32,
    last_revision: u32,
    next_eval: u32,
    replay: EventQueue,
    pending: EventQueue,
    actions: Actions,
    error: Option<WorkflowError>,
}

impl WorkflowStateInternal {
    fn new(now: time::OffsetDateTime) -> Self {
        Self {
            now,
            last_event: 0,
            last_revision: 0,
            next_eval: 0,
            actions: Default::default(),
            replay: Default::default(),
            pending: Default::default(),
            error: None,
        }
    }

    fn record_new_revision(&mut self, now: EventTime, event: Event) {
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
                Event::ActionResponse(response) => self.actions.apply_response(&response)?,
                _ => return Err(WorkflowError::Panic(None)),
            }
        }
        Ok(())
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

    fn try_this(&mut self, func: impl FnOnce(&mut Self) -> WorkflowResult<()>) {
        let result = func(self);
        if self.error.is_none() {
            return;
        }
        if let Err(error) = result {
            self.error = Some(error);
        }
    }
}
