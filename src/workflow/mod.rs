pub mod action;
pub mod actions;
pub mod context;
pub mod events;
pub mod factory;
pub mod state;

#[cfg(test)]
pub mod test;

use serde::{Deserialize, Serialize};

use events::{EventId, EventRecord};

pub use context::WorkflowContext;
pub use factory::WorkflowFactory;

pub type WorkflowResult<T> = Result<T, WorkflowError>;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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
    fn from(error: serde_json::error::Error) -> Self {
        Self::ParseError(format!("{error:?}"))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions_sorted::assert_eq;

    use crate::actions::action_type_name;
    use crate::workflow::events::{Event, EventTime};
    use crate::workflow::test::*;

    use super::*;

    #[test]
    #[rustfmt::skip]
    fn start() {
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = EventTime::now_utc();
        let factory = WorkflowFactory::new(test::test_workflow);

        let (mut workflow, start_events) = factory.start(now, "42".to_string()).unwrap();
        assert_eq!(start_events, vec![
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name.clone(), 0, "[86400,0]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, (time_sleep_name.clone(), 0, "null".to_owned())).unwrap(), vec![
            (3, 1, now, Event::ActionResponse((time_sleep_name.clone(), 0, "null".to_owned()))),
            (4, 1, now, Event::ActionRequested((time_sleep_name.clone(), 1, "[172800,0]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, (time_sleep_name.clone(), 1, "null".to_owned())).unwrap(), vec![
            (5, 2, now, Event::ActionResponse((time_sleep_name.clone(), 1, "null".to_owned()))),
            (6, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (7, 2, now, Event::ActionRequested((time_sleep_name.clone(), 2, "[259200,0]".to_owned()))),
            (8, 2, now, Event::ActionRequested((http_put_name.clone(), 3, "[42,\"42 666\"]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, (http_put_name.clone(), 3, "\"24\"".to_owned())).unwrap(), vec![
            (9, 3, now, Event::ActionResponse((http_put_name.clone(), 3, "\"24\"".to_owned()))),
            (10, 3, now, Event::ActionCanceled((time_sleep_name.clone(), 2))),
            (11, 3, now, Event::Finished(Ok("102".to_owned()))),
        ]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay() {
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = time::OffsetDateTime::now_utc();
        let factory = WorkflowFactory::new(test::test_workflow);

        let mut workflow = factory.replay([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name.clone(), 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested((http_put_name.clone(), 1, "[42,\"42 666\"]".to_owned()))),
            (4, 1, now, Event::ActionResponse((time_sleep_name.clone(), 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested((time_sleep_name.clone(), 2, "[172800,0]".to_owned()))),
        ].into()).unwrap();

        assert_eq!(workflow.drain_pending_events(), vec![]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay_all() {
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = time::OffsetDateTime::now_utc();
        let factory = WorkflowFactory::new(test::test_workflow);

        let mut workflow = factory.replay([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name.clone(), 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested((http_put_name.clone(), 1, "[42,\"42 666\"]".to_owned()))),
            (4, 1, now, Event::ActionResponse((time_sleep_name.clone(), 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested((time_sleep_name.clone(), 2, "[172800,0]".to_owned()))),
            (6, 2, now, Event::ActionResponse((time_sleep_name.clone(), 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (8, 2, now, Event::ActionRequested((time_sleep_name.clone(), 3, "[259200,0]".to_owned()))),
            (9, 3, now, Event::ActionResponse((http_put_name.clone(), 1, "\"24\"".to_owned()))),
            (10, 3, now, Event::ActionCanceled((time_sleep_name.clone(), 3))),
            (11, 3, now, Event::Finished(Ok("102".to_owned()))),
        ].into()).unwrap();

        assert_eq!(workflow.drain_pending_events(), vec![]);
    }
}
