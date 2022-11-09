pub mod actions;
pub mod context;
pub mod factory;
pub mod state;

use state::{EventId, EventRecord};

pub use context::WorkflowContext;
pub use factory::WorkflowFactory;

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
    fn from(error: serde_json::error::Error) -> Self {
        Self::ParseError(format!("{error:?}"))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions_sorted::assert_eq;
    use reqwest::Url;

    use super::state::Event;
    use super::*;
    use crate::actions::action_type_name;

    struct Time;
    impl Time {
        async fn sleep(&self, duration: time::Duration) -> () {
            tokio::time::sleep(duration.try_into().unwrap()).await
        }
    }

    #[derive(Clone)]
    struct HttpService(Url);
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
        context: &mut WorkflowContext,
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
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = time::OffsetDateTime::now_utc();
        let factory = WorkflowFactory::new(test_workflow);

        let mut workflow = factory.start(now, "42".to_string()).unwrap();
        assert_eq!(workflow.drain_pending_events(), vec![
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name, 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested((http_put_name, 1, "[42,\"42 666\"]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, time_sleep_name, 0, "null".to_owned()), Ok(()));
        assert_eq!(workflow.drain_pending_events(), vec![
            (4, 1, now, Event::ActionResponse((time_sleep_name, 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested((time_sleep_name, 2, "[172800,0]".to_owned()))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, time_sleep_name, 2, "null".to_owned()), Ok(()));
        assert_eq!(workflow.drain_pending_events(), vec![
            (6, 2, now, Event::ActionResponse((time_sleep_name, 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (8, 2, now, Event::ActionRequested((time_sleep_name, 3, "[259200,0]".to_owned()))),
            (9, 2, now, Event::ActionDropped((time_sleep_name, 3))),
        ]);

        let now = now + time::Duration::seconds(1);
        assert_eq!(workflow.handle_action_response(now, http_put_name, 1, "\"24\"".to_owned()), Ok(()));
        assert_eq!(workflow.drain_pending_events(), vec![
            (10, 3, now, Event::ActionResponse((http_put_name, 1, "\"24\"".to_owned()))),
            (11, 3, now, Event::Finished(Ok("102".to_owned()))),
        ]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay() {
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = time::OffsetDateTime::now_utc();
        let factory = WorkflowFactory::new(test_workflow);

        let mut workflow = factory.replay([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name, 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested((http_put_name, 1, "[42,\"42 666\"]".to_owned()))),
            (4, 1, now, Event::ActionResponse((time_sleep_name, 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested((time_sleep_name, 2, "[172800,0]".to_owned()))),
        ].into()).unwrap();

        assert_eq!(workflow.drain_pending_events(), vec![]);
    }

    #[test]
    #[rustfmt::skip]
    fn replay_all() {
        let time_sleep_name = action_type_name(Time::sleep);
        let http_put_name = action_type_name(HttpService::put);
        let now = time::OffsetDateTime::now_utc();
        let factory = WorkflowFactory::new(test_workflow);

        let mut workflow = factory.replay([
            (0, 0, now, Event::Started("42".to_owned())),
            (1, 0, now, Event::Evaluated((0, "666".to_owned()))),
            (2, 0, now, Event::ActionRequested((time_sleep_name, 0, "[86400,0]".to_owned()))),
            (3, 0, now, Event::ActionRequested((http_put_name, 1, "[42,\"42 666\"]".to_owned()))),
            (4, 1, now, Event::ActionResponse((time_sleep_name, 0, "null".to_owned()))),
            (5, 1, now, Event::ActionRequested((time_sleep_name, 2, "[172800,0]".to_owned()))),
            (6, 2, now, Event::ActionResponse((time_sleep_name, 2, "null".to_owned()))),
            (7, 2, now, Event::Evaluated((1, "100".to_owned()))),
            (8, 2, now, Event::ActionRequested((time_sleep_name, 3, "[259200,0]".to_owned()))),
            (9, 2, now, Event::ActionDropped((time_sleep_name, 3))),
            (10, 3, now, Event::ActionResponse((http_put_name, 1, "\"24\"".to_owned()))),
            (11, 3, now, Event::Finished(Ok("102".to_owned()))),
        ].into()).unwrap();

        assert_eq!(workflow.drain_pending_events(), vec![]);
    }
}
