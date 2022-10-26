use std::sync::{Arc, Mutex};
use std::task::Poll;

use futures::future::BoxFuture;
use futures::FutureExt;
use pretty_assertions_sorted::assert_eq;

use crate::workflow::*;

#[derive(Debug, PartialEq)]
struct StartTestWorkflow {}

#[derive(Debug, PartialEq)]
enum TestSuspended {
    TimerStarted(time::Duration),
    HttpRequest(String),
}

#[derive(Debug, PartialEq)]
enum TestAwakened {
    TimerExpired,
    HttpResponse(String),
}

struct TimerAction;
impl WorkflowAction<TestWorkflow> for TimerAction {
    type Input = time::Duration;
    type Output = ();

    fn suspend(input: Self::Input) -> TestSuspended {
        TestSuspended::TimerStarted(input)
    }

    fn matches_with(event: &TestAwakened) -> bool {
        matches!(event, TestAwakened::TimerExpired)
    }

    fn extract_output(event: TestAwakened) -> Option<Self::Output> {
        match event {
            TestAwakened::TimerExpired => Some(()),
            _ => None,
        }
    }
}

struct HttpAction;
impl WorkflowAction<TestWorkflow> for HttpAction {
    type Input = String;
    type Output = String;

    fn suspend(input: Self::Input) -> TestSuspended {
        TestSuspended::HttpRequest(input)
    }

    fn matches_with(event: &TestAwakened) -> bool {
        matches!(event, TestAwakened::HttpResponse(_))
    }

    fn extract_output(event: TestAwakened) -> Option<Self::Output> {
        match event {
            TestAwakened::HttpResponse(response) => Some(response),
            _ => None,
        }
    }
}

struct TestWorkflowContext(Arc<Mutex<WorkflowState<TestWorkflow>>>);

impl TestWorkflowContext {
    fn new(state: Arc<Mutex<WorkflowState<TestWorkflow>>>) -> Self {
        Self(state)
    }
}

impl TestWorkflowContext {
    pub fn wait(
        &mut self,
        duration: time::Duration,
    ) -> SuspensionFuture<TestWorkflow, TimerAction> {
        SuspensionFuture::<TestWorkflow, TimerAction>::new(self.0.clone(), duration)
    }

    pub fn send_request(&mut self, request: String) -> SuspensionFuture<TestWorkflow, HttpAction> {
        SuspensionFuture::<TestWorkflow, HttpAction>::new(self.0.clone(), request)
    }
}

#[derive(Debug, PartialEq)]
struct TestWorkflow;
impl WorkflowTrait for TestWorkflow {
    type Request = StartTestWorkflow;
    type Response = String;
    type SuspensionEvent = TestSuspended;
    type AwakenEvent = TestAwakened;

    fn start(
        state: Arc<Mutex<WorkflowState<Self>>>,
        request: Self::Request,
    ) -> BoxFuture<'static, Result<(), String>> {
        let future = async move {
            let mut context = TestWorkflowContext::new(state.clone());
            let result = Self::execute(&mut context, request).await;
            state.lock().unwrap().ensure_expected_result(result)
        };
        future.boxed()
    }
}

impl TestWorkflow {
    pub async fn execute(
        context: &mut TestWorkflowContext,
        _request: StartTestWorkflow,
    ) -> Result<String, String> {
        let wait1 = context.wait(time::Duration::days(1));
        let response = context.send_request("test".to_owned());
        let _ = wait1.await;
        let wait2 = context.wait(time::Duration::days(2));
        let _ = wait2.await;
        response.await
    }
}

use TestSuspended::*;
use TestAwakened::*;

#[test]
#[rustfmt::skip]
fn test() {
    let now = time::OffsetDateTime::now_utc();
    let state = WorkflowState::<TestWorkflow>::new(now, StartTestWorkflow {});

    let mut execution = WorkflowExecution::new(state).unwrap();
    assert_eq!(execution.pop_pending(),Some((1, now, Event::Suspended((0, TimerStarted(time::Duration::days(1)))))));
    assert_eq!(execution.pop_pending(), Some((2,now,Event::Suspended((1, HttpRequest("test".to_owned()))))));
    assert_eq!(execution.poll(), Poll::Pending);
    assert_eq!(execution.pop_pending(), None);

    let now = now + time::Duration::hours(1);
    execution.awaken(3, now, (0, TimerExpired));
    assert_eq!(execution.poll(), Poll::Pending);
    assert_eq!(execution.pop_pending(), Some((4, now, Event::Suspended((2, TimerStarted(time::Duration::days(2)))))));

    execution.awaken(5, now, (2, TimerExpired));
    assert_eq!(execution.poll(), Poll::Pending);
    assert_eq!(execution.pop_pending(), None);

    execution.awaken(6, now, (1, HttpResponse("response".to_owned())));
    assert_eq!(execution.poll(), Poll::Ready(Ok(())));
    assert_eq!(execution.pop_pending(), Some((7, now, Event::Finished(Ok("response".to_owned())))));
}
