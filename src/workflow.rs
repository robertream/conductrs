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

pub trait WorkflowTrait: Debug + PartialEq {
    type Request: Debug + PartialEq;
    type Response: Debug + PartialEq;
    type SuspensionEvent: Debug + PartialEq;
    type AwakenEvent: Debug + PartialEq;

    fn start(
        state: Arc<Mutex<WorkflowState<Self>>>,
        input: Self::Request,
    ) -> BoxFuture<'static, Result<(), String>>;
}

pub trait WorkflowAction<W: WorkflowTrait + ?Sized> {
    type Input;
    type Output;

    fn suspend(input: Self::Input) -> W::SuspensionEvent;
    fn matches_with(event: &W::AwakenEvent) -> bool;
    fn extract_output(event: W::AwakenEvent) -> Option<Self::Output>;
}

pub struct SuspensionFuture<W: WorkflowTrait, A: WorkflowAction<W>> {
    suspension: u32,
    state: Arc<Mutex<WorkflowState<W>>>,
    action: PhantomData<A>,
}

impl<W: WorkflowTrait, A: WorkflowAction<W>> SuspensionFuture<W, A> {
    pub fn new(state: Arc<Mutex<WorkflowState<W>>>, input: A::Input) -> SuspensionFuture<W, A> {
        let suspension = state
            .lock()
            .unwrap()
            .ensure_expected_suspension(A::suspend(input))
            // TODO: maybe don't panic on failure?
            //  but do we want replay failure to show up in the signature of all async functions in a workflow?
            .expect("ResponseFuture");
        Self {
            suspension,
            state,
            action: Default::default(),
        }
    }
}

impl<W: WorkflowTrait + Unpin, A: WorkflowAction<W> + Unpin> Future for SuspensionFuture<W, A> {
    type Output = Result<A::Output, String>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.lock().unwrap();
        match state.check_expected_action_output::<A>(this.suspension) {
            Some(output) => Poll::Ready(Ok(output)),
            _ => Poll::Pending,
        }
    }
}

struct TestWaker {}

impl ArcWake for TestWaker {
    fn wake_by_ref(_: &Arc<Self>) {}
}

#[derive(Debug, PartialEq)]
pub enum Event<W: WorkflowTrait + ?Sized> {
    Started(W::Request),
    Suspended((u32, W::SuspensionEvent)),
    Awakened((u32, W::AwakenEvent)),
    Finished(Result<W::Response, String>),
}

type EventQueue<W> = VecDeque<(u32, time::OffsetDateTime, Event<W>)>;

pub struct WorkflowState<W: WorkflowTrait + ?Sized> {
    now: time::OffsetDateTime,
    next_event: u32,
    next_suspension: u32,
    replay: EventQueue<W>,
    awaken: HashMap<u32, W::AwakenEvent>,
    pending: EventQueue<W>,
}

impl<W: WorkflowTrait + ?Sized> WorkflowState<W> {
    pub fn new(now: time::OffsetDateTime, start: W::Request) -> Self {
        Self {
            now,
            next_event: 1,
            next_suspension: 0,
            replay: [(0, now, Event::Started(start))].into(),
            awaken: Default::default(),
            pending: Default::default(),
        }
    }

    pub fn ensure_expected_suspension(
        &mut self,
        suspended: W::SuspensionEvent,
    ) -> Result<u32, String> {
        let now = self.now;
        let next_event = self.next_event;
        let next_suspension = self.next_suspension;
        if let Some(next) = self.replay.pop_front() {
            let expected = (
                next_event,
                now,
                Event::Suspended((next_suspension, suspended)),
            );
            if next != expected {
                return Err(
                    format!("Expected event: {expected:?}\nActual event: {next:?}").to_owned(),
                );
            }
            self.next_event = next_event + 1;
        } else {
            let next_event = next_event + self.pending.len() as u32;
            self.pending.push_back((
                next_event,
                now,
                Event::Suspended((next_suspension, suspended)),
            ));
        }
        self.next_suspension = next_suspension + 1;
        Ok(next_suspension)
    }

    pub fn check_expected_action_output<A: WorkflowAction<W>>(
        &mut self,
        expected: u32,
    ) -> Option<A::Output> {
        while let Some((_, _, Event::Awakened((_, _)))) = self.replay.front() {
            if let (event, now, Event::Awakened((suspension, awakened))) =
                self.replay.pop_front()?
            {
                self.now = now;
                self.next_event = event + 1;
                match self.awaken.entry(suspension) {
                    Entry::Vacant(entry) => {
                        entry.insert(awakened);
                    }
                    Entry::Occupied(_) => {
                        // TODO: maybe handle duplicates?
                    }
                }
            } else {
                break;
            }
        }
        if A::matches_with(self.awaken.get(&expected)?) {
            let awakened = self.awaken.remove(&expected)?;
            return A::extract_output(awakened);
        }
        None
    }

    pub fn ensure_expected_result(
        &mut self,
        result: Result<W::Response, String>,
    ) -> Result<(), String> {
        let now = self.now;
        let next_event = self.next_event;
        if let Some(next) = self.replay.pop_front() {
            let expected = (next_event, now, Event::Finished(result));
            if next != expected {
                return Err(
                    format!("Expected event: {expected:?}\nActual event: {next:?}").to_owned(),
                );
            }
            self.next_event = next_event + 1;
        } else {
            let next_event = next_event + self.pending.len() as u32;
            self.pending
                .push_back((next_event, now, Event::Finished(result)));
        }
        Ok(())
    }

    pub fn pending(&mut self) -> &mut EventQueue<W> {
        &mut self.pending
    }
}

pub struct WorkflowExecution<W: WorkflowTrait> {
    state: Arc<Mutex<WorkflowState<W>>>,
    waker: Arc<TestWaker>,
    future: Mutex<BoxFuture<'static, Result<(), String>>>,
}

impl<W: WorkflowTrait> WorkflowExecution<W> {
    pub fn new(mut state: WorkflowState<W>) -> Result<Self, String> {
        match state.replay.pop_front() {
            Some((0, now, Event::Started(start))) => {
                state.now = now;
                let state = Arc::new(Mutex::new(state));
                let future = Mutex::new(W::start(state.clone(), start));
                let waker = Arc::new(TestWaker {});
                let mut execution = Self {
                    state,
                    future,
                    waker,
                };
                if execution.poll().is_pending() {
                    Ok(execution)
                } else {
                    Err("Not pending".to_owned())
                }
            }
            Some(next) => Err(format!("Unexpected event {next:?}")),
            None => Err("Not pending".to_owned()),
        }
    }

    pub fn awaken(
        &mut self,
        event: u32,
        now: time::OffsetDateTime,
        awakened: (u32, W::AwakenEvent),
    ) {
        let mut state = self.state.lock().unwrap();
        state.replay.push_back((event, now, Event::Awakened(awakened)));
    }

    pub fn pop_pending(&mut self) -> Option<(u32, time::OffsetDateTime, Event<W>)> {
        self.state.lock().unwrap().pending().pop_front()
    }

    pub fn poll(&mut self) -> Poll<Result<(), String>> {
        let mut future = self.future.lock().unwrap();
        let waker = waker_ref(&self.waker);
        let cx = &mut Context::from_waker(&*waker);
        Pin::new(future.deref_mut()).poll(cx)
    }
}
