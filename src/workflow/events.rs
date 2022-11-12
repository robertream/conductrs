use std::collections::VecDeque;

use super::actions::{ActionRequestId, ActionType};
use super::WorkflowError;

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

pub type EventQueue = VecDeque<EventRecord>;
