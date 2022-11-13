use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::action::{ActionId, ActionType};
use super::WorkflowError;

pub type EventId = u32;
pub type Revision = u32;
pub type EventTime = time::OffsetDateTime;
pub type EvaluationId = u32;

// type SubscriptionType = &'static str;
// type SubscriptionId = u32;
// type ItemRequestId = u32;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Event {
    Started(String),
    Evaluated((EvaluationId, String)),
    ActionRequested((ActionType, ActionId, String)),
    ActionResponse((ActionType, ActionId, String)),
    ActionCanceled((ActionType, ActionId)),
    // StreamRequested((SubscriptionType, SubscriptionId, String)),
    // StreamOpened((SubscriptionType, SubscriptionId, String)),
    // ItemRequested((SubscriptionType, SubscriptionId, ItemRequestId, String)),
    // ItemResponse((SubscriptionType, SubscriptionId, ItemRequestId, String)),
    // StreamClosed((SubscriptionType, SubscriptionId)),
    Finished(Result<String, WorkflowError>),
}

pub type EventRecord = (EventId, Revision, EventTime, Event);

pub type EventQueue = VecDeque<EventRecord>;
