use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::{WorkflowError, WorkflowResult};

#[derive(Default)]
pub struct Actions {
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
        action_type: &str,
        action_id: ActionRequestId,
    ) -> Result<(ActionType, ActionRequestId), WorkflowError> {
        match self.responses.entry(action_id) {
            Entry::Occupied(entry)
                if entry.get().action_type == action_type && entry.get().response.is_none() =>
            {
                Ok((entry.get().action_type, action_id))
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

struct ActionRequestEntry {
    action_type: ActionType,
    response: Option<String>,
}

pub type ActionType = &'static str;
pub type ActionRequestId = u32;
