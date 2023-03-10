use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::action::{ActionId, ActionResultHandler, ActionType};
use super::{WorkflowError, WorkflowResult};

#[derive(Default)]
pub struct Actions {
    next: ActionId,
    responses: HashMap<ActionId, ActionRequestEntry>,
}

impl Actions {
    pub fn add_action(&mut self, action_type: ActionType, handler: ActionResultHandler) -> u32 {
        let action_id = self.next;
        let entry = ActionRequestEntry {
            action_type,
            result_handler: Some(handler),
        };
        self.responses.insert(action_id, entry);
        self.next += 1;
        action_id
    }

    pub fn apply_response(
        &mut self,
        response: &(ActionType, ActionId, String),
    ) -> WorkflowResult<()> {
        let (action_type, action_id, response) = response;
        match self.responses.entry(*action_id) {
            Entry::Occupied(entry) if entry.get().action_type == *action_type => {
                if let Some(handler) = entry.get().result_handler.as_ref() {
                    handler.invoke(Ok(response))?;
                }
                Ok(())
            }
            _ => {
                // TODO: maybe return better error for logging?
                Err(WorkflowError::Panic(None))
            }
        }
    }

    pub fn remove_action(&mut self, expected_id: ActionId) -> WorkflowResult<ActionType> {
        match self.responses.get_mut(&expected_id) {
            Some(ActionRequestEntry {
                action_type,
                result_handler,
            }) => {
                *result_handler = None;
                Ok(action_type.clone())
            }
            // TODO: return a better error
            None => Err(WorkflowError::Panic(None)),
        }
    }
}

struct ActionRequestEntry {
    action_type: ActionType,
    result_handler: Option<ActionResultHandler>,
}
